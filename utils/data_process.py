import os
from io import StringIO
import pandas as pd
import re
import time



# Configure Tolveet Logger
from config import TolveetLogger, Config, MAIN_DIR
TL = TolveetLogger()
logger = TL.get_tolveet_logger()


def get_folders():
    project_folder = os.path.join(MAIN_DIR, Config.FOLDER_DATA, Config.FOLDER_TRADE)
    columns_folder = os.path.join(project_folder, Config.FOLDER_COLUMNS)
    dimensions_folder = os.path.join(project_folder, Config.FOLDER_DIMENSIONS)
    trade_folder = os.path.join(project_folder, Config.FOLDER_IMPORTS_EXPORTS)
    temp_folder = os.path.join(trade_folder, "temp")
    return project_folder, columns_folder, dimensions_folder, trade_folder, temp_folder

def get_years_month_loaded(conn, schema_name):
    dfs = []
    for table_name in ['imports', 'exports']:
        query = "WITH CTE AS (SELECT '{1}' AS trade_type, CAST(period_id AS varchar(10)) as period_id, reference_id " \
                "FROM {0}.{1}) " \
                "SELECT trade_type, period_id, COUNT(reference_id) as num_records " \
                "FROM CTE " \
                "GROUP BY trade_type, period_id " \
                "ORDER BY trade_type, period_id;".format(schema_name, table_name)
        try:
            df = conn.read_query(query)
            df['period_id'] = df.apply(lambda row: row['period_id'].rstrip(), axis=1)
            df["num_records"] = df["num_records"].astype("Int64")
            dfs.append(df)
        except Exception as e:
            logger.warning("Could not get loaded dates from table: "+table_name)
                           # +" . Exception: " + str(e))
        try:
            output = pd.concat(dfs)
        except Exception as e:
            logger.warning(str(e))
            output = pd.DataFrame(columns=['trade_type', 'period_id', 'num_records'])

    return output

def get_files_to_load(is_sample=False):
    """ Read project files to load """
    project_folder, columns_folder, dimensions_folder, trade_folder, temp_folder = get_folders()

    output = {}
    import_files = []
    export_files = []
    headers_files = []

    for folders in [columns_folder, trade_folder]:
        logger.info("Reading content from folder: " + folders)
        for filename in os.listdir(folders):
            name = os.path.splitext(filename)[0]
            extension = os.path.splitext(filename)[1]
            if extension == ".zip" and name.startswith('import'):
                import_files.append(str(filename))
            elif extension == ".zip" and name.startswith('export'):
                export_files.append(str(filename))
            elif extension == ".xlsx" and name.startswith('descripcion-y-estructura-de-datos'):
                headers_files.append(str(filename))

    output['import_files'] = import_files
    output['export_files'] = export_files
    output['headers_files'] = headers_files

    dimension_files = []

    for folders in [dimensions_folder]:
        logger.info("Reading content from folder: " + folders)
        for filename in os.listdir(folders):
            name = os.path.splitext(filename)[0]
            extension = os.path.splitext(filename)[1]
            if extension == ".csv" and name.startswith('aduana_codigos'):
                dimension_files.append(str(filename))

    output['dimension_files'] = dimension_files
    logger.info(dimension_files)


    # Remove Samples?
    for item in import_files[:]:
        if is_sample:
            if "sample" not in item:
                import_files.remove(item)
        else:
            if "sample" in item:
                import_files.remove(item)

    for item in export_files[:]:
        if is_sample:
            if "sample" not in item:
                export_files.remove(item)
        else:
            if "sample" in item:
                export_files.remove(item)

    logger.info("Files to use:")
    logger.info(import_files)
    logger.info(export_files)
    logger.info(headers_files)

    return output

def get_dimensions(dimension_files):
    """ Load dimensions """
    project_folder, columns_folder, dimensions_folder, trade_folder, temp_folder = get_folders()
    logger.info("Reading dimensions...")
    dimension_list_name = []
    dimension_list_data = []
    for f in dimension_files:
        name = os.path.splitext(f)[0]
        dimension_list_name.append(name)
        logger.info(f)
        with open(os.path.join(dimensions_folder, f), 'rt', encoding='utf-8') as fileObject:
            temp_txt = StringIO(fileObject.read())
            df_temp = pd.read_csv(temp_txt, sep=";", low_memory=False)
            df_temp.name = name
        logger.info("Appending files...")
        dimension_list_data.append(df_temp)
    dimensions_dict = dict(zip(dimension_list_name, dimension_list_data))
    logger.info("Appending Complete.")

    return dimensions_dict

def generate_temp_csv(imports, exports):
    """ Save dataframe to .csv"""
    project_folder, columns_folder, dimensions_folder, trade_folder, temp_folder = get_folders()

    # csv GENERATION
    logger.info("Removing Imports and Exports if they exist....")
    for f in ["imports.csv", "exports.csv"]:
        if os.path.exists(os.path.join(temp_folder, f)):
            logger.info("File " + f + " exist. Removing...")
            os.remove(os.path.join(temp_folder, f))

    logger.info("Saving Imports and Exports as .csv....")
    time.sleep(1)
    imports.to_csv(os.path.join(temp_folder, "imports.csv"), index=False, sep=";", header=False)
    exports.to_csv(os.path.join(temp_folder, "exports.csv"), index=False, sep=";", header=False)
    logger.info(".csv saved.")



def col_to_str(df_dict):
    logger.info("Columns to String...")
    for dim in df_dict.values():
        all_columns = list(dim)  # Creates list of all column headers
        dim[all_columns] = dim[all_columns].astype(str)

def drop_db_objects(conn, raw_connection, tables_to_drop, schema_name, sql_drop_commands):
    logger.info("Dropping objects created in query...")
    conn.execute_sql_batch(raw_connection=raw_connection, queryParsed=sql_drop_commands, debug=False)

    try:
        logger.info("Dropping tables...")
        for t in tables_to_drop:
            conn.execute_sql_batch(raw_connection=raw_connection,
                                   queryParsed=["DROP TABLE IF EXISTS "+schema_name+"." + t], debug=False)
    except Exception as e:
        logger.error("Error: %s" % e)
    for t in ['DROP SCHEMA  IF EXISTS '+schema_name, 'CREATE SCHEMA '+schema_name]:
        conn.execute_sql_batch(raw_connection=raw_connection, queryParsed=[t], debug=False)

def recreate_db(conn, raw_connection, imports,exports,dimensions_dict, schema_name, sql_init_commands):

    try:
        for df in [imports, exports]:
            logger.info("Creating empty DB schemas for " + df.name)
            df_head = df.head(10).copy()
            all_columns = list(df_head)  # Creates list of all column headers
            df_head[all_columns] = df_head[all_columns].astype(str)
            # logger.info(df_head.head(0))
            conn.write_table_from_dataframe(df=df_head.head(0), table_name=df.name, schema=schema_name, if_exists='truncate')
        logger.info("DB schema created for imports and exports.")

        for df in list(dimensions_dict.values()):
            logger.info("Creating empty DB schemas for " + df.name)
            df_head = df.head(10).copy()
            df_head.iloc[:, 0] = df_head.iloc[:, 0].astype(int)
            # logger.info(df_head.head(0))
            conn.write_table_from_dataframe(df=df_head.head(0), table_name=df.name, schema=schema_name, if_exists='truncate')
        logger.info("DB schema created for dimensions.")
    except Exception as e:
        logger.error("Error: %s" % e)

    logger.info("Running initialization SQL commands...")
    conn.execute_sql_batch(raw_connection=raw_connection, queryParsed=sql_init_commands, debug=False)

def copy_csv_into_db(conn, raw_connection,imports,exports,schema_name,dimensions_dict=None):

    project_folder, columns_folder, dimensions_folder, trade_folder, temp_folder = get_folders()

    ## Copy dimensions
    logger.info("Copying dimensions....")
    for k, v in dimensions_dict.items():
        # Truncate dimensions first (to prevent duplication).
        conn._truncate_table(table=k, schema=schema_name)
        conn.copy_from_file(raw_connection=raw_connection, schema=schema_name, df=v, table_name=k, path_csv=temp_folder)
    ## Copy imports and exports
    if len(imports) != 0:
        logger.info("Copying imports to DB....")
        conn.copy_from_file(raw_connection=raw_connection, schema=schema_name, df=imports, table_name=imports.name, path_csv=temp_folder)
        logger.info("Copy imports to DB complete. ")
    else:
        logger.info("No imports to load to DB. ")
    if len(exports) != 0:
        logger.info("Copying exports to DB....")
        conn.copy_from_file(raw_connection=raw_connection, schema=schema_name, df=exports, table_name=exports.name, path_csv=temp_folder)
        logger.info("Copy exports to DB complete. ")
    else:
        logger.info("No exports to load to DB. ")