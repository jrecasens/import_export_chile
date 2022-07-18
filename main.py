import os.path
import time
from utils.data_process import get_years_month_loaded, get_files_to_load, get_dimensions, col_to_str, \
    recreate_db, copy_csv_into_db, generate_temp_csv, drop_db_objects
from utils.csv_load import load_trade_files
from utils.sql_server_connector import SqlServerConnector
from utils.azure_blob_storage import AzureBlogStorage
# Configure Tolveet Logger
from config import TolveetLogger, Config, MAIN_DIR
TL = TolveetLogger()
logger = TL.get_tolveet_logger()

### ///// PARAMETERS

schema_name = "canola"
trade_type = ['imports', 'exports']
is_init = False
is_sample = False
is_execute_queries = True
# https://datos.gob.cl/dataset/registro-de-importacion-2021
# https://datos.gob.cl/dataset/registro-de-exportacion-2021
start_total = time.time()

### ///// DB CONNECTORS
logger.info("///////////////////////////////////////////////////////")
logger.info("////// FILE EXTRACTION AND DATAFRAME APPENDING ///////")
logger.info("//////////////////////////////////////////////////////")

start_db_connectors = time.time()
storage = AzureBlogStorage(conn_str=Config.AZURE_STORAGE_CONNECT_STR)
conn = SqlServerConnector(conn_str=Config.SQLALCHEMY_DATABASE_URI, storage=storage)
raw_connection = conn.engine.raw_connection()
# Get year and months that have been loaded
years_month_loaded = get_years_month_loaded(conn, schema_name)
# Get all file names that will be used
files_to_load = get_files_to_load(is_sample)
# Read SQL queries
utils_folder = os.path.join(MAIN_DIR, "utils")
sql_init_file = "create_db_objects.sql"
sql_report_files = "report_queries.sql"
sql_init_commands = conn.parse_sql(os.path.join(utils_folder, sql_init_file))
sql_report_commands = conn.parse_sql(os.path.join(utils_folder, sql_report_files), is_debug=False)
# Get only section that drop stuff (to run first and facilitate debugging).
sql_drop_commands = [x for x in sql_init_commands if x.lower().startswith('drop')]
end_db_connectors = time.time() - start_db_connectors

### FILES TO LOAD

start_csv_load = time.time()
# Load trade data from .txt
years_month_to_load, imports, exports = load_trade_files(files_to_load, is_init)
# Load dimensions into a DF
dimensions_dict = get_dimensions(files_to_load['dimension_files'])
# Change all columns to string (to prevent data type issues).
col_to_str(dimensions_dict)
end_csv_load = time.time() - start_csv_load

if is_init:
    logger.info("///////////////////////////////////")
    logger.info("/// RECREATE EMPTY DATABASE ///////")
    logger.info("///////////////////////////////////")

    start_init_db = time.time()

    tables_to_drop = [imports.name, exports.name] + list(dimensions_dict.keys())
    logger.info("Dropping DB objects...")
    drop_db_objects(conn, raw_connection, tables_to_drop, schema_name, sql_drop_commands)
    logger.info("Recreating DB...")
    recreate_db(conn,
                raw_connection,
                imports,
                exports,
                dimensions_dict,
                schema_name,
                sql_init_commands
                )
    logger.info("/////////// DB CREATION WITH EMPTY SCHEMA IS COMPLETE.")
    end_init_db = time.time() - start_init_db

else:
    logger.info("///////////////////////////////////")
    logger.info("//// INCREMENTAL DATA LOADS ///////")
    logger.info("///////////////////////////////////")

    start_data_process = time.time()

    incremental_loads = years_month_loaded.merge(years_month_to_load, how='outer', on=["trade_type", "period_id"])

    periods_to_load = incremental_loads[(incremental_loads['num_records_x'].isnull())]
    periods_to_delete = incremental_loads[(~incremental_loads['num_records_x'].isnull()) &
                                          (~incremental_loads['num_records_y'].isnull()) &
                                          (incremental_loads['num_records_x'] != incremental_loads['num_records_y'])]

    periods_to_load_dict = {}
    for t in trade_type:
        df = periods_to_load[periods_to_load["trade_type"] == t]
        periods = []
        for index, row in df.iterrows():
            periods.append(row['period_id'])
        periods_to_load_dict[t] = periods

    periods_to_delete_dict = {}
    for t in trade_type:
        df = periods_to_delete[periods_to_delete["trade_type"] == t]
        periods = []
        for index, row in df.iterrows():
            periods.append(row['period_id'])
        periods_to_delete_dict[t] = periods

    logger.info("periods_to_load_dict:")
    logger.info(periods_to_load_dict)

    logger.info("periods_to_delete_dict:")
    logger.info(periods_to_delete_dict)

    # DELETE: Remove records from DB
    for t in trade_type:
        if len(periods_to_delete_dict[t]) != 0:
            lst = periods_to_delete_dict[t]
            lst_str = "('{0}".format("', '".join(lst))+"')"
            query = "DELETE FROM {0}.{1} " \
                    "WHERE period_id IN {2};".format(schema_name, t, lst_str)
            conn.execute_sql_batch(raw_connection=raw_connection, queryParsed=[query], debug=True)

    # LOAD: Filter imports and exports
    # Add deleted periods
    for t in trade_type:
        periods_to_load_dict[t] = periods_to_load_dict[t] + periods_to_delete_dict[t]

    # Filter export and import df (select only what needs to be loaded
    imports = imports[imports.period_id.isin(periods_to_load_dict['imports'])]
    exports = exports[exports.period_id.isin(periods_to_load_dict['exports'])]

    imports.name = "imports"
    exports.name = "exports"
    end_data_process = time.time() - start_data_process

    start_data_load = time.time()
    logger.info("Loading DF into DB....")
    generate_temp_csv(imports, exports)
    copy_csv_into_db(conn, raw_connection, imports, exports, schema_name, dimensions_dict)
    end_data_load = time.time() - start_data_load

if not is_init and is_execute_queries:
    logger.info("///////////////////////////////////")
    logger.info("//// SQL REPORTING LAYER /////////")
    logger.info("///////////////////////////////////")

    start_sql_report_queries = time.time()
    logger.info("Running SQL Reporting queries...")
    conn.execute_sql_batch(raw_connection=raw_connection, queryParsed=sql_report_commands, debug=True)
    end_sql_report_queries = time.time() - start_sql_report_queries

end_total = time.time() - start_total

try:
    end_db_connectors = str(round((end_db_connectors % 3600) / 60, 2))
    print("DB CONNECTORS: {}".format(end_db_connectors))
except NameError:
    pass

try:
    end_csv_load = str(round((end_csv_load % 3600) / 60, 2))
    print("CSV LOAD: {}".format(end_csv_load))
except NameError:
    pass

try:
    end_init_db = str(round((end_init_db % 3600) / 60, 2))
    print("INIT DB: {}".format(end_init_db))
except NameError:
    pass

try:
    end_data_process = str(round((end_data_process % 3600) / 60, 2))
    print("DATA PROCESS {}".format(end_data_process))
except NameError:
    pass

try:
    end_data_load = str(round((end_data_load % 3600) / 60, 2))
    print("DATA LOAD TO DB {}".format(end_data_load))
except NameError:
    pass

try:
    end_sql_report_queries = str(round((end_sql_report_queries % 3600) / 60, 2))
    print("SQL REPORTING QUERIES {}".format(end_sql_report_queries))
except NameError:
    pass

try:
    end_total = str(round((end_total % 3600) / 60,2))
    print("TOTAL TIME:  {}".format(end_total))
except NameError:
    pass