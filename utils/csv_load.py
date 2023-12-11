import pandas as pd
import os
import re
from datetime import datetime
from utils.data_process import get_folders
import time
from io import StringIO
from zipfile import ZipFile

# Configure Tolveet Logger
from config import TolveetLogger, Config, MAIN_DIR
TL = TolveetLogger()
logger = TL.get_tolveet_logger()

def get_headers(headers_files):
    project_folder, columns_folder, dimensions_folder, currency_folder, trade_folder, temp_folder = get_folders(Config.TEMP_FOLDER)
    logger.info("Importing Headers...")
    for f in headers_files:
        if "din" in f:
            import_headers_file = f
        elif "dus" in f:
            export_headers_file = f

    # Column Names
    import_headers_raw = pd.read_excel(os.path.join(columns_folder, import_headers_file), sheet_name=None)
    export_headers_raw = pd.read_excel(os.path.join(columns_folder, export_headers_file), sheet_name=None)
    import_export_headers = []

    for df in [import_headers_raw, export_headers_raw]:
        xxx = df[list(df.keys())[1]].iloc[0].tolist()
        xxx = [x for x in xxx if str(x) != 'nan']
        df_headers = [s.strip() for s in xxx]
        length = len(df_headers)
        regex = re.compile("[^0-9a-zA-Z]+")
        for i in range(length):
            df_headers[i] = re.sub(regex, '_', df_headers[i])
        import_export_headers.append(df_headers)

    logger.info("Loading of headers complete")
    return import_export_headers[0], import_export_headers[1]


def get_import_export(import_files, export_files, is_init=False):

    project_folder, columns_folder, dimensions_folder, currency_folder, trade_folder, temp_folder = get_folders(Config.TEMP_FOLDER)
    # Check whether the specified path exists or not
    if not os.path.exists(temp_folder):
        # Create a new directory because it does not exist
        os.makedirs(temp_folder)

    ### ///// UNZIP IMPORT AND EXPORT FILES
    start = time.process_time()
    logger.info("Extracting files to temporary folder...")
    for z in [import_files, export_files]:
        for f in z:
            # Create a ZipFile Object and load sample.zip in it
            with ZipFile(os.path.join(trade_folder, f), 'r') as zipObj:
               # Extract all the contents of zip file in different directory
               zipObj.extractall(temp_folder)

    logger.info("Zip extraction Done!" + str((time.process_time() - start)))
    # 1.5 years take 9 min

    ### ///// READ IMPORTS CSVs (from Temp folder)
    start = time.process_time()
    all_extracted_files = []
    for filename in os.listdir(temp_folder):
        extension = os.path.splitext(filename)[1]
        if extension == ".txt":
           all_extracted_files.append(str(filename))
    logger.info(all_extracted_files)

    import_files_extracted = []
    export_files_extracted = []
    for f in all_extracted_files:
        name = os.path.splitext(f)[0]
        extension = os.path.splitext(f)[1]
        if extension == ".txt" and name.startswith('Import'):
            import_files_extracted.append(str(f))
        elif extension == ".txt" and name.startswith('Export'):
            export_files_extracted.append(str(f))

    logger.info("Reading files...")
    imports = pd.DataFrame()
    exports = pd.DataFrame()

    for f in [import_files_extracted, export_files_extracted]:
        for i in f:
            f_path = str(os.path.join(temp_folder, i))
            with open(f_path, 'rt', encoding='utf-8') as fileObject:
                temp_txt = StringIO(fileObject.read())
                df_temp = pd.read_csv(temp_txt, sep=";", header=None, low_memory=False)
            logger.info("Appending file: " + i)
            if i.startswith('Export'):
                exports = pd.concat([exports, df_temp], ignore_index=True, sort=False)
            else:
                imports = pd.concat([imports, df_temp], ignore_index=True, sort=False)
            del df_temp
            logger.info("Append complete.")
        logger.info("Appending Complete for files: " + ",".join(f))

    logger.info("Created Import file with " + str(len(imports.index)) + " rows")
    logger.info("Created Export file with " + str(len(exports.index)) + " rows")

    for f in import_files_extracted + export_files_extracted:
        try:
            logger.info("Removing file " + f)
            path_del = os.path.join(temp_folder, f)
            time.sleep(2)
            os.remove(path_del)
        except Exception as e:
            logger.warning("Cannot remove file " + f)
            logger.error(e)

    logger.info("Done! " + str((time.process_time() - start)))
    # 1.5 years take 9 min

    imports.name = "imports"
    exports.name = "exports"

    return imports, exports

def set_headers(imports, exports, import_headers, export_headers):
    """ SET HEADERS """
    logger.info("Setting headers...")
    try:
        imports.columns = import_headers
        # logger.info(imports.head())
    except:
        logger.info("skipping imports")
    try:
        exports.columns = export_headers
        # logger.info(exports.head())
    except:
        logger.info("skipping exports")
    logger.info("Headers Set...")
    return imports, exports

def get_years_month_to_load(trade_data):
    dfs = []

    logger.info("Getting Months to load...")

    for trade_table in trade_data:

        logger.info("Transforming columns to string and removing spaces...")
        name = trade_table.name
        temp = trade_table.columns.astype(str)
        trade_table.columns = temp.str.replace(' ', '')

        logger.info("Extracting subset of columns (Only extract what is needed: trade_type and period_id")
        if len(trade_table) > 0:
            df = trade_table[['period_id']].copy()
            df['trade_type'] = name
            df_g = df.groupby(['trade_type', 'period_id']).agg(
                num_records=pd.NamedAgg(column='period_id', aggfunc=pd.Series.count)
            ).reset_index()
            df_g["num_records"] = df_g["num_records"].astype("Int64")
            try:
                dfs.append(df_g)
            except Exception as e:
                logger.warning("Could not append " + name)
                logger.warning(str(e))


    if len(dfs) > 1:
        try:
            output = pd.concat(dfs)
            output = output[['trade_type', 'period_id', 'num_records']]
        except Exception as e:
            logger.warning("Could not concatenate ")
            logger.warning(str(e))
            output = pd.DataFrame(columns=['trade_type', 'period_id', 'num_records'])
    else:
        output = dfs[0]

    return output

def load_trade_files(files_to_load, is_init):
    """ Load export and exports into dataframes """
    # Get import and export headers
    import_headers, export_headers = get_headers(files_to_load['headers_files'])
    # Load import and export into a DF (no column names)
    imports, exports = get_import_export(files_to_load['import_files'], files_to_load['export_files'], is_init)
    # Set headers for all DFs
    imports, exports = set_headers(imports, exports, import_headers, export_headers)

    if len(imports) != 0:
        logger.info("Imports: Adding new columns fecha, period_id and reference_id... ")
        imports = imports.dropna(subset=['FECTRA'])
        imports['fecha'] = imports.apply(lambda row: datetime.strptime(str(int(row['FECTRA'])).zfill(8), '%d%m%Y'), axis=1)
        imports['period_id'] = imports.apply(lambda row: str(row['fecha'].year) + "-" + str(row['fecha'].month), axis=1)
        imports['reference_id'] = imports.apply(
            lambda row: str(row['NUMENCRIPTADO'] or 'Unknown') + "-" + str(row['NUMITEM' or 'Unknown']), axis=1)
        imports[['MEDIDA']] = imports[['MEDIDA']].fillna(value=999)
        imports[['MEDIDA']] = imports[['MEDIDA']].round().astype(int)

    if len(exports) != 0:
        logger.info("Exports: Adding new columns fecha, period_id and reference_id... ")
        exports = exports.dropna(subset=['FECHAACEPT'])
        # Remove .00000 from:
        exports[['PAISCIATRANSP']] = exports[['PAISCIATRANSP']].fillna(value=999)
        exports[['MONEDA']] = exports[['MONEDA']].fillna(value=900)
        exports[['ADUANA','VIATRANSPORTE','UNIDADMEDIDA']] = exports[['ADUANA','VIATRANSPORTE','UNIDADMEDIDA']].fillna(value=-1)
        exports[['PAISCIATRANSP','UNIDADMEDIDA','MONEDA','ADUANA','VIATRANSPORTE']] = exports[['PAISCIATRANSP','UNIDADMEDIDA','MONEDA','ADUANA','VIATRANSPORTE']].round().astype(int)

        exports['FECHAACEPT'] = exports['FECHAACEPT'].round().astype(int)
        # pd.options.mode.chained_assignment = None.
        exports['fecha'] = exports.apply(
            lambda row: datetime.strptime(str(row['FECHAACEPT'])[:8].zfill(8), '%d%m%Y'), axis=1)
        exports['period_id'] = exports.apply(lambda row: str(row['fecha'].year) + "-" + str(row['fecha'].month), axis=1)
        exports['reference_id'] = exports.apply(
            lambda row: str(row['NUMEROIDENT'] or 'Unknown') + "-" + str(row['NUMEROITEM' or 'Unknown']), axis=1)
        # Remove from ADUANA all non integer
        exports = exports[exports.ADUANA.astype(str).str.isnumeric()]

    logger.info("New Columns added ")

    imports.name = "imports"
    exports.name = "exports"

    years_month_to_load = get_years_month_to_load([imports, exports])

    return years_month_to_load, imports, exports