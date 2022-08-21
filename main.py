import os.path
import time
from utils.data_process import get_years_month_loaded, get_files_to_load, get_dimensions, col_to_str, \
    recreate_db, copy_csv_into_db, generate_temp_csv, drop_db_objects, get_currency
from utils.csv_load import load_trade_files
from utils.sql_server_connector import SqlServerConnector
from utils.azure_blob_storage import AzureBlogStorage
from utils.plot_functions import aggregate_canola_imports, generate_missing_dates, create_imports_canola_plot

# Configure Tolveet Logger
from config import TolveetLogger, Config, MAIN_DIR
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import subprocess

import plotly.io as pio
pio.renderers.default = "browser"

TL = TolveetLogger()
logger = TL.get_tolveet_logger()

### ///// URLs

# Import and Export Data
# https://datos.gob.cl/dataset/registro-de-importacion-2021
# https://datos.gob.cl/dataset/registro-de-exportacion-2021

# Codigos Monedas, paises, puertos, etc.
# http://comext.aduana.cl:7001/codigos/

# Currency Exchange rates
# https://si3.bcentral.cl/Indicadoressiete/secure/IndicadoresDiarios.aspx

### ///// PARAMETERS

schema_name = "canola"
trade_type = ['imports', 'exports']
excel_password = 'canolachile.com'
azure_storage_container = "other"
azure_storage_folder = "canolachile"
is_init = False
is_sample = False
is_execute_queries = True
is_create_plots = True
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

# Load dimensions into a DF
dimensions_dict = get_dimensions(files_to_load['dimension_files'])
# Change all columns to string (to prevent data type issues).
col_to_str(dimensions_dict)

# Get currency exchange rates
currency_converter = get_currency(currency_files=files_to_load['currency_files'],
                                  currencies_of_interest=Config.CURRENCIES)

# Load trade data from .txt
years_month_to_load, imports, exports = load_trade_files(files_to_load, is_init)
end_csv_load = time.time() - start_csv_load

if is_init:
    logger.info("///////////////////////////////////")
    logger.info("/// RECREATE EMPTY DATABASE ///////")
    logger.info("///////////////////////////////////")

    start_init_db = time.time()

    tables_to_drop = [imports.name, exports.name, currency_converter.name] + list(dimensions_dict.keys())
    logger.info("Dropping DB objects...")
    drop_db_objects(conn, raw_connection, tables_to_drop, schema_name, sql_drop_commands)
    logger.info("Recreating DB...")
    recreate_db(conn,
                raw_connection,
                imports,
                exports,
                dimensions_dict,
                currency_converter,
                schema_name,
                sql_init_commands
                )
    logger.info("/////////// DB CREATION WITH EMPTY SCHEMA IS COMPLETE.")
    end_init_db = time.time() - start_init_db

else:
    logger.info("///////////////////////////////////////////////")
    logger.info("//// INCREMENTAL DATA LOADS (per month) ///////")
    logger.info("//////////////////////////////////////////////")

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
    copy_csv_into_db(conn, raw_connection, imports, exports, schema_name, currency_converter, dimensions_dict, remove_tmp=True)
    end_data_load = time.time() - start_data_load


if not is_init and is_execute_queries:
    logger.info("///////////////////////////////////")
    logger.info("//// SQL REPORTING LAYER /////////")
    logger.info("///////////////////////////////////")

    start_sql_report_queries = time.time()
    logger.info("Running SQL Reporting queries...")
    conn.execute_sql_batch(raw_connection=raw_connection, queryParsed=sql_report_commands, debug=True)

    blob_upload_lst = []
    excel_file_lst = []
    # Export views as .xlsx
    views_to_zip = [
                    'vw_imports_canola_trigo',
                    'vw_imports_canola_report',
                    'vw_exports_canola_trigo'
    ]
    for t in views_to_zip:
        logger.info("Reading SQL view "+t+" for extraction...")
        vw_df = conn.read_table(table_name=t, schema=schema_name)
        logger.info("Saving "+t+" to Excel...")
        excel_file = t + ".xlsx"
        zip_file = t + ".zip"
        excel_path = str(os.path.join(MAIN_DIR, excel_file))
        zip_path = str(os.path.join(MAIN_DIR, zip_file))
        vw_df.to_excel(excel_path)
        logger.info("Compressing to ZIP...")
        rc = subprocess.call([r'C:\Program Files\7-Zip\7z.exe',
                              'a',
                              '-p' + excel_password,
                              '-y',
                              zip_path,
                              excel_path
                              ]
                             , shell=True)
        blob_upload_lst.append(zip_file)
        excel_file_lst.append(excel_file)
        if t == 'vw_imports_canola_report':
            vw_imports_canola_report = vw_df

    logger.info("Further post-processing of vw_imports_canola_report...")
    # Get non seed records
    vw_imports_canola_report = vw_imports_canola_report[vw_imports_canola_report['is_siembra'] == 0]

    # remove \n and \r from country
    vw_imports_canola_report = vw_imports_canola_report.assign(
        pais_nombre_origen=vw_imports_canola_report.pais_nombre_origen.str.rstrip())

    # Currency of Interest for Price (and transform to Quintal)
    vw_imports_canola_report.loc[:, 'precio_fob_usd'] = vw_imports_canola_report['PRE_UNIT_MOD'] * \
                                                        vw_imports_canola_report['to_usd'] * 100
    vw_imports_canola_report.loc[:, 'precio_fob_cad'] = vw_imports_canola_report['PRE_UNIT_MOD'] * \
                                                        vw_imports_canola_report['to_cad'] * 100
    vw_imports_canola_report.loc[:, 'precio_cif_usd'] = vw_imports_canola_report['PRE_UNIT_CIF'] * \
                                                        vw_imports_canola_report['to_usd'] * 100
    vw_imports_canola_report.loc[:, 'precio_cif_cad'] = vw_imports_canola_report['PRE_UNIT_CIF'] * \
                                                        vw_imports_canola_report['to_usd'] * 100

    vw_imports_canola_report.loc[:, 'CANT_MERC_MOD'] = (vw_imports_canola_report['CANT_MERC_MOD'] / 100).round(
        0)

    imports_canola_agg = vw_imports_canola_report.groupby(by=['fecha_month', 'pais_nombre_origen'], axis=0,
                                                          as_index=False).agg(
        cantidad_quintal=pd.NamedAgg(column='CANT_MERC_MOD', aggfunc=sum),
        precio_fob_usd_quintal=pd.NamedAgg(column='precio_fob_usd', aggfunc=np.mean),
        precio_fob_cad_quintal=pd.NamedAgg(column='precio_fob_cad', aggfunc=np.mean),
        precio_cif_usd_quintal=pd.NamedAgg(column='precio_cif_usd', aggfunc=np.mean),
        precio_cif_cad_quintal=pd.NamedAgg(column='precio_cif_cad', aggfunc=np.mean)
    )

    # Aggregate all countries and compute weighted average
    imports_canola_agg_all = aggregate_canola_imports(input_df=imports_canola_agg, country_name='TODOS')
    imports_canola_agg_canada = imports_canola_agg[imports_canola_agg['pais_nombre_origen'] == 'CANADA']
    imports_canola_agg_argentina = imports_canola_agg[imports_canola_agg['pais_nombre_origen'] == 'ARGENTINA']
    imports_canola_agg_otros = imports_canola_agg[
        ~imports_canola_agg['pais_nombre_origen'].isin(['ARGENTINA', 'CANADA'])]
    if len(imports_canola_agg_otros) > 0:
        imports_canola_agg_otros = aggregate_canola_imports(input_df=imports_canola_agg_otros,
                                                            country_name='OTROS')

    # Generate missing timestamps
    # generate daily forecast (naive method).
    date_from = imports_canola_agg_all["fecha_month"].min()
    date_to = imports_canola_agg_all["fecha_month"].max()

    csv_dict = {'imports_canola_agg_all': imports_canola_agg_all,
                'imports_canola_agg_canada': imports_canola_agg_canada,
                'imports_canola_agg_argentina': imports_canola_agg_argentina,
                'imports_canola_agg_otros': imports_canola_agg_otros}
    # Add missing dates (for reporting)
    for k, v in csv_dict.items():
        logger.info("Working on " + k +" ...")
        if len(v) > 0:
            v = generate_missing_dates(df=v, date_from=date_from, date_to=date_to)
            csv_dict[k] = v
        # Export report data as .csv
        csv_file = k + ".csv"
        csv_path = str(os.path.join(MAIN_DIR, csv_file))
        v.to_csv(csv_path, index=False)
        blob_upload_lst.append(csv_file)

    logger.info("Uploading to Azure Blob Storage...")
    for f in set(blob_upload_lst):
        storage.file_upload(container=azure_storage_container,
                            file_name=f,
                            file_path=MAIN_DIR,
                            des_folder=azure_storage_folder)

    for f in blob_upload_lst + excel_file_lst:
        os.remove(f)

    end_sql_report_queries = time.time() - start_sql_report_queries

if is_create_plots:

    csv_lst = ['imports_canola_agg_all', 'imports_canola_agg_canada',
                'imports_canola_agg_argentina', 'imports_canola_agg_otros']
    for f in csv_lst:
        title = "Pais: " + f.split('_')[-1].capitalize()
        df = pd.read_csv(f + '.csv')
        if len(df) > 0:
            fig = create_imports_canola_plot(df=df, title=title)
            fig.show()


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