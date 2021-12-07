import sys
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO
import pandas as pd
import os
from pandasql import sqldf
import re
import os.path
from os import path
import time
import psycopg2
from collections import defaultdict
import sqlalchemy
from zipfile import ZipFile
import db_utils

### ///// PARAMETERS

is_sample = False
is_excel = False
is_csv_ready = False
is_remove_stuff = True
execute_queries = True

folder_path = "C:/Github/import_export_chile/"
raw_folder = folder_path + "raw/"

### ///// READ ALL EXISTING FILES
import_files = []
export_files = []
dimension_files = []
import_headers_file = ["descripcion-y-estructura-de-datos-din.xlsx"]
export_headers_file = ["descripcion-y-estructura-de-datos-dus.xlsx"]

for filename in os.listdir(raw_folder):
    name = os.path.splitext(filename)[0]
    extension = os.path.splitext(filename)[1]
    if extension == ".zip" and name.startswith('import'):
        import_files.append(str(filename))
    elif extension == ".zip" and name.startswith('export'):
        export_files.append(str(filename))
    elif extension == ".csv" and name.startswith('aduana_codigos'):
        dimension_files.append(str(filename))

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

print(import_files)
print(export_files)
print(dimension_files)
print(import_headers_file)
print(export_headers_file)

### ///// LOAD DIMENSIONS

print("Reading dimensions...")
dimension_list_name = []
dimension_list_data = []
for f in dimension_files:
    name = os.path.splitext(f)[0]
    dimension_list_name.append(name)
    print(f)
    with open(str(raw_folder + f), 'rt', encoding='utf-8') as fileObject:
        temp_txt = StringIO(fileObject.read())
        df_temp = pd.read_csv(temp_txt, sep=";", low_memory=False)
        df_temp.name = name
    print("Appending files...")
    dimension_list_data.append(df_temp)

dimensions_dict = dict(zip(dimension_list_name, dimension_list_data))

print(dimensions_dict)
print("Appending Complete.")

### ///// GET HEADERS

# Column Names
import_headers_raw = pd.read_excel(raw_folder + import_headers_file[0], sheet_name=None)
export_headers_raw = pd.read_excel(raw_folder + export_headers_file[0], sheet_name=None)
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

print("Got headers:")
print(import_export_headers)

### ///// UNZIP IMPORT AND EXPORT FILES
start = time.process_time()
if not is_csv_ready:
    print("Extracting files...")
    for z in [import_files, export_files]:
        for f in z:
            # Create a ZipFile Object and load sample.zip in it
            with ZipFile(raw_folder + f, 'r') as zipObj:
               # Extract all the contents of zip file in different directory
               zipObj.extractall(raw_folder + 'temp')

print("Zip extraction Done!" + str((time.process_time() - start)))
# 1.5 years take 9 min

### ///// READ IMPORTS CSVs
start = time.process_time()
if not is_csv_ready:

    all_extracted_files = []
    for filename in os.listdir(raw_folder + 'temp'):
        extension = os.path.splitext(filename)[1]
        if extension == ".txt":
           all_extracted_files.append(str(filename))
    print(all_extracted_files)

    import_files_extracted = []
    export_files_extracted = []
    for f in all_extracted_files:
        name = os.path.splitext(f)[0]
        extension = os.path.splitext(f)[1]
        if extension == ".txt" and name.startswith('Import'):
            import_files_extracted.append(str(f))
        elif extension == ".txt" and name.startswith('Export'):
            export_files_extracted.append(str(f))

    print("Reading files...")
    imports = pd.DataFrame()
    exports = pd.DataFrame()

    for f in [import_files_extracted, export_files_extracted]:
        for i in f:
            f_path = str(raw_folder + 'temp/' + i)
            with open(f_path, 'rt', encoding='utf-8') as fileObject:
                temp_txt = StringIO(fileObject.read())
                df_temp = pd.read_csv(temp_txt, sep=";", header=None, low_memory=False)
            print("Appending files..." + i)
            if i.startswith('Export'):
                exports = exports.append(df_temp)
            else:
                imports = imports.append(df_temp)
            del df_temp
        print("Appending Complete for " + ",".join(f))

    print("Created Import file with " + str(len(imports.index)) + " rows")
    print("Created Export file with " + str(len(exports.index)) + " rows")

    # csv GENERATION
    print("Saving as .csv....")
    time.sleep(1)
    imports.to_csv(folder_path + "imports.csv", index=False, sep=";", header=False)
    exports.to_csv(folder_path + "exports.csv", index=False, sep=";", header=False)

else:
    try:
        print("Reading .csv....")
        imports = pd.read_csv(folder_path + " imports.csv", sep=";", header=None)
        exports = pd.read_csv(folder_path + " exports.csv", sep=";", header=None)

    except (Exception, psycopg2.DatabaseError) as error:
        print("There is no CSV available. Error: %s" % error)

print("csv generation Done!" + str((time.process_time() - start)))
# 1.5 years take 9 min

### ///// SET HEADERS
print("Setting headers...")
imports.columns = import_export_headers[0]
exports.columns = import_export_headers[1]
print(imports.head())
print(exports.head())

### ///// COLUMNS TO STRING for dimensions

print("Dimension Columns to String...")
for dim in dimensions_dict.values():
    all_columns = list(dim)  # Creates list of all column headers
    dim[all_columns] = dim[all_columns].astype(str)

# 1.5 years take 9 min

### ///// DATABASE

# Database Credentials by machine
db_cred = defaultdict(dict)
db_cred['local'] = {"drivername": "postgresql",
                         "dbserverName": "localhost",
                         "port": "5432",
                         "dbusername": "postgres",
                         "dbpassword": "abc123",
                         "dbname": "agricola"}

username = db_cred['local']['dbusername']
password = db_cred['local']['dbpassword']
ipaddress = db_cred['local']['dbserverName']
port = db_cred['local']['port']
dbname = db_cred['local']['dbname']

# A long string that contains the necessary Postgres login information
postgres_str = f'postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'

# Create the connection
cnx = sqlalchemy.create_engine(postgres_str)
raw_con = cnx.raw_connection()
raw_con.set_isolation_level(0)

# Create schema
print("Creating table in DB....")
imports.name = "imports"
exports.name = "exports"

try:
    cur = raw_con.cursor()
    for t in [imports.name, exports.name]:
        cur.execute("drop table if exists " + t + " cascade")
    for dim in dimension_list_name:
        cur.execute("drop table if exists " + dim + " cascade")
    cur.close()
except (Exception, psycopg2.DatabaseError) as error:
    print("Error: %s" % error)


try:
    for df in [imports, exports] + dimension_list_data:
        df_head = df.head(10).copy()
        all_columns = list(df_head)  # Creates list of all column headers
        df_head[all_columns] = df_head[all_columns].astype(str)
        df_head.head(0).to_sql(df.name, con=cnx, index=False)

except (Exception, psycopg2.DatabaseError) as error:
    print("Error: %s" % error)

# cur = raw_con.cursor()
# out = StringIO()

# con = psycopg2.connect(dbname=db_cred['local']['dbname'],
#                                    user=db_cred['local']['dbusername'],
#                                    host=db_cred['local']['dbserverName'],
#                                    password=db_cred['local']['dbpassword'])


def copy_from_file(conn, df, table_name, folder_path):
    """
    Here we are going save the dataframe on disk as
    a csv file, load the csv file
    and use copy_from() to copy it to the table
    """
    # Save the dataframe to disk
    tmp_df = folder_path + df.name + ".csv"

    if not path.exists(df.name + ".csv"):
        print("Saving to csv...")
        df.to_csv(tmp_df, index=False, sep=";", header=False)
    f = open(tmp_df, 'r')
    cursor = conn.cursor()
    try:
        cursor.copy_from(f, table_name, sep=";", null='')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_file() done")
    cursor.close()

start = time.process_time()

## Copy dimensions
print("Copying dimensions....")
for k, v in dimensions_dict.items():
    copy_from_file(raw_con, v, k, folder_path)

## Copy imports and exports

print("Copying imports to DB....")
copy_from_file(raw_con, imports, imports.name, folder_path)
print("Copying exports to DB....")
copy_from_file(raw_con, exports, exports.name, folder_path)
print("Copy imports and exports to DB complete: " + str((time.process_time() - start)))


# 1.5 years take X seconds

# # QUERY
# print("Querying...")
# #column_of_interest1 = 'dnombre'
# column_of_interest1 = 'ARANC_NAC'
# list_contains1 = ["1001","0910"]
# is_search_start = False
# is_search_end = True
#
# list_contains_str = str(("%'" if is_search_end else "'") + " OR "+column_of_interest1+" LIKE " + ("'%" if is_search_start else "'")).join(list_contains1)
# list_contains_str = column_of_interest1 + " LIKE " + ("'%" if is_search_start else "'") + list_contains_str + ("%'" if is_search_end else "'")
#
# print(list_contains_str)
#
# print("Apply filter: " + list_contains_str)
# df_query_output = sqldf("SELECT * FROM df WHERE " + list_contains_str)


# if is_excel:
#     print("Saving pandas df to Excel....")
#     df.to_excel(folder_path + "output.xlsx")

if is_remove_stuff:
    try:
        for f in ["imports.csv","exports.csv"] + dimension_files:
            os.remove(f)
    except:
        print("No output files to delete.")

    # checking whether file exists or not
    if os.path.exists(raw_folder + "temp"):
        try:
            import stat
            os.chmod(raw_folder + "temp", stat.S_IWRITE)
            import shutil
            shutil.rmtree(raw_folder + "temp")
            # os.unlink(raw_folder + "temp")
            # os.remove(raw_folder + "temp")
        except:
            print("error. cannot remove temp.")
    else:
        # file not found message
        print("File not found in the directory")

### ///// SQL QUERIES


start = time.process_time()

def parse_sql(filename):
    data = open(filename, 'r').readlines()
    stmts = []
    DELIMITER = ';'
    stmt = ''

    for lineno, line in enumerate(data):
        if not line.strip():
            continue

        if line.startswith('--'):
            continue

        if 'DELIMITER' in line:
            DELIMITER = line.split()[1]
            continue

        if (DELIMITER not in line):
            stmt += line.replace(DELIMITER, ';')
            continue

        if stmt:
            stmt += line
            stmts.append(stmt.strip())
            stmt = ''
        else:
            stmts.append(line.strip())
    return stmts

if execute_queries:
    # Scripts to create SQL schema
    # files = ["export_queries.sql", "import_queries.sql"]
    files = ["import_queries.sql"]
    for f in files:
        print("running", f)
        # Parse and execute .sql files
        queryParsed = parse_sql(folder_path + f)

        # Commit Transactions by batch
        with raw_con.cursor() as cur:
            for stmt in queryParsed:
                if stmt != "":
                    cur.execute(stmt)
                    raw_con.commit()

print("SQL query execution: " + str((time.process_time() - start)))

print("Done!")