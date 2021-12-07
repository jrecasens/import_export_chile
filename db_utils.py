__author__ = "Mohsen Moarefdoost, Pete Cacioppi, and Sean Kelley"
__copyright__ = "Opex Analytics LLC"
__credits__ = ["Mohsen Moarefdoost", "Pete Cacioppi", "Sean Kelley",
               "Rohit Karvekar", "Mohit Mahajan", "Javier Recasens"]
__doc__ = """Centered around a class that establishes a database connection
and provides a few methods for reading, writing and deleting database objects"""



import sys
import re
import json
import os
import warnings
from datetime import datetime
from io import StringIO
import time
import warnings
# from framework_utils.pgtd import PostgresTicFactory
# from ticdat import TicDatFactory

try:
    import pandas as pd
except:
    pd = None

try:
    import psycopg2
except:
    psycopg2 = None

try:
    from sqlalchemy import create_engine, inspect, MetaData
    from sqlalchemy.engine.url import URL
except:
    sqlalchemy = None
    create_engine = inspect = MetaData = URL = None


class AppDBConnException(Exception):
    pass


class CreateTableWithWarning(UserWarning):
    pass

class CreateTableWithException(UserWarning):
    pass


def verify(b, msg):
    if not b:
        raise AppDBConnException(msg)


def warn_if(b, msg):
    if b:
        warnings.warn(msg)

database_example = ("\n {\n \t'database': {" +
                    "\n\t\t 'drivername': 'postgresql'," +
                    "\n\t\t 'dbserverName': 'training.opexanalytics.com'," +
                    "\n\t\t 'port': '5432', \n\t\t 'dbusername': 'opexapp'," +
                    "\n\t\t 'dbpassword': 'this&iswa%first1'," +
                    "\n\t\t 'dbname': 'marie_kondo_143' \n\t } \n }")

def db_create(db_conn
              , app_id=''
              , db_name='enframe_app'):
    """ Creates an empty PostgreSQL Database in your local machine (In localhost default server. NOT in memory).
        The DB has the same definition properties of an Enframe 3 database. To create a new DB it requires connection
        to an existing DB:
         1.- Use a "default" postgres DB connection (localhost)
             To reset password use: ALTER USER postgres WITH PASSWORD 'abc123';
         2.- Or if using an in-memory testing.postgresql DB (127.0.0.1)
        CAUTION: If the DB exists it will be dropped.

            :param con: (connection) psycopg2 raw connection.
            :param app_id: (str) Aplication ID used for the Report Role.
            :param db_name: (str) New Database Name
    """

    # Perform this operation in a local context only
    if db_conn.get_dsn_parameters()['host'] in ['localhost', '127.0.0.1']:
        # To automate process, set Isolation level to auto commit
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
        db_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Any active connection needs to be terminated internally
        _sql_disconnect = (
            "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{"
            "}' AND pid <> pg_backend_pid();").format(
            db_name)
        # Drop DB
        _sql_drop = "DROP DATABASE IF EXISTS \"{}\";".format(db_name)

        # Create Enframe like roles
        _sql_app_role = "DROP ROLE IF EXISTS opexapp; " \
                        "CREATE ROLE opexapp WITH PASSWORD 'abc123' " \
                        "LOGIN SUPERUSER CREATEDB CREATEROLE INHERIT REPLICATION CONNECTION LIMIT - 1;"
        _sql_report_role = (
            "CREATE ROLE \"{}\" WITH "
            "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION CONNECTION LIMIT - 1;").format(
            "reports" + ('_' if app_id != '' else '') + app_id)

        # Create Empty Enframe DB
        _sql_create = (
            "CREATE DATABASE \"{}\" WITH OWNER = 'opexapp' TEMPLATE = 'template0' ENCODING = 'SQL_ASCII' TABLESPACE = "
            "'pg_default' LC_COLLATE = 'C' LC_CTYPE = 'C';").format(
            db_name, db_name)
        # Date Style works in the US (may not work in India).
        _sql_date_style = "SET datestyle = 'ISO, MDY';"

        _cur = db_conn.cursor()
        _cur.execute(_sql_disconnect)
        # DROP DATABASE cannot be executed from a function or multi-command string
        _cur.execute(_sql_drop)
        # Check Roles
        try:
            _cur.execute(_sql_app_role)
            _cur.execute(_sql_report_role)
        except Exception as exc:
            warnings.warn("%s: %s" % (exc.__class__.__name__, exc), Warning)
            pass
        # Create DB
        _cur.execute(_sql_create)
        _cur.execute(_sql_date_style)
        _cur.close()
    # Close connection to local DB
    db_conn.close()

class AppDBConn:
    """Class with a sqlalchemy engine and schema corresponding to live database
    with convenience routines for reading, writing, and deleting
    """

    metadata_cols = ['jqgrid_id', 'op_created_by', 'op_created_at',
                     'op_updated_by', 'op_updated_at']
    # this includes the columns added for the master schema
    all_reserved_cols = set(metadata_cols).union({'scenario_name', 'scenario_id'})
    req_keys = ['dbusername', 'dbserverName', 'port', 'dbpassword', 'dbname']

    # PostgreSQL to Python data type map dictionary
    data_type_dict = {'date': 'datetime.date', 'character varying': 'str', 'integer': 'int',
					  'timestamp with time zone': 'datetime.datetime', 'text': 'str', 'numeric': 'float', 
					  'boolean': 'bool', 'timestamp without time zone': 'datetime.datetime'}

    def _verify_schema(self, schema):
        if not self.override_schema:
            verify(schema in self.get_all_schema(), """Schema {} does not
                   exist in the database with the following connection string:
                   {}.\nThe available schemas are as follows:\n
                   {}""".format(schema, self.engine, self.get_all_schema()))
        return schema

    def generate_update_execution_query(self):
        """generate the sql query to run update_execution depending on which
        version of analytics center called the execution with this connection"""

        query = "SELECT MIN(id) AS id FROM public.execution"
        result = self.engine.execute(query).fetchone()
        arg_map = {
            'scenario_id': self._scenario_number,
            'execution_id': str(result['id']),
            'username': "'Administrator'"
        }

        # find out which arguments are required for `update_execution`
        query = """SELECT pg_catalog.pg_get_function_identity_arguments(p.oid) AS stmt
            FROM   pg_catalog.pg_proc p
            JOIN   pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            WHERE  p.proname = 'update_execution'
            AND n.nspname = 'public'
            ORDER BY 1;"""
        result = self.engine.execute(query).fetchone()
        args = result['stmt'].split(', ')
        args = [arg.partition(' ')[0] for arg in args]

        query = "SELECT update_execution({});".format(
            ",".join([arg_map[arg] for arg in args]))
        return query

    def generate_metadata_creation_query(self, schema, table):
        query = """ALTER TABLE {0}.\"{1}\"  ADD COLUMN IF NOT EXISTS "jqgrid_id" serial,
                    ADD COLUMN IF NOT EXISTS "op_created_by" VARCHAR(255) DEFAULT 'Administrator',
                    ADD COLUMN IF NOT EXISTS "op_created_at" TIMESTAMP WITH TIME ZONE DEFAULT now(),
                    ADD COLUMN IF NOT EXISTS "op_updated_by" VARCHAR(255) DEFAULT 'Administrator',
                    ADD COLUMN IF NOT EXISTS "op_updated_at" TIMESTAMP WITH TIME ZONE DEFAULT now();""".format(schema, table)
        return query

    def generate_metadata_columns(self, table, schema=None):
        schema = self._verify_schema(schema) if schema else self.schema
        query = self.generate_metadata_creation_query(schema,table)
        self.execute_query(query)
        return

    def generate_refresh_master_table_query(self, schema, table):
        scenario_id = schema.split("_")[1]
        query = """SELECT fn_populate_master_table({0},'{1}');""".format(scenario_id, table)
        return query

    def refresh_master_table(self, table, schema=None):
        """This works only in enframe"""
        schema = self._verify_schema(schema) if schema else self.schema
        query = self.generate_refresh_master_table_query(schema, table)
        try:
            self.execute_query(query)
        except Exception as ex:
            print(ex)
            warnings.warn("FAILED : Refresh master data for table {}".format(table), CreateTableWithException)
        return

    def __init__(self, schema=None, config=None, encoding='utf-8', echo=False,
                 override_schema=False):
        """
        :param schema: (str) If none, seeks sys.argv[1]
        :param config: (dict) If none, seeks the json file at sys.argv[2], which
        is read into a dict. Either route must yield a dictionary with keys and
        levels matching "example_config" below:
        example_config = {
            'database': {
                'drivername': <drivername>,
                'dbusername': <username>,
                'dbserverName': <host>,
                'port': <port>,
                'dbpassword': <password>,
                'dbname': <database>
            }
        }
        NOTE: 'drivername' is optional if using postgres and 'dbpassword'
        optional if your database is not password protected
        :param encoding: (str) protocol sqlalchemy uses when en/decoding
        :param echo: (str) whether or not to log database statements
        :param override_schema: (bool) whether or not we can connect to schema
        that does not exist yet
        """

        # validate that we have inputs
        if not schema:
            try:
                schema = sys.argv[1]
                schema_fail = None
            except IndexError as e:
                schema_fail = e
                verify(not schema_fail, "Please provide a schema in the first " +
                       "argument via the command line or AppDBCon constructor.")

        if not config:
            try:
                app_config_path = sys.argv[2]
                app_config_path_fail = None
            except IndexError as e:
                app_config_path_fail = e
                verify(not app_config_path_fail, "Please provide a json file with database " +
                       "configuration in the second argument to run compatibly with platform")
            verify(os.path.isfile(app_config_path),
                   "config argument {} is not a valid file path".format(app_config_path))
            verify(app_config_path[-4:] == 'json',
                   "Please provide the config as a json file")
            with open(app_config_path) as p:
                config = json.load(p)

        # validate we can connect to our database and schema
        verify('database' in config, "Please ensure the top level" +
               " keys of database config include 'database'. For " +
               "example, your dictionary should look something like: " +
               database_example)
        verify(all(req_key in config['database'] for req_key in self.req_keys),
               "The top-level 'database' key-value pair must include the " +
               "following keys with values relevant to your database: " +
               database_example)
        drivername = config['database'].get('drivername')
        warn_if(drivername is None, "Assuming the database driver is " +
                "'postgresql' since 'drivername' was not given as a " +
                "key-value pair in the config")
        db_dict = dict(drivername=drivername if drivername else 'postgresql',
                       username=config['database']['dbusername'],
                       host=config['database']['dbserverName'],
                       port=config['database']['port'],
                       password=config['database'].get('dbpassword'),  # password is optional
                       database=config['database']['dbname'])
        try:
            engine = create_engine(URL(**db_dict), encoding=encoding, echo=echo,
                                   pool_pre_ping=True)
            con = engine.connect()  # make sure our engine has valid connection
            con.close()
            engine_fail = None
        except Exception as e:
            engine_fail = e
        verify(not engine_fail, "The DB dictionary {} fails with message: {}".format(db_dict, engine_fail))
        self.config = config
        self.engine = engine
        self.override_schema = override_schema
        self.schema = self._verify_schema(schema)
        self.dsn = {'host': db_dict["host"],
                    'port': int(db_dict["port"]),
                    'dbname': db_dict["database"],
                    'password': db_dict["password"],
                    'user': db_dict['username']}

        # check if we're on enframe by looking for stored sql functions
        enframe_query = ("SELECT routine_name " +
                         "FROM information_schema.routines " +
                         "WHERE routine_type='FUNCTION' " +
                         "AND specific_schema='public' " +
                         "AND routine_name LIKE 'update_execution';")
        self._on_enframe = (True if self.engine.name == 'postgresql' and
                            list(self.engine.execute(enframe_query)) else False)
        self._scenario_number = self.schema.rpartition('_')[2] if self._on_enframe else None

    def read_table(self, table, schema=None, metadata=False):
        """ read <table> at our database connection into a pandas dataframe

        :param table: (str) name of table as it appears in the database to pull
        :param schema: (str) from which schema <table> should be read. If not
        specified, defaults to self.schema
        :param metadata: (bool) True includes metadata columns  in returned
        table if on enframe
        :return: a dataframe of <table>
        """
        schema = self._verify_schema(schema) if schema else self.schema
        query = 'SELECT * FROM ' + schema + '.' + table + ';'
        return self.read_query(query, schema, metadata)

    def read_query(self, query, schema=None, metadata=False):
        """ read a custom sql query into a dataframe

        :param query: (str) a sql query
        :param schema: (str) on which schema this query should run. If not
        specified, defaults to self.schema
        :param metadata: (bool) True includes metadata columns  in returned
        table if on enframe and if metadata columns are selected within sql quuery
        :return: a dataframe of table resulting from <query>
        """
        schema = self._verify_schema(schema) if schema else self.schema
        query = "SET search_path TO {}; ".format(schema) + query  # ensure schema given
        df = pd.read_sql(sql=query, con=self.engine)
        return df if not self._on_enframe or metadata else \
            df[[col for col in df.columns if col not in self.metadata_cols]]

    def write_table(self, data, table, schema=None, if_exists='truncate',
                    chunksize=None):
        """Sends a dataframe to a table at self's database connection. If
        writing to an Enframe postgres instance, this method will write
        metadata columns for you and will fail if you try to on your own. Note,
        this method will also drop the index on your dataframe.

        TODO: this function needs a fast write option for larger dataframes

        :param data: (dataframe) What will be sent to the database
        :param table: (str) Where <data> will be sent
        :param schema: (str) In which schema to write <data>. If not specified,
        defaults to self.schema
        :param if_exists: (str) Accepts 'replace', 'append', and 'fail'. See
        pandas.dataframe.to_sql() for more information
        :param chunksize: (int) How many rows to write to the database at once.
        Highly suggested to use this if your table is > 100 MB
        :param method: Accepts None/multi, multi inserts data into chunks
        :return:
        """

        schema = self._verify_schema(schema) if schema else self.schema
        verify(not any(col in self.metadata_cols for col in data.columns),
               'Please do not write metadata columns back to the database. ' +
               'Enframe writes the values for those columns for you. ' +
               'The metadata columns are as follows: {}'.format(self.metadata_cols))

        if if_exists == "replace":
            data.to_sql(table, con=self.engine, schema=schema, if_exists="replace",
                        index=False, chunksize=chunksize)
            if self._on_enframe:
                self.generate_metadata_columns(table)
            msg = """\n*** WARNING ***\nFunction copy_table argument if_exists = 'replace' creates a new table in the database\n
            schema :: <{0}>
            table_name :: <{1}>\n
            NOTE: This operation leads to loss of column properties
            To avoid such behaviour in future use safe options >> if_exists = 'truncate' or if_exists = 'append'\n""".format(schema, table)
            warnings.warn(msg, CreateTableWithWarning)

        else:
            if if_exists == "truncate":
                self.truncate_table(table, schema=schema)
                # Update if_exists variable after truncate
                if_exists = "append"
            data.to_sql(table, con=self.engine, schema=schema, if_exists=if_exists,
                        index=False, chunksize=chunksize)

        # If platform, add jqgrid_id and op columns if they've been deleted
        if self._on_enframe:
            self.refresh_master_table(table, schema=schema)

    def copy_table(self, data, table, schema=None, if_exists='truncate', silence=True):
        """
        This is fast write option for larger dataframes.
        Copies a dataframe to a table using database connection. If
        writing to an Enframe postgres instance, this method will write
        metadata columns for you and will fail if you try to on your own.
        Note, this method will also drop the index on your dataframe.

        :param data: (dataframe) What will be sent to the database
        :param table:  (str) table name in the db (where data is copied)
        :param schema: (str) In which schema to write <data>.
        :param if_exists: (str) Accepts 'replace', 'append', 'truncate'
        :return:
        """
        engine = self.engine

        # Verify if the database is postgres.
        verify((engine.name == 'postgresql'),
               'Database other than postgres is specified. ' +
               'copy_table works only with postgres.' +
               'The database specified is: {}'.format(engine.name))

        # Verify schema if given as argument else self.schema
        schema = self._verify_schema(schema) if schema else self.schema

        # Check whether you're passing metainfo cols or not
        verify(not any(col in self.metadata_cols for col in data.columns),
               'Please do not write metadata columns back to the database. ' +
               'Enframe writes the values for those columns for you. ' +
               'The metadata columns are as follows: {}'.format(self.metadata_cols))

        start_time = time.time()  # start the clock

        # Create a new connection
        conn = engine.raw_connection()
        cur = conn.cursor()
        tableExists = True

        # if  if_exists == 'append', check if the table exists within the schema of the db. \
        # Store the check in tableExists variable as True/False
        if if_exists in ['append', 'truncate']:
            query = """SELECT EXISTS (
                            SELECT 1 AS result
                            FROM pg_tables
                            WHERE schemaname = '{0}'
                            AND tablename = '{1}');""".format(schema, table)
            cur.execute(query)
            tableExists = cur.fetchone()[0]

        # if the table exists (tableExists = True), check typecast columns of the dataframe as needed.
        if tableExists:
            query = """SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = '{0}'
                        AND table_schema = '{1}'
                        AND data_type in ('integer','bigint');""".format(table, schema)
            col_types = set(pd.read_sql_query(sql=query, con=engine).column_name)

            try:
                col_types.remove('jqgrid_id')
            except KeyError:
                pass

            cols_int = set(data[:0].select_dtypes(include=['int64', 'int32']))
            col_astype = col_types - cols_int

            for i in col_astype:
                try:
                    data[i] = data[i].astype(int)
                    if not silence:
                        log_string = "Column: <{0}> of the table : <{1}> casted to int"
                        print(log_string.format(i, table))
                except:
                    pass

        if if_exists == 'replace' or not tableExists:
            # Add metainfo columns >>> Not Required >> Handled via update execution query
            data.reset_index(inplace=True, drop=True)
            data[:0].to_sql(table, con=engine, schema=schema, if_exists='replace', index=False)
            if self._on_enframe:
                self.generate_metadata_columns(table, schema=schema)
            msg = """\n*** WARNING ***\nFunction copy_table argument if_exists = 'replace' creates a new table in the database\n
            schema :: <{0}>
            table_name :: <{1}>\n
            NOTE: This operation leads to loss of column properties
            To avoid such behaviour in future use safe options >> if_exists = 'truncate' or if_exists = 'append'\n""".format(schema, table)
            warnings.warn(msg, CreateTableWithWarning)

        if if_exists == 'truncate':
            self.truncate_table(table, schema=schema)

        try:
            output = StringIO()
            data.to_csv(output, index=False, sep='|')
            output.seek(0)

            cols = "({0})".format(', '.join('"{0}"'.format(column) for column in data.columns))
            sql = "COPY \"{0}\".\"{1}\" ".format(schema, table) + cols + \
                " FROM STDIN DELIMITER '|' CSV HEADER"
            cur.copy_expert(sql, output)
            conn.commit()
            cur.close()

            if self._on_enframe:
                self.refresh_master_table(table, schema=schema)

            if not silence:
                print("Copied {0} records into the table :: <{1}> in {2} sec.".format(
                    len(data), table, round(time.time() - start_time, 2)))
        except Exception as ex:
            msg = "\n*** WARNING ***\nCan't upload data into table :: <{0}>\nEXCEPTION ::\n<{1}>".format(
                table, ex)
            warnings.warn(msg, Warning)

    def get_all_schema(self, common_string_in_schema_name=None):
        """Return all schema in a database (that optionally match a string)

        :param common_string_in_schema_name: (str) the string to match on
        :return: (list of str) List of schemas in the database
        """
        insp = inspect(self.engine)
        if common_string_in_schema_name:
            return [name for name in insp.get_schema_names() if
                    name.startswith(common_string_in_schema_name)]
        else:
            return insp.get_schema_names()

    def get_db_tables(self, schema=None):
        """Return all tables in the schema in the database

        :param schema: (str) From which schema to report the list of tables.
        If not specified, defaults to self.schema
        :return: a list of tables and their corresponding metadata
        """
        schema = self._verify_schema(schema) if schema else self.schema
        meta_data = MetaData()
        meta_data.reflect(bind=self.engine, schema=schema)
        return meta_data.tables.values()

    def drop_all_tables(self, schema=None):
        """Clear the schema of all of its tables. Will delete all data from each
        table then remove the tables themselves. If you want to just have the
        data removed from your tables, please see AppDBCon.delete_all_rows()

        :param schema: (str) From which schema to remove all of its tables
        :return: annihilation
        """
        schema = self._verify_schema(schema) if schema else self.schema
        meta_data = MetaData(bind=self.engine)
        meta_data.reflect(self.engine, schema=schema)
        meta_data.drop_all()

    def read_all_tables(self, schema=None, metadata=False):
        """Send each table in the given schema to a dataframe

        :param schema: (str) From which schema to read all of its tables. If not
        specified, defaults to self.schema
        :param metadata: (bool) True includes metadata columns  in returned
        table if on enframe
        :return: (dict of dataframe) each dataframe is keyed by the name of its
        corresponding table as it appears in the database
        """
        df_dict = {}
        schema = self._verify_schema(schema) if schema else self.schema
        for table in self.get_db_tables(schema):
            name = table.fullname
            query = 'SELECT * FROM %s' % name
            df = self.read_query(query, schema, metadata)
            df_dict[table.name] = df
        return df_dict

    def execute_query(self, command):
        """Run a custom query on the database and schema provided in self.
        Differs from AppDBCon.read_query() by not requiring or returning any
        output from the query.

        :param command: (str) the SQL query to execute
        :return:
        """
        _connection = self.engine.connect()
        _transaction = _connection.begin()
        try:
            _connection.execute(command)
            _transaction.commit()
        except:
            _transaction.rollback()
            raise
        finally:
            _connection.close()

    def delete_all_rows(self, table, schema=None):
        """ Remove all rows from a table within self's database connection. This
        will retain the table's column structure.

        :param table: (str) the name of the table as it appears in the database
        to have all of its records removed
        :param schema: From which schema to delete the records from <table>. If
        not specified, defaults to self.schema
        :return:
        """
        schema = self._verify_schema(schema) if schema else self.schema
        query = 'DELETE FROM \"' + schema + '\".\"' + table + '\";'
        self.execute_query(query)

    def truncate_table(self, table, schema=None):
        """ Remove all rows from a table within self's database connection. This
        will retain the table's column structure, difference from delete rows is
        that this operation is irreversible

        :param table: (str) the name of the table as it appears in the database
        to have all of its records removed
        :param schema: From which schema to delete the records from <table>. If
        not specified, defaults to self.schema
        :return:
        """
        schema = self._verify_schema(schema) if schema else self.schema
        query = 'TRUNCATE TABLE \"' + schema + '\".\"' + table + '\";'
        self.execute_query(query)

    def search_for_column(self, column_name, strict=True):
        """Search self's database within all schemas and tables for columns matching
        the supplied parameter column_name.

        :param column_name: (str) the name of the column to search for in the database
        :param strict: (bool) If only columns that strictly match column_name should be returned
        :return: A list of tuples (schema, table, column_name) where that
        column is found.
        """
        query = 'SELECT table_schema, table_name, column_name FROM information_schema.columns WHERE column_name like '
        if strict:
            query += "'" + column_name + "'"
        else:
            query += "'%%" + column_name + "%%'"
        search_results = self.read_query(query)
        return [tuple(row) for row in search_results.values.tolist()]

    def deduplicate_table(self, table, business_key, schema=None, order_by="Null", index=True):
        """Deduplicate the table by the business_key. Break ties using the order_by
        SQL statement. Puts an index on the business_key if index is set to True.

        :param table: (str) the table to perform the deuplication on
        :param business_key: (list) set of columns that should uniquely identify
        a record in the table
        :param schema: (str) the schema of the table in parameter one
        :param order_by: (str) SQL syntax string that will be used to break ties
        when 2 records occupy the same business_key. The first record will be choosen
        according to the order_by statment.
        :param index: (bool) if the business_key should be indexed at the end of
        the deduplication process
        """
        schema = self._verify_schema(schema) if schema else self.schema

        partition_by = ', '.join(business_key)
        index_name = schema + '_' + table + '_' + '_'.join(business_key)[:59] + '_idx'

        query = "DELETE \n" \
                f"FROM {schema}.{table} \n" \
                "WHERE CTID IN ( \n" \
                "SELECT CTID \n" \
                "FROM ( \n" \
                "SELECT CTID, \n" \
                f"ROW_NUMBER() OVER( PARTITION BY {partition_by} ORDER BY {order_by}) as dedup_rn \n" \
                f"FROM {schema}.{table} \n" \
                ") rownumbered \n" \
                "WHERE dedup_rn > 1 \n" \
                "); \n"

        self.execute_query(query)

        if index:
            index_query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema}.{table} ( {partition_by} );"
            self.execute_query(index_query)

    def execute_sql_batch(self, query='', debug=False):
        """Executes SQL script by batch. Use instead of execute() if large transactions need to be isolated
        and committed independently.

        :query query: List of SQL scripts.
        :query debug: Print SQL script before Commit
        :return:
        """

        _engine = self.engine

        # Verify if the database is postgres.
        verify((_engine.name == 'postgresql'),
               'Database other than postgres is specified. ' +
               'execute_sql_batch works only with postgres.' +
               'The database specified is: {}'.format(_engine.name))

        # Get existing RAW connection from engine (is not creatig a new one so no need to close it)
        _conn = _engine.raw_connection()

        # Commit Transactions by batch
        with _conn.cursor() as cur:
            for stmt in query:
                if stmt != "":
                    if debug:
                        print(stmt)
                    cur.execute(stmt)
                    _conn.commit()

    def get_schema_metadata(self, schema=None):
        """Get Input/Output schema metadata. Removes metadata columns (jqgrid_id, op_created_by, etc.)
        :return: Dataframe with schema details such as: table_name, position, type, column_name, postgres_data_type
        and python_data_type.
        """
        _engine = self.engine

        schema = self._verify_schema(schema) if schema else self.schema

        # Verify if the database is postgres.
        verify((_engine.name == 'postgresql'),
               'Database other than postgres is specified. ' +
               'The database specified is: {}'.format(_engine.name))

        # Check if there is any scenario schema, if not use any available.
        if "scenario_1" not in schema:
            _sql_query = ("SELECT table_schema::text FROM information_schema.tables WHERE table_schema LIKE '%%scenario_%%' LIMIT 1;")
            schema = pd.read_sql_query(sql=_sql_query, con=_engine)['table_schema'][0]

        # Get schema metadata
        _sql_query = ("SELECT pg_namespace.nspname AS table_schema " +
                        ", pg_class.relname AS table_name " +
                        ", pg_attribute.attname AS column_name " +
                        ", pg_attribute.attnum AS ordinal_position " +
                        ", enframe_tables.visible, enframe_tables.type " +
                        ", CASE WHEN pg_class.relkind = 'm' THEN 'View' ELSE 'Table' END AS postgres_data_type " +
                        "FROM pg_catalog.pg_class " +
                        "INNER JOIN pg_catalog.pg_namespace " +
                        "ON pg_class.relnamespace = pg_namespace.oid " +
                        "INNER JOIN pg_catalog.pg_attribute " +
                        "ON pg_class.oid = pg_attribute.attrelid " +
                        "LEFT JOIN (SELECT DISTINCT tablename, visible, type FROM public.lkp_data_upload_tables) AS enframe_tables " +
                        "ON pg_catalog.pg_class.relname = enframe_tables.tablename " +
                        "WHERE pg_namespace.nspname = '" + schema + "' " +
                        "AND pg_class.relkind IN ('r','m') " +
                        "AND pg_attribute.attnum >= 1 " +
                        "AND pg_class.relname <> 'parameters' " +
                        "ORDER BY table_schema, table_name, ordinal_position")

        # Get dataframe
        _sql_df = pd.read_sql_query(sql=_sql_query, con=_engine)

        if self._on_enframe:
            # Remove metadata columns
            _sql_df = _sql_df[~_sql_df['column_name'].isin(self.metadata_cols)]
            # Add python data types
            _sql_df["python_data_type"] = _sql_df["postgres_data_type"]
            _sql_df.replace({"python_data_type": self.data_type_dict}, inplace=True)

        return(_sql_df)
        
    def db_restore(self, file_path, app_id=''):
        """ Restores a DB Backup from a plain .sql file in your LOCAL machine.
        Requires an existing empty Enframe DB (to be used with framework_utils.db_create()).
        Requirements of the Enframe DB .sql Backup:
                Format: Plain
                Encoding: SQL_ASCII
                Role Name: opexapp
                Dump Options - > Use Insert Commands: ON

        :param file_path: (str) File Location (e.g. C:/Github/python_framework_utils/test_framework_utils/example_enframe3_db.sql)
        :param app_id: (str) Used to replace report_<app id> ROLE name.
        """

        _engine = self.engine

        # Verify if the database is postgres.
        verify((_engine.name == 'postgresql'),
               'Database other than postgres is specified. ' +
               'The database specified is: {}'.format(_engine.name))

        #G et Database name
        _db_name = self.config['database']['dbname']

        # Check if DB exists
        _sql_is_db_exists = (
            "SELECT count(*) FROM pg_database WHERE datname = '{}';").format(
            _db_name)
        _result = self.read_query(_sql_is_db_exists)
        verify(bool(_result['count'][0]),
               'The database {} does not exists'.format(_db_name))

        # Get existing RAW connection from engine. execute_query() fails. Use cursor instead
        _conn = _engine.raw_connection()

        # Read backup DB as plain text file
        with open(file_path, "r") as f:
            _sql_db_backup = f.read()
            # Replace <app_id>
            p1 = re.compile(r'TO reports_[0-9a-zA-Z]+')
            _sql_db_backup = p1.sub("TO reports" + ('_' if app_id != '' else '') + app_id, _sql_db_backup)
            # Execute SQL Query
            with _conn.cursor() as cur:
                cur.execute(_sql_db_backup)
                try:
                    _conn.commit()
                except:
                    _conn.rollback()
                    raise


    def close(self):
        """ Close all of the active connections to the database. Does not
        disconnect the engine.

        :return:
        """
        self.engine.dispose()