try:
    import config
except ImportError:
    from . import config

import sqlalchemy as db
import pandas as pd
import uuid
import urllib
from os import path
import os

def sqlalchemy_db_uri(conn_dict):
    """Create SQL Alchemy Database URI"""

    # import pyodbc
    # driver = sorted(pyodbc.drivers()).pop()

    AZURE_SQL_DRIVER = conn_dict["database"]["AZURE_SQL_DRIVER"]
    AZURE_SQL_SERVER = conn_dict["database"]["AZURE_SQL_SERVER"]
    AZURE_SQL_DB_NAME = conn_dict["database"]["AZURE_SQL_DB_NAME"]
    AZURE_SQL_DB_USER = conn_dict["database"]["AZURE_SQL_DB_USER"]
    AZURE_SQL_DB_PWD = conn_dict["database"]["AZURE_SQL_DB_PWD"]
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(
                                    urllib.parse.quote_plus(r'Driver=' + AZURE_SQL_DRIVER + ';'
                                    r'Server=' + AZURE_SQL_SERVER + ';'
                                    r'Database=' + AZURE_SQL_DB_NAME + ';'
                                    r'Uid=' + AZURE_SQL_DB_USER + ';'
                                    r'Pwd=' + AZURE_SQL_DB_PWD + ';'
                                    r'Encrypt=yes;'
                                    r'TrustServerCertificate=yes;'
                                    r'Connection Timeout=30;')
    )
    return conn_str

class SqlServerConnector:
    """Connector to interact with a sql server database"""

    def __init__(self, conn_str, storage):
        # Configure Tolveet Logger
        TL = config.TolveetLogger()
        self.logger = TL.get_tolveet_logger()

        self.engine = self._create_database_engine(conn_str)
        self.storage = storage

    def _create_database_engine(self, conn_str):
        """Create a sqlalchemy database engine """
        # Turn off SQLAlchemy logging
        # logging.getLogger('sqlalchemy').setLevel(logging.DEBUG)
        engine = db.create_engine(conn_str, fast_executemany=True)
        return engine

    def get_schema_metadata(self, table_name="camera_event_detection"):
        """ Get the DB schema of a table (e.g. columns and data types) """

        _metadata = db.MetaData()
        _engine = self.engine
        try:
            _table_schema = db.Table(table_name, _metadata, autoload_with=_engine, extend_existing=True)
            self.logger.info(str("SQL Connector - Successful loading of schema for DB table: " + table_name))
        except Exception as ex:
            _table_schema = None
            self.logger.error("SQL Connector - Exception:" + str(ex))
        return _table_schema

    def generate_image_name(self, device_source_id, detection_type, date_detection, extension="jpeg"):
        # Universally unique identifier (UUID)
        id_image = str(uuid.uuid4().hex)
        # Set extension
        extension = extension.replace('.', '')
        if type(date_detection) != str:
            date_detection = date_detection.strftime("%Y%m%d%H%M%S")
        image_name_new = device_source_id + '_' + detection_type + '_' + date_detection + '_' + id_image + '.' + extension
        return image_name_new

    def insert_camera_event(self, entity, container):
        """ Insert Camera Event Detection and related image when applicable """
        result = False
        try:
            self.logger.info("SQL Connector - Connecting to DB...")
            con = self.engine.connect()
            self.logger.info("SQL Connector - Checking camera registration in customer DB...")
            is_authorized, device_subtype = self.is_camera_authorized(entity.device_source_id,
                                                                      entity.device_mac_address)
            if is_authorized:
                self.logger.info("SQL Connector - Camera is registered in DB.")
                self.logger.info("SQL Connector - Insert to camera_event_detection table...")
                camera_event_detection = self.get_schema_metadata("camera_event_detection")
                stmt_1 = db.insert(camera_event_detection).values(
                    device_source_id=entity.device_source_id,
                    event_identifier=entity.event_identifier,
                    date_detection=entity.date_detection,
                    detection_type=entity.detection_type,
                    details_json=entity.details_json
                )
                con.execute(stmt_1)
                self.logger.info("SQL Connector - Successful insert.")
                result = True
                if device_subtype == 'anpr':
                    try:
                        self.logger.info("SQL Connector - Insert to camera_event_images table...")
                        image_name = self.generate_image_name(device_source_id=entity.device_source_id,
                                                              detection_type=entity.detection_type,
                                                              date_detection=entity.date_detection)
                        camera_event_images = self.get_schema_metadata("camera_event_images")
                        stmt_2 = db.insert(camera_event_images).values(
                            device_source_id=entity.device_source_id,
                            event_identifier=entity.event_identifier,
                            date_detection=entity.date_detection,
                            image_url=self.storage.image_upload(container=container,
                                                                image_file=entity.image,
                                                                image_name=image_name,
                                                                des_folder='')
                        )
                        con.execute(stmt_2)
                        self.logger.info("SQL Connector - Successful insert.")
                        self.logger.info("SQL Connector - Insert to camera_event_vehicle_attributes table...")
                        camera_event_vehicle_attributes = self.get_schema_metadata("camera_event_vehicle_attributes")
                        stmt_3 = db.insert(camera_event_vehicle_attributes).values(
                            device_source_id=entity.device_source_id,
                            event_identifier=entity.event_identifier,
                            date_detection=entity.date_detection,
                            vehicle_license_plate=entity.vehicle_license_plate,
                            vehicle_license_plate_confidence_level=entity.vehicle_license_plate_confidence_level,
                            vehicle_direction=entity.vehicle_direction,
                            vehicle_type=entity.vehicle_type,
                            vehicle_color=entity.vehicle_color
                        )
                        con.execute(stmt_3)
                        self.logger.info("SQL Connector - Successful insert.")
                        result = True
                    except Exception as ex:
                        result = False
                        self.logger.error("SQL Connector - Could not insert ANPR image and/or attributes. PENDING IMPLEMENTATION, ROLLBACK OF EVENT.")
                        self.logger.error("SQL Connector - Exception:" + str(ex))
            else:
                result = False
                status = "SQL Connector - Camera " + entity.device_source_id + " is not registered in customer DB.", 401
                self.logger.error(status[0])
            con.close()
            self.logger.info("SQL Connector - Connection closed.")
        except Exception as ex:
            result = False
            self.logger.error("SQL Connector - Exception:" + str(ex))
        return result

    def delete_camera_events(self, device_source_id):
        """ delete camera detection records """
        with self.engine.connect() as con:
            query = "DELETE FROM camera_event_detection WHERE device_source_id = '{}';".format(device_source_id)
            con.execute(query)
            query = "DELETE FROM camera_event_images WHERE device_source_id = '{}';".format(device_source_id)
            con.execute(query)
            query = "DELETE FROM camera_event_vehicle_attributes WHERE device_source_id = '{}';".format(device_source_id)
            con.execute(query)

    def get_devices(self):
        """ Get IoT devices used by Tolveet client (e.g. cameras, sensors, etc.) """
        device_source_id_lst = []
        device_serial_number_lst = []
        device_mac_address_lst = []
        device_type_lst = []
        device_subtype_lst = []
        device_brand_lst = []
        with self.engine.connect() as con:
            query = "SELECT device_source_id, device_serial_number, device_mac_address, device_type, device_subtype, " \
                    "device_brand FROM devices"
            records = con.execute(query)
            for record in records:
                device_source_id_lst.append(record[0])
                device_serial_number_lst.append(record[1])
                device_mac_address_lst.append(record[2])
                device_type_lst.append(record[3])
                device_subtype_lst.append(record[4])
                device_brand_lst.append(record[5])

        return list(map(lambda a, b, c, d, e, f: (a, b, c, d, e, f), device_source_id_lst, device_serial_number_lst,
                        device_mac_address_lst, device_type_lst, device_subtype_lst, device_brand_lst))

    def is_camera_authorized(self, device_source_id, device_mac_address):
        """ Checks if the camera is registered in the DB. If it is, return also de device_subtype"""
        result = False, 'Not Applicable'
        devices = self.get_devices()
        for device in devices:
            if device_source_id in device:
                if device_mac_address in device:
                    result = True, device[4]
        return result

    def close(self):
        """ Close all of the active connections to the database. Does not
        disconnect the engine.
        """
        self.engine.dispose()

    def read_table(self, table_name, schema='dbo', add_conditions=None, columns="*"):
        """Read table named table_name"""
        query = f'SELECT {columns} FROM {schema}.{table_name} '
        if add_conditions is not None:
            query = query + add_conditions
        query = query + ';'
        df = pd.read_sql(sql=query, con=self.engine)
        return df

    def read_query(self, query):
        """Read table named table_name"""
        df = pd.read_sql(sql=query, con=self.engine)
        return df

    # def insert_from_dataframe(self, connection, df, table_name, schema='dbo'):
    #     """Insert row by row from DF"""
    #     # Commit Transactions by batch
    #     with connection.cursor() as cursor:
    #         # Insert Dataframe into SQL Server:
    #         for index, row in df.iterrows():
    #             cursor.execute("INSERT INTO HumanResources.DepartmentTest (DepartmentID,Name,GroupName) values(?,?,?)",
    #                            row.DepartmentID, row.Name, row.GroupName)
    #         connection.commit()

    def write_table_from_dataframe(self, df, table_name, schema='dbo', if_exists='append'):
        """Write dataframe df to table table_name"""

        self.logger.info(f'SQL Connector - Writing {table_name} table...')

        if len(df):
            self.logger.warning(f'SQL Connector - Empty Dataframe.')
        # convert datetime to expected format our driver wants
        for col in df.select_dtypes(include=['datetime']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        self._write_table(data=df, table=table_name, schema=schema, if_exists=if_exists)
        self.logger.info(f'SQL Connector - Insert Complete.')

    def _write_table(self, data, table, schema, if_exists='append', chunksize=100000):
        if if_exists == "truncate":
            self.logger.info(f'SQL Connector - Truncating first...')
            self._truncate_table(table, schema)
        self.logger.info(f'SQL Connector - Inserting Data...')
        data.to_sql(table, con=self.engine, schema=schema, if_exists='append',
                    index=False, method='multi', chunksize=chunksize)

    def _truncate_table(self, table, schema):
        try:
            query = f'TRUNCATE TABLE {schema}.{table};'
            self._execute_command(query)
        except:
            self.logger.warning("SQL Connector - Could not TRUNCATE table.")
        # except Exception as ex:
        #     self.logger.warning("Could not truncate table. Exception:" + str(ex))
            try:
                self.logger.warning("SQL Connector - Trying DELETE FROM instead...")
                query = f'DELETE FROM {schema}.{table};'
                self._execute_command(query)
                self.logger.warning("SQL Connector - DELETE was successful.")
            except Exception as ex:
                self.logger.warning("SQL Connector - Could not delete records from table. Exception:")


    def _clear_table(self, table, schema):
        query = f'DELETE FROM {schema}.{table};'
        self._execute_command(query)

    def _execute_command(self, command):
        with self.engine.begin() as conn:
            conn.execute(command)

    @staticmethod
    def parse_sql(filename, is_debug=False):
        """
        Read .sql file and batch separates the entire script as a list (by removing comments and batch delimiters)
        """
        data = open(filename, 'r').readlines()
        stmts = []
        DELIMITER = ';'
        stmt = ''

        is_between_comments = False
        is_between_begin_end = False
        for lineno, line in enumerate(data):
            if is_debug:
                print(lineno)
                print(line)
            ## Skips empty lines
            if not line.strip():
                if is_debug:
                    print("Skipping empty line: ")
                    print(line.strip())
                continue

            # Skip one line comments
            if line.startswith('--'):
                if is_debug:
                    print("Skipping one line comments: ")
                    print(line.startswith('--'))
                continue


            # Skip GO
            if ('GO' in line) and (len(line.strip()) <= 3):
                if is_debug:
                    print("Skipping GO ")
                    print('GO' in line)
                continue

            ## Skips lines that start with:
            if line.strip().startswith('/**') and line.strip().endswith('/**'):
                if is_debug:
                    print("Skips lines that start with: /** and /**")
                    print(line.strip().startswith('/**') and line.strip().endswith('/**'))
                is_between_comments = False
                continue

            ## Skips lines that end with:
            if line.strip().endswith('**/'):
                if is_debug:
                    print("Skips lines that end with: **/")
                    print(line.strip().endswith('**/'))
                is_between_comments = False
                continue

            ## Skips lines that start with:
            if line.strip().startswith('**/'):
                if is_debug:
                    print("Skips lines that start with: **/")
                    print(line.strip().startswith('**/'))
                is_between_comments = False
                continue

            ## Skips lines that start with:
            if line.strip().startswith('/**'):
                if is_debug:
                    print("Skips lines that start with: /**")
                    print(line.strip().startswith('/**'))
                is_between_comments = True
                continue

            if is_between_comments:
                if is_debug:
                    print("Skipping in between comments")
                continue

            if line.lower().strip().startswith('begin'):
                is_between_begin_end = True
                stmt += line
                continue

            if line.lower().strip().startswith('end;'):
                is_between_begin_end = False
                stmt += line
                stmts.append(stmt.strip())
                stmt = ''
                continue

            if is_between_begin_end:
                stmt += line
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

    @staticmethod
    def execute_sql_batch(raw_connection, queryParsed, debug=False):
        # Commit Transactions by batch
        with raw_connection.cursor() as cursor:
            for stmt in queryParsed:
                if stmt != "":
                    try:
                        cursor.execute(stmt)
                        raw_connection.commit()
                    except Exception as e:
                        if debug:
                            print(stmt)
                        print("Error: %s" % e)

    @staticmethod
    def copy_from_file(raw_connection, schema, df, table_name, path_csv):
        """
        Here we are going save the dataframe on disk as
        a csv file, load the csv file
        and use copy_from() to copy it to the table
        """
        # Check whether the specified path exists or not
        if not os.path.exists(path_csv):
            # Create a new directory because it does not exist
            os.makedirs(path_csv)
        # Save the dataframe to disk
        tmp_df = os.path.join(path_csv, df.name + ".csv")
        if not path.exists(tmp_df):
            # print("File " + df.name + ".csv does NOT exists, writing to file server.")
            print(table_name + ": writting DF to .csv...")
            df.to_csv(tmp_df, index=False, sep=";", header=False)
        # f = open(tmp_df, 'r')
        cursor = raw_connection.cursor()
        cursor.fast_executemany = True
        try:
            query = "BULK INSERT {}.{} FROM '{}' WITH (FORMAT = 'CSV', FIELDTERMINATOR = ';', ROWTERMINATOR = '0x0a');".format(schema, table_name, tmp_df)
            cursor.execute(query)
            raw_connection.commit()
        except Exception as e:
            print(query)
            print("Error: %s" % e)
            raw_connection.rollback()
            cursor.close()
            return 1
        cursor.close()