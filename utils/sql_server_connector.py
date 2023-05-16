
import config
# except ImportError as e:
#     from app.connectors import config
try:
    import app.http_status_codes as status_code
except ImportError as e:
    import utils.http_status_codes as status_code
import sqlalchemy as db
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
import pandas as pd
import uuid
from urllib import parse
from os import path
import os
import re
import logging
from datetime import datetime
import pytz
import ast
import numpy as np

def sqlalchemy_db_uri(datacreds):
    """Create SQL Alchemy connection string"""
    conn_str = pyodbc_db_uri(datacreds)
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(parse.quote_plus(conn_str))
    return conn_str

def pyodbc_db_uri(datacreds):
    """Create pyodbc connection string"""
    AZURE_SQL_DRIVER = datacreds["database"]["AZURE_SQL_DRIVER"]
    AZURE_SQL_SERVER = datacreds["database"]["AZURE_SQL_SERVER"]
    AZURE_SQL_DB_NAME = datacreds["database"]["AZURE_SQL_DB_NAME"]
    AZURE_SQL_DB_USER = datacreds["database"]["AZURE_SQL_DB_USER"]
    AZURE_SQL_DB_PWD = datacreds["database"]["AZURE_SQL_DB_PWD"]
    conn_str = ("Driver="+AZURE_SQL_DRIVER+";"
                "Server="+AZURE_SQL_SERVER+";"
                "Database="+AZURE_SQL_DB_NAME+";"
                "UID="+ AZURE_SQL_DB_USER+";"
                "PWD="+AZURE_SQL_DB_PWD+";"        
                "TrustServerCertificate=yes;"
                "Encrypt=yes;"
                "Connection Timeout=30;"
                )
    return conn_str

def timezone_conversion(logger, log_prefix, dt_obj, tz_origin, tz_destination, verbose=True):
    """
        Deal with datime objects and correct time zone.

        The App API and Platform backend uses EST (e.g. Clients SQL Server, Azure applications and IP cameras
        will always record data in "Eastern Standard Time" or "America/New_York"). DB timestamps should have either
        (UTC -4) or (UTC -5) offset. However, the front end is client dependent, hence, timezone dependent.

        Args:
        - dt_obj (datetime): A datetime value (could be timezone aware or unaware).
        - to_db (boolean): If True then direction is TO the DB. dt_obj is transformed to TIME_ZONE_BACKEND_TZ.
                           If False then direction is FROM the DB. dt_obj is transformed to TIME_ZONE_FRONTEND_TZ.
        - verbose (boolean): If True then logger will show INFO.
        Returns:
        - output (datetime): timezone aware datetime object with proper time zone.

    """

    if verbose:
        logger.info('Using Origin TZ: ' + str(tz_origin) + ' and Destination TZ: ' + str(tz_destination))

    try:
        if not isinstance(dt_obj, datetime):
            if not isinstance(dt_obj, str):
                if verbose:
                    logger.warning(log_prefix + "Trying to convert to datetime.")
                dt_obj = dt_obj.to_pydatetime()
            else:
                if verbose:
                    logger.warning(log_prefix + "String provided. Transforming to datetime format...")
                # Handle case a datetime object was not provided.
                try:
                    if bool(datetime.strptime(dt_obj, config.Config.TIME_FORMAT_NO_TZ)):
                        if verbose:
                            logger.info('Try parsing with format: ' + str(config.Config.TIME_FORMAT_NO_TZ))
                        # Transform to time zone unaware datetime object
                        dt_obj = datetime.strptime(dt_obj, config.Config.TIME_FORMAT_NO_TZ)
                except ValueError:
                    if verbose:
                        logger.warning(log_prefix + "Cannot parse date object with format:" + str(
                        config.Config.TIME_FORMAT_NO_TZ))
                    try:
                        if verbose:
                            logger.info('Try parsing with format: ' + str(config.Config.DATE_FORMAT))
                        if bool(datetime.strptime(dt_obj, config.Config.DATE_FORMAT)):
                            # Transform to time zone unaware datetime object
                            dt_obj = datetime.strptime(dt_obj, config.Config.DATE_FORMAT)
                    except ValueError:
                        if verbose:
                            logger.warning(log_prefix + "Cannot parse date object with format:" + str(
                            config.Config.DATE_FORMAT))
                        return None

        # Check if timezone is present
        if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
            if verbose:
                logger.info(log_prefix + "No timezone found. Setting origin timezone: " + tz_origin)
            # Assign the origin timezone.
            output = pytz.timezone(tz_origin).localize(dt_obj)
            if verbose:
                logger.info(log_prefix + ' Setting destination timezone: ' + tz_destination)
            output = output.astimezone(pytz.timezone(tz_destination))
        else:
            if str(dt_obj.tzinfo) != tz_destination:
                if verbose:
                    logger.info(log_prefix + "Timezone found. Replacing existing timezone: " + str(dt_obj.tzinfo) +
                                     ", with destination timezone: " + tz_destination)
                # Handle case when there is a different timezone. Change timezone to destination TZ
                output = dt_obj.astimezone(pytz.timezone(tz_destination))
            else:
                if verbose:
                    logger.info(log_prefix + "Timezone found. Datetime object already in destination timezone: " + tz_destination)
                output = dt_obj
    except Exception as e:
        if verbose:
            logger.error(log_prefix + "datetime check and time zone conversion failed. Exception:" + str(e))
        return None

    return output

def df_col_tz_conversion(logger, log_prefix, df, meta=None, to_tz=None, str_format=None, verbose=False):
    """
    Transform all date columns in dataframe to Backend TZ.

    DATETIMEOFFSET data type is not properly supported in SQL Alchemy, we need to parse manually.
    read_sql or read_query get all date columns in UTC +0. Adjust to Backend TZ.

    :param logger:
    :param log_prefix:
    :param df:
    :param meta:
    :return:
    """
    # Sometimes the Backend TZ is not loaded in the config (when using SQLalchemy models we assign after its declared).
    if to_tz is None:
        to_tz = config.Config.TIME_ZONE_BACKEND_TZ
    # Transform date columns to proper timezone and format.
    datetime_type_lst = ['datetime64[ns, UTC]', 'datetime64[ns]', 'datetime']
    if len(df) > 0:
            for c in list(df.columns):
                if meta is not None:
                    column_type = str(meta.columns[c].type.python_type.__name__)
                else:
                    column_type = df[c].dtype
                if column_type in datetime_type_lst:
                    if verbose:
                        logger.info(log_prefix + "Handling date column: " + str(c) + ". Type: " + str(df.dtypes[c]))
                    df[c] = pd.to_datetime(df[c], utc=True, errors='coerce')
                    df[c] = df[c].dt.tz_convert(to_tz)
                    if str_format is not None:
                        df[c] = df[c].dt.strftime(str_format)
                        df[c].replace({np.nan: None}, inplace=True)

    return df


def _create_database_engine(conn_str):
    """Create a sqlalchemy database engine """
    if config.Config.SQLALCHEMY_ECHO is None:
        config.Config.SQLALCHEMY_ECHO = False
    engine = db.create_engine(url=conn_str,
                              fast_executemany=config.Config.FAST_EXECUTEMANY,
                              echo=config.Config.SQLALCHEMY_ECHO)
    return engine

class SqlServerConnector:
    """Connector to interact with a sql server database"""

    def __init__(self, conn_str, storage=None, log_prefix='', engine=None, is_sql_alchemy_logging=config.Config.SQLALCHEMY_ECHO):
        if config.Config.SQLALCHEMY_ECHO is None:
            is_sql_alchemy_logging = False
        # Configure App Logger
        tl = config.TolveetLogger()
        self.logger = tl.get_tolveet_logger()
        self.log_prefix = log_prefix + 'SQL Connector - '
        if engine is None:
            self.logger.info(self.log_prefix + 'Creating database engine...')
            self.engine = _create_database_engine(conn_str)
        else:
            self.logger.info(self.log_prefix + 'Reusing database engine...')
            self.engine = engine
        self.storage = storage
        if not is_sql_alchemy_logging:
            self.logger.info(self.log_prefix + 'Turning off SQLAlchemy logging...')
            logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

    def get_schema_metadata(self, table_name="camera_event_detection"):
        """ Get the DB schema of a table (e.g. columns and data types) """

        _metadata = db.MetaData()
        _engine = self.engine
        try:
            _table_schema = db.Table(table_name, _metadata, autoload_with=_engine, extend_existing=True)
            # self.logger.info(self.log_prefix + str("Successful loading of schema for DB table: " + table_name))
        except Exception as e:
            _table_schema = None
            self.logger.error(self.log_prefix + "Exception:" + str(e))
        return _table_schema

    def generate_image_name(self, device_source_id, detection_type, date_detection, extension="jpeg"):
        # Universally unique identifier (UUID)
        id_image = str(uuid.uuid4().hex)
        # Set extension
        extension = extension.replace('.', '')
        if type(date_detection) != str:
            date_detection = date_detection.strftime(config.Config.CAMERA_IMAGE_NAME_DATE_FORMAT_HIKVISION)
        img_name_new = device_source_id + '_' + detection_type + '_' + date_detection + '_' + id_image + '.' + extension
        return img_name_new

    def insert_camera_event(self, entity, extra_prefix=''):
        """ Insert Camera Event to DB """
        table_name = 'camera_event_detection'
        log_prefix = self.log_prefix + extra_prefix + "Table: " + table_name + "  - "
        with self.engine.connect() as con:
            try:
                self.logger.info(log_prefix + "Inserting data...")
                camera_event_detection = self.get_schema_metadata(table_name)
                stmt = db.insert(camera_event_detection).values(
                    device_source_id=entity.device_source_id,
                    event_identifier=entity.event_identifier,
                    date_detection=timezone_conversion(logger=self.logger,
                                                        log_prefix=log_prefix,
                                                        dt_obj=entity.date_detection,
                                                        tz_origin=config.Config.TIME_ZONE_BACKEND_TZ,
                                                        tz_destination=config.Config.TIME_ZONE_BACKEND_TZ,
                                                        verbose=False),
                    detection_type=entity.detection_type,
                    details_json=entity.details_json
                )
                sql_output = con.execute(stmt)
                # Since this table does not have a trigger we can fetch the inserted PK key:
                inserted_id = int(sql_output.inserted_primary_key[0])
                result = (inserted_id, status_code.HTTP_200_OK)
                self.logger.info(log_prefix + "Successful insert of ID: " + str(inserted_id))
            except Exception as e:
                result = (0, status_code.HTTP_400_BAD_REQUEST)
                self.logger.error(log_prefix + "Could not insert data. Exception:" + str(e))
            return result


    def insert_camera_image(self, entity, container, extra_prefix=''):
        """ Insert Camera Image to DB """
        table_name = 'camera_event_images'
        log_prefix = self.log_prefix + extra_prefix + "Table: " + table_name + "  - "
        with self.engine.connect() as con:
            try:
                self.logger.info(log_prefix + "Inserting data...")
                image_name = self.generate_image_name(device_source_id=entity.device_source_id,
                                                      detection_type=entity.detection_type,
                                                      date_detection=entity.date_detection)
                if ('.jpg' not in image_name) and ('.jpeg' not in image_name):
                    image_name = image_name + '.jpeg'
                camera_event_images = self.get_schema_metadata(table_name)
                stmt = db.insert(camera_event_images).values(
                    device_source_id=entity.device_source_id,
                    event_identifier=entity.event_identifier,
                    date_detection=timezone_conversion(logger=self.logger,
                                                       log_prefix=log_prefix,
                                                       dt_obj=entity.date_detection,
                                                       tz_origin=config.Config.TIME_ZONE_BACKEND_TZ,
                                                       tz_destination=config.Config.TIME_ZONE_BACKEND_TZ,
                                                       verbose=False),
                    image_url=self.storage.image_upload(container=container,
                                                        image_file=entity.image,
                                                        image_name=image_name,
                                                        des_folder='',
                                                        verbose=False),
                    image_name=image_name
                )
                sql_output = con.execute(stmt)
                # Since this table does not have a trigger we can fetch the inserted PK key:
                inserted_id = int(sql_output.inserted_primary_key[0])
                result = (inserted_id, status_code.HTTP_200_OK)
                self.logger.info(log_prefix + "Successful insert of ID: " + str(inserted_id))
            except Exception as e:
                image_name = ''
                result = (0, status_code.HTTP_400_BAD_REQUEST)
                self.logger.error(log_prefix + "Could not insert data. Exception:" + str(e))

        return result, image_name

    def insert_camera_image_predictions(self, entity, image_name, model_name, extra_prefix=''):
        """ Insert Camera Image to DB """
        table_name = 'image_predictions'
        log_prefix = self.log_prefix + extra_prefix + "Table: " + table_name + "  - "
        with self.engine.connect() as con:
            try:
                self.logger.info(log_prefix + "Inserting data...")
                image_predictions = self.get_schema_metadata(table_name)
                stmt = db.insert(image_predictions).values(
                    image_name=image_name,
                    model_name=model_name,
                    pred_raw_output=entity.pred_raw_output,
                    device_source_id=entity.device_source_id
                )
                sql_output = con.execute(stmt)
                # Since this table does not have a trigger we can fetch the inserted PK key:
                inserted_id = int(sql_output.inserted_primary_key[0])
                result = (inserted_id, status_code.HTTP_200_OK)
                self.logger.info(log_prefix + "Successful insert of ID: " + str(inserted_id))
            except Exception as e:
                result = (0, status_code.HTTP_400_BAD_REQUEST)
                self.logger.error(log_prefix + "Could not insert data. Exception:" + str(e))

            return result

    def insert_vehicle_attributes(self, entity, image_name, unknown_member_name=config.Config.UNKNOWN_MEMBER, is_plate_required=False, extra_prefix=''):
        """ Insert Camera Image to DB """
        table_name = 'camera_event_vehicle_attributes'
        log_prefix = self.log_prefix + extra_prefix + "Table: " + table_name + "  - "
        if unknown_member_name is None:
            self.logger.warning(log_prefix + 'Unknown Member not provided. Using Default "Unknown".')
            unknown_member_name = config.Config.UNKNOWN_MEMBER
        # Check if the license plate is required before an insert
        if not is_plate_required:
            is_continue = True
        else:
            # Check if the license plate is present
            if (entity.vehicle_license_plate == unknown_member_name) or (entity.vehicle_license_plate is None):
                is_continue = False
            else:
                is_continue = True

        if is_continue:
            with self.engine.connect() as con:
                try:
                    self.logger.info(log_prefix + "Inserting data...")
                    # Insert values for single camera event.
                    camera_event_vehicle_attributes = self.get_schema_metadata(table_name)
                    stmt = db.insert(camera_event_vehicle_attributes).values(
                        device_source_id=entity.device_source_id,
                        event_identifier=entity.event_identifier,
                        image_name=image_name,
                        date_detection=timezone_conversion(logger=self.logger,
                                                           log_prefix=self.log_prefix,
                                                           dt_obj=entity.date_detection,
                                                           tz_origin=config.Config.TIME_ZONE_BACKEND_TZ,
                                                           tz_destination=config.Config.TIME_ZONE_BACKEND_TZ,
                                                           verbose=False),
                        vehicle_license_plate=unknown_member_name if entity.vehicle_license_plate is None else entity.vehicle_license_plate,
                        vehicle_license_plate_manual=entity.vehicle_license_plate_manual,
                        vehicle_license_plate_score=entity.vehicle_license_plate_score,
                        vehicle_license_plate_score_characters=entity.vehicle_license_plate_score_characters,
                        vehicle_direction_id=entity.vehicle_direction,
                        vehicle_type_id=entity.vehicle_type,
                        vehicle_type_score=entity.vehicle_type_score,
                        vehicle_color_cab_id=entity.vehicle_color_cab,
                        vehicle_color_body_id=entity.vehicle_color_body,
                        vehicle_orientation_id=entity.vehicle_orientation,
                        vehicle_make_id=entity.vehicle_make,
                        vehicle_license_plate_coordinates=entity.vehicle_license_plate_coordinates,
                        vehicle_orientation_score=entity.vehicle_orientation_score,
                        vehicle_coordinates=entity.vehicle_coordinates,
                        vehicle_number=entity.vehicle_number
                    )
                    sql_output = con.execute(stmt)
                    # Since this table does not have a trigger we can fetch the inserted PK key:
                    inserted_id = int(sql_output.inserted_primary_key[0])
                    result = (inserted_id, status_code.HTTP_200_OK)
                    self.logger.info(log_prefix + "Successful insert of ID: " + str(inserted_id))
                except Exception as e:
                    result = (0, status_code.HTTP_400_BAD_REQUEST)
                    self.logger.error(log_prefix + "Could not insert data. Exception:" + str(e))
        else:
            result = (0, status_code.HTTP_406_NOT_ACCEPTABLE)
            self.logger.warning(log_prefix + "License Plate not available. Skipping insert...")

        return result

    def insert_entrance_registrations(self, entity, reference_id=None, extra_prefix=''):
        """ Insert records into entrance_registrations table """
        table_name = 'entrance_registrations'
        log_prefix = extra_prefix + self.log_prefix + "Table: " + table_name + "  - "
        with self.engine.connect() as con:
            try:
                self.logger.info(log_prefix + "Inserting data...")
                entrance_registrations = self.get_schema_metadata(table_name)
                # inline=True is required for Triggers to work
                stmt = db.insert(entrance_registrations, inline=True).values(
                    gate_id=entity.gate_id,
                    driver_id=entity.driver_id,
                    vehicle_license_plate=entity.vehicle_license_plate,
                    vehicle_license_plate_related=entity.vehicle_license_plate_related,
                    client_id=entity.client_id,
                    date_entrance=timezone_conversion(logger=self.logger,
                                                       log_prefix=self.log_prefix,
                                                       dt_obj=entity.date_entrance,
                                                       tz_origin=config.Config.TIME_ZONE_BACKEND_TZ,
                                                       tz_destination=config.Config.TIME_ZONE_BACKEND_TZ,
                                                       verbose=False),
                    is_auto=entity.is_auto,
                    insert_by=entity.insert_by
                )
                sql_output = con.execute(stmt)
                # print(sql_output)
                # print(stmt)
                # print(stmt.compile().params)
                # We cannot retrieve (using returning or fetch for execute) data from trigger associated tables.
                # Hence, we create a temporary global table to store the ID. We need to use SET nocount ON to read from it.
                stmt = 'SET nocount ON; SELECT * FROM ##EntranceID;'
                sql_output = pd.read_sql(sql=stmt, con=self.engine)

                if len(sql_output) > 0:
                    inserted_id = int(sql_output['entrance_id'].iloc[0])
                    result = (inserted_id, status_code.HTTP_200_OK)
                    self.logger.info(log_prefix + "Successful insert of ID: " + str(inserted_id))
                    # If a reference_id was provided (from camera_event_vehicle_attributes) then link with vehicle attributes.
                    if reference_id is not None:
                        stmt = f'''
                                   UPDATE camera_event_vehicle_attributes
                                   SET entrance_id = {str(inserted_id)}
                                   WHERE reference_id = {str(reference_id)};
                               '''
                        update_result = self._execute_command(stmt)
                else:
                    result = (0, status_code.HTTP_204_NO_CONTENT)
                    self.logger.warning(log_prefix + "No record to insert.")
            except Exception as e:
                result = (0, status_code.HTTP_400_BAD_REQUEST)
                self.logger.error(log_prefix + "Could not insert data. Exception:" + str(e))

        return result

    def insert_exit_registrations(self, entity, reference_id=None, extra_prefix=''):
        """ Insert records into exit_registrations table """
        table_name = 'exit_registrations'
        log_prefix = extra_prefix + self.log_prefix + "Table: " + table_name + "  - "
        with self.engine.connect() as con:
            try:
                self.logger.info(log_prefix + "Inserting data...")
                exit_registrations = self.get_schema_metadata(table_name)
                # inline=True is required for Triggers to work
                stmt = db.insert(exit_registrations, inline=True).values(
                    entrance_id=entity.entrance_id,
                    gate_id=entity.gate_id,
                    driver_id=entity.driver_id,
                    vehicle_license_plate=entity.vehicle_license_plate,
                    vehicle_license_plate_related=entity.vehicle_license_plate_related,
                    client_id=entity.client_id,
                    date_entrance=timezone_conversion(logger=self.logger,
                                                      log_prefix=self.log_prefix,
                                                      dt_obj=entity.date_entrance,
                                                      tz_origin=config.Config.TIME_ZONE_BACKEND_TZ,
                                                      tz_destination=config.Config.TIME_ZONE_BACKEND_TZ,
                                                      verbose=False),
                    material_id=entity.material_id,
                    document_id=entity.document_id,
                    document_number=entity.document_number,
                    vehicle_load=entity.vehicle_load,
                    material_price=entity.material_price,
                    date_exit=timezone_conversion(logger=self.logger,
                                                      log_prefix=self.log_prefix,
                                                      dt_obj=entity.date_exit,
                                                      tz_origin=config.Config.TIME_ZONE_BACKEND_TZ,
                                                      tz_destination=config.Config.TIME_ZONE_BACKEND_TZ,
                                                      verbose=False),
                    payment_id=entity.payment_id,
                    is_tax=entity.is_tax,
                    site_id=entity.site_id,
                    insert_by=entity.insert_by
                )
                con.execute(stmt)
                # We cannot retrieve (using returning or fetch for execute) data from trigger associated tables.
                # Hence, we create a temporary global table to store the ID. We need to use SET nocount ON to read from it.
                stmt = 'SET nocount ON; SELECT * FROM ##ExitID;'
                sql_output = pd.read_sql(sql=stmt, con=self.engine)
                if len(sql_output) > 0:
                    inserted_id = int(sql_output['exit_id'].iloc[0])
                    result = (inserted_id, status_code.HTTP_200_OK)
                    self.logger.info(log_prefix + "Successful insert of ID: " + str(inserted_id))
                    # If a reference_id was provided (from camera_event_vehicle_attributes) then link with vehicle attributes.
                    if reference_id is not None:
                        stmt = f'''
                                   UPDATE camera_event_vehicle_attributes
                                   SET exit_id = {str(inserted_id)}
                                   WHERE reference_id = {str(reference_id)};
                               '''
                        update_result = self._execute_command(stmt)
                else:
                    result = (0, status_code.HTTP_204_NO_CONTENT)
                    self.logger.warning(log_prefix + "No record to insert.")
            except Exception as e:
                result = (0, status_code.HTTP_400_BAD_REQUEST)
                self.logger.error(log_prefix + "Could not insert data. Exception:" + str(e))

        return result

    def record_deletion(self, entity):
        """ Insert Camera Image to DB """
        table_name = 'deleted_records'
        log_prefix = self.log_prefix + "Table: " + table_name + "  - "
        with self.engine.connect() as con:
            try:
                self.logger.info(log_prefix + "Recording details of row to delete...")
                deleted_records = self.get_schema_metadata(table_name)
                stmt = db.insert(deleted_records).values(
                                            schema_name=entity.schema_name,
                                            table_name=entity.table_name,
                                            id_column=entity.id_column,
                                            id_value=entity.id_value,
                                            record_json=entity.record_json,
                                            category=entity.category,
                                            reason=entity.reason,
                                            username=entity.username
                )
                con.execute(stmt)
                self.logger.info(log_prefix + "Delete record from table...")
                stmt = f'''
                            DELETE FROM {entity.schema_name}.{entity.table_name}
                            WHERE {entity.id_column} = {entity.id_value};
                        '''
                self._execute_command(stmt)
                # Remove related camera detection
                if entity.table_name == 'entrance_registrations':
                    self.logger.info(log_prefix + "Updating camera event detection...")
                    stmt = f'''
                                UPDATE camera_event_vehicle_attributes
                                SET entrance_id = NULL, exit_id = NULL
                                WHERE entrance_id = {entity.id_value};
                            '''
                    self._execute_command(stmt)
                self.logger.info(log_prefix + "Successful insert and delete.")
                id_value = entity.id_value
            except Exception as e:
                id_value = 0
                sql_statement = f'''
                                DELETE FROM dbo.deleted_records 
                                WHERE table_name='{entity.table_name}' AND 
                                      id_column='{entity.id_column}' AND 
                                      id_value={entity.id_value};
                                '''
                self._execute_command(sql_statement)
                self.logger.error(log_prefix + "Could not insert and delete. Exception:" + str(e))
        return id_value

    def get_client_db_parameters(self):
        ''' Get the App parameters from the clients DB '''
        camera_db_parameters = pd.read_sql_table(table_name='app_parameters',
                                                 schema='dbo',
                                                 con=self.engine)
        camera_db_parameters = camera_db_parameters[['param_name', 'param_value']]
        camera_param_dict = dict(zip(camera_db_parameters.param_name, camera_db_parameters.param_value))
        for k, v in camera_param_dict.items():
            try:
                camera_param_dict[k] = ast.literal_eval(str(v))
            except Exception:
                camera_param_dict[k] = str(v)
        return camera_param_dict

    def delete_camera_events(self, device_source_id, event_identifier="all"):
        """ delete camera detection records """
        with self.engine.connect() as con:
            if event_identifier == "all":
                stmt = f'''
                            DELETE FROM camera_event_detection 
                            WHERE device_source_id = '{device_source_id}';
                        '''
                con.execute(text(stmt))

                stmt = f'''
                            DELETE FROM camera_event_images 
                            WHERE device_source_id = '{device_source_id}';
                        '''
                con.execute(text(stmt))

                stmt = f'''
                            DELETE FROM camera_event_vehicle_attributes
                            WHERE device_source_id = '{device_source_id}';
                        '''
                con.execute(text(stmt))

                stmt = f'''
                            DELETE FROM image_predictions
                            WHERE image_name IN (SELECT DISTINCT(image_name)
                                                FROM camera_event_images
                                                WHERE device_source_id = '{device_source_id}');
                        '''
                con.execute(text(stmt))

            else:
                stmt = f'''
                            DELETE FROM camera_event_detection 
                            WHERE event_identifier = '{event_identifier}';
                        '''
                con.execute(text(stmt))

                stmt = f'''
                            DELETE FROM camera_event_images 
                            WHERE event_identifier = '{event_identifier}';
                        '''
                con.execute(text(stmt))

                stmt = f'''
                            DELETE FROM camera_event_vehicle_attributes
                            WHERE event_identifier = '{event_identifier}';
                        '''
                con.execute(text(stmt))

                stmt = f'''
                            DELETE FROM image_predictions
                            WHERE image_name IN (SELECT DISTINCT(image_name)
                                                FROM camera_event_images
                                                WHERE event_identifier = '{event_identifier}');
                        '''
                con.execute(text(stmt))

    def get_devices(self):
        """ Get IoT devices used by App client (e.g. cameras, sensors, etc.) """
        device_source_id_lst = []
        device_serial_number_lst = []
        device_mac_address_lst = []
        device_type_lst = []
        device_subtype_lst = []
        device_brand_lst = []
        with self.engine.connect() as con:
            stmt = "SELECT device_source_id, device_serial_number, device_mac_address, device_type, device_subtype, " \
                    "device_brand FROM devices"
            records = con.execute(text(stmt))
            for record in records:
                device_source_id_lst.append(record[0])
                device_serial_number_lst.append(record[1])
                device_mac_address_lst.append(record[2])
                device_type_lst.append(record[3])
                device_subtype_lst.append(record[4])
                device_brand_lst.append(record[5])

        return list(map(lambda a, b, c, d, e, f: (a, b, c, d, e, f), device_source_id_lst, device_serial_number_lst,
                        device_mac_address_lst, device_type_lst, device_subtype_lst, device_brand_lst))

    def get_gate_id(self, device_source_id):
        """ Get gate_id from device_source_id """
        query = f'''    
                      SELECT gate_id
                      FROM devices
                      WHERE device_source_id = '{str(device_source_id)}';
                        '''
        with self.engine.connect() as con:
            gate_id = pd.read_sql(sql=text(query), con=con)
        if len(gate_id) > 0:
            gate_id = gate_id['gate_id'].iloc[0]
        else:
            gate_id = None
        return gate_id

    def get_curly(self, table, schema, add_conditions=None, order_by='curly'):
        """ Get list in curly format """
        # Get curly column name
        # remove 's' from table name (last digit).
        table_temp = table.rstrip(table[-1])
        curly_column = table_temp + '_curly'

        # Get ID column name
        # e.g. table = 'materials' or "entrance_registrations" ---> IDs are material_id and entrance_id
        # Get everything before 1st underscore
        table_temp = table_temp.split('_')[0]
        # We would get "material" and "entrance"
        # Check if its plural and remove s when applicable.
        id_column = table_temp + '_id'

        # Prepare query
        query = f'SELECT DISTINCT {id_column}, {curly_column} FROM {schema}.{table} '
        if add_conditions is not None:
            query = query + ' ' + add_conditions
        if order_by is not None:
            if order_by == 'id':
                order_by_col = id_column
            else:
                order_by_col = curly_column
            query = query + f' ORDER BY {order_by_col}'
        query = query + ';'
        try:
            with self.engine.connect() as con:
                output = pd.read_sql(sql=text(query), con=con)[curly_column].tolist()
            return output
        except Exception as e:
            self.logger.warning(str(e))
            return [None]

    def is_camera_authorized(self, device_source_id, device_mac_address):
        """ Checks if the camera is registered in the DB. If it is, return also de device_subtype"""
        result = False, 'Not Applicable'
        devices = self.get_devices()
        for device in devices:
            if device_source_id in device:
                if device_mac_address in device:
                    result = True, device[4]
        return result

    def read_table(self,
                   table_name,
                   schema='dbo',
                   add_conditions=None,
                   columns="*",
                   add_top=None,
                   add_order_by=None,
                   to_tz=None,
                   verbose=False
                   ):
        """ Read table named table_name.
            Reads DB as UTC by default but converts to Backend TZ """
        query = f'SELECT {columns} FROM {schema}.{table_name} '
        if add_top is not None:
            query = re.sub(r'(SELECT)', r'\1 TOP(' + str(add_top) + ')', query)
        if add_conditions is not None:
            query = query + ' ' + add_conditions
        if add_order_by is not None:
            query = query + ' ' + add_order_by
        query = query + ';'
        with self.engine.connect() as con:
            df = pd.read_sql(sql=text(query), con=con)
        # Transform all date columns to backend TZ.
        meta = self.get_schema_metadata(table_name=table_name)
        if to_tz is None:
            # Does not work if passed as a function argument (initial load is None, only when loaded via models).
            to_tz = config.Config.TIME_ZONE_BACKEND_TZ
        df = df_col_tz_conversion(logger=self.logger,
                                  log_prefix=self.log_prefix,
                                  df=df,
                                  meta=meta,
                                  to_tz=to_tz,
                                  verbose=verbose)

        return df

    def read_query(self, query):
        """Read table named table_name"""
        with self.engine.connect() as con:
            df = pd.read_sql(sql=text(query), con=con)
        return df

    def write_table_from_dataframe(self, df, table_name, schema='dbo', if_exists='append', extra_prefix='', method='multi'):
        """Write dataframe df to table table_name"""
        log_prefix = extra_prefix + self.log_prefix
        try:
            self.logger.info(log_prefix + f'Writing {table_name} table...')
            if len(df) == 0:
                self.logger.warning(log_prefix + f'Empty Dataframe.')
            # convert datetime to expected format our driver wants
            for col in df.select_dtypes(include=['datetime']).columns:
                df[col] = df[col].dt.strftime(config.Config.TIME_FORMAT_NO_TZ)
            self._write_table(data=df, table=table_name, schema=schema, if_exists=if_exists, extra_prefix=extra_prefix,method=method)
            self.logger.info(log_prefix + f'Insert Complete.')
            return True
        except Exception as e:
            self.logger.warning(log_prefix + "Could not write dataframe. Exception:" + str(e))
            return False

    def _write_table(self, data, table, schema, if_exists='append', chunksize=100000, extra_prefix='', method='multi'):
        log_prefix = extra_prefix + self.log_prefix
        try:
            if if_exists == "truncate":
                self.logger.info(log_prefix + f'Truncating first...')
                self._truncate_table(table, schema)
            self.logger.info(log_prefix + f'Inserting Data to ' + str(table) + '...')
            data.to_sql(name=table,
                        con=self.engine,
                        schema=schema,
                        if_exists='append',
                        index=False,
                        index_label=None,
                        chunksize=chunksize,
                        method=method)
            return True
        except Exception as e:
            self.logger.warning(log_prefix + "Could not write dataframe. Exception:" + str(e))
            return False

    def _truncate_table(self, table, schema):
        try:
            query = f'TRUNCATE TABLE {schema}.{table};'
            self._execute_command(query)
        except Exception as e:
            self.logger.warning(self.log_prefix + "Could not TRUNCATE table " + table)
            try:
                self.logger.info(self.log_prefix + "Trying DELETE FROM " + table + " instead...")
                query = f'DELETE FROM {schema}.{table};'
                self._execute_command(query)
                self.logger.info(self.log_prefix + "DELETE was successful.")
            except Exception as e:
                self.logger.warning(
                    self.log_prefix + "Could not DELETE records from table " + table + ". Exception:" + str(e))

    def _clear_table(self, table, schema, add_conditions=None):
        query = f'DELETE FROM {schema}.{table} '
        if add_conditions is not None:
            query = query + ' ' + add_conditions
        query = query + ';'
        self._execute_command(query)

    def _execute_command(self, stmt):
        """
        The Connection object provides a Connection.begin() method which returns a Transaction object. Like the
        Connection itself, this object is usually used within a Python with: block so that its scope is managed:
        """
        with self.engine.begin() as conn:
            conn.execute(text(stmt))

    @staticmethod
    def parse_sql(filename, is_debug=False):
        """
        Read .sql file and batch separates the entire script as a list (by removing comments and batch delimiters)
        """
        data = open(filename, 'r').readlines()
        stmts = []
        delimiter = ';'
        stmt = ''

        is_between_comments = False
        is_between_begin_end = False
        for lineno, line in enumerate(data):
            if is_debug:
                print(lineno)
                print(line)
            # Skips empty lines
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

            # Skips lines that start with:
            if line.strip().startswith('/**') and line.strip().endswith('**/'):
                if is_debug:
                    print("Skips lines that start with /** and end with **/")
                    print(line.strip().startswith('/**') and line.strip().endswith('**/'))
                is_between_comments = False
                continue

            # Skips lines that end with:
            if line.strip().endswith('**/'):
                if is_debug:
                    print("Skips lines that end with: **/")
                    print(line.strip().endswith('**/'))
                is_between_comments = False
                continue

            # ## Skips lines that start with:
            # if line.strip().startswith('**/'):
            #     if is_debug:
            #         print("Skips lines that start with: **/")
            #         print(line.strip().startswith('**/'))
            #     is_between_comments = False
            #     continue

            # Skips lines that start with:
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

            if delimiter not in line:
                stmt += line.replace(delimiter, ';')
                continue

            if stmt:
                stmt += line
                stmts.append(stmt.strip())
                stmt = ''
            else:
                stmts.append(line.strip())
        return stmts

    @staticmethod
    def execute_sql_batch(logger, log_prefix, raw_connection, query_parsed, debug=False):
        # Commit Transactions by batch
        with raw_connection.cursor() as cursor:
            for stmt in query_parsed:
                if stmt != "":
                    try:
                        cursor.execute(stmt)
                        raw_connection.commit()
                    except Exception as e:
                        if debug:
                            logger.error(log_prefix + "SQL Statement: \n " + str(stmt))
                        logger.error(log_prefix + ". Error: %s" % e)

    @staticmethod
    def create_azure_storage_credential(logger, log_prefix, conn, database_scoped_credential_name, master_key,sas_token):

        logger.info(log_prefix + "Dropping all external data sources...")

        with conn.engine.connect() as con:
            query = 'SELECT name FROM sys.external_data_sources;'
            external_datasource_list = list(pd.read_sql(sql=text(query), con=con)['name'])

        if len(external_datasource_list) > 0:
            for ds in external_datasource_list:
                try:
                    stmt = f'''
                            DROP EXTERNAL DATA SOURCE {ds};
                            '''
                    conn._execute_command(stmt)
                except Exception as e:
                    pass

        logger.info(log_prefix + "Dropping master key and scoped credential...")
        try:
            stmt_list = [f'''DROP DATABASE SCOPED CREDENTIAL {database_scoped_credential_name};''',
                         f'''DROP MASTER KEY;''']
            conn.execute_sql_batch(logger=logger,
                                   log_prefix=log_prefix,
                                   raw_connection=conn.engine.raw_connection(),
                                   query_parsed=stmt_list,
                                   debug=False)
        except Exception as e:
            pass

        logger.info(log_prefix + "Creating DB Master key and DB Scoped Credential for Blob Storage access... ")
        try:
            stmt_list = [f'''CREATE MASTER KEY ENCRYPTION BY PASSWORD = '{master_key}';''',
                         f'''CREATE DATABASE SCOPED CREDENTIAL {database_scoped_credential_name}
                             WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
                             SECRET = '{sas_token}';''']
            conn.execute_sql_batch(logger=logger,
                                   log_prefix=log_prefix,
                                   raw_connection=conn.engine.raw_connection(),
                                   query_parsed=stmt_list,
                                   debug=False)
        except Exception as e:
            logger.warning(log_prefix + str(e))
            pass
    @staticmethod
    def create_external_data_source(logger, log_prefix, conn, endpoint, container, database_scoped_credential_name,
                                    external_data_source_name):
        try:
            stmt = f'''
                    CREATE EXTERNAL DATA SOURCE {external_data_source_name}
                        WITH (
                            TYPE = BLOB_STORAGE,
                            LOCATION = '{endpoint}{container}',
                            CREDENTIAL = {database_scoped_credential_name}
                        );
                    '''
            conn._execute_command(stmt)
        except Exception as e:
            logger.warning(log_prefix + str(e))
            pass

    @staticmethod
    def copy_from_file(logger, log_prefix, conn, schema_name, table_name, df=None, path_to_csv=None, is_local=True,
                       external_data_source_name=None):
        """
        Here we are going save the dataframe to disk as a csv file, load the csv file and use BULK operation to copy
        to a DB table.
        Always assume the .csv files are stored as "<schema_name>.<table_name>.csv"
        """
        file_name = schema_name + "." + table_name + ".csv"
        try:
            if path_to_csv is None:
                logger.info(log_prefix + "A path to the .csv file is needed.")
                return False
            else:
                if is_local:
                    # Check whether the specified path exists or not
                    if not os.path.exists(path_to_csv):
                        logger.warning(log_prefix + "Path to save .csv does not exists. Creating new directory...")
                        os.makedirs(path_to_csv)
                    csv_full_path = os.path.join(path_to_csv, file_name)
                    # If a DF was provided:
                    if df is not None:
                        # Save the dataframe to disk
                        if not path.exists(csv_full_path):
                            logger.info(log_prefix + "CSV file DOES NOT exists. Writing dataframe to CSV: " + str(csv_full_path))
                            df.to_csv(csv_full_path, index=False, sep=";", header=False)
                        else:
                            logger.info(log_prefix + "CSV file exists. Skipping df.to_csv(). Using file " + str(csv_full_path))
                    else:
                        # A DF was not provided. We assume the file exists as a .csv
                        if not path.exists(csv_full_path):
                            logger.error(log_prefix + "Dataframe not provided and .csv file does not exist.")
                            return False

                    stmt = f'''
                            BULK INSERT {schema_name}.{table_name} FROM '{csv_full_path}' 
                            WITH (FORMAT = 'CSV', FIELDTERMINATOR = ';', ROWTERMINATOR = '0x0a');
                            '''
                else:
                    stmt = f'''
                            BULK INSERT {schema_name}.{table_name} FROM '{path_to_csv}/{file_name}' 
                            WITH (DATA_SOURCE = '{external_data_source_name}', FIRSTROW = 2, FIELDTERMINATOR = ';', ROWTERMINATOR = '\n')
                            '''

                logger.info(log_prefix + "Executing BULK statement...")
                with conn.engine.begin() as conn:
                    conn.execute(text(stmt))
                    output = True
                logger.info(log_prefix + "BULK load complete.")

        except Exception as e:
            logger.error(log_prefix + ". Error: %s" % e)
            output = False

            # cursor = raw_connection.cursor()
            # cursor.fast_executemany = True
            # try:
            #     cursor.execute(stmt)
            #     raw_connection.commit()
            #     output = True
            # except Exception as e:
            #     logger.error("Error: %s" % e)
            #     raw_connection.rollback()
            #     cursor.close()
            #     output = False
            # cursor.close()

        return output

    def close(self, extra_prefix=''):
        log_prefix = extra_prefix + self.log_prefix
        """ Close all the active connections to the database. Does not
        disconnect the engine.
        """
        self.logger.info(log_prefix + 'Closing connection...')
        self.engine.dispose()