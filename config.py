import os
from dotenv import load_dotenv
import logging
from logging import Formatter
from pytz import timezone
import urllib

MAIN_DIR = os.path.dirname(os.path.abspath(__file__))

# Get environment variables
load_dotenv(dotenv_path=os.path.join(MAIN_DIR, '.env'))

class Config:
    """Base config."""

    # Flask
    FLASK_HOST = '0.0.0.0'
    FLASK_PORT = '8000'

    # localized datetime
    TIME_ZONE_PYTZ = timezone("America/New_York")
    TIME_ZONE_WINDOWS = "Eastern Standard Time"

    CURRENCIES = ['usd', 'eur', 'cad', 'aud']
    FOLDER_DATA = "data"
    FOLDER_TRADE = "chile_trade"
    FOLDER_COLUMNS = "columns"
    FOLDER_DIMENSIONS = "dimensions"
    FOLDER_CURRENCY = "currency"
    FOLDER_IMPORTS_EXPORTS = "imports_exports"

    AZURE_SQL_SERVER = os.getenv('AZURE_SQL_SERVER')
    AZURE_SQL_DB_NAME = os.getenv('AZURE_SQL_DB_NAME')
    AZURE_SQL_DB_USER = os.getenv('AZURE_SQL_DB_USER')
    AZURE_SQL_DB_PWD = os.getenv('AZURE_SQL_DB_PWD')
    AZURE_SQL_DRIVER = os.getenv('AZURE_SQL_DRIVER')

    # Azure Storage
    AZURE_STORAGE_CONNECT_STR = os.getenv('AZURE_STORAGE_CONNECT_STR')

    SQLALCHEMY_DATABASE_URI = 'mssql+pyodbc:///?odbc_connect={}'.format(
                urllib.parse.quote_plus(r'Driver=' + AZURE_SQL_DRIVER + ';'
                r'Server=' + AZURE_SQL_SERVER + ';'
                r'Database=' + AZURE_SQL_DB_NAME + ';'
                r'Uid=' + AZURE_SQL_DB_USER + ';'
                r'Pwd=' + AZURE_SQL_DB_PWD + ';'
                r'Encrypt=yes;'
                r'TrustServerCertificate=yes;'
                r'Connection Timeout=30;')
    )



class ProdConfig(Config):
    FLASK_ENV = 'production'
    ENV = FLASK_ENV
    DEBUG = False
    TESTING = False


class DevConfig(Config):
    FLASK_ENV = 'development'
    ENV = FLASK_ENV
    DEBUG = True
    TESTING = True
    CLIENT_URL = "http://localhost:5000"


class TolveetLogger:
    """Logger to use in Tolveet Platform"""
    init = False
    def __init__(self, re_init=False):
        self.re_init = re_init

    def get_tolveet_logger(self):
        """Get a logger and initialize if necessary"""
        logger = logging.getLogger("main")
        logger.propagate = False
        if (not TolveetLogger.init) or (not logger.handlers):
            handler = logging.StreamHandler()
            # create formatter and add to handler
            formatter = Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            if not logger.handlers:
                logger.addHandler(handler)
            # set the logging level
            logger.setLevel(logging.INFO)
            TolveetLogger.init = True

        return logger
