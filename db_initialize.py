import psycopg2
import db_utils
from collections import defaultdict

# Database Credentials by machine
db_cred = defaultdict(dict)

db_cred['default_postgres'] = {"drivername": "postgresql",
                         "dbserverName": "localhost",
                         "port": "5432",
                         "dbusername": "postgres",
                         "dbpassword": "abc123",
                         "dbname": "postgres"}

db_cred['local'] = {"drivername": "postgresql",
                         "dbserverName": "localhost",
                         "port": "5432",
                         "dbusername": "postgres",
                         "dbpassword": "abc123",
                         "dbname": "agricola"}

try:
    con = db_utils.AppDBConn()
except:
    # Connect to the default postgres DB
    con_default = psycopg2.connect(dbname=db_cred['default_postgres']['dbname'],
                                   user=db_cred['default_postgres']['dbusername'],
                                   host=db_cred['default_postgres']['dbserverName'],
                                   password=db_cred['default_postgres']['dbpassword'])
    # Create DB
    print("#businesslog Creating Enframe DB")
    db_utils.db_create(db_conn=con_default
                                       , app_id="123"
                                       , db_name=db_cred['local']['dbname'])

    # Disconnect from default postgres DB
    con_default.close()
    # Connect to newly created DB
    con = psycopg2.connect(dbname=db_cred['local']['dbname'],
                                   user=db_cred['local']['dbusername'],
                                   host=db_cred['local']['dbserverName'],
                                   password=db_cred['local']['dbpassword'])

print("Successfully connected to database")
# ----------------------------------------------------------------------------------------------------------------------

con.close()
