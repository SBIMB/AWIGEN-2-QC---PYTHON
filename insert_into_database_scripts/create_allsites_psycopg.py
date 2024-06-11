import psycopg2
import pandas as pd
import sys

from os.path import dirname, abspath
# Setup the path to the project root
d = dirname(dirname(abspath(__file__)))
# Add the project root to sys.path
sys.path.append(d)

from insert_into_database_scripts.create_statements.CreateStatementallsites_2 import CreateStatementallsites_2

from postgres_db_config import config

params_ = config()

conn = None
cur = None

try: 
    conn = psycopg2.connect( **params_)

    cur = conn.cursor()

    create_script = CreateStatementallsites_2.CreateStatementAllPhase2()   
                  
    cur.execute(create_script)
    
    conn.commit()

except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
