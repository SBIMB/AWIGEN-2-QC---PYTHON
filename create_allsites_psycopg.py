import psycopg2
import pandas as pd
#from sqlalchemy import create_engine
import CreateStatementallsites_2
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
