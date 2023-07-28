import psycopg2
from postgres_db_config import config
#import pandas as pd
#from sqlalchemy import create_engine
import CreateStatementSoweto
import CreateStatementDIMAMO
import CreateStatementNairobi
import CreateStatementNanoro
import CreateStatementNavrongo
import CreateStatementAgincourt

params_ = config()

conn = None
cur = None

try: 
    conn = psycopg2.connect( **params_)

    cur = conn.cursor()

    sites = ['soweto', 'dimamo', 'nairobi', 'nanoro', 'navrongo', 'agincourt']
    for site in sites:

        if site == 'soweto':

            create_script = CreateStatementSoweto.CreateStatementSoweto() 
          

        if site == 'dimamo':

            create_script = CreateStatementDIMAMO.CreateStatementDIMAMO()
        

        if site == 'nairobi':

            create_script = CreateStatementNairobi.CreateStatementNairobi()


        if site == 'nanoro':

            create_script = CreateStatementNanoro.CreateStatementNanoro()
        

        if site == 'navrongo':

            create_script = CreateStatementNavrongo.CreateStatementNavrongo()
        
        if site == 'agincourt':

            create_script = CreateStatementAgincourt.CreateStatementAgincourt()

   
        cur.execute(create_script)
    
        conn.commit()

except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
