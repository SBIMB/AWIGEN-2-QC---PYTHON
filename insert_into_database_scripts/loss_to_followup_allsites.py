import sys
import pandas as pd
import numpy as np

from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))

sys.path.append(d)

import psycopg2
from create_statements import CreateStatementlosstofollowup
from postgres_db_config import config

file_path= '../loss_to_follow_up/LTFU_csv/'

total_ltfu = pd.read_csv(file_path+'loss_to_follow_up_total.csv', sep = ';')

print(total_ltfu.shape)

# create the database    
params_ = config()

conn = None
cur = None

try: 
    conn = psycopg2.connect( **params_)

    cur = conn.cursor()

    create_script = CreateStatementlosstofollowup.CreateStatementltfu() 

    cur.execute(create_script)

    conn.commit()

except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()

#insert the data
conn = None
cur = None

try: 
    conn = psycopg2.connect( **params_)

    cur = conn.cursor()
            
    # Convert the data frame to a list of tuples
    data = [tuple(row) for row in total_ltfu.values]


    # Generate the parameterized query string
    columns = ', '.join(total_ltfu.columns)
    placeholders = ', '.join(['%s' for _ in total_ltfu.columns])

    query = f"INSERT INTO loss_to_follow_up ({columns}) VALUES ({placeholders})"
                  
    # Write data frame to the SQL table using parameterized queries and executemany
    #with engine.begin() as connection:
    cur.executemany(query, data)
    conn.commit()

except Exception as error:
    print(error)
    
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()