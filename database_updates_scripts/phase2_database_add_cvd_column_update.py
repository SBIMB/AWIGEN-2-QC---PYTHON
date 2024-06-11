import pandas as pd
import psycopg2
import sys

from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))

sys.path.append(d)

from postgres_db_config import config
import numpy as np

def cvd(df):

    if ((df['stroke_ever']==1) or (df['trans_isc']==1) or (df['hrt_atck_ever'] == 1) or (df['hrt_fail'] == 1)\
                        or (df['angina_ever'] == 1)):
        return 1
    elif (((df['stroke_ever']==0) or (df['stroke_ever']==-999)) and
           ((df['trans_isc']==0) or (df['trans_isc']==-999)) and 
           ((df['hrt_atck_ever']==0) or (df['hrt_atck_ever']==-999)) and 
           ((df['hrt_fail']==0) or (df['hrt_fail']==-999)) and 
           ((df['angina_ever']==0) or (df['angina_ever']==999))):
        return 0

#data['cvd_status'] = data.apply(cvd, axis=1)


# create the database    
params_ = config()



#insert the data
#conn = None
#cur = None
#
#try:

    # Create a cursor object to interact with the database
#    conn = psycopg2.connect( **params_)
#    cur = conn.cursor()

    # Execute an SQL query to alter the table and add a new column

#    cur.execute("ALTER TABLE all_sites_phase2 ADD COLUMN cadiovascular_current integer;")
#    print("Column added successfully.")
#except psycopg2.Error as e:
#    print("Error:", e)
#    conn.rollback()  # Rollback changes if an error occurs

# Commit the transaction to make the changes permanent
#conn.commit()

#cur.close()
#conn.close()


# add data to the new column

#insert the data
#conn = None
#cur = None

#try:
#    cur.execute("UPDATE all_sites_phase2 SET cadiovascular_current = %s WHERE study_id = %s")
#    print("Data added to the new column successfully.")
#except psycopg2.Error as e:
#    print("Error:", e)
#    conn.rollback()  # Rollback changes if an error occurs
