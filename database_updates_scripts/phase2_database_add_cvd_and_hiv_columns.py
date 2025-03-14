import pandas as pd
import json
import psycopg2
import sys

'''
md hash key : a9f3eaab19db91b53c4ce2b4ee359419
'''

from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))

sys.path.append(d)

from postgres_db_config import config
import numpy as np

params_ = config()
# Create a cursor object to interact with the database
conn = psycopg2.connect( **params_)
cur = conn.cursor()

# Execute an SQL query: Theo to provide VIEW name
query = "SELECT * FROM all_sites_phase2 "
cur.execute(query)

# Fetch the query results (you can use fetchone(), fetchall(), etc.)
phase2_data  = cur.fetchall()
colnames = [desc[0] for desc in cur.description]

phase2_data =  pd.DataFrame(phase2_data, columns= colnames)

hiv_phase2 = pd.read_csv('./resources/AwigenPhase2_Final_HIV_12June2024.csv', sep = ',')

hiv_phase2.rename(columns={'p2_hiv_final_status_calculated':'hiv_final_status_c'}, inplace=True)

#add the hiv status to phase2_data
phase2_data = phase2_data.merge(hiv_phase2, on = 'study_id', how = 'left')

def cvd(df):

    if ((df['stroke']==1) or (df['transient_ischemic_attack']==1) or (df['heartattack'] == 1) or (df['congestive_heart_failure'] == 1)\
                        or (df['angina'] == 1)):
        return 1
    elif (((df['stroke']==0) or (df['stroke']==-999) or (df['stroke']==-8) or (df['stroke']==2)) and
           ((df['transient_ischemic_attack']==0) or (df['transient_ischemic_attack']==-999) or(df['transient_ischemic_attack']==-8) or (df['transient_ischemic_attack']==2) ) and 
           ((df['heartattack']==0) or (df['heartattack']==-999) or (df['heartattack']==-8) or (df['heartattack']==2)) and 
           ((df['congestive_heart_failure']==0) or (df['congestive_heart_failure']==-999) or (df['congestive_heart_failure']==-8) or (df['congestive_heart_failure']==2)) and 
           ((df['angina']==0) or (df['angina']==999) or (df['angina']==-8) or (df['angina']==2))):
        return 0

phase2_data['cadiovascular_current'] = phase2_data.apply(cvd, axis=1)

phase2_data['hiv_final_status_c'] = phase2_data['hiv_final_status_c_y']

phase2_data = phase2_data.drop(columns=['hiv_final_status_c_y'], axis=1)

# columns to add 
data = phase2_data[['study_id', 'cadiovascular_current', 'hiv_final_status_c']]

data[['cadiovascular_current', 'hiv_final_status_c']] = data[['cadiovascular_current', 'hiv_final_status_c']].replace(np.nan,-999)

#checking validity of cardiovascular formular
#print((data[['stroke', 'transient_ischemic_attack',  'heartattack', 'congestive_heart_failure', 'angina', 'cadiovascular_current']].groupby(['stroke', 'transient_ischemic_attack',  'heartattack', 'congestive_heart_failure', 'angina'])).first())

#alter table and add the new columns
#conn = None
#cur = None
#
#try:

    # Create a cursor object to interact with the database
#    conn = psycopg2.connect( **params_)
#    cur = conn.cursor()

    # Execute an SQL query to alter the table and add a new column

#    cur.execute("ALTER TABLE all_sites_phase2 ADD COLUMN cadiovascular_current integer;")
#    cur.execute("ALTER TABLE all_sites_phase2 ADD COLUMN hiv_final_status_c integer;")
#    print("Columns added successfully.")

    # Commit the transaction to make the changes permanent
#    conn.commit()

#except psycopg2.Error as e:
#    print("Error:", e)
#    conn.rollback()  # Rollback changes if an error occurs

#cur.close()
#conn.close()


# add data to the new column

#insert the data to the columns added
conn = None
cur = None

try:

    # Create a connection and a cursor object to interact with the database
    conn = psycopg2.connect(**params_)
    cur = conn.cursor()

    table_name = 'all_sites_phase2'

    #Iterate through rows in the DataFrame and update corresponding rows in the database
    for index, row in data.iterrows():
        #'condition_column' i.e study_id is the column used to identify the row to be updated
        condition_column_value = row['study_id']
    
        # cadiovascular_current and hiv_final_status_c are columns to be updated
        new_cadiovascular_current_value = row['cadiovascular_current']
        new_hiv_final_status_c_value = row['hiv_final_status_c']

        # Define the UPDATE query
        update_query = """
            UPDATE {} 
            SET cadiovascular_current = %s, hiv_final_status_c = %s 
            WHERE study_id = %s """.format(table_name)


        cur.execute(update_query, (new_cadiovascular_current_value, new_hiv_final_status_c_value, condition_column_value))
        print("Data added to the new column successfully.")

     # Commit the transaction to make the changes permanent
    conn.commit()

except psycopg2.Error as e:
    print("Error:", e)
    conn.rollback()  # Rollback changes if an error occurs

    cur.close()
    conn.close()