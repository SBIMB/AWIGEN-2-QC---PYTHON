import pandas as pd
import json
import psycopg2
import sys

'''
md hash key: a9f3eaab19db91b53c4ce2b4ee359419
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


#eaxtect data fpor soweto
soweto_data = phase2_data[phase2_data['site']==6]

#update the ultrasound qc data using the pre qc data
soweto_data['visceral_fat']= soweto_data['pre_qc_visceral_fat']
soweto_data['subcutaneous_fat'] = soweto_data['pre_qc_subcutaneous_fat']
soweto_data['min_cimt_right'] = soweto_data['pre_qc_min_cimt_right']
soweto_data['max_cimt_right'] = soweto_data['pre_qc_max_cimt_right']
soweto_data['mean_cimt_right'] = soweto_data['pre_qc_mean_cimt_right']
soweto_data['min_cimt_left'] = soweto_data['pre_qc_min_cimt_left']
soweto_data['max_cimt_left'] = soweto_data['pre_qc_max_cimt_left']
soweto_data['mean_cimt_left'] = soweto_data['pre_qc_mean_cimt_left']

soweto_data['ultrasound_qc_results_complete'] = np.where((soweto_data['cimt_measured']==1 ), 2, soweto_data['ultrasound_qc_results_complete'])

soweto_data['cimt_mean_max'] = (soweto_data['max_cimt_right']+soweto_data['max_cimt_left'])/2


soweto_data= soweto_data[['study_id', 'min_cimt_right', 'max_cimt_right', 'mean_cimt_right', 'min_cimt_left', 'max_cimt_left', 'mean_cimt_left', 'visceral_fat', 'subcutaneous_fat',
         'cimt_mean_max', 'ultrasound_qc_results_complete']]


soweto_data = soweto_data.replace(-555, -999)

print(soweto_data)



#alter table and add the new columns
conn = None
cur = None
#
try:

    # Create a cursor object to interact with the database
    conn = psycopg2.connect( **params_)
    cur = conn.cursor()

    # Create a connection and a cursor object to interact with the database
    conn = psycopg2.connect(**params_)
    cur = conn.cursor()

    table_name = 'all_sites_phase2'

    #Iterate through rows in the DataFrame and update corresponding rows in the database
    for index, row in soweto_data.iterrows():
        #'condition_column' i.e study_id is the column used to identify the row to be updated
        condition_column_value = row['study_id']
    
        # cadiovascular_current and hiv_final_status_c are columns to be updated
        new_min_cimt_right_value = row['min_cimt_right']
        new_max_cimt_right_value = row['max_cimt_right']
        new_mean_cimt_right_value = row['mean_cimt_right']
        new_min_cimt_left_value = row['min_cimt_left']
        new_max_cimt_left_value = row['max_cimt_left']
        new_mean_cimt_left_value = row['mean_cimt_left']
        new_visceral_fat_value = row['visceral_fat']
        new_subcutaneous_fat_value = row['subcutaneous_fat']
        new_cimt_mean_max_value = row['cimt_mean_max']
        new_ultrasound_qc_results_complete_value = row['ultrasound_qc_results_complete']
        
        # Define the UPDATE query
        update_query = """
            UPDATE {} 
            SET min_cimt_right = %s, max_cimt_right = %s,  mean_cimt_right = %s, min_cimt_left = %s, max_cimt_left = %s, mean_cimt_left = %s, visceral_fat =%s, 
            subcutaneous_fat = %s, cimt_mean_max = %s, ultrasound_qc_results_complete = %s
            WHERE study_id = %s """.format(table_name)


        cur.execute(update_query, (new_min_cimt_right_value, new_max_cimt_right_value, new_mean_cimt_right_value, 
                                   new_min_cimt_left_value, new_max_cimt_left_value, new_mean_cimt_left_value,
                                   new_visceral_fat_value, new_subcutaneous_fat_value, new_cimt_mean_max_value, 
                                   new_ultrasound_qc_results_complete_value, condition_column_value))
        print("Data added  successfully.")

     # Commit the transaction to make the changes permanent
    conn.commit()

except psycopg2.Error as e:
    print("Error:", e)
    conn.rollback()  # Rollback changes if an error occurs

    cur.close()
    conn.close()