import pandas as pd
import psycopg2
import numpy as np
import sys
import psycopg2
import matplotlib.pyplot as plt

#insert md5 hash key
#md5_hask key = 

from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))
sys.path.append(d)
from postgres_db_config import config

#upload phase2 data from the database
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

#ultrsound data
agincourt_ultrasound_data = pd.read_csv('/home/theomathema/biomarkers/ultrasound_belinda/agincourt_ultrasound_results_16_01_25.csv')

agincourt_ultrasound_data = agincourt_ultrasound_data.drop(columns=['STUDY ID', '?', 'Unnamed: 20', 'match'], axis =0)

agincourt_ultrasound_data = agincourt_ultrasound_data[~agincourt_ultrasound_data['study_id'].isna()]

agincourt_ultrasound_data['VALID IMT'] = agincourt_ultrasound_data['VALID IMT'].replace({'no':0, 'yes':1, 'partial':2})
agincourt_ultrasound_data['VALID BIFURCATION'] = agincourt_ultrasound_data['VALID BIFURCATION'].replace({'no':0, 'yes':1, 'partial':2, 'limited':2, 'ltd rt': 2})

#print(agincourt_ultrasound_data.head())

print(agincourt_ultrasound_data[agincourt_ultrasound_data['study_id'].duplicated()])

agincourt_ultrasound_data = agincourt_ultrasound_data.drop_duplicates(subset='study_id', keep = False)

#merge phase 1 and phase 2 data
phase2_data = pd.merge(phase2_data, agincourt_ultrasound_data, on = 'study_id', how='inner')
phase2_data = phase2_data.sort_values(by='study_id')

print(phase2_data.head())

#update the ultrsound values 
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'date_ultrasound_taken'] = phase2_data['DATE']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'time_ultrasound_taken'] = phase2_data['TIME']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'ultrasound_num_images'] = phase2_data['IMAGES']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'birfucations_comment'] = phase2_data['COMMENT']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'right_plaque_thickness'] = phase2_data['RT']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'left_plaque_thickness'] = phase2_data['LT']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'imt_valid'] = phase2_data['VALID IMT']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'bifurcation_valid'] = phase2_data['VALID BIFURCATION']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'ultrasound_rt_points'] = phase2_data['RT POINTS ECG']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'min_cimt_right'] = phase2_data['RT C MIN']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'max_cimt_right'] = phase2_data['RT C MAX']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'mean_cimt_right'] = phase2_data['RT C MEAN']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'ultrasound_lt_points'] = phase2_data['LT POINTS ECG']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'min_cimt_left'] = phase2_data['LT C MIN']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'max_cimt_left'] = phase2_data['LT C MAX']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'mean_cimt_left'] = phase2_data['LT C MEAN']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'subcutaneous_fat'] = phase2_data['SCAT ']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'visceral_fat'] = phase2_data['VAT']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'visceral_comment'] = phase2_data['COMMENT.1']
phase2_data.loc[phase2_data['study_id'].isin(agincourt_ultrasound_data['study_id']), 'ultrasound_qc_results_complete'] = 2

phase2_data.drop(columns=['DATE', 'TIME', 'IMAGES', 'COMMENT', 'RT', 
            'LT', 'VALID IMT', 'VALID BIFURCATION', 'RT POINTS ECG',
            'RT C MIN', 'RT C MAX', 'RT C MEAN', 'LT POINTS ECG',
            'LT C MIN','LT C MAX', 'LT C MEAN', 'SCAT ',
            'VAT', 'COMMENT.1'], inplace=True)

print(phase2_data[['study_id', 'max_cimt_left', 'mean_cimt_left', 'subcutaneous_fat', 'visceral_fat',
                   'visceral_comment', 'ultrasound_qc_results_complete' ]].tail(10))

phase2_data['cimt_mean_max'] = (phase2_data['max_cimt_right']+phase2_data['max_cimt_left'])/2


#preparing the data for the database.

data = phase2_data.replace(np.nan,-999)

# create the database    
params_ = config()

#insert the data
conn = None
cur = None

try: 
    conn = psycopg2.connect( **params_)

    cur = conn.cursor()
    

    # Assuming you already have a Pandas DataFrame named 'your_dataframe' with updated data
    # Replace 'your_table_name' with the actual table name in your database
    table_name = 'all_sites_phase2'

        # Iterate through rows in the DataFrame and update corresponding rows in the database
    for index, row in data.iterrows():
       # Assuming 'condition_column' is the column used to identify the row to be updated
        condition_column_value = row['study_id']
    
        # Assuming 'column1' and 'column2' are columns to be updated
    
        new_date_ultrasound_taken_values = row['date_ultrasound_taken']
        new_time_ultrasound_taken_values = row['time_ultrasound_taken']
        new_ultrasound_num_images_values = row['ultrasound_num_images']
        new_birfucations_comment_taken_values = row['birfucations_comment']
        new_time_right_plaque_thickness_values = row['right_plaque_thickness']
        new_left_plaque_thickness_values = row['left_plaque_thickness']
        new_imt_valid_values = row['imt_valid']
        new_bifurcation_valid_values = row['bifurcation_valid']
        new_ultrasound_rt_points_values = row['ultrasound_rt_points']
        new_min_cimt_right_values = row['min_cimt_right']
        new_max_cimt_right_values = row['max_cimt_right']
        new_mean_cimt_right_values = row['mean_cimt_right']
        new_ultrasound_lt_points_values = row['ultrasound_lt_points']
        new_min_cimt_left_values = row['min_cimt_left']
        new_max_cimt_left_values = row['max_cimt_left']
        new_mean_cimt_left_values = row['mean_cimt_left']
        new_subcutaneous_fat_values = row['subcutaneous_fat']
        new_visceral_fat_values = row['visceral_fat']
        new_cimt_mean_max_values = row['cimt_mean_max']
        new_ultrasound_qc_results_complete_values = row['ultrasound_qc_results_complete']
        

#        # Define the UPDATE query
        update_query = """
            UPDATE {} 
            SET date_ultrasound_taken = %s, time_ultrasound_taken = %s, ultrasound_num_images = %s,
            birfucations_comment = %s, right_plaque_thickness = %s, left_plaque_thickness = %s,
            imt_valid = %s, bifurcation_valid = %s, ultrasound_rt_points = %s,
            min_cimt_right = %s, max_cimt_right = %s, mean_cimt_right = %s,
            ultrasound_lt_points = %s, min_cimt_left = %s, max_cimt_left = %s,
            mean_cimt_left = %s, subcutaneous_fat = %s, visceral_fat = %s, cimt_mean_max = %s,
            ultrasound_qc_results_complete = %s
            WHERE study_id = %s
            """.format(table_name)

        # Execute the UPDATE query with the provided values
        cur.execute(update_query, (new_date_ultrasound_taken_values, new_time_ultrasound_taken_values, new_ultrasound_num_images_values,
                                   new_birfucations_comment_taken_values, new_time_right_plaque_thickness_values, new_left_plaque_thickness_values,
                                   new_imt_valid_values, new_bifurcation_valid_values, new_ultrasound_rt_points_values, new_min_cimt_right_values,
                                   new_max_cimt_right_values, new_mean_cimt_right_values, new_ultrasound_lt_points_values, new_min_cimt_left_values,
                                   new_max_cimt_left_values, new_mean_cimt_left_values, new_subcutaneous_fat_values, new_visceral_fat_values,
                                   new_cimt_mean_max_values, new_ultrasound_qc_results_complete_values, condition_column_value))

    conn.commit()

except Exception as error:
    print(error)
    
#finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()



