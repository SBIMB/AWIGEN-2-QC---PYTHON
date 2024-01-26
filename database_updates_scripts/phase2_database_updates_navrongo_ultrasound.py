import pandas as pd
import psycopg2
from postgres_db_config import config
import numpy as np

phase2_data = pd.read_csv('./resources/combined_phase2data_encoded.csv',
                     delimiter=',', low_memory=False)

# dvc hash key 7d4c465f5a1c0aeb157e6f873378574b

biomarker_data = pd.read_csv('./resources/AWIGen2BiomarkerResu-AWIGen2Biomarkers_DATA_2024-01-26_1132.csv',
                     delimiter=';', low_memory=False)

# dropping participant information from the biomarkers data, already have the information on the main dataset
biomarker_data = biomarker_data.drop(columns=['unique_site_id',	'sex', 'site', 'participant_data_complete', 
                                              'glucose', 'glucose_test_date', 'plasma_results_complete',	
                                              'serum_test_date',	'serum_creatinine',	'insulin',
                                              'lipids_hdl', 'lipids_ldl_calculated', 'lipids_ldl_measured', 'lipids_cholesterol',
                                              'lipids_triglycerides', 'serum_results_complete', 'urine_batch', 'urine_box', 'urine_date_received',
                                              'urine_creatinine', 'urine_creatinine_test_date', 'urine_albumin',
                                              'urine_albumin_test_date', 'urine_acr', 'urine_protein', 'urine_protein_test_date', 
                                              'urine_results_complete'])



#rename biormarker column variables to match the database ones
biomarker_data.rename(columns={'awigen_id': 'study_id',  'ultr_qc_date': 'date_ultrasound_taken',
            'ultr_qc_time': 'time_ultrasound_taken', 'ultr_qc_num_images': 'ultrasound_num_images',
            'ultr_qc_comment': 'birfucations_comment', 'ultr_qc_right_plaque': 'right_plaque_thickness',
            'ultr_qc_left_plaque': 'left_plaque_thickness', 'ultr_qc_imt_valid': 'imt_valid',
            'ultr_qc_bifurcation_valid': 'bifurcation_valid', 'ultr_qc_rt_points': 'ultrasound_rt_points',
            'ultr_qc_rt_t_min': 'min_cimt_right', 'ultr_qc_rt_t_max': 'max_cimt_right',
            'ultr_qc_rt_t_mean': 'mean_cimt_right', 'ultr_qc_lt_points': 'ultrasound_lt_points',
            'ultr_qc_lt_t_min': 'min_cimt_left', 'ultr_qc_lt_t_max': 'max_cimt_left',
            'ultr_qc_lt_t_mean': 'mean_cimt_left', 'ultr_qc_scat': 'subcutaneous_fat',
            'ultr_qc_vat': 'visceral_fat', 'ultr_qc_visceral_comment': 'visceral_comment'}, inplace=True)

#remove the variables to be replaced.
phase2_data_biomarkers = phase2_data.drop(columns = ['date_ultrasound_taken',
                        'time_ultrasound_taken', 'ultrasound_num_images', 'birfucations_comment', 
                        'right_plaque_thickness', 'left_plaque_thickness', 'imt_valid',
                        'bifurcation_valid', 'ultrasound_rt_points', 'min_cimt_right', 'max_cimt_right',
                        'mean_cimt_right', 'ultrasound_lt_points', 'min_cimt_left', 'max_cimt_left', 
                        'mean_cimt_left', 'visceral_fat', 'subcutaneous_fat', 'visceral_comment',
                        'cimt_mean_max', 'ultrasound_qc_results_complete'], axis = 0)

#merge the biomarker data with the phenotype data
phase2_data_biomarkers = phase2_data_biomarkers.merge(biomarker_data[['study_id',                                        
                        'date_ultrasound_taken', 'time_ultrasound_taken', 'ultrasound_num_images',
                        'birfucations_comment', 'right_plaque_thickness', 'left_plaque_thickness', 
                        'imt_valid', 'bifurcation_valid', 'ultrasound_rt_points', 'min_cimt_right', 
                        'max_cimt_right', 'mean_cimt_right', 'ultrasound_lt_points', 'min_cimt_left', 
                        'max_cimt_left', 'mean_cimt_left', 'visceral_fat', 'subcutaneous_fat', 
                        'visceral_comment', 'ultrasound_qc_results_complete' ]], left_on='study_id', right_on='study_id', how='left')


phase2_data_biomarkers['cimt_mean_max'] = (phase2_data_biomarkers['max_cimt_right']+phase2_data_biomarkers['max_cimt_left'])/2

data = phase2_data_biomarkers.replace(np.nan, -999)

data = data[data['site']==5]

data = data[['study_id', 'date_ultrasound_taken', 'time_ultrasound_taken', 'ultrasound_num_images',
        'birfucations_comment',	'right_plaque_thickness', 'left_plaque_thickness',
        'imt_valid', 'bifurcation_valid', 'ultrasound_rt_points', 'min_cimt_right',
        'max_cimt_right', 'mean_cimt_right', 'ultrasound_lt_points', 'min_cimt_left',
        'max_cimt_left', 'mean_cimt_left', 'visceral_fat', 'subcutaneous_fat',
        'visceral_comment', 'cimt_mean_max', 'ultrasound_qc_results_complete'
 ]]

print(data.ultrasound_qc_results_complete.value_counts())


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
        new_date_ultrasound_taken_value = row['date_ultrasound_taken']
        new_time_ultrasound_taken_value = row['time_ultrasound_taken']
        new_ultrsound_num_images_value = row['ultrasound_num_images']
        new_birfucations_comment_value = row['birfucations_comment']
        new_right_plaque_thickness_value = row['right_plaque_thickness']
        new_left_plaque_thickness_value = row['left_plaque_thickness']
        new_imt_valid = row['imt_valid']
        new_bifurcation_valid = row['bifurcation_valid']
        new_ultrasound_rt_points_value = row['ultrasound_rt_points']
        new_min_cimt_right_value = row['min_cimt_right']
        new_max_cimt_right_value = row['max_cimt_right']
        new_mean_cimt_right_value = row['mean_cimt_right']
        new_ultrasound_lt_points_value = row['ultrasound_lt_points']
        new_min_cimt_left_value = row['min_cimt_left']
        new_max_cimt_left_value = row['max_cimt_left']
        new_mean_cimt_left_value = row['mean_cimt_left']
        new_visceral_fat_value = row['visceral_fat']
        new_subcateneous_fat_value = row['subcutaneous_fat']
        new_visceral_comment_value  = row['visceral_comment']
        new_cimt_mean_max_value = row['cimt_mean_max']
        new_ultrasound_qc_results_complete_value = row['ultrasound_qc_results_complete']


        # Define the UPDATE query
        update_query = """
            UPDATE {} 
            SET date_ultrasound_taken = %s, time_ultrasound_taken = %s,
            ultrasound_num_images = %s, birfucations_comment = %s, right_plaque_thickness = %s,
            left_plaque_thickness = %s, imt_valid = %s, bifurcation_valid = %s, ultrasound_rt_points = %s,
            min_cimt_right = %s, max_cimt_right = %s, mean_cimt_right = %s, ultrasound_lt_points = %s,
            min_cimt_left = %s, max_cimt_left = %s, mean_cimt_left = %s, visceral_fat = %s, 
            subcutaneous_fat = %s, visceral_comment = %s, cimt_mean_max = %s,
            ultrasound_qc_results_complete = %s
            WHERE study_id = %s
            """.format(table_name)

        # Execute the UPDATE query with the provided values
        cur.execute(update_query, (
                    new_date_ultrasound_taken_value, new_time_ultrasound_taken_value, 
                    new_ultrsound_num_images_value, new_birfucations_comment_value, new_right_plaque_thickness_value, 
                    new_left_plaque_thickness_value, new_imt_valid, new_bifurcation_valid, new_ultrasound_rt_points_value, 
                    new_min_cimt_right_value, new_max_cimt_right_value, new_mean_cimt_right_value, new_ultrasound_lt_points_value, 
                    new_min_cimt_left_value, new_max_cimt_left_value, new_mean_cimt_left_value, new_visceral_fat_value,
                    new_subcateneous_fat_value, new_visceral_comment_value, 
                    new_cimt_mean_max_value, new_ultrasound_qc_results_complete_value,                        
                    condition_column_value))

    conn.commit()

except Exception as error:
    print(error)
    
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()






