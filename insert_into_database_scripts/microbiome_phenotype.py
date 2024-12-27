import os

import sys

from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))

sys.path.append(d)

from postgres_db_config import config

import pandas as pd
import numpy as np
import psycopg2
#from insert_into_database_scripts.create_statements.CREATESTATEMENTmicrobiome import CREATESTATEMENTmicrobiome


#datauploads
file_path= '../microbiome/'

biomarker_data = pd.read_csv('../biomarkers/AWIGen2BiomarkerResu-AWIGen2Biomarkers_DATA_2023-11-29_1355.csv', sep=';')

agincourt_all_data = pd.read_csv(file_path + 'AWIGEN2AgincourtDraf_DATA_2023-07-26_1335.csv', sep = ';')
print(agincourt_all_data[agincourt_all_data['study_id'] == 'AB0008']['micr_worm_intestine_treat'])

micro_participants = pd.read_csv(file_path + 'microbiome_study_ids_27_07_23.csv', sep = ';')

agincourt_missing_study_ids = pd.read_csv(file_path + 'agincourt_missing_phenotype_study_ids.csv', sep=';')

phase2_data = pd.read_csv('resources/combined_phase2data_encoded.csv',
                     delimiter=',', low_memory=False)

phase2_data = phase2_data.merge(biomarker_data[['awigen_id', 'ultr_qc_scat', 'ultr_qc_vat', 'ultr_qc_rt_t_mean',
                                               'ultr_qc_lt_t_mean', 'ultr_qc_comment',	'ultr_qc_right_plaque',	'ultr_qc_left_plaque',]], left_on='study_id', right_on='awigen_id', how='left')


phase2_data = phase2_data.drop(columns=['visceral_fat', 'subcutaneous_fat', 'mean_cimt_right', 'mean_cimt_left', 'visceral_comment', 'right_plaque_thickness',
                    'left_plaque_thickness'], axis = 0)

phase2_data['visceral_fat'] = phase2_data['ultr_qc_vat']
phase2_data['subcutaneous_fat'] = phase2_data['ultr_qc_scat']
phase2_data['mean_cimt_right'] = phase2_data['ultr_qc_rt_t_mean']
phase2_data['mean_cimt_left'] = phase2_data['ultr_qc_lt_t_mean']
phase2_data['visceral_comment'] = phase2_data['ultr_qc_comment']
phase2_data['right_plaque_thickness'] = phase2_data['ultr_qc_right_plaque']
phase2_data['left_plaque_thickness'] = phase2_data['ultr_qc_left_plaque']


#extract microbiome informatiom from agincourt alldata
agincourt_missing_study_ids_microbiome_phenotype = agincourt_all_data[agincourt_all_data['study_id'].isin(agincourt_missing_study_ids['study_id'])][['study_id', 'micr_take_antibiotics',
                                                    'micr_diarrhea_last_time', 'micr_worm_intestine_treat', 'micr_probiotics_t_period',
                                                    'micr_wormintestine_period', 'micr_probiotics_taken']]


#print(agincourt_missing_study_ids_microbiome_phenotype[['study_id', 'micr_take_antibiotics']])

agincourt_missing_study_ids_microbiome_phenotype = agincourt_missing_study_ids_microbiome_phenotype.drop_duplicates(subset= ['study_id'], keep = 'last')

print(agincourt_missing_study_ids_microbiome_phenotype[['study_id', 'micr_take_antibiotics']])

agincourt_missing_study_ids_microbiome_phenotype['site']=1

agincourt_missing_study_ids_microbiome_phenotype[['sex', 'enrolment_date', 'age',  
                    'country',  'bmi_c', 'waist_hip_r_c', 'waist_circumference', 
                    'hip_circumference', 'hiv_treatment_when', 'menopause_status_c', 'empl_status', 
                    'household_size', 'electricity', 'cattle', 'other_livestock', 'poultry', 
                    'refrigerator','toilet_facilities', 'portable_water', 'ses_site_quintile_c', 
                    'tobacco_use', 'current_smoker', 'smoking_frequence', 'chewing_tobacco_use', 
                    'smokeless_tobacco_use', 'work_weekend', 'work_vigorous', 'mvpa_c', 
                    'hypertension_status_c', 'hypertension_meds_current', 'friedewald_ldl_c', 
                    'ldl_measured', 'hdl', 'cholesterol_1', 'triglycerides', 'chol_treatment_ever', 
                    'diabetes_status_c', 'diabetes_treatment', 'diabetes_treat_curr', 'diabetes_treat_insulin',
                    'diabetes_treat_pills', 'diabetes_treat_diet', 'diabetes_treat_weight_loss',
                    'diabetes_treat_other', 'diabetes_treat_other_specify', 'insulin_result', 'glucose_result', 
                    'fasting_confirmed', 'arthritis_results', 'rheumatoid_factor', 'esr_crp',
                    'pesticide', 'breast_cancer', 'cervical_cancer', 'visceral_fat', 'subcutaneous_fat',
                    'mean_cimt_right', 'mean_cimt_left', 'visceral_comment', 'right_plaque_thickness',
                    'left_plaque_thickness', 'hiv_arv_start_with', 'hiv_arv_meds_specify', 'tb']] = -999



#merge microbiome study ids with phase 2 data
micro_data_2 = micro_participants.merge(phase2_data, left_on='Sample_ID', right_on='study_id', how='left')

micro_data = micro_participants.merge(phase2_data[['study_id', 'site', 'a_microbiome_complete', 'participant_identification_complete', 'sex']], left_on='Sample_ID', right_on='study_id', how='left')

#micro_data_complete = micro_data[micro_data['a_microbiome_complete'] == 2]

micro_data_complete = micro_data_2[['study_id', 'sex', 'site', 'enrolment_date', 'age', 
                    'country',  'micr_take_antibiotics', 'micr_diarrhea_last_time', 
                    'micr_worm_intestine_treat', 'micr_probiotics_t_period', 'micr_wormintestine_period',
                    'micr_probiotics_taken', 'bmi_c', 'waist_hip_r_c', 'waist_circumference', 
                    'hip_circumference', 'hiv_treatment_when', 'menopause_status_c', 'empl_status', 
                    'household_size', 'electricity', 'cattle', 'other_livestock', 'poultry', 
                    'refrigerator','toilet_facilities', 'portable_water', 'ses_site_quintile_c', 
                    'tobacco_use', 'current_smoker', 'smoking_frequence', 'chewing_tobacco_use', 
                    'smokeless_tobacco_use', 'work_weekend', 'work_vigorous', 'mvpa_c', 
                    'hypertension_status_c', 'hypertension_meds_current', 'friedewald_ldl_c', 
                    'ldl_measured', 'hdl', 'cholesterol_1', 'triglycerides', 'chol_treatment_ever', 
                    'diabetes_status_c', 'diabetes_treatment', 'diabetes_treat_curr', 'diabetes_treat_insulin',
                    'diabetes_treat_pills', 'diabetes_treat_diet', 'diabetes_treat_weight_loss',
                    'diabetes_treat_other', 'diabetes_treat_other_specify', 'insulin_result', 'glucose_result', 
                    'fasting_confirmed', 'arthritis_results', 'rheumatoid_factor', 'esr_crp',
                    'pesticide', 'breast_cancer', 'cervical_cancer', 'visceral_fat', 'subcutaneous_fat',
                    'mean_cimt_right', 'mean_cimt_left', 'visceral_comment', 'right_plaque_thickness',
                    'left_plaque_thickness', 'hiv_arv_start_with', 'hiv_arv_meds_specify', 'tb']]

micro_data_complete = micro_data_complete.drop_duplicates()

study_ids = ['CDW0J', 'CEROH', 'DSD0A', 'CSQ0X', 'CER0H', 'RRT0G', 'VVD0G', 'VWI0N', 'CZQ0X']

mistyped_ids_phenotype_data = phase2_data[phase2_data['study_id'].isin(study_ids)]

mistyped_ids_phenotype_data = mistyped_ids_phenotype_data[['study_id', 'sex', 'site', 'enrolment_date', 'age',  
                    'country',  'micr_take_antibiotics', 'micr_diarrhea_last_time', 
                    'micr_worm_intestine_treat', 'micr_probiotics_t_period', 'micr_wormintestine_period',
                    'micr_probiotics_taken', 'bmi_c', 'waist_hip_r_c', 'waist_circumference', 
                    'hip_circumference', 'hiv_treatment_when', 'menopause_status_c', 'empl_status', 
                    'household_size', 'electricity', 'cattle', 'other_livestock', 'poultry', 
                    'refrigerator','toilet_facilities', 'portable_water', 'ses_site_quintile_c', 
                    'tobacco_use', 'current_smoker', 'smoking_frequence', 'chewing_tobacco_use', 
                    'smokeless_tobacco_use', 'work_weekend', 'work_vigorous', 'mvpa_c', 
                    'hypertension_status_c', 'hypertension_meds_current', 'friedewald_ldl_c', 
                    'ldl_measured', 'hdl', 'cholesterol_1', 'triglycerides', 'chol_treatment_ever', 
                    'diabetes_status_c', 'diabetes_treatment', 'diabetes_treat_curr', 'diabetes_treat_insulin',
                    'diabetes_treat_pills', 'diabetes_treat_diet', 'diabetes_treat_weight_loss',
                    'diabetes_treat_other', 'diabetes_treat_other_specify', 'insulin_result', 'glucose_result', 
                    'fasting_confirmed', 'arthritis_results', 'rheumatoid_factor', 'esr_crp',
                    'pesticide', 'breast_cancer', 'cervical_cancer', 'visceral_fat', 'subcutaneous_fat',
                    'mean_cimt_right', 'mean_cimt_left', 'visceral_comment', 'right_plaque_thickness',
                    'left_plaque_thickness', 'hiv_arv_start_with', 'hiv_arv_meds_specify', 'tb']]



microbiome_phenotype_data = pd.concat([micro_data_complete, mistyped_ids_phenotype_data, agincourt_missing_study_ids_microbiome_phenotype], ignore_index=True)
print(microbiome_phenotype_data.shape)

microbiome_phenotype_data = microbiome_phenotype_data.replace({np.nan:-999})

microbiome_phenotype_data.to_csv(file_path + 'microbiome_phenotype_data.csv', index=False)

tb_microbiome_data = microbiome_phenotype_data[['study_id', 'tb']]

tb_microbiome_data.to_csv(file_path + 'tb_microbiome_data.csv', index = False)

# create the database    
#params_ = postgres_db_config.config()

#conn = None
#cur = None

try: 
    conn = psycopg2.connect( **params_)

    cur = conn.cursor()

    create_script = CREATESTATEMENTmicrobiome.CreateStatementmicrobiome()

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
    data = [tuple(row) for row in microbiome_phenotype_data.values]


    # Generate the parameterized query string
    columns = ', '.join(microbiome_phenotype_data.columns)
    placeholders = ', '.join(['%s' for _ in microbiome_phenotype_data.columns])

    query = f"INSERT INTO microbiome_phenotype ({columns}) VALUES ({placeholders})"
                  
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


