import sys
import sys

from os.path import dirname, abspath

# Setup the path to the project root
d = dirname(dirname(abspath(__file__)))
# Add the project root to sys.path
sys.path.append(d)

from insert_into_database_scripts.create_statements.CreateStatementChangeincont_var import CreateStatementChangeincont_var

import pandas as pd
import numpy as np
import psycopg2
from postgres_db_config import config
from datetime import datetime

path = './resources/'

#data uploads
phase2_data = pd.read_csv(path + 'combined_phase2data_encoded.csv', sep = ',', low_memory=False)
phase1_data = pd.read_csv(path + 'all_sites_20_12_22.txt', sep = ',', low_memory=False)

#merge the 2 datasets

phase1_phase2_merge = pd.merge(phase1_data, phase2_data, how='inner',left_on="study_id", right_on="study_id")

# Convert strings to datetime objects
phase1_phase2_merge['enrolment_date_y'] = pd.to_datetime(phase1_phase2_merge['enrolment_date_y'])
phase1_phase2_merge['enrolment_date_x'] = pd.to_datetime(phase1_phase2_merge['enrolment_date_x'])

#create variables
phase1_phase2_merge['months_from_baseline'] = (phase1_phase2_merge['enrolment_date_y'].dt.year\
                               - phase1_phase2_merge['enrolment_date_x'].dt.year) * 12 \
                                 + phase1_phase2_merge['enrolment_date_y'].dt.month - phase1_phase2_merge['enrolment_date_x'].dt.month

#create variables
phase1_phase2_merge['years_from_baseline'] = (phase1_phase2_merge['enrolment_date_y'].dt.year\
                               - phase1_phase2_merge['enrolment_date_x'].dt.year) 

def sub(A, B, C, df):
    mask = (df[A] > 0) & (df[B] > 0)
    df[C] = pd.Series(dtype='float64')  # initialize the column if it doesn't exist
    df.loc[mask, C] = df[A] - df[B]
    return df[C]
   
# change between phase 1 and 2v
phase1_phase2_merge['height_change'] = sub('standing_height', 'standing_height_qc', 'height_change', phase1_phase2_merge)
phase1_phase2_merge['weight_change'] = sub('weight', 'weight_qc', 'weight_change', phase1_phase2_merge)
phase1_phase2_merge['bmi_change'] = sub('bmi_c_y', 'bmi_c_qc', 'bmi_change', phase1_phase2_merge)
phase1_phase2_merge['glucose_change'] = sub('glucose_result', 'glucose', 'glucose_change', phase1_phase2_merge)
phase1_phase2_merge['ldl_c_change'] = sub('friedewald_ldl_c', 'friedewald_ldl_c_c_qc', 'ldl_c_change', phase1_phase2_merge)
phase1_phase2_merge['hdl_change'] = sub('hdl_y', 'hdl_qc', 'hdl_change', phase1_phase2_merge)
phase1_phase2_merge['cholesterol_change'] = sub('cholesterol_1_y', 'cholesterol_1_qc', 'cholesterol_change', phase1_phase2_merge)
phase1_phase2_merge['triglycerides_change'] = sub('triglycerides_y', 'triglycerides_qc', 'triglycerides_change', phase1_phase2_merge)
phase1_phase2_merge['egfr_change'] = sub('egfr_c_y', 'egfr_c_qc', 'egfr_change', phase1_phase2_merge)
phase1_phase2_merge['insulin_change'] = sub('insulin_result', 'insulin_qc', 'insulin_change', phase1_phase2_merge)
phase1_phase2_merge['ur_creatinine_change'] = sub('ur_creatinine_y', 'ur_creatinine_qc', 'ur_creatinine_change', phase1_phase2_merge)
phase1_phase2_merge['s_creatinine_change'] = sub('s_creatinine_y', 's_creatinine_qc', 's_creatinine_change', phase1_phase2_merge)
phase1_phase2_merge['mean_cimt_right_change'] = sub('mean_cimt_right_y', 'mean_cimt_right_qc', 'mean_cimt_right_change', phase1_phase2_merge)
phase1_phase2_merge['mean_cimt_left_change'] = sub('mean_cimt_left_y', 'mean_cimt_left_qc', 'mean_cimt_left_change', phase1_phase2_merge)
phase1_phase2_merge['VAT_change'] = sub('visceral_fat_y', 'visceral_fat_qc', 'VAT_change', phase1_phase2_merge)
phase1_phase2_merge['SCAT_change'] = sub('subcutaneous_fat_y', 'subcutaneous_fat_qc', 'SCAT_change', phase1_phase2_merge)

#relative change per year
phase1_phase2_merge['height_change_per_yr'] = phase1_phase2_merge['height_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['weight_change_per_yr'] = phase1_phase2_merge['weight_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['bmi_change_per_yr'] = phase1_phase2_merge['bmi_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['glucose_change_per_yr'] = phase1_phase2_merge['glucose_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['ldl_c_change_per_yr'] = phase1_phase2_merge['ldl_c_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['hdl_change_per_yr'] = phase1_phase2_merge['hdl_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['cholesterol_change_per_yr'] = phase1_phase2_merge['cholesterol_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['triglycerides_change_per_yr'] = phase1_phase2_merge['triglycerides_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['egfr_change_per_yr'] = phase1_phase2_merge['egfr_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['insulin_change_per_yr'] = phase1_phase2_merge['insulin_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['ur_creatinine_change_per_yr'] = phase1_phase2_merge['ur_creatinine_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['s_creatinine_change_per_yr'] = phase1_phase2_merge['s_creatinine_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['mean_cimt_right_change_per_yr'] = phase1_phase2_merge['mean_cimt_right_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['mean_cimt_left_change_per_yr'] = phase1_phase2_merge['mean_cimt_left_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['VAT_change_per_yr'] = phase1_phase2_merge['VAT_change']/phase1_phase2_merge['years_from_baseline']
phase1_phase2_merge['SCAT_change_per_yr'] = phase1_phase2_merge['SCAT_change']/phase1_phase2_merge['years_from_baseline']

phase1_phase2_merge = phase1_phase2_merge[['study_id', 'site_x', 'height_change', 'weight_change', 'bmi_change', 'glucose_change',
                            'ldl_c_change', 'hdl_change', 'cholesterol_change', 'triglycerides_change',
                            'egfr_change', 'insulin_change', 'ur_creatinine_change', 's_creatinine_change',
                            'mean_cimt_right_change', 'mean_cimt_left_change', 'VAT_change', 'SCAT_change',
                            'months_from_baseline', 'years_from_baseline', 'height_change_per_yr', 
                            'weight_change_per_yr', 'bmi_change_per_yr', 'glucose_change_per_yr',
                            'ldl_c_change_per_yr', 'hdl_change_per_yr', 'cholesterol_change_per_yr',
                            'triglycerides_change_per_yr', 'egfr_change_per_yr', 'insulin_change_per_yr',
                            'ur_creatinine_change_per_yr', 's_creatinine_change_per_yr', 'mean_cimt_right_change_per_yr',
                            'mean_cimt_left_change_per_yr', 'VAT_change_per_yr', 'SCAT_change_per_yr']]

phase1_phase2_merge.rename(columns={'site_x':'site'}, inplace =True)

# create the database    
params_ = config()

conn = None
cur = None

try: 
   conn = psycopg2.connect( **params_)

   cur = conn.cursor()

   create_script = CreateStatementChangeincont_var.CreateStatementChangeincont_var()   

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
   data = [tuple(row) for row in phase1_phase2_merge.values]


    # Generate the parameterized query string
   columns = ', '.join(phase1_phase2_merge.columns)
   placeholders = ', '.join(['%s' for _ in phase1_phase2_merge.columns])

   query = f"INSERT INTO change_in_contvar ({columns}) VALUES ({placeholders})"
                  
    # Write data frame to the SQL table using parameterized queries and executemany
   
   cur.executemany(query, data)
   conn.commit()

except Exception as error:
    print(error)
    
finally:
   if cur is not None:
      cur.close()
   if conn is not None:
      conn.close()