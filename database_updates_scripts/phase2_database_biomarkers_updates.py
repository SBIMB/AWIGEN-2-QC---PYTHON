import pandas as pd
import psycopg2
import sys

from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))
sys.path.append(d)

from postgres_db_config import config
import numpy as np

phase2_data = pd.read_csv('./resources/combined_phase2data_encoded.csv',
                     delimiter=',', low_memory=False)

#md hash key:  8f7b86bd47405e2338bb43d5206fdf5

biomarker_data = pd.read_csv('./resources/AWIGen2BiomarkerResu-AWIGen2Biomarkers_DATA_2024-01-18_1810.csv',
                     delimiter=';', low_memory=False)

# dropping participant information from the biomarkers data, already have the information on the main dataset
biomarker_data = biomarker_data.drop(columns=['unique_site_id',	'sex', 'site', 'participant_data_complete', 'urine_acr'])

# mistyped ids correction
biomarker_data['awigen_id'] = biomarker_data['awigen_id'].replace({'909': 'DSY0V'})
biomarker_data.loc[biomarker_data['awigen_id'] == 'ASZ0I', 'awigen_id'] = 'ASZ0L'
biomarker_data.loc[biomarker_data['awigen_id']=='ASZ0I', 'awigen_id'] = 'ASZ0L'
biomarker_data.loc[biomarker_data['awigen_id']== 'AKR0W', 'awigen_id'] = 'AKR0N'
biomarker_data.loc[biomarker_data['awigen_id']== 'BSN0F', 'awigen_id'] = 'BSN0E'
biomarker_data.loc[biomarker_data['awigen_id']== 'CGT0Y', 'awigen_id'] = 'CGT0N'
biomarker_data.loc[biomarker_data['awigen_id']== 'CTZ0Y', 'awigen_id'] = 'CTZ0V'
biomarker_data.loc[biomarker_data['awigen_id']== 'CVTH0I', 'awigen_id'] = 'VTH0I'
biomarker_data.loc[biomarker_data['awigen_id']== 'DGX0N', 'awigen_id'] = 'DGX0V'
biomarker_data.loc[biomarker_data['awigen_id']== 'DRI0B', 'awigen_id'] = 'DRI0D'
biomarker_data.loc[biomarker_data['awigen_id']== 'FZX0W', 'awigen_id'] = 'FZX0N'
biomarker_data.loc[biomarker_data['awigen_id']== 'GDZ08', 'awigen_id'] = 'GDZ0B'
biomarker_data.loc[biomarker_data['awigen_id']== 'HQD01', 'awigen_id'] = 'HQD0I'
biomarker_data.loc[biomarker_data['awigen_id']== 'HWX09', 'awigen_id'] = 'HWX0N'
biomarker_data.loc[biomarker_data['awigen_id']== 'KES0G', 'awigen_id'] = 'KES0J'
biomarker_data.loc[biomarker_data['awigen_id']== 'KHX', 'awigen_id'] = 'KHX0V'
biomarker_data.loc[biomarker_data['awigen_id']== 'KII00', 'awigen_id'] = 'KII0I'
biomarker_data.loc[biomarker_data['awigen_id']== 'KJM0F', 'awigen_id'] = 'KJM0P'
biomarker_data.loc[biomarker_data['awigen_id']== 'KMM0N', 'awigen_id'] = 'KMM0W'
biomarker_data.loc[biomarker_data['awigen_id']== 'KPP0B', 'awigen_id'] = 'KPP0D'
biomarker_data.loc[biomarker_data['awigen_id']== 'KRH0K', 'awigen_id'] = 'KRH0A'
biomarker_data.loc[biomarker_data['awigen_id']== 'KXA0B', 'awigen_id'] = 'KXA0D'
biomarker_data.loc[biomarker_data['awigen_id']== 'LAZ0R', 'awigen_id'] = 'LAZ0K'
biomarker_data.loc[biomarker_data['awigen_id']== 'LBB', 'awigen_id'] = 'LBB0N'
biomarker_data.loc[biomarker_data['awigen_id']== 'LBS0J', 'awigen_id'] = 'LBS0G'
biomarker_data.loc[biomarker_data['awigen_id']== 'LDC0P', 'awigen_id'] = 'LDC0T'
biomarker_data.loc[biomarker_data['awigen_id']== 'LHW0S', 'awigen_id'] = 'LHW0X'
biomarker_data.loc[biomarker_data['awigen_id']== 'LR0W', 'awigen_id'] = 'LRB0W'
biomarker_data.loc[biomarker_data['awigen_id']== 'MCZ0F', 'awigen_id'] = 'MCZ0S'
biomarker_data.loc[biomarker_data['awigen_id']== 'MIA0J', 'awigen_id'] = 'MIA0G'
biomarker_data.loc[biomarker_data['awigen_id']== 'MIY0L', 'awigen_id'] = 'MIY0F'
biomarker_data.loc[biomarker_data['awigen_id']== 'MJQ0H', 'awigen_id'] = 'MJQ0A'
biomarker_data.loc[biomarker_data['awigen_id']== 'MKI0J', 'awigen_id'] = 'MKI0T'
biomarker_data.loc[biomarker_data['awigen_id']== 'MNH0H', 'awigen_id'] = 'MNH0A'
biomarker_data.loc[biomarker_data['awigen_id']== 'MPQ0A', 'awigen_id'] = 'MPQ0K'
biomarker_data.loc[biomarker_data['awigen_id']== 'MQJ0Q', 'awigen_id'] = 'MQJ0G'
biomarker_data.loc[biomarker_data['awigen_id']== 'NMR0X', 'awigen_id'] = 'NMR0K'
biomarker_data.loc[biomarker_data['awigen_id']== 'PFG0D', 'awigen_id'] = 'PFG0M'
biomarker_data.loc[biomarker_data['awigen_id']== 'PKC0K', 'awigen_id'] = 'PKC0T'
biomarker_data.loc[biomarker_data['awigen_id']== 'PRP0A', 'awigen_id'] = 'PRP0V'
biomarker_data.loc[biomarker_data['awigen_id']== 'PVW0Y', 'awigen_id'] = 'PVW0I'
biomarker_data.loc[biomarker_data['awigen_id']== 'RSX0C', 'awigen_id'] = 'RSX0L'
biomarker_data.loc[biomarker_data['awigen_id']== 'SBF0X', 'awigen_id'] = 'SBF0M'
biomarker_data.loc[biomarker_data['awigen_id']== 'SIL0T', 'awigen_id'] = 'SIL0J'
biomarker_data.loc[biomarker_data['awigen_id']== 'SMJ0K', 'awigen_id'] = 'SMJ0Q'
biomarker_data.loc[biomarker_data['awigen_id']== 'STR0J', 'awigen_id'] = 'STR0L'
biomarker_data.loc[biomarker_data['awigen_id']== 'SWT0Z', 'awigen_id'] = 'SWT0S'
biomarker_data.loc[biomarker_data['awigen_id']== 'TBP0N', 'awigen_id'] = 'TBP0B'
biomarker_data.loc[biomarker_data['awigen_id']== 'TDN0X', 'awigen_id'] = 'TDN0E'
biomarker_data.loc[biomarker_data['awigen_id']== 'TEA0K', 'awigen_id'] = 'TEA0R'
biomarker_data.loc[biomarker_data['awigen_id']== 'TIK0I', 'awigen_id'] = 'TIK0L'
biomarker_data.loc[biomarker_data['awigen_id']== 'VCK0K', 'awigen_id'] = 'VCK0C'
biomarker_data.loc[biomarker_data['awigen_id']== 'VEM01', 'awigen_id'] = 'VEM0I'
biomarker_data.loc[biomarker_data['awigen_id']== 'VNL0B', 'awigen_id'] = 'VNL0C'
biomarker_data.loc[biomarker_data['awigen_id']== 'VRV0X', 'awigen_id'] = 'VRV0R'
biomarker_data.loc[biomarker_data['awigen_id']== 'VTF0Q', 'awigen_id'] = 'VTF0G'

#biomarker_data.to_csv('./resources/biomarker_data_ne.csv')

#becoz of the updates above some 2 rows had the same ids, had to scorrect this manually.
biomarker_data = pd.read_csv('../biomarkers/biomarker_data_ne.csv', sep = ';')

# encoding values <2 and greater than 300 of insulin
biomarker_data['insulin'] = biomarker_data['insulin'].replace({'<2': -111, '>300': -222, 'empty cryotube': -999, 
                                        '': -999, 'insufficient sample': -999, 'Insufficient':-999})
biomarker_data['insulin'] = biomarker_data["insulin"].astype("float")

#rename biormarker column variables to match the database ones
biomarker_data.rename(columns={'awigen_id': 'study_id', 'glucose': 'glucose_result',
            'diabetes_status_lab': 'diabetes_status_c', 'serum_test_date': 'date_serum_tested',
            'serum_creatinine': 's_creatinine', 'insulin': 'insulin_result',
            'lipids_hdl': 'hdl', 'lipids_ldl_calculated': 'friedewald_ldl_c',
            'lipids_ldl_measured': 'ldl_measured', 'lipids_cholesterol': 'cholesterol_1',
            'lipids_triglycerides': 'triglycerides',  'lipids_nonhdl': 'non_hdl_c',
            'dyslipidemia': 'dyslipidemia_c', 'egfr': 'egfr_c', 'urine_date_received': 'date_urine_received',
            'urine_creatinine': 'ur_creatinine',  'urine_albumin': 'ur_albumin', 
            'urine_protein': 'ur_protein', 'ultr_qc_date': 'date_ultrasound_taken',
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
phase2_data_biomarkers = phase2_data.drop(columns = ['glucose_test_date', 'glucose_result', 'diabetes_status_c', 
                        'date_serum_tested', 'plasma_results_complete', 'serum_results_complete', 
                        'urine_results_complete', 'ultrasound_qc_results_complete',
                        's_creatinine', 'insulin_result', 'hdl', 'friedewald_ldl_c',
                        'ldl_measured', 'cholesterol_1', 'triglycerides', 'non_hdl_c',
                        'dyslipidemia_c', 'egfr_c','urine_batch', 'urine_box', 'date_urine_received', 'ur_creatinine', 
                        'urine_creatinine_test_date', 'ur_albumin', 'urine_albumin_test_date', 'acr',
                        'ur_protein', 'urine_protein_test_date', 'ckd_c', 'date_ultrasound_taken',
                        'time_ultrasound_taken', 'ultrasound_num_images', 'birfucations_comment', 
                        'right_plaque_thickness', 'left_plaque_thickness', 'imt_valid',
                        'bifurcation_valid', 'ultrasound_rt_points', 'min_cimt_right', 'max_cimt_right',
                        'mean_cimt_right', 'ultrasound_lt_points', 'min_cimt_left', 'max_cimt_left', 
                        'mean_cimt_left', 'visceral_fat', 'subcutaneous_fat', 'visceral_comment',
                        'cimt_mean_max'], axis = 0)

#merge the biomarker data with the phenotype data
phase2_data_biomarkers = phase2_data_biomarkers.merge(biomarker_data[['study_id', 'glucose_test_date', 'glucose_result',  
                        'date_serum_tested', 's_creatinine', 'insulin_result', 'hdl', 'friedewald_ldl_c',
                        'plasma_results_complete', 'serum_results_complete', 
                        'urine_results_complete', 'ultrasound_qc_results_complete',
                        'ldl_measured', 'cholesterol_1', 'triglycerides', 
                        'urine_batch', 'urine_box','date_urine_received',
                        'urine_creatinine_test_date', 'ur_creatinine', 'urine_albumin_test_date',
                        'ur_albumin', 'ur_protein', 'urine_protein_test_date',                                          
                        'date_ultrasound_taken', 'time_ultrasound_taken', 'ultrasound_num_images',
                        'birfucations_comment', 'right_plaque_thickness', 'left_plaque_thickness', 
                        'imt_valid', 'bifurcation_valid', 'ultrasound_rt_points', 'min_cimt_right', 
                        'max_cimt_right', 'mean_cimt_right', 'ultrasound_lt_points', 'min_cimt_left', 
                        'max_cimt_left', 'mean_cimt_left', 'visceral_fat', 'subcutaneous_fat', 
                        'visceral_comment' ]], left_on='study_id', right_on='study_id', how='left')

#calculations
#acr
def acr(df):
        if ((df['ur_albumin'] > 0) and (df['ur_creatinine'] > 0)):
                return df['ur_albumin']/df['ur_creatinine']
        elif ((df['ur_albumin']==0) or (pd.isna(df['ur_albumin'])) or (df['ur_creatinine']==0) or
              (pd.isna(df['ur_creatinine']))):
              return -999
        elif ((df['ur_albumin']== -111) or (df['ur_creatinine'] ==-111)):
              return -111      
        
phase2_data_biomarkers['acr'] = phase2_data_biomarkers.apply(acr, axis=1)

# non_hdl calculation
def lipids_nonhdl(df):

        if ((df['cholesterol_1'] > 0) and (df['hdl'] > 0)):
                return df['cholesterol_1'] - df['hdl']
        else:
                np.nan

phase2_data_biomarkers['non_hdl_c'] = phase2_data_biomarkers.apply(lipids_nonhdl, axis=1)

phase2_data_biomarkers['cimt_mean_max'] = (phase2_data_biomarkers['max_cimt_right']+phase2_data_biomarkers['max_cimt_left'])/2

# dyslipidemia calculation
def dyslipidemia(df):

        if ((pd.isna(df['chol_treatment_ever'])) and (pd.isna(df['cholesterol_1'])) and \
                    (pd.isna(df['hdl'])) and \
                    (pd.isna(df['friedewald_ldl_c'])) and (pd.isna(df['triglycerides'])) and \
                    (df['chol_treatment_ever']==-999) and (df['cholesterol_1']==-999) and \
                    (df['hdl']==-999) and (df['chol_treatment_ever']==-555)\
                    (df['friedewald_ldl_c']==-999) and (df['triglycerides']==-999)):
                return np.nan
        elif ((df['chol_treatment_ever'] == 1) or (df['cholesterol_1'] >= 5) or \
                  ((df['hdl']<1) and (df['sex'] == 1)) or \
                  ((df['hdl']<1.3) and (df['sex'] == 0)) or (df['friedewald_ldl_c'] >= 3) or \
                  (df['triglycerides'] >= 1.7)):
                return 1
        else:
                return 0

phase2_data_biomarkers['dyslipidemia_c'] = phase2_data_biomarkers.apply(dyslipidemia, axis=1)

# egrf calculation
phase2_data_biomarkers['serum_creatinine_2'] = phase2_data_biomarkers['s_creatinine'].replace({-111:np.nan, -999:np.nan})
phase2_data_biomarkers['one'] = 1
phase2_data_biomarkers['female_cal'] = phase2_data_biomarkers['serum_creatinine_2']/61.9
phase2_data_biomarkers['male_cal'] = phase2_data_biomarkers['serum_creatinine_2']/79.6
phase2_data_biomarkers['female_min'] = phase2_data_biomarkers[['female_cal', 'one']].min(axis=1)
phase2_data_biomarkers['female_max'] = phase2_data_biomarkers[['female_cal', 'one']].max(axis=1)

phase2_data_biomarkers['male_min'] = phase2_data_biomarkers[['male_cal', 'one']].min(axis=1)
phase2_data_biomarkers['male_max'] = phase2_data_biomarkers[['male_cal', 'one']].max(axis=1)

def egfr(df):
        if (df['serum_creatinine_2'] == 0):
                return np.nan
        elif (df['sex'] == 0):
                return 141 * ((df['female_min'])**(-0.329))*((df['female_max'])**(-1.209))*((0.993)**(df['age']))*1.018
        elif (df['sex'] == 1):
                return 141*((df['male_min'])**(-0.411))*((df['male_max'])**(-1.209))*((0.993)**(df['age']))
        else:
                return np.nan

phase2_data_biomarkers['egfr_c'] = phase2_data_biomarkers.apply(egfr, axis=1)

# ckd calculation
def ckd(df):
        if any(pd.isna(df[['egfr_c', 'acr']])) or (df['acr'] == -999):
                return np.nan
        elif ((df['egfr_c'] < 60) or (df['acr'] > 3)):
                return 1
        elif (df['egfr_c'] >= 60) or (df['acr'] <= 33):
                return 0
        else:
                np.nan

phase2_data_biomarkers['ckd_c'] = phase2_data_biomarkers.apply(ckd, axis=1)

phase2_data_biomarkers = phase2_data_biomarkers.drop(columns = ['serum_creatinine_2', 'one', 'female_cal',
                                        	'male_cal', 'female_min', 'female_max',	'male_min', 'male_max'])

phase2_data_biomarkers.to_csv('./resources/phase2_data_with_biomarkers.csv')   

data = phase2_data_biomarkers.replace(np.nan, -999)

data = data[['study_id', 'glucose_test_date', 'glucose_result', 'date_serum_tested', 's_creatinine',	
        'insulin_result', 'plasma_results_complete', 'serum_results_complete', 
        'urine_results_complete', 'ultrasound_qc_results_complete',
        'hdl', 'friedewald_ldl_c', 'ldl_measured', 'cholesterol_1', 'triglycerides', 'urine_batch',
        'urine_box', 'date_urine_received', 'urine_creatinine_test_date', 'ur_creatinine', 
        'urine_albumin_test_date', 'ur_albumin', 'ur_protein', 'urine_protein_test_date', 
        'date_ultrasound_taken', 'time_ultrasound_taken', 'ultrasound_num_images',
        'birfucations_comment',	'right_plaque_thickness', 'left_plaque_thickness',
        'imt_valid', 'bifurcation_valid', 'ultrasound_rt_points', 'min_cimt_right',
        'max_cimt_right', 'mean_cimt_right', 'ultrasound_lt_points', 'min_cimt_left',
        'max_cimt_left', 'mean_cimt_left', 'visceral_fat', 'subcutaneous_fat',
        'visceral_comment', 'acr', 'non_hdl_c', 'cimt_mean_max', 'dyslipidemia_c', 'egfr_c', 'ckd_c'
 ]]

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
        new_glucose_test_date_value = row['glucose_test_date']
        new_glucose_value = row['glucose_result']
        new_date_serum_tested_value = row['date_serum_tested']
        new_s_creatinine_value = row['s_creatinine']
        new_insulin_result_value = row['insulin_result']
        new_hdl_value = row['hdl']
        new_friedewald_ldl_c_value = row['friedewald_ldl_c']
        new_ldl_measured_value  = row['ldl_measured']
        new_cholesterol_1_value = row['cholesterol_1']
        new_trigrlycerides_value = row['triglycerides']
        new_urine_batch_value = row['urine_batch']
        new_urine_box_value = row['urine_box']
        new_date_urine_received = row['date_urine_received']
        new_urine_creatinine_test_date_value = row['urine_creatinine_test_date']
        new_ur_creatinine_value = row['ur_creatinine']
        new_urine_albumin_test_date_value = row['urine_albumin_test_date']
        new_ur_albumin_value = row['ur_albumin']
        new_ur_protein_value = row['ur_protein']
        new_urine_protein_test_date_value = row['urine_protein_test_date']
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
        new_acr_value = row['acr']
        new_nonhdl_c_value = row['non_hdl_c']
        new_cimt_mean_max_value = row['cimt_mean_max']
        new_dyslipedemia_c_value = row['dyslipidemia_c']
        new_egfc_c_value = row['egfr_c']
        new_ckd_c_value = row['ckd_c']    
        new_plasma_results_complete_value = row['plasma_results_complete']
        new_serum_results_complete_value = row['serum_results_complete']
        new_urine_results_complete_value = row['urine_results_complete']
        new_ultrasound_qc_results_complete_value = row['ultrasound_qc_results_complete']


        # Define the UPDATE query
        update_query = """
            UPDATE {} 
            SET glucose_test_date = %s, glucose_result = %s, date_serum_tested = %s, s_creatinine = %s, insulin_result = %s,
            hdl = %s, friedewald_ldl_c = %s, ldl_measured = %s, cholesterol_1 = %s, triglycerides = %s,
            urine_batch = %s, urine_box = %s, date_urine_received = %s, urine_creatinine_test_date = %s,
            ur_creatinine = %s, urine_albumin_test_date = %s, ur_albumin = %s, ur_protein = %s, 
            urine_protein_test_date = %s, date_ultrasound_taken = %s, time_ultrasound_taken = %s,
            ultrasound_num_images = %s, birfucations_comment = %s, right_plaque_thickness = %s,
            left_plaque_thickness = %s, imt_valid = %s, bifurcation_valid = %s, ultrasound_rt_points = %s,
            min_cimt_right = %s, max_cimt_right = %s, mean_cimt_right = %s, ultrasound_lt_points = %s,
            min_cimt_left = %s, max_cimt_left = %s, mean_cimt_left = %s, visceral_fat = %s, 
            subcutaneous_fat = %s, visceral_comment = %s, acr = %s, non_hdl_c = %s, 
            cimt_mean_max = %s, dyslipidemia_c = %s, egfr_c = %s, ckd_c = %s,
            plasma_results_complete = %s, serum_results_complete = %s, urine_results_complete = %s,
            ultrasound_qc_results_complete = %s
            WHERE study_id = %s
            """.format(table_name)

        # Execute the UPDATE query with the provided values
        cur.execute(update_query, (new_glucose_test_date_value, new_glucose_value, new_date_serum_tested_value, 
                    new_s_creatinine_value, new_insulin_result_value,
                    new_hdl_value, new_friedewald_ldl_c_value, new_ldl_measured_value, new_cholesterol_1_value,
                    new_trigrlycerides_value, new_urine_batch_value, new_urine_box_value, new_date_urine_received,
                    new_urine_creatinine_test_date_value, new_ur_creatinine_value, new_urine_albumin_test_date_value,
                    new_ur_albumin_value, new_ur_protein_value, new_urine_protein_test_date_value,
                    new_date_ultrasound_taken_value, new_time_ultrasound_taken_value, 
                    new_ultrsound_num_images_value, new_birfucations_comment_value, new_right_plaque_thickness_value, 
                    new_left_plaque_thickness_value, new_imt_valid, new_bifurcation_valid, new_ultrasound_rt_points_value, 
                    new_min_cimt_right_value, new_max_cimt_right_value, new_mean_cimt_right_value, new_ultrasound_lt_points_value, 
                    new_min_cimt_left_value, new_max_cimt_left_value, new_mean_cimt_left_value, new_visceral_fat_value,
                    new_subcateneous_fat_value, new_visceral_comment_value, new_acr_value, new_nonhdl_c_value,
                    new_cimt_mean_max_value, new_dyslipedemia_c_value, new_egfc_c_value, new_ckd_c_value, 
                    new_plasma_results_complete_value, new_serum_results_complete_value, 
                    new_urine_results_complete_value,  new_ultrasound_qc_results_complete_value,                        
                    condition_column_value))

    conn.commit()

except Exception as error:
    print(error)
    
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()






