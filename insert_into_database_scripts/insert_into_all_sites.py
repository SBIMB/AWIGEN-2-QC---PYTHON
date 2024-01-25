import psycopg2
import pandas as pd
import numpy as np
from RedcapApiHandler import RedcapApiHandler
from datetime import datetime
from analysis_class_phase2 import AnalysisClassPhase2
#from encoding import Encodings
from NamingConversions import naming_conversion
from logic import BranchingLogic
from postgres_db_config import config
from ethnicities_mapping import ethnicities_mapping 
from ethnicities_mapping_agincourt import ethnicities_mapping_agincourt
import math

sites = ['agincourt',  'dimamo', 'nairobi', 'nanoro', 'navrongo', 'soweto'] #'nanoro',
datestr = datetime.today().strftime('%Y%m%d')

path = './resources/'

df_out = pd.DataFrame()

for site in sites:
    
    csv = path + 'data_{}_{}.txt'.format(site, datestr)
    phase2_data = RedcapApiHandler(site).export_from_redcap(csv)
    phase2_data = phase2_data[phase2_data['redcap_event_name'] == 'phase_2_arm_1']
    phase2_data = phase2_data[phase2_data['participant_identification_complete'] == 2]

    #exclude unnecessary columns(common in the 6 sites)

    if site in ['dimamo', 'nairobi', 'nanoro', 'navrongo', 'soweto'] :

        excluded_variables = ['phase_1_site_id_1', 'phase_1_enrolment_date', 'phase_1_gender',
                            'phase_1_dob_known', 'phase_1_dob',	'phase_1_yob',
                            'phase_1_age', 'phase_1_unique_site_id', 	'phase_1_home_language',
                            'phase_1_ethnicity', 'ethnolinguistc_available', 'a_phase_1_data_complete', 
                            'demo_dob_new', 'demo_approx_dob_new', 'cogn_words_remember_p1____999',
                            'cogn_delayed_recall____999', 'cogn_word_cognition_list____999', 
                            'subs_smoke_cigarettes____999',  'subs_alcoholtype_consumed____999',
                            'genh_starchy_staple_food____999', 'genh_energy_source_type____999', 
                            'carf_diabetes_treat____999',  'carf_pain_location____999', 
                            'carf_chol_treatment_now____999',  'anth_measurementcollector', 
                            'bppm_measurementcollector', 'ultr_technician', 'ultr_cimt_technician', 
                            'ultr_plaque_technician', 'resp_copd_suffer____999',  'resp_measles_suffer____999', 
                            'rspe_infection____999', 'rspir_researcher',  'bloc_phlebotomist_name', 
                            'poc_pre_test_worker', 'poc_technician_name', 'poc_post_test_worker',
                            'spiro_researcher', 'bloc_urine_collector', 'comp_sections_1_13', 
                            'comp_comment_no_1_13', 'comp_section_14', 'comp_comment_no_14',
                            'comp_section_15', 'comp_comment_no_15', 'comp_section_16', 'comp_comment_no_16',
                            'comp_section_17', 'comp_comment_no_17', 'comp_section_18', 'comp_comment_no_18',
                            'comp_section_19', 'comp_comment_no_19', 'comp_section_20', 'comp_comment_no_20'] 
                            
        
        phase2_data = phase2_data.drop(excluded_variables, axis=1)

    if site =='agincourt':
        excluded_variables = [ 'phase_1_site_id_1', 'phase_1_gender', 'phase_1_dob_known', 'phase_1_home_language',
                              'phase_1_ethnicity', 'ethnolinguistc_available',	'a_phase_1_data_complete', 
                            'demo_dob_new', 'demo_approx_dob_new', 'cogn_words_remember_p1____999',
                            'cogn_delayed_recall____999', 'cogn_word_cognition_list____999', 
                            'subs_smoke_cigarettes____999',  'subs_alcoholtype_consumed____999',
                            'genh_starchy_staple_food____999', 'genh_energy_source_type____999', 
                            'carf_diabetes_treat____999',  'carf_pain_location____999', 
                            'carf_chol_treatment_now____999',  'anth_measurementcollector', 
                            'bppm_measurementcollector', 'ultr_technician', 'ultr_cimt_technician', 
                            'ultr_plaque_technician', 'resp_copd_suffer____999',  'resp_measles_suffer____999', 
                            'rspe_infection____999', 'rspir_researcher',  'bloc_phlebotomist_name', 
                            'poc_pre_test_worker', 'poc_technician_name', 'poc_post_test_worker',
                            'spiro_researcher', 'bloc_urine_collector',  'demo_date_of_birth',  'bscan','interviewer_app',
                            'scansuccess','specifybarcode',
                            'section_respondent', 'ethnolinguistic_section_respondent', 'famc_section_respondent', 'preg_section_respondent',
                            'civil_section_respondent', 'cogn_section_respondent', 'frai_section_respondent','cogn2_section_respondent',
                            'hous_section_respondent','subs_section_respondent','genh_section_respondent','genh2_section_respondent',
                            'genhd_section_respondent','genhe_section_respondent','infh_section_respondent','carf_section_respondent',
                            'carf2_section_respondent','carf23_section_respondent','carf4_section_respondent','gpaq_section_respondent',
                            'anth_section_respondent','bppm_section_respondent','ultra_section_respondent','resp_section_respondent',
                            'respe_section_respondent','spiro_section_respondent','rspir_section_respondent','micr_section_respondent',
                            'bloc_section_respondent','blocu_section_respondent','poc_section_respondent','tram_section_respondent',
                            'comp_section_respondent', 'phase1_section_respondent',
                            'cogn_words_remember_p1___navu','cogn_delayed_recall___navu','cogn_word_cognition_list___navu',
                            'subs_smoke_cigarettes___navu','subs_alcoholtype_consumed___navu','genh_starchy_staple_food___navu',
                            'genh_energy_source_type___navu','carf_diabetes_treat___navu','carf_pain_location___navu',
                            'carf_chol_treatment_now___navu','carf_osteo_sites___navu','resp_copd_suffer___navu','resp_measles_suffer___navu',
                            'rspe_infection___navu', 'barcodescan', 'scansuccess3', 'barcodenum', 'bar_code_scanner',
                            'scan_work_2', 'barcode_manual_entry', 'bar_code_scanner_2', 'scan_work', 'barcode_manual_entry_2', 
                            'comp_end_time', 'comp_sections_1_13', 
                            'comp_section_14', 'comp_section_15', 'comp_section_16', 'comp_section_17',	'comp_section_18',
                            'comp_section_19',	'comp_section_20',	'comp_end_time', 'comp_section_respondent',
                            'completion_of_questionnaire_complete']

        phase2_data = phase2_data.drop(excluded_variables, axis=1)

    if site == 'soweto':
        #only in soweto
        excluded_variables = ['demo_dob']
           
        phase2_data = phase2_data.drop(excluded_variables, axis=1)

        phase2_data['study_id'] = phase2_data['study_id'].replace({'909':'DSY0V'})
    
    if site == 'nanoro':
        #only in nanoro
        excluded_variables = ['phase_1_home_language_other', 'phase_1_ethnicity_other']

        phase2_data = phase2_data.drop(excluded_variables, axis=1)
    
    if site in ['agincourt', 'dimamo', 'nairobi', 'navrongo', 'soweto']:
        #not in nanoro
        excluded_variables = ['poc_researcher_name', 'carf_osteo_sites____999']
        phase2_data = phase2_data.drop(excluded_variables, axis=1)

    #phase 1(this will be extracted from sql after setting it up)
    phase1 = pd.read_csv(path + "/all_sites_20_12_22.txt", delimiter=",", low_memory=False)
    filter_columns = pd.read_csv(path + "/filter_columns.csv")

    # all sites do not have these variables
    if site in ['agincourt', 'dimamo', 'nairobi', 'nanoro', 'navrongo', 'soweto']:
        
        # round to the nearest whole number systolic and diastolic and pulse average values
        phase2_data['bppm_systolic_avg']= phase2_data['bppm_systolic_avg'].round()
        phase2_data['bppm_diastolic_avg']= phase2_data['bppm_diastolic_avg'].round()
        phase2_data['bppm_pulse_avg']= phase2_data['bppm_pulse_avg'].round()

        phase2_data['infh_hiv_diagnosed'].replace({'Agincourt clinic': np.nan, 20000: 2000, 1219: '12-2019', 101998: '10-1998', -2022: 2022, 5: np.nan,
                -2023:2023, '00 00': np.nan, 2: np.nan, 22008: '02-2008', 'Mary':np.nan, '010/2015': '10-2015', '06-019': '06-2019', 3: np.nan }, inplace=True)

    if site in ['agincourt', 'dimamo', 'nairobi', 'nanoro', 'navrongo']:
            # renaming gene_site_id column to gene_uni_site_id_is_correct 
            phase2_data.rename(columns = {'gene_site_id':'gene_uni_site_id_is_correct'}, inplace = True)
        
    #variables only in nanoro
    if site in ['agincourt', 'dimamo', 'nairobi', 'navrongo', 'soweto']:
        phase2_data['genh_starchy_staple_food___13']=''
        phase2_data['genh_starchy_staple_food___14']=''
        phase2_data['genh_starchy_staple_food___15']=''
        phase2_data['genh_starchy_staple_food___16']=''
        phase2_data['subs_smoke_cigarettes___6']=''
        phase2_data['carf_diabetes_treat___6']=''
        phase2_data['genh_oes_cancer_dad']=''

    #Agincourt site encoding
    if site == 'agincourt':

        phase2_data['gene_site'] = 1
        phase2_data['preg_last_period_mon'] = phase2_data['preg_last_period_mon'].replace({0: 1})
        # agincourt standing height to mm
        # replace -999
        phase2_data['anth_standing_height'] = phase2_data['anth_standing_height'].replace(-999, np.nan)
        #convert cm to mm standing height
        phase2_data['anth_standing_height'] = phase2_data['anth_standing_height'] * 10

        phase2_data['cogn_recognition_score'][phase2_data['cogn_recognition_score']>20] = phase2_data[['cogn_word_cognition_list___1', 'cogn_word_cognition_list___2', 'cogn_word_cognition_list___3', 
                                                        'cogn_word_cognition_list___4', 'cogn_word_cognition_list___5', 'cogn_word_cognition_list___6', 'cogn_word_cognition_list___7', 'cogn_word_cognition_list___8',
                                                        'cogn_word_cognition_list___9', 'cogn_word_cognition_list___10', 'cogn_word_cognition_list___11', 'cogn_word_cognition_list___12', 'cogn_word_cognition_list___13',
                                                        'cogn_word_cognition_list___14', 'cogn_word_cognition_list___15', 'cogn_word_cognition_list___16', 'cogn_word_cognition_list___16', 'cogn_word_cognition_list___17',
                                                        'cogn_word_cognition_list___18', 'cogn_word_cognition_list___19', 'cogn_word_cognition_list___20']].sum(axis=1)

        phase2_data['subs_smoking_start_age'].replace({1973:47}, inplace = True)

        #phase2_data['year_stop_smoking']

        #missing variables
        phase2_data['gene_end_time'] = ''
        phase2_data['ethn_father_ethn_ot'] = ''
        phase2_data['ethn_father_lang_ot'] = ''
        phase2_data['ethn_pat_gfather_ethn_ot']= ''
        phase2_data['ethn_pat_gfather_lang_ot'] = ''
        phase2_data['ethn_pat_gmother_ethn_ot'] = ''
        phase2_data['ethn_pat_gmother_lang_ot'] = ''
        phase2_data['ethn_mother_ethn_ot'] = ''
        phase2_data['ethn_mother_lang_ot'] = ''
        phase2_data['ethn_mat_gfather_ethn_ot'] = ''
        phase2_data['ethn_mat_gfather_lang_ot'] = ''
        phase2_data['ethn_mat_gmother_ethn_ot'] = ''
        phase2_data['ethn_mat_gmother_lang_ot'] = ''

    if site == 'dimamo':
        study_id7 = ['SJH0H', 'SKD0F', 'SKZ0C', 'SSX0P']
        phase2_data['anth_standing_height'] = np.where(phase2_data['study_id'].isin(study_id7),
                                        phase2_data['anth_standing_height']*10, phase2_data['anth_standing_height'])
    
    if site == 'nanoro':
        #renaming the column 'bloc_two_purple_tube' to 'bloc_two_purple_tube'
        phase2_data = phase2_data.rename(columns={'bloc_two_purple_tube': 'bloc_one_purple_tube'})

        #adding empty columns for variables not in nanoro redcap
        phase2_data['infh_hiv_treatment'] = ''
        phase2_data['infh_hiv_arv_meds'] = ''
        phase2_data['infh_hiv_arv_meds_now'] = ''
        phase2_data['infh_hiv_arv_meds_specify'] = ''            
        phase2_data['infh_hiv_arv_single_pill'] = ''
        phase2_data['infh_hiv_pill_size'] = ''
        phase2_data['infh_painful_feet_hands'] = ''
        phase2_data['infh_hypersensitivity'] = ''
        phase2_data['infh_kidney_problems'] = ''
        phase2_data['infh_liver_problems'] = ''
        phase2_data['infh_change_in_body_shape'] = ''
        phase2_data['infh_mental_state_change'] = ''
        phase2_data['infh_chol_levels_change'] = ''
        phase2_data['infh_hiv_test'] = ''
        phase2_data['infh_hiv_counselling'] = ''
        phase2_data['carf_osteo'] = ''
        phase2_data['carf_osteo_sites___1'] = ''
        phase2_data['carf_osteo_sites___2'] = ''
        phase2_data['carf_osteo_sites___3'] = ''
        phase2_data['carf_osteo_sites___4'] = ''
        phase2_data['carf_osteo_sites___5'] = ''
        phase2_data['carf_osteo_sites___6'] = ''
        phase2_data['carf_osteo_hip_replace'] = ''
        phase2_data['carf_osteo_hip_repl_site'] = ''
        phase2_data['carf_osteo_hip_repl_age'] = ''
        phase2_data['carf_osteo_knee_replace'] = ''
        phase2_data['carf_osteo_knee_repl_age'] = ''
        phase2_data['carf_osteo_knee_repl_site'] = ''
        phase2_data['genh_starchy_staple_food___1'] = -555
        phase2_data['genh_starchy_staple_food___4'] = -555
        phase2_data['genh_starchy_staple_food___5'] = -555
        phase2_data['genh_starchy_staple_food___6'] = -555
        phase2_data['genh_starchy_staple_food___7'] = -555
        phase2_data['genh_starchy_staple_food___8'] = -555
        phase2_data['genh_starchy_staple_food___9'] = -555
        phase2_data['genh_starchy_staple_food___10'] = -555
        phase2_data['genh_starchy_staple_food___11'] = -555
        phase2_data['genh_starchy_staple_food___12'] = -555
        phase2_data['hous_microwave'] = ''
        phase2_data['hous_computer_or_laptop'] = ''
        phase2_data['hous_internet_by_computer'] = ''
        phase2_data['hous_internet_by_m_phone'] = ''
        phase2_data['hous_electric_iron'] = ''
        phase2_data['hous_portable_water'] = ''      

    if site in ['navrongo', 'nanoro']:
        #mislabeled female participants
        study_id5 = ['ILI0I', 'INN0S', 'IWA0T', 'NEW0X', 'QEH0P', 'QKB0W', 'QKE0A', 'QSV0G']

        phase2_data['demo_gender'] = np.where(phase2_data['study_id'].isin(study_id5),
                                                     0, phase2_data['demo_gender'])

    if site =='soweto':

        #-992 instead of -999
        phase2_data['educ_formal_years'] = phase2_data['educ_formal_years'].replace({-992:-999})

        #participants from the first redcap project, waist  and hicircumference was measured in mms
        study_id_2 = ['DSW0S', 'DGT0R', 'DFT0P', 'DFP0K', 'CTI0E', 'CRL0D', 'CQG0V', 'CJK0L', 'CHB0X']

        phase2_data['anth_waist_circumf'] = np.where(phase2_data['study_id'].isin(study_id_2),
                                      phase2_data['anth_waist_circumf']/10,phase2_data['anth_waist_circumf'])

        phase2_data['anth_hip_circumf'] = np.where(phase2_data['study_id'].isin(study_id_2),
                                      phase2_data['anth_hip_circumf']/10, phase2_data['anth_hip_circumf'])
        
        phase2_data['carf_osteo_hip_repl_site'] = ''

    if site == 'nairobi':
    #fasting confirmation for aprticipants in nairobi
        study_id_4 = ['HJE0W', 'HLM0J', 'HRL0T']
        phase2_data['bloc_fasting_confirmed'] = np.where(phase2_data['study_id'].isin(study_id_4), 1,
                                                    phase2_data['bloc_fasting_confirmed'])

    if site in ['nairobi', 'nanoro', 'dimamo', 'navrongo']:
        # gender correction particpnats with no gender values in phase2, 
        phase1 = phase1.merge(phase2_data['study_id'], left_on='study_id', right_on='study_id', how='right')
        phase2_data['demo_gender'] = np.where(phase2_data['demo_gender'].isnull(), phase1.sex, phase2_data['demo_gender'])


    if site in ['agincourt', 'dimamo', 'nairobi', 'nanoro', 'navrongo', 'soweto']:
    # ethnicity derived from phase 1
        phase1 = phase1.merge(phase2_data['study_id'], left_on='study_id', right_on='study_id', how='right')
        phase2_data['ethnicity'] = np.where(phase2_data['ethnicity'].isnull(), phase1.ethnicity, phase2_data['ethnicity'])
                 
    # site_id of derived from phase 1
        phase2_data['gene_uni_site_id_correct'] = np.where(phase2_data['gene_uni_site_id_correct'].isnull(), phase1.site_id, phase2_data['gene_uni_site_id_correct'])  
    
    if site in ['agincourt', 'dimamo', 'nairobi', 'nanoro', 'navrongo', 'soweto']:
    #ethnicity of parents and grandparents from phase1
    # Define a mapping between the column names in Phase 1 and Phase 2
        column_mapping = {
                'father_ethnicity_qc': 'ethn_father_ethn_sa',
                'father_ethnicity_other': 'ethn_father_ethn_ot',
                'father_language_qc': 'ethn_father_lang_sa',
                'father_language_other': 'ethn_father_lang_ot',
                'pat_gfather_ethnicity_qc': 'ethn_pat_gfather_ethn_sa',
                'pat_gfather_ethnicity_other': 'ethn_pat_gfather_ethn_ot',
                'pat_gfather_language_qc': 'ethn_pat_gfather_lang_sa',
                'pat_gfather_language_other': 'ethn_pat_gfather_lang_ot',
                'pat_gmother_ethnicity_qc': 'ethn_pat_gmother_ethn_sa',
                'pat_gmother_ethnicity_other': 'ethn_pat_gmother_ethn_ot',
                'pat_gmother_language_qc': 'ethn_pat_gmother_lang_sa',
                'pat_gmother_language_other': 'ethn_pat_gmother_lang_ot',
                'mother_ethnicity_qc': 'ethn_mother_ethn_sa',
                'mother_ethnicity_other': 'ethn_mother_ethn_ot',
                'mother_language_qc':'ethn_mother_lang_sa',
                'mother_language_other': 'ethn_mother_lang_ot',
                'mat_gfather_ethnicity_qc': 'ethn_mat_gfather_ethn_sa',
                'mat_gfather_ethnicity_other': 'ethn_mat_gfather_ethn_ot',
                'mat_gfather_language_qc': 'ethn_mat_gfather_lang_sa',
                'mat_gfather_language_other': 'ethn_mat_gfather_lang_ot',
                'mat_gmother_ethnicity_qc': 'ethn_mat_gmother_ethn_sa',
                'mat_gmother_ethnicity_other': 'ethn_mat_gmother_ethn_ot',
                'mat_gmother_language_qc': 'ethn_mat_gmother_lang_sa',
                'mat_gmother_language_other': 'ethn_mat_gmother_lang_ot'
}

    # Use merge() and update() operations to populate columns in Phase 2 from Phase 1
        for col in column_mapping:
            mask = phase2_data[column_mapping[col]].isnull()
            phase2_data.loc[mask, column_mapping[col]] = phase2_data.loc[mask, 'study_id'].map(phase1.set_index('study_id')[col])
 
    #appending data from all sites to make it 1 dataframe
    df_out = df_out.append(phase2_data)

    #biomarkers data

#to extract from REDCap and later from the database
biomarkers = pd.read_csv(path + "/AWIGen2BiomarkerResu_DATA_2023-06-05_1152.csv", delimiter=";",
                            low_memory=False)
biomarkers.rename(columns={'awigen_id': 'study_id'}, inplace=True)


# dropping participant information from the biomarkers data, already have the information on the main dataset
biomarkers = biomarkers.drop(columns=['unique_site_id',	'sex', 'site', 'participant_data_complete'])

# encoding values <2 and greater than 300 of insulin
biomarkers['insulin'] = biomarkers['insulin'].replace({'<2': -111, '>300': -222, 'empty cryotube': -999, '': -999, 'insufficient sample': -999, 'Insufficient':-999})
biomarkers['insulin'] = biomarkers["insulin"].astype("float")

# combining phenotype data with biomarkers
phase2_data = df_out.merge(biomarkers, on=['study_id', 'study_id'], how='left')
 
# adding calculated variables
phase2_data = AnalysisClassPhase2(phase2_data).add_calculated_variables(phase2_data)

#phase2_data = phase2_data

#phase2_data.to_csv(path + 'combined_phase2data.csv', index = False)

#implementing branching logic from REDCap
phase2_data = BranchingLogic(phase2_data).family_composition_logic()
phase2_data = BranchingLogic(phase2_data).pregnancy_and_menopause_logic()
phase2_data = BranchingLogic(phase2_data).civil_status_marital_status_education_employment_logic()
phase2_data = BranchingLogic(phase2_data).frailty_measurements_logic()
phase2_data = BranchingLogic(phase2_data).household_attributes_logic()
phase2_data = BranchingLogic(phase2_data).substance_use_logic()
phase2_data = BranchingLogic(phase2_data).a_general_health_cancer_logic()
phase2_data = BranchingLogic(phase2_data).c_general_health_diet_logic()
phase2_data = BranchingLogic(phase2_data).d_general_health_exposure_to_pesticides_pollutants_logic()
phase2_data = BranchingLogic(phase2_data).infection_history_logic()
phase2_data = BranchingLogic(phase2_data).a_cardiometabolic_risk_factors_diabetes_logic()
phase2_data = BranchingLogic(phase2_data).b_cardiometabolic_risk_factors_heart_conditions_logic()
phase2_data = BranchingLogic(phase2_data).c_cardiometabolic_risk_factors_hypertension_choles_logic()
phase2_data = BranchingLogic(phase2_data).d_cardiometabolic_risk_factors_kidney_thyroid_ra_logic()
phase2_data = BranchingLogic(phase2_data).physical_activity_and_sleep_logic()
phase2_data = BranchingLogic(phase2_data).ultrasound_and_dxa_measurements_logic()
phase2_data = BranchingLogic(phase2_data).a_respiratory_health_logic()
phase2_data = BranchingLogic(phase2_data).b_spirometry_eligibility_logic()
phase2_data = BranchingLogic(phase2_data).c_spirometry_test()
phase2_data = BranchingLogic(phase2_data).d_reversibility_test()
phase2_data = BranchingLogic(phase2_data).a_microbiome_logic()
phase2_data = BranchingLogic(phase2_data).b_blood_collection()
phase2_data = BranchingLogic(phase2_data).c_urine_collection()
phase2_data = BranchingLogic(phase2_data).point_of_care_testing()

phase2_data.to_csv(path + 'combined_phase2data.csv', index = False)

phase2_data.rename(columns = naming_conversion(), inplace=True)

phase2_data = phase2_data.replace({np.nan: -999, '':-999})

phase2_data['bmi_cat_c'] = phase2_data['bmi_cat_c'].replace({np.nan:-999})

phase2_data.to_csv(path + 'combined_phase2data_encoded.csv', index = False)

phase2_data[phase2_data['site']==6].to_csv(path + 'sowoto_phase2_data.txt', index= False, sep = '\t')

phase2_data = phase2_data[filter_columns['columns'].tolist()]

print(phase2_data.shape)

#phase2_data = phase2_data.iloc[:100]      
params_ = config()

conn = None
cur = None

try: 
    conn = psycopg2.connect( **params_)

    cur = conn.cursor()

    #batch_size = 100  # Define the batch size as per your requirement
            
    # Convert the data frame to a list of tuples
    data = [tuple(row) for row in phase2_data.values]
    print('\n \t ##############')

    #while data:
    #    batch_data = data[:batch_size]
    #    data = data[batch_size:]

    # Generate the parameterized query string
    columns = ', '.join(phase2_data.columns)
    placeholders = ', '.join(['%s' for _ in phase2_data.columns])

    query = f"INSERT INTO all_sites_phase2 ({columns}) VALUES ({placeholders})"
                  
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