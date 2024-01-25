from RedcapApiHandler import RedcapApiHandler

import ApiKeys
import requests

import re
import pandas as pd
from io import StringIO
import csv 

import numpy as np
import xlsxwriter
from datetime import datetime

outputDir = './resources/'
sites = ['agincourt', 'dimamo', 'nairobi', 'nanoro', 'navrongo', 'soweto']
datestr = datetime.today().strftime('%Y%m%d')

df_out = pd.DataFrame()

for site in sites:
    csv_str = outputDir + 'data_{}_{}.csv'.format(site, datestr)
    print(csv_str)

    input_data = RedcapApiHandler(site).export_from_redcap(csv_str)
    # input_data = pd.read_csv(csv_str, low_memory=False, sep='\t')

    redcap_data = input_data[input_data['redcap_event_name'] == 'phase_2_arm_1']
    redcap_data = redcap_data.set_index('study_id')

    phase1_data = input_data[input_data['redcap_event_name'] == 'phase_1_arm_1']
    phase1_data = phase1_data.set_index('study_id')

    redcap_data['demo_gender'][redcap_data['demo_gender'].isna()] = phase1_data['phase_1_gender'][redcap_data['demo_gender'].isna()]

    if site == 'agincourt':
        redcap_data['gene_site'] = 1
        redcap_data['anth_standing_height'][redcap_data['anth_standing_height'] > 0] = redcap_data['anth_standing_height'][redcap_data['anth_standing_height'] > 0] * 10
        redcap_data['anth_standing_height'] = redcap_data['anth_standing_height'].round(0)

    df_out = df_out.append(redcap_data)

# Drop unwanted fields
df_out = df_out.drop(columns=[
### Phase 1 data 
'phase_1_site_id_1','phase_1_enrolment_date','phase_1_gender',
'phase_1_dob_known','phase_1_dob','phase_1_yob','phase_1_age', 'phase_1_unique_site_id',
'phase_1_home_language','phase_1_ethnicity', 'ethnolinguistc_available', 'a_phase_1_data_complete',
### REDCap specific data
'redcap_event_name', 'demo_approx_dob_is_correct', 'demo_dob_is_correct', 'demo_date_of_birth_known',
'demo_dob_new', 'demo_approx_dob_new', 'demo_date_of_birth', 'gene_uni_site_id_is_correct', 'demo_dob',
# 'participant_identification_complete', 'ethnolinguistic_information_complete', 'family_composition_complete',
# 'pregnancy_and_menopause_complete', 'civil_status_marital_status_education_employment_complete',
# 'a_cognition_one_complete', 'b_frailty_measurements_complete', 'c_cognition_two_complete',
# 'household_attributes_complete', 'substance_use_complete', 'a_general_health_cancer_complete',
# 'b_general_health_family_history_complete', 'c_general_health_diet_complete',
# 'd_general_health_exposure_to_pesticides_pollutants_complete', 'infection_history_complete',
# 'a_cardiometabolic_risk_factors_diabetes_complete', 'b_cardiometabolic_risk_factors_heart_conditions_complete',
# 'c_cardiometabolic_risk_factors_hypertension_choles_complete', 'd_cardiometabolic_risk_factors_kidney_thyroid_ra_complete',
# 'physical_activity_and_sleep_complete', 'anthropometric_measurements_complete', 'blood_pressure_and_pulse_measurements_complete',
# 'ultrasound_and_dxa_measurements_complete', 'a_respiratory_health_complete', 'b_spirometry_eligibility_complete',
# 'c_spirometry_test_complete', 'd_reversibility_test_complete', 'a_microbiome_complete', 'b_blood_collection_complete',
# 'c_urine_collection_complete', 'point_of_care_testing_complete', 'trauma_complete', 'completion_of_questionnaire_complete',
### Agincourt specific data
'bscan','interviewer_app','scansuccess','specifybarcode',
'barcode_confirm','barcodescan','scansuccess3','barcodenum',
'bar_code_scanner_2','scan_work','barcode_manual_entry_2',
'bar_code_scanner','scan_work_2','barcode_manual_entry',
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
'rspe_infection___navu'])

df_out = pd.DataFrame(df_out, dtype=object)

df_out[df_out == -999] = np.nan
df_out[df_out == '-999'] = np.nan

df = df_out.astype(str)
df = df.replace(to_replace = "\.0+$", value = "", regex = True)
df.to_csv(outputDir + 'all_data_{0}.csv'.format(datestr), quoting=csv.QUOTE_NONE, encoding='utf-8', sep='\t')

# \i create_awigen2_table_all_data.sql
# \copy all_data FROM './all_data_20210824.csv' WITH (FORMAT CSV, DELIMITER E'\t', NULL 'nan', HEADER)
