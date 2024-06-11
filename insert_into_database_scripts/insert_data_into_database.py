import psycopg2
import pandas as pd
import numpy as np
import sys
from RedcapApiHandler import RedcapApiHandler

from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))

sys.path.append(d)

from datetime import datetime
from postgres_db_config import config

sites =  ['agincourt','dimamo', 'nairobi', 'nanoro', 'navrongo', 'soweto']
datestr = datetime.today().strftime('%Y%m%d')

path = './resources/'

for site in sites:
    
    csv = path + 'data_{}_{}.txt'.format(site, datestr)
    phase2_data = RedcapApiHandler(site).export_from_redcap(csv)
    phase2_data = phase2_data[phase2_data['redcap_event_name'] == 'phase_2_arm_1']
    phase2_data = phase2_data[phase2_data['participant_identification_complete'] == 2]

    if site == 'soweto':

        excluded_variables = ['phase_1_site_id_1',	'phase_1_enrolment_date',	'phase_1_gender',
                          'phase_1_dob_known',	'phase_1_dob',	'phase_1_yob',
                          'phase_1_age', 'phase_1_unique_site_id', 	'phase_1_home_language',
                           'phase_1_ethnicity',	'ethnolinguistc_available',	'a_phase_1_data_complete', 
                           'demo_dob', 'demo_dob_new', 'demo_approx_dob_new']
        
    if site in ['dimamo', 'nairobi', 'navrongo'] :

        excluded_variables = ['phase_1_site_id_1',	'phase_1_enrolment_date',	'phase_1_gender',
                          'phase_1_dob_known',	'phase_1_dob',	'phase_1_yob',
                          'phase_1_age', 'phase_1_unique_site_id', 	'phase_1_home_language',
                           'phase_1_ethnicity',	'ethnolinguistc_available',	'a_phase_1_data_complete', 
                           'demo_dob_new', 'demo_approx_dob_new']
        
    if site in ['nanoro'] :

        excluded_variables = ['phase_1_gender', 'phase_1_site_id_1', 'phase_1_enrolment_date', 'phase_1_dob_known', 'phase_1_dob',
                            'phase_1_yob', 'phase_1_age', 'phase_1_unique_site_id', 'phase_1_home_language', 'phase_1_home_language_other',
                            'phase_1_ethnicity', 'phase_1_ethnicity_other', 'ethnolinguistc_available', 'a_phase_1_data_complete',
                           'demo_dob_new', 'demo_approx_dob_new']
        
    if site in ['agincourt']:

        excluded_variables = ['phase_1_site_id_1',	'phase_1_gender', 'phase_1_dob_known', 'phase_1_home_language',
                            'phase_1_ethnicity', 'ethnolinguistc_available', 'a_phase_1_data_complete',  'demo_date_of_birth',
                            'demo_dob_new', 'demo_approx_dob_new', 'bscan','interviewer_app','scansuccess','specifybarcode',
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
                            'comp_end_time']
        
        phase2_data['gene_site'] = 1
    
    phase2_data = phase2_data.drop(excluded_variables, axis=1)

    phase2_data = phase2_data.replace(np.nan, -999)

    params_ = config()

    conn = None
    cur = None

    try: 
        conn = psycopg2.connect( **params_)

        cur = conn.cursor()
    
        # Convert the data frame to a list of tuples
        data = [tuple(row) for row in phase2_data.values]
        
        # Generate the parameterized query string
        columns = ', '.join(phase2_data.columns)
        placeholders = ', '.join(['%s' for _ in phase2_data.columns])

        if site == 'soweto': 
            query = f"INSERT INTO soweto_redcap_data ({columns}) VALUES ({placeholders})"
            
        if site == 'dimamo': 
            query = f"INSERT INTO dimamo_redcap_data ({columns}) VALUES ({placeholders})"

        if site == 'nairobi': 
            query = f"INSERT INTO nairobi_redcap_data ({columns}) VALUES ({placeholders})"
            
        if site == 'nanoro': 
            query = f"INSERT INTO nanoro_redcap_data ({columns}) VALUES ({placeholders})"

        if site == 'navrongo': 
            query = f"INSERT INTO navrongo_redcap_data ({columns}) VALUES ({placeholders})"
        
        if site == 'agincourt': 
            query = f"INSERT INTO agincourt_redcap_data ({columns}) VALUES ({placeholders})"

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