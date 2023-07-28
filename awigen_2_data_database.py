import pandas as pd
import numpy as np
from analysis_class_phase2 import AnalysisClassPhase2
from RedcapApiHandler import RedcapApiHandler
from encoding import Encodings
from logic import BranchingLogic
from datetime import datetime

if __name__ == '__main__':

    #data uploads, will be extracting from the database after setting it up.
    #phase 2
    path = './resources/'
    datestr = datetime.today().strftime('%Y%m%d')
    sites = ['agincourt', 'dimamo', 'nairobi', 'nanoro', 'navrongo', 'soweto']
    datestr = datetime.today().strftime('%Y%m%d')

    df_out = pd.DataFrame()

    for site in sites:
        csv = path + 'data_{}_{}.csv'.format(site, datestr)
        phase2_data = RedcapApiHandler(site).export_from_redcap(csv)
        phase2_data = phase2_data[phase2_data['redcap_event_name'] == 'phase_2_arm_1']
        phase2_data = phase2_data[phase2_data['participant_identification_complete'] == 2]

        #phase 1(this will be extracted from sql after setting it up)
        phase1 = pd.read_csv(path + "/all_sites_20_12_22.txt", delimiter=",", low_memory=False)

        # all sites do not have these variables

        if site in ['agincourt', 'dimamo', 'nairobi', 'nanoro', 'navrongo', 'soweto']:
            phase2_data['carf_osteo_hip_repl_site___1'] = ''
            phase2_data['carf_osteo_hip_repl_site___2'] = ''
            phase2_data['carf_osteo_hip_repl_site___3'] = ''
            phase2_data['carf_osteo_hip_repl_site____999'] = ''

        #Agincourt site encoding
        if site == 'agincourt':

            phase2_data['gene_site'] = 1
            phase2_data['preg_last_period_mon'] = phase2_data['preg_last_period_mon'].replace({0: 1})
            # agincourt standing height to mm
            # replace -999
            phase2_data['anth_standing_height'] = phase2_data['anth_standing_height'].replace(-999, np.nan)
            #convert cm to mm standing height
            phase2_data['anth_standing_height'] = phase2_data['anth_standing_height'] * 10

            phase2_data['gpaq_transport_phy_time'] = phase2_data['gpaq_transport_phy_time'].replace({-59939:-999,
                                                                                                     -59937:-999})

            phase2_data['gpaq_work_moderate_time'] = phase2_data['gpaq_work_moderate_time'].replace({-59939: -999})
            #missing variables
            phase2_data['gene_end_time'] = ''

        if site == 'dimamo':
            phase2_data['gpaq_transport_phy_time'] = phase2_data['gpaq_transport_phy_time'].replace({-60939: -999})
            phase2_data['gpaq_transport_phy_time'] = phase2_data['gpaq_transport_phy_time'].replace({-59939: -999,
                                                                                                     -59937:-999})

        if site == 'nanoro':
            #renaming the column 'bloc_two_purple_tube' to 'bloc_two_purple_tube'
            phase2_data = phase2_data.rename(columns={'bloc_two_purple_tube': 'bloc_one_purple_tube'})

            phase2_data['gpaq_work_day_time'] = phase2_data['gpaq_work_day_time'].replace({-60939:-999})

            phase2_data['gpaq_work_day_stng_time'] = phase2_data['gpaq_work_day_stng_time'].replace({-60939:-999})

            phase2_data['gpaq_non_work_day_time'] = phase2_data['gpaq_non_work_day_time'].replace({-60939: -999})

            #adding empty columns for variables not in the nanoro redcap
            phase2_data['infh_hiv_arv_meds_now'] = ''
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
            phase2_data['infh_hiv_treatment'] = ''
            phase2_data['infh_hiv_arv_meds'] = ''
            phase2_data['infh_hiv_arv_meds_specify'] = ''
            phase2_data['carf_osteo_hip_replace'] = ''
            phase2_data['carf_osteo_knee_replace'] = ''
            phase2_data['carf_osteo_knee_repl_site'] = ''
            phase2_data['carf_osteo'] = ''
            phase2_data['carf_osteo_sites___3'] = ''
            phase2_data['carf_osteo_sites___4'] = ''
            phase2_data['genh_starchy_staple_food___1'] = ''
            phase2_data['genh_starchy_staple_food___4'] = ''
            phase2_data['genh_starchy_staple_food___5'] = ''
            phase2_data['genh_starchy_staple_food___6'] = ''
            phase2_data['genh_starchy_staple_food___7'] = ''
            phase2_data['genh_starchy_staple_food___8'] = ''
            phase2_data['genh_starchy_staple_food___9'] = ''
            phase2_data['genh_starchy_staple_food___10'] = ''
            phase2_data['genh_starchy_staple_food___11'] = ''
            phase2_data['genh_starchy_staple_food___12'] = ''
            phase2_data['hous_microwave'] = ''
            phase2_data['hous_computer_or_laptop'] = ''
            phase2_data['hous_internet_by_computer'] = ''
            phase2_data['hous_internet_by_m_phone'] = ''
            phase2_data['hous_electric_iron'] = ''
            phase2_data['hous_portable_water'] = ''
            phase2_data['carf_osteo_sites___1'] = ''
            phase2_data['carf_osteo_sites___2'] = ''
            phase2_data['carf_osteo_sites___5'] = ''
            phase2_data['carf_osteo_sites___6'] = ''
            phase2_data['carf_osteo_sites____999'] = ''
            phase2_data['carf_osteo_hip_repl_age'] = ''
            phase2_data['carf_osteo_knee_repl_age'] = ''
            phase2_data['bloc_one_purple_tube'] = ''
            phase2_data['poc_researcher_name'] = ''

        if site in ['navrongo', 'nanoro']:
            study_id5 = ['ILI0I', 'INN0S', 'IWA0T', 'NEW0X', 'QEH0P', 'QKB0W', 'QKE0A', 'QSV0G']

            phase2_data['demo_gender'] = np.where(phase2_data['study_id'].isin(study_id5),
                                                     0, phase2_data['demo_gender'])

        if site =='soweto':

            #participants from the first redcap project, waist  and hicircumference was measured in cms

            phase2_data['educ_formal_years'] = phase2_data['educ_formal_years'].replace({-992:-999})

            study_id_2 = ['DSW0S', 'DGT0R', 'DFT0P', 'DFP0K', 'CTI0E', 'CRL0D', 'CQG0V', 'CJK0L', 'CHB0X']

            phase2_data['anth_waist_circumf'] = np.where(phase2_data['study_id'].isin(study_id_2),
                                      phase2_data['anth_waist_circumf']/10,phase2_data['anth_waist_circumf'])

            phase2_data['anth_hip_circumf'] = np.where(phase2_data['study_id'].isin(study_id_2),
                                              phase2_data['anth_hip_circumf']/10, phase2_data['anth_hip_circumf'])
            #missing columns
            phase2_data['gene_site_id'] = ''

            phase2_data['gpaq_work_day_stng_time'] = phase2_data['gpaq_work_day_stng_time'].replace({-60939: -999})


        if site == 'nairobi':
        #fasting confirmation
            study_id_4 = ['HJE0W', 'HLM0J', 'HRL0T']
            phase2_data['bloc_fasting_confirmed'] = np.where(phase2_data['study_id'].isin(study_id_4), 1,
                                                    phase2_data['bloc_fasting_confirmed'])

        if site in ['nairobi', 'nanoro', 'dimamo', 'navrongo']:
        # gender correction
            phase1 = phase1.merge(phase2_data['study_id'], left_on='study_id', right_on='study_id', how='right')
            phase2_data['demo_gender'] = np.where(phase2_data['demo_gender'].isnull(), phase1.sex, phase2_data['demo_gender'])



        if site in ['agincourt', 'dimamo', 'nairobi', 'nanoro', 'navrongo', 'soweto']:
        # gender derived from phase 1
            phase1 = phase1.merge(phase2_data['study_id'], left_on='study_id', right_on='study_id', how='right')
            phase2_data['ethnicity'] = np.where(phase2_data['ethnicity'].isnull(), phase1.ethnicity, phase2_data['ethnicity'])

        # filtering common columns
        phase2_data = AnalysisClassPhase2(phase2_data).filter_redcap_columns(phase2_data).reset_index(drop=True)

        df_out = df_out.append(phase2_data)

    #biomarkers data

    #to extract from REDCap and later from the database
    biomarkers = pd.read_csv(path + "/AWIGen2BiomarkerResu-AWIGen2Biomarkers_DATA_2023-03-23_1114.csv", delimiter=";",
                             low_memory=False)
    biomarkers.rename(columns={'awigen_id': 'study_id'}, inplace=True)

    # encoding values <2 and greater than 300 of insulin
    biomarkers['insulin'] = biomarkers['insulin'].replace({'<2': -111, '>300': -222, 'empty cryotube': -999, '': -999, 'insufficient sample': -999, 'Insufficient':-999})
    biomarkers['insulin'] = biomarkers["insulin"].astype("float")

    # combining phenotype data with biomarkers
    phase2_data = df_out.merge(biomarkers, on=['study_id', 'study_id'], how='left')

    # adding calculated variables
    phase2_data = AnalysisClassPhase2(phase2_data).add_calculated_variables(phase2_data)

    phase2_data.to_csv(path + 'combined_phase2data.csv')

    #implementing branching logic from REDCap
    phase2_data = BranchingLogic(phase2_data).family_composition_logic()
    phase2_data = BranchingLogic(phase2_data).pregnancy_and_menopause_logic()
    phase2_data = BranchingLogic(phase2_data).civil_status_marital_status_education_employment_logic()
    #phase2_data = BranchingLogic(phase2_data).frailty_measurements_logic()
    phase2_data = BranchingLogic(phase2_data).household_attributes_logic()
    phase2_data = BranchingLogic(phase2_data).substance_use_logic()
    phase2_data = BranchingLogic(phase2_data).a_general_health_cancer_logic()
    phase2_data = BranchingLogic(phase2_data).c_general_health_diet_logic()
    #phase2_data = BranchingLogic(phase2_data).d_general_health_exposure_to_pesticides_pollutants_logic()
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

    #print(phase2_data)

    phase2_data.to_csv(path + 'combined_phase2_data_encoded.csv')

    #phase2_data.drop(['hous_ses', 'hous_ses_perc', 'hous_ses_quintiles', 'subs_alcohol_use_status',
    #                'unique_site_id', 'sex', 'site', 'participant_data_complete',
    #                            'acr', 'egfr', 'ckd', 'dyslipidemia'], axis=1, inplace=True)

    soweto_phase2_encoded = phase2_data[phase2_data['gene_site']==6]
    nairobi_phase2_encoded = phase2_data[phase2_data['gene_site']==3]
    dimamo_phase2_encoded = phase2_data[phase2_data['gene_site']==2]
    nanoro_phase2_encoded = phase2_data[phase2_data['gene_site']==4]
    navrongo_phase2_encoded = phase2_data[phase2_data['gene_site']==5]

    soweto_phase2_encoded.to_csv(path + 'phase2_data_soweto_encoded.csv')
    dimamo_phase2_encoded.to_csv(path + 'phase2_data_dimamo_encoded.csv')
    nairobi_phase2_encoded.to_csv(path + 'phase2_data_nairobi_encoded.csv')
    nanoro_phase2_encoded.to_csv(path + 'phase2_data_nanoro_encoded.csv')
    navrongo_phase2_encoded.to_csv(path + 'phase2_data_navrongo_encoded.csv')

