#from builtins import print

import pandas as pd
import csv
import os


class BranchingLogicHandler:

    def __init__(self, csv_link):

        # initiate data frame
        self.data = pd.read_csv(csv_link, index_col=0)

        # remove report csv file
        if os.path.exists('../resources/report.csv'):
            os.remove("../resources/report.csv")

    def write_report(self):

        ignored_cols = ['ethnolinguistc_available',
                        'a_phase_1_data_complete',
                        'gene_site_id',
                        'demo_approx_dob_is_correct',
                        'demo_dob_is_correct',
                        'demo_date_of_birth_known',
                        'demo_dob_new',
                        'demo_approx_dob_new',
                        'cogn_comments',
                        'rspe_participation',
                        'rspe_participation_note',
                        'spiro_comment',
                        'rspir_salb_admin',
                        'rspir_comment',
                        'ultr_dxa_scan_completed',
                        'comp_comment_no_14',
                        'comp_comment_no_15',
                        'comp_comment_no_16',
                        'comp_comment_no_17',
                        'comp_comment_no_18',
                        'comp_comment_no_19',
                        'comp_comment_no_20']

        instrument_dict = {
            "phas": "a_phase_1_data",
            "gene": "participant_identification",
            "demo": "participant_identification",
            "home": "participant_identification",
            "ethn": "ethnolinguistic_information",
            "famc": "family_composition",
            "preg": "pregnancy_and_menopause",
            "mari": "civil_status_marital_status_education_employment",
            "educ": "civil_status_marital_status_education_employment",
            "empl": "civil_status_marital_status_education_employment",
            "cogn": "a_cognition_one or two",
            "frai": "b_frailty_measurements",
            "hous": "household_attributes",
            "subs": "substance_use",
            "genh": "general_health",
            "infh": "infection_history",
            "carf": "a_cardiometabolic_risk_factors",
            "gpaq": "physical_activity_and_sleep",
            "anth": "anthropometric_measurements",
            "bppm": "blood_pressure_and_pulse_measurements",
            "ultr": "ultrasound_and_dxa_measurements",
            "resp": "a_respiratory_health",
            "rspe": "b_spirometry_eligibility",
            "spir": "c_spirometry_test",
            "rspi": "d_reversibility_test",
            "micr": "a_microbiome",
            "bloc": "b_blood_collection or c_urine_collection",
            "poc_": "point_of_care_testing",
            "tram": "trauma",
            "comp": "completion_of_questionnaire"
                          }



        ethnicity_cols = ['ethn_father_ethn_sa',
                            'ethn_father_ethn_ot',
                            'ethn_father_lang_sa',
                            'ethn_father_lang_ot',
                            'ethn_pat_gfather_ethn_sa',
                            'ethn_pat_gfather_ethn_ot',
                            'ethn_pat_gfather_lang_sa',
                            'ethn_pat_gfather_lang_ot',
                            'ethn_pat_gmother_ethn_sa',
                            'ethn_pat_gmother_ethn_ot',
                            'ethn_pat_gmother_lang_sa',
                            'ethn_pat_gmother_lang_ot',
                            'ethn_mother_ethn_sa',
                            'ethn_mother_ethn_ot',
                            'ethn_mother_lang_sa',
                            'ethn_mother_lang_ot',
                            'ethn_mat_gfather_ethn_sa',
                            'ethn_mat_gfather_ethn_ot',
                            'ethn_mat_gfather_lang_sa',
                            'ethn_mat_gfather_lang_ot',
                            'ethn_mat_gmother_ethn_sa',
                            'ethn_mat_gmother_ethn_ot',
                            'ethn_mat_gmother_lang_sa',
                            'ethn_mat_gmother_lang_ot']

        pregnancy_cols = ['preg_pregnant',
                            'preg_num_of_pregnancies',
                            'preg_num_of_live_births',
                            'preg_birth_control',
                            'preg_hysterectomy',
                            'preg_regular_periods',
                            'preg_last_period_remember',
                            'preg_last_period_mon',
                            'preg_last_period_mon_2',
                            'preg_period_more_than_yr']

        household_cols = ['hous_power_generator',
                            'hous_washing_machine',
                            'hous_telephone',
                            'hous_microwave',
                            'hous_computer_or_laptop',
                            'hous_internet_by_computer',
                            'hous_internet_by_m_phone',
                            'hous_electric_iron	hous_fan',
                            'hous_kerosene_stove',
                            'hous_plate_gas',
                            'hous_electric_plate',
                            'hous_torch',
                            'hous_gas_lamp',
                            'hous_kerosene_lamp',
                            'hous_toilet_facilities',
                            'hous_portable_water',
                            'hous_grinding_mill',
                            'hous_table',
                            'hous_sofa',
                            'hous_wall_clock',
                            'hous_bed',
                            'hous_mattress',
                            'hous_blankets',
                            'hous_cattle',
                            'hous_other_livestock',
                            'hous_poultry',
                            'hous_tractor',
                            'hous_plough',
                            'hous_electric_iron',
                            'hous_fan']

        with open('../resources/report.csv', mode='w') as report_file:
            report_writer = csv.writer(report_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            # write headers
            report_writer.writerow(['study_id', 'instrument', 'variable_name', 'number_missing', 'comment'])

            for i, j in self.data.iterrows():
                for col in self.data.columns:
                    if col == 'demo_age_at_collection':
                        if pd.isnull(j[col]) and pd.notna(j['demo_dob_new']):
                            pass
                        elif pd.notna(j[col]):
                            pass
                        else:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                    elif col == 'demo_gender':
                        if pd.isnull(j[col]):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'demo_home_language' or col == 'other_home_language':
                        if pd.isnull(j['demo_home_language']):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        elif pd.isnull('other_home_language') and j['demo_home_language'] == 98:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'ethnicity':
                        if pd.isnull(j[col]) and j['ethnicity_confirmation'] == 1:
                            pass
                        elif pd.notna(j[col]) and j['ethnicity_confirmation'] == 0:
                            pass
                        else:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                    elif col == 'other_ethnicity':
                        if pd.isnull(j[col]) and j['ethnicity'] == 98:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ethnicity_cols:
                        if pd.isnull(j[col]) and (j['gene_site'] == 2 or j['gene_site'] == 6):
                            pass
                        elif pd.notna(j[col]):
                            pass
                        else:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                    elif col == 'famc_siblings':
                        if pd.isnull(j[col]):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'siblings data is missing'])
                        else:
                            pass

                    elif col == 'famc_number_of_brothers':
                        if pd.isnull(j[col]) and (j['famc_siblings'] == 0 or pd.isnull(j['famc_siblings'])):
                            pass
                        elif j[col] in range(0, 100) and j['famc_siblings'] == 1:
                            pass
                        elif pd.notnull(j[col]) and j[col] not in range(0, 100) and j['famc_siblings'] == 1:
                            report_writer.writerow(
                                [str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'out of range'])
                        else:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                    elif col == 'famc_living_brothers':
                        if pd.isnull(j[col]) and (pd.isnull(j['famc_number_of_brothers']) or j['famc_number_of_brothers']==0):
                            pass
                        elif j[col] in range(0, 100):
                            pass
                        elif pd.notnull(j[col]) and j[col] not in range(0, 100):
                            report_writer.writerow(
                                [str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'out of range'])
                        else:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                    elif col == 'famc_number_of_sisters':
                        if pd.isnull(j[col]) and (j['famc_siblings'] == 0 or pd.isnull(j['famc_siblings'])):
                            pass
                        elif j[col] in range(0, 100):
                            pass
                        elif pd.notnull(j[col]) and j[col] not in range(0, 100):
                            report_writer.writerow(
                                [str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'out of range'])
                        else:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                    elif col == 'famc_living_sisters':
                        if pd.isnull(j[col]) and (j['famc_siblings'] == 0 or pd.isnull(j['famc_siblings']) or j['famc_number_of_sisters']==0):
                            pass
                        elif j[col] in range(0, 100):
                            pass
                        elif pd.notnull(j[col]) and j[col] not in range(0, 100):
                            report_writer.writerow(
                                [str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'out of range'])
                        else:
                            report_writer.writerow(
                                [str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                    elif col == 'famc_children':
                        if pd.isnull(j[col]):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['famc_bio_sons', 'famc_bio_daughters']:
                        if pd.isnull(j[col]) and (j['famc_children'] == 0 or pd.isnull(j['famc_children'])):
                            pass
                        elif j[col] in range(0, 100):
                            pass
                        elif pd.notnull(j[col]) and j[col] not in range(0, 100):
                            report_writer.writerow(
                                [str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'out of range'])
                        else:
                            report_writer.writerow(
                                [str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                    elif col in ['famc_living_bio_sons', 'famc_living_bio_daughters']:
                        if (col == 'famc_living_bio_sons' and pd.isnull(j[col])) and (j['famc_bio_sons'] == 0 or pd.isnull(j['famc_bio_sons'])):
                            pass
                        elif (col == 'famc_living_bio_daughters' and pd.isnull(j[col])) and (j['famc_bio_daughters'] == 0 or pd.isnull(j['famc_bio_daughters'])):
                            pass
                        elif j[col] in range(0, 100):
                            pass
                        elif pd.notnull(j[col]) and j[col] not in range(0, 100):
                            report_writer.writerow(
                                [str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'out of range'])
                        else:
                            report_writer.writerow(
                                [str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                    elif col in pregnancy_cols:
                        if pd.isnull(j[col]) and (j['demo_gender'] == 1 or pd.isnull(j['demo_gender'])):
                            pass
                        elif pd.isnull(j[col]) and j['preg_pregnant'] == 0:
                            if col == 'preg_num_of_pregnancies':
                                report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                            elif col == 'preg_num_of_live_births' and j['preg_num_of_pregnancies'] > 0:
                                report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                            elif col in ['preg_last_period_mon', 'preg_last_period_mon_2'] and j['preg_last_period_remember'] == 1:
                                report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                            elif col == 'preg_period_more_than_yr' and (j['preg_last_period_remember'] == 0 or j['preg_last_period_remember'] == 2):
                                report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        elif pd.notnull(j[col]) and j['preg_pregnant'] == 0:
                            if col in ['preg_num_of_pregnancies', 'preg_num_of_live_births']:
                                if j[col] in range(0, 37):
                                    pass
                                elif j[col] not in range(0, 37):
                                    report_writer.writerow(
                                        [str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'out of range'])
                        else:
                            pass

                    elif col in ['educ_highest_years', 'educ_formal_years', 'empl_days_work']:
                        if col == 'educ_highest_years' and j['educ_highest_level'] in [2, 3, 4]:
                            if j[col] in range(1, 21):
                                pass
                            elif pd.isnull(j[col]):
                                report_writer.writerow(
                                    [str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                            elif j[col] not in range(1, 21):
                                report_writer.writerow(
                                    [str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'out of range'])
                        elif col == 'educ_formal_years' and pd.isnull(j[col]) and j['educ_highest_level'] in [2, 3, 4]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        elif col == 'empl_days_work' and pd.isnull(j[col]) and j['empl_status'] in [2, 3, 4]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'frai_comment':
                        if pd.isnull(j[col]) and j['frai_sit_stands_completed'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'frai_comment_why':
                        if pd.isnull(j[col]) and j['frai_complete_procedure'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'frai_please_comment_why':
                        if pd.isnull(j[col]) and j['frai_procedure_walk_comp'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # household attributes
                    elif col in household_cols:
                        if col == 'hous_microwave' and pd.isnull(j[col]) and j['gene_site'] in [1, 2, 3, 6]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        elif col in ['hous_power_generator', 'hous_telephone', 'hous_toilet_facilities'] and pd.isnull(j[col]) \
                                and j['gene_site'] in [1, 2, 4, 5, 6]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                        elif col in ['hous_washing_machine', 'hous_computer_or_laptop', 'hous_internet_by_m_phone'] and pd.isnull(j[col]) \
                                and j['gene_site'] in [1, 2, 3, 5, 6]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                        elif col == 'hous_internet_by_computer' and pd.isnull(j[col]) and j['gene_site'] in [1, 2, 5, 6]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                        elif (col == 'hous_electric_iron' and pd.isnull(j[col])) and j['gene_site'] in [3, 5]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                        elif col in ['hous_fan', 'hous_table', 'hous_sofa', 'hous_bed', 'hous_mattress', 'hous_blankets']:
                            if pd.isnull(j[col]) and j['gene_site'] in [3, 4, 5]:
                                report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                            else:
                                pass

                        elif col in ['hous_kerosene_stove', 'hous_electric_plate', 'hous_torch', 'hous_gas_lamp', 'hous_kerosene_lamp', 'hous_wall_clock'] \
                                and pd.isnull(j[col]) and j['gene_site'] in [3, 4]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                        elif col in ['hous_plate_gas', 'hous_grinding_mill'] and pd.isnull(j[col]) and j['gene_site'] in [4, 5]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                        elif col == 'hous_portable_water' and pd.isnull(j[col]) and j['gene_site'] == 5:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                        elif col in ['hous_cattle', 'hous_other_livestock', 'hous_poultry'] and pd.isnull(j[col]) and j['gene_site'] in [1, 2, 3, 4, 5]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

                        elif col in ['hous_tractor', 'hous_plough'] and pd.isnull(j[col]) and j['gene_site'] in [1, 2, 4, 5]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # substance use
                    elif col in ['subs_smoke_100', 'subs_smoke_now']:
                        if pd.isnull(j[col]) and j['subs_tobacco_use'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['subs_smoke_last_hour', 'subs_smoking_frequency', 'subs_smoking_start_age']:
                        if pd.isnull(j[col]) and j['subs_smoke_now'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'subs_smoke_specify':
                        if pd.isnull(j[col]) and j['subs_smoke_cigarettes___5'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'subs_smoke_per_day':
                        if pd.isnull(j[col]) and j['subs_smoking_frequency'] in [1, 2, 3, 4, 5]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'subs_smoking_stop_year':
                        if pd.isnull(j[col]) and j['subs_tobacco_use'] == 1 and j['subs_smoke_now'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['subs_snuff_use', 'subs_tobacco_chew_use']:
                        if pd.isnull(j[col]) and j['subs_smokeless_tobacc_use'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['subs_snuff_method_use', 'subs_snuff_use_freq']:
                        if pd.isnull(j[col]) and j['subs_snuff_use'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'subs_freq_snuff_use':
                        if pd.isnull(j[col]) and j['subs_snuff_use_freq'] in [1, 2, 3, 4, 5]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'subs_tobacco_chew_freq':
                        if pd.isnull(j[col]) and j['subs_tobacco_chew_use'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'subs_tobacco_chew_d_freq':
                        if pd.isnull(j[col]) and j['subs_tobacco_chew_freq'] in [1, 2, 3, 4, 5]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'subs_alcohol_consume_now':
                        if pd.isnull(j[col]) and j['subs_alcohol_consump'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in['subs_alcohol_consump_freq', 'subs_alcohol_criticize', ' subs_alcohol_guilty', 'subs_alcohol_hangover',
                                'subs_alcoholtype_consumed___1', 'subs_alcoholtype_consumed___2', 'subs_alcoholtype_consumed___3',
                                'subs_alcoholtype_consumed___4', 'subs_alcoholtype_consumed___5'
                                ]:
                        if pd.isnull(j[col]) and (j['subs_alcohol_consume_now'] == 1 or j['subs_alcohol_consump'] == 1):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'subs_alcohol_consume_freq':
                        if pd.isnull(j[col]) and j['subs_alcohol_consump_freq'] in [1, 2, 3, 4, 5]:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'subs_alcohol_specify':
                        if pd.isnull(j[col]) and j['subs_alcoholtype_consumed___5'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # general health cancer
                    elif col in ['genh_breast_cancer_treat', 'genh_bre_cancer_trad_med']:
                        if pd.isnull(j[col]) and j['genh_breast_cancer'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_bre_cancer_treat_now':
                        if pd.isnull(j[col]) and j['genh_breast_cancer_treat'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_breast_cancer_meds':
                        if pd.isnull(j[col]) and j['genh_bre_cancer_treat_now'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_cervical_cancer':
                        if pd.isnull(j[col]) and j['demo_gender'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['genh_cer_cancer_treat', 'genh_cer_cancer_trad_med']:
                        if pd.isnull(j[col]) and j['genh_cervical_cancer'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_cer_cancer_treat_now':
                        if pd.isnull(j[col]) and j['genh_cer_cancer_treat'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_cervical_cancer_meds':
                        if pd.isnull(j[col]) and j['genh_cer_cancer_treat_now'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_prostate_cancer':
                        if pd.isnull(j[col]) and j['demo_gender'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['genh_pro_cancer_treat', 'genh_pro_cancer_trad_med']:
                        if pd.isnull(j[col]) and j['genh_prostate_cancer'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_pro_cancer_treat_now':
                        if pd.isnull(j[col]) and j['genh_pro_cancer_treat'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_prostate_cancer_meds':
                        if pd.isnull(j[col]) and j['genh_pro_cancer_treat_now'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['genh_oes_cancer_treat', 'genh_oesophageal_trad_med']:
                        if pd.isnull(j[col]) and j['genh_oesophageal_cancer'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_oes_cancer_treat_now':
                        if pd.isnull(j[col]) and j['genh_oes_cancer_treat'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_oes_cancer_meds':
                        if pd.isnull(j[col]) and j['genh_oes_cancer_treat_now'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['genh_cancer_specify_other', 'genh_oth_cancer_trad_med']:
                        if pd.isnull(j[col]) and j['genh_other_cancers'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_oth_cancer_treat_now':
                        if pd.isnull(j[col]) and j['genh_other_cancer_treat'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_other_cancer_meds':
                        if pd.isnull(j[col]) and j['genh_oth_cancer_treat_now'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # general health diet
                    elif col in ['genh_starchy_staple_freq', 'genh_staple_servings']:
                        if pd.isnull(j[col]) and (j['genh_starchy_staple_food___1'] == 1 or
                                                  j['genh_starchy_staple_food___2'] == 1 or
                                                  j['genh_starchy_staple_food___3'] == 1 or
                                                  j['genh_starchy_staple_food___4'] == 1 or
                                                  j['genh_starchy_staple_food___5'] == 1 or
                                                  j['genh_starchy_staple_food___6'] == 1 or
                                                  j['genh_starchy_staple_food___7'] == 1 or
                                                  j['genh_starchy_staple_food___8'] == 1 or
                                                  j['genh_starchy_staple_food___9'] == 1 or
                                                  j['genh_starchy_staple_food___10'] == 1 or
                                                  j['genh_starchy_staple_food___11'] == 1 or
                                                  j['genh_starchy_staple_food___12'] == 1):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # exposure to pesticides
                    elif col == 'genh_pesticide_years':
                        if pd.isnull(j[col]) and j['genh_pesticide'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_pesticide_list':
                        if pd.isnull(j[col]) and j['genh_pesticide_type'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_cookingplace_specify':
                        if pd.isnull(j[col]) and j['genh_cooking_place'] == 3:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_energy_specify':
                        if pd.isnull(j[col]) and j['genh_energy_source_type___6'] == 6:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'genh_smoke_freq_someone':
                        if pd.isnull(j[col]) and j['genh_smoker_in_your_house'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # infection history
                    elif col == 'infh_malaria_month':
                        if pd.isnull(j[col]) and j['infh_malaria'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['infh_tb_12months', 'infh_tb_treatment', 'infh_tb_meds', 'infh_tb_counselling']:
                        if pd.isnull(j[col]) and j['infh_tb'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'infh_tb_diagnosed':
                        if pd.isnull(j[col]) and j['infh_tb_12months'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'infh_tb_diagnosed':
                        if pd.isnull(j[col]) and j['infh_tb_12months'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['infh_hiv_tested', 'infh_hiv_status', 'infh_hiv_positive']:
                        if pd.isnull(j[col]) and j['infh_hiv_que_answering'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['infh_hiv_diagnosed', 'infh_hiv_medication', 'infh_hiv_traditional_meds', 'infh_painful_feet_hands',
                                 'infh_hypersensitivity', 'infh_kidney_problems', 'infh_liver_problems',
                                 'infh_change_in_body_shape', 'infh_mental_state_change', 'infh_chol_levels_change']:
                        if pd.isnull(j[col]) and j['infh_hiv_positive'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['infh_hiv_treatment', 'infh_hiv_arv_meds']:
                        if pd.isnull(j[col]) and j['infh_hiv_medication'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'infh_hiv_arv_meds_now':
                        if pd.isnull(j[col]) and j['infh_hiv_status'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['infh_hiv_arv_meds_specify', 'infh_hiv_arv_single_pill']:
                        if pd.isnull(j[col]) and j['infh_hiv_arv_meds_now'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'infh_hiv_pill_size':
                        if pd.isnull(j[col]) and j['infh_hiv_arv_single_pill'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'infh_hiv_counselling':
                        if pd.isnull(j[col]) and j['infh_hiv_test'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # Cardiometabolic risk factors
                    elif col in ['carf_diabetes_12months', 'carf_diabetes_treatment']:
                        if pd.isnull(j[col]) and j['carf_diabetes'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'carf_diabetes_treat_now':
                        if pd.isnull(j[col]) and j['carf_diabetes_treatment'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'carf_diabetetreat_specify':
                        if pd.isnull(j[col]) and j['carf_diabetes_treat___5'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'carf_diabetes_meds_2':
                        if pd.isnull(j[col]) and j['carf_diabetes_treat_now'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'carf_diabetes_traditional':
                        if pd.isnull(j[col]) and j['carf_diabetes_12months'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['carf_diabetes_mother', 'carf_diabetes_father', 'carf_diabetes_brother_1','carf_diabetes_brother_2',
                                 'carf_diabetes_brother_3',	'carf_diabetes_brother_4', 'carf_diabetes_sister_1', 'carf_diabetes_sister_2',
                                 'carf_diabetes_sister_3', 'carf_diabetes_sister_4', 'carf_diabetes_son_1', 'carf_diabetes_son_2',
                                 'carf_diabetes_son_3',	'carf_diabetes_son_4', 'carf_daughter_diabetes_1', 'carf_diabetes_daughter_2',
                                 'carf_diabetes_daughter_3', 'carf_diabetes_daughter_4', 'carf_diabetes_fam_other']:
                        if pd.isnull(j[col]) and j['carf_diabetes_history'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'carf_diabetes_fam_specify':
                        if pd.isnull(j[col]) and j['carf_diabetes_fam_other'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col == 'carf_stroke_diagnosed':
                        if pd.isnull(j[col]) and j['carf_stroke'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['carf_angina_treatment', 'carf_angina_treat_now', 'carf_angina_meds', 'carf_angina_traditional', 'carf_pain']:
                        if pd.isnull(j[col]) and j['carf_angina'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass
                    elif col in ['carf_pain_action_stopslow', 'carf_relief_standstill']:
                        if pd.isnull(j[col]) and j['carf_pain2'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['carf_heartattack_treat', 'carf_heartattack_trad']:
                        if pd.isnull(j[col]) and j['carf_heartattack'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'carf_heartattack_meds':
                        if pd.isnull(j[col]) and j['carf_heartattack_treat'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['carf_chf_treatment', 'carf_chf_treatment_now', 'carf_chf_meds']:
                        if pd.isnull(j[col]) and j['carf_congestiv_heart_fail'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['carf_hypertension_12mnths', 'carf_hypertension_treat', 'carf_hypertension_meds', 'carf_hypertension_medlist']:
                        if pd.isnull(j[col]) and j['carf_hypertension'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['carf_chol_treatment', 'carf_chol_medicine']:
                        if pd.isnull(j[col]) and j['carf_h_cholesterol'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'carf_chol_treat_specify':
                        if pd.isnull(j[col]) and j['carf_chol_treatment_now___4'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['carf_thyroid_type', 'carf_thyroid_treatment', 'carf_parents_thyroid']:
                        if pd.isnull(j[col]) and j['carf_thyroid'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'carf_thryroid_specify':
                        if pd.isnull(j[col]) and j['carf_thyroid_type'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'carf_thyroid_treat_use':
                        if pd.isnull(j[col]) and j['carf_thyroid_treatment'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'carf_thyroidparnt_specify':
                        if pd.isnull(j[col]) and j['carf_parents_thyroid'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['carf_kidney_disease_known', 'carf_kidney_function_low']:
                        if pd.isnull(j[col]) and j['carf_kidney_disease'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'carf_kidneydiseas_specify':
                        if pd.isnull(j[col]) and j['carf_kidney_disease_known'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['carf_kidney_family_mother', 'carf_kidney_family_father', 'carf_kidney_family_other']:
                        if pd.isnull(j[col]) and j['carf_kidney_family'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'carf_kidney_fam_specify':
                        if pd.isnull(j[col]) and j['carf_kidney_family_other'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'carf_kidney_family_type':
                        if pd.isnull(j[col]) and (j['carf_kidney_family_other'] == 1 or
                                                  j['carf_kidney_family_mother'] == 1 or
                                                  j['carf_kidney_family_father'] == 1):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'carf_kidney_fam_tspecify':
                        if pd.isnull(j[col]) and j['carf_kidney_family_type'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['carf_joints_swollen', 'carf_joints_involved', 'carf_when_they_hurt', 'carf_symptoms_how_long']:
                        if pd.isnull(j[col]) and j['carf_joints_swollen_pain'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['carf_rheumatoid_factor', 'carf_acpa', 'carf_esr_crp']:
                        if pd.isnull(j[col]) and j['carf_arthritis_results'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # physical activity and sleep
                    elif col in ['gpaq_work_vigorous_days', 'gpaq_work_vigorous_time', 'gpaq_work_vigorous_hrs', 'gpaq_work_vigorous_mins']:
                        if pd.isnull(j[col]) and j['gpaq_work_vigorous'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['gpaq_work_moderate_days', 'gpaq_work_moderate_time', 'gpaq_work_moderate_hrs', 'gpaq_work_moderate_mins']:
                        if pd.isnull(j[col]) and j['gpaq_work_moderate'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['gpaq_transport_phy_days', 'gpaq_transport_phy_time', 'gpaq_transport_phy_hrs', 'gpaq_transport_phy_mins']:
                        if pd.isnull(j[col]) and j['gpaq_transport_phy'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'gpaq_leisurevigorous_days':
                        if pd.isnull(j[col]) and (j['gpaq_leisure_vigorous'] or j['gpaq_leisure_phy'] == 1):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['gpaq_leisurevigorous_time', 'gpaq_leisurevigorous_hrs', 'gpaq_leisurevigorous_mins']:
                        if pd.isnull(j[col]) and pd.notna(j['gpaq_leisurevigorous_days']):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'gpaq_leisuremoderate_days':
                        if pd.isnull(j[col]) and j['gpaq_leisuremoderate'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['gpaq_leisuremoderate_time', 'gpaq_leisuremoderate_hrs', 'gpaq_leisuremoderate_mins']:
                        if pd.isnull(j[col]) and pd.notna(j['gpaq_leisuremoderate_days']):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # ultrasound measurements
                    elif col == 'ultr_comment':
                        if pd.isnull(j[col]) and j['ultr_vat_scat_measured'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['ultr_technician', 'ultr_visceral_fat', 'ultr_subcutaneous_fat']:
                        if pd.isnull(j[col]) and j['ultr_vat_scat_measured'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'ultr_cimt_comment':
                        if pd.isnull(j[col]) and j['ultr_cimt'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['ultr_cimt_technician', 'ultr_cimt_right_min',	'ultr_cimt_right_max',
                                 'ultr_cimt_right_mean', 'ultr_cimt_left_min', 'ultr_cimt_left_max', 'ultr_cimt_left_mean']:
                        if pd.isnull(j[col]) and j['ultr_cimt'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'ultr_plaque_comment':
                        if pd.isnull(j[col]) and j['ultr_plaque'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['ultr_plaque_technician', 'ultr_plaque_present', 'ultr_plaque_right_min', 'ultr_plaque_right_max',
                                 'ultr_plaque_right_mean',	'ultr_plaque_left_min',	'ultr_plaque_left_max',	'ultr_plaque_left_mean']:
                        if pd.isnull(j[col]) and j['ultr_plaque'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'ultr_dxa_scan_comment':
                        if pd.isnull(j[col]) and j['ultr_dxa_scan_completed'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['ultr_dxa_measurement_1', 'ultr_dxa_measurement_2', 'ultr_dxa_measurement_3',
                                 'ultr_dxa_measurement_4',	'ultr_dxa_measurement_5']:
                        if pd.isnull(j[col]) and j['ultr_dxa_scan_completed'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # respiratory health
                    elif col in ['resp_age_diagnosed', 'resp_asthma_treat']:
                        if pd.isnull(j[col]) and j['resp_asthma_diagnosed'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'resp_asthma_treat_now':
                        if pd.isnull(j[col]) and j['resp_asthma_treat'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'resp_copd_treat':
                        if pd.isnull(j[col]) and (j['resp_copd_suffer___1'] == 1 or
                                                  j['resp_copd_suffer___2'] == 1 or
                                                  j['resp_copd_suffer___3'] == 1):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['resp_medication_list', 'resp_puffs_time', 'resp_puffs_times_day']:
                        if pd.isnull(j[col]) and j['resp_inhaled_medication'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'rspe_chest_pain':
                        if pd.isnull(j[col]) and (j['rspe_major_surgery'] == 0 or j['rspe_major_surgery'] == 9):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'rspe_coughing_blood':
                        if pd.isnull(j[col]) and (j['rspe_chest_pain'] == 0 or j['rspe_chest_pain'] == 9):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'rspe_acute_retinal_detach':
                        if pd.isnull(j[col]) and (j['rspe_coughing_blood'] == 0 or j['rspe_coughing_blood'] == 9):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'rspe_any_pain':
                        if pd.isnull(j[col]) and (j['rspe_acute_retinal_detach'] == 0 or j['rspe_acute_retinal_detach'] == 9):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'rspe_diarrhea':
                        if pd.isnull(j[col]) and (j['rspe_any_pain'] == 0 or j['rspe_any_pain'] == 9):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'rspe_high_blood_pressure':
                        if pd.isnull(j[col]) and (j['rspe_diarrhea'] == 0 or j['rspe_diarrhea'] == 9):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'rspe_tb_diagnosed':
                        if pd.isnull(j[col]) and (j['rspe_high_blood_pressure'] == 0 or j['rspe_high_blood_pressure'] == 9):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'rspe_tb_treat_past4wks':
                        if pd.isnull(j[col]) and j['rspe_tb_diagnosed'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # spirometry eligibility and test
                    elif col in ['spiro_researcher', 'spiro_num_of_blows', 'spiro_num_of_vblows', 'spiro_pass']:
                        if pd.isnull(j[col]) and j['spiro_eligible'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # reversibility test
                    elif col in ['rspir_salb_time_admin', 'rspir_time_started',
                                 'rspir_researcher', 'rspir_num_of_blows', 'rspir_num_of_vblows']:
                        if pd.isnull(j[col]) and j['rspir_salb_admin'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # microbiome
                    elif col == 'micr_probiotics_t_period':
                        if pd.isnull(j[col]) and j['micr_probiotics_taken'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'micr_wormintestine_period':
                        if pd.isnull(j[col]) and j['micr_worm_intestine_treat'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # blood collection
                    elif col == 'bloc_last_ate_hrs':
                        if pd.isnull(j[col]) and pd.notna(j['bloc_last_eat_time']):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'bloc_red_tubes_num':
                        if pd.isnull(j[col]) and j['bloc_two_red_tubes'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'bloc_if_no_purple_tubes':
                        if pd.isnull(j[col]) and j['bloc_one_purple_tube'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'bloc_grey_tubes_no':
                        if pd.isnull(j[col]) and j['bloc_one_grey_tube'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'bloc_specify_reason':
                        if pd.isnull(j[col]) and j['bloc_urine_collected'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['bloc_urcontainer_batchnum', 'bloc_urine_tube_expiry']:
                        if pd.isnull(j[col]) and j['bloc_urine_collected'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # point of care testing
                    elif col == 'poc_comment':
                        if pd.isnull(j[col]) and j['poc_test_conducted'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['poc_instrument_serial_num', 'poc_test_strip_batch_num', 'poc_glucose_test_result', 'poc_chol_results_provided',
                                 'poc_teststrip_expiry_date', 'poc_test_date', 'poc_test_time', 'poc_chol_result', 'poc_hiv_strip_batch_num',
                                 'poc_hiv_strip_expiry_date']:
                        if pd.isnull(j[col]) and j['poc_test_conducted'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'poc_gluc_results_notes':
                        if pd.isnull(j[col]) and j['poc_gluc_results_provided'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'poc_chol_results_notes':
                        if pd.isnull(j[col]) and j['poc_chol_results_provided'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'poc_hiv_comment':
                        if pd.isnull(j[col]) and j['poc_hiv_test_conducted'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col in ['poc_hiv_pre_test', 'poc_test_kit_serial_num', 'poc_hiv_test_date_done', 'poc_technician_name', 'poc_hiv_test_result',
                                 'poc_result_provided', 'poc_post_test_counselling']:
                        if pd.isnull(j[col]) and j['poc_hiv_test_conducted'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'poc_pre_test_worker':
                        if pd.isnull(j[col]) and j['poc_hiv_pre_test'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'poc_post_test_worker':
                        if pd.isnull(j[col]) and j['poc_post_test_counselling'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    elif col == 'poc_hivpositive_firsttime':
                        if pd.isnull(j[col]) and j['poc_hiv_test_result'] == 1:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    # completion of questionnaire
                    elif col == 'comp_comment_no_1_13':
                        if pd.isnull(j[col]) and j['comp_sections_1_13'] == 0:
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])
                        else:
                            pass

                    else:
                        if col not in ignored_cols and pd.isnull(j[col]):
                            report_writer.writerow([str(i), str(instrument_dict.get(col[:4])), str(col), '1', 'missing value'])

        return '../resources/report.csv'

    def get_report_summary(self):
        report = pd.read_csv(self.write_report())
        df = report.groupby('variable_name')['study_id'].nunique()
        report_summary = pd.DataFrame(df)
        report_summary.to_csv("../resources/report_summary.csv")
