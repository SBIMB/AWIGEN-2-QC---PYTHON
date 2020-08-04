#from builtins import print

import pandas as pd
import csv
import os


class BranchingLogicHandler:

    def __init__(self, outputDir, csv_link, excel_writer):
        self.outputDir = outputDir

        # initiate data frame
        self.data = pd.read_csv(csv_link, index_col=0)

        self.excelWriter = excel_writer

    def get_missing_ids(self, input_df):
        if input_df.size == 0: return

        # Get all study_ids with missing values for each column
        output_df = input_df[input_df == True].dropna(how='all').stack().to_frame().reset_index()
        output_df.rename(columns={'level_1':'Data Field'}, inplace=True)
        output_df = output_df[['study_id', 'Data Field']]

        return output_df

    def check_a_phase_1_data(self):
        colNames = self.data.columns[(self.data.columns.str.contains('phase_1')) ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_participant_identification(self):
        colNames = self.data.columns[(self.data.columns.str.contains('gene_')) |
                                     (self.data.columns.str.contains('demo_')) |
                                     (self.data.columns.str.contains('language')) ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'demo_age_at_collection':
                mask = ( self.data[col].isna() & self.data['demo_dob_new'].isna() )
            elif col == 'demo_home_language':
                mask = ( self.data[col].isna() |
                       ( ( self.data[col] == 98 ) & self.data['other_home_language'].isna() ) )
            elif col == 'demo_gender_correction':
                mask = ( self.data[col].isna() &
                       ( self.data['demo_gender_is_correct'] == 0 ) )
            elif col == 'other_home_language':
                mask = ( self.data['demo_home_language'].isna() |
                       ( ( self.data['demo_home_language'] == 98 ) & self.data[col].isna() ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_ethnolinguistic_information(self):
        colNames = self.data.columns[(self.data.columns.str.contains('ethn')) ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'ethnicity':
                mask = ( ( (self.data[col].isna()) & (self.data['ethnicity_confirmation'] == 0) ) |
                         ( (self.data[col].isna()) & (self.data['ethnicity_confirmation'].isna()) ) )
            elif col == 'other_ethnicity':
                mask = ( self.data[col].isna() & ( self.data['ethnicity'] == 98 ) )
            elif col in self.ethnicity_cols:
                mask = ( self.data[col].isna() &
                      ~( self.data['gene_site'] == 2 ) &
                      ~( self.data['gene_site'] == 6 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_family_composition(self):
        colNames = self.data.columns[(self.data.columns.str.contains('famc')) ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'famc_number_of_brothers' or col == 'famc_number_of_sisters':
                mask = ( self.data[col].isna() & ( self.data['famc_siblings'] > 0 ) )
            elif col == 'famc_living_brothers':
                mask = ( self.data[col].isna() &
                         ( self.data['famc_siblings'] > 0 ) &
                         ( self.data['famc_number_of_brothers'] > 0 ) )
            elif col == 'famc_living_sisters':
                mask = ( self.data[col].isna() &
                         ( self.data['famc_siblings'] > 0 ) &
                         ( self.data['famc_number_of_sisters'] > 0 ) )
            elif col == 'famc_bio_sons' or col == 'famc_bio_daughters':
                mask = ( self.data[col].isna() &
                         ( self.data['famc_children'] > 0 ) )
            elif col == 'famc_living_bio_sons':
                mask = ( self.data[col].isna() &
                         ( self.data['famc_bio_sons'] > 0 ) )
            elif col == 'famc_living_bio_daughters':
                mask = ( self.data[col].isna() &
                         ( self.data['famc_bio_daughters'] > 0 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_pregnancy_and_menopause(self):
        colNames = self.data.columns[(self.data.columns.str.contains('preg')) ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'preg_num_of_live_births':
                mask = ( self.data[col].isna() &
                       ( self.data['preg_num_of_pregnancies'] > 0 ) )
            elif col == 'preg_last_period_mon' or col == 'preg_last_period_mon_2':
                mask = ( self.data[col].isna() &
                       ( self.data['preg_last_period_remember'] == 1 ) )
            elif col == 'preg_period_more_than_yr':
                mask = ( self.data[col].isna() &
                       ( ( self.data['preg_last_period_remember'] == 0 ) |
                         ( self.data['preg_last_period_remember'] == 2 ) ) )
            else:
                mask = ( self.data[col].isna() &
                        ( self.data['demo_gender'] == 0 ) &
                        ( self.data['preg_pregnant'] == 0 ) )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_civil_status_marital_status_education_employment(self):
        colNames = self.data.columns[(self.data.columns.str.contains('mari_')) |
                                     (self.data.columns.str.contains('educ_')) |
                                     (self.data.columns.str.contains('empl_')) |
                                     (self.data.columns.str.contains('civil_')) ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'educ_highest_years' or col == 'educ_formal_years':
                mask = ( self.data[col].isna() &
                         self.data['educ_highest_level'].between(2, 4, inclusive=True) )
            elif col == 'empl_days_work':
                mask = ( self.data[col].isna() &
                         self.data['empl_status'].between(2, 4, inclusive=True) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_cognition(self):
        colNames = self.data.columns[self.data.columns.str.contains('cogn_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_b_frailty_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('frai_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'frai_comment':
                mask = ( self.data[col].isna() &
                         ( self.data['frai_sit_stands_completed'] == 0 ) )
            elif col == 'frai_comment_why':
                mask = ( self.data[col].isna() &
                         ( self.data['frai_complete_procedure'] == 0 ) )
            elif col == 'frai_please_comment_why':
                mask = ( self.data[col].isna() &
                         ( self.data['frai_procedure_walk_comp'] == 0 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_household_attributes(self):
        colNames = self.data.columns[self.data.columns.str.contains('hous_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'hous_microwave':
                mask = ( self.data[col].isna() &
                         self.data['gene_site'].isin( [1, 2, 3, 6] ) )
            elif col == 'hous_power_generator' or col == 'hous_telephone' or col == 'hous_toilet_facilities':
                mask = ( self.data[col].isna() &
                         self.data['gene_site'].isin( [1, 2, 4, 5, 6] ) )
            elif col == 'hous_washing_machine' or col == 'hous_computer_or_laptop' or col == 'hous_internet_by_m_phone':
                mask = ( self.data[col].isna() &
                         self.data['gene_site'].isin( [1, 2, 3, 5, 6] ) )
            elif col == 'hous_internet_by_computer':
                mask = ( self.data[col].isna() &
                         self.data['gene_site'].isin( [1, 2, 5, 6] ) )
            elif col == 'hous_electric_iron':
                mask = ( self.data[col].isna() &
                         self.data['gene_site'].isin( [3, 5] ) )
            elif col in ['hous_fan', 'hous_table', 'hous_sofa', 'hous_bed', 'hous_mattress', 'hous_blankets']:
                mask = ( self.data[col].isna() &
                         self.data['gene_site'].isin( [3, 4, 5] ) )
            elif col in ['hous_kerosene_stove', 'hous_electric_plate', 'hous_torch', 'hous_gas_lamp', 'hous_kerosene_lamp', 'hous_wall_clock']:
                mask = ( self.data[col].isna() &
                         self.data['gene_site'].isin( [3, 4] ) )
            elif col in ['hous_plate_gas', 'hous_grinding_mill']:
                mask = ( self.data[col].isna() &
                         self.data['gene_site'].isin( [4, 5] ) )
            elif col == 'hous_portable_water':
                mask = ( self.data[col].isna() &
                         ( self.data['gene_site'] == 5 ) )
            elif col in ['hous_cattle', 'hous_other_livestock', 'hous_poultry']:
                mask = ( self.data[col].isna() &
                         self.data['gene_site'].isin( [1, 2, 3, 4, 5] ) )
            elif col in ['hous_tractor', 'hous_plough']:
                mask = ( self.data[col].isna() &
                         self.data['gene_site'].isin( [1, 2, 4, 5] ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_substance_use(self):
        colNames = self.data.columns[self.data.columns.str.contains('subs_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col in ['subs_smoke_100', 'subs_smoke_now']:
                mask = ( self.data[col].isna() &
                         ( self.data['subs_tobacco_use'] == 1 ) )
            elif col in ['subs_smoke_last_hour', 'subs_smoking_frequency', 'subs_smoking_start_age']:
                mask = ( self.data[col].isna() &
                         ( self.data['subs_smoke_now'] == 1 ) )
            elif col == 'subs_smoke_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['subs_smoke_cigarettes___5'] == 1 ) )
            elif col == 'subs_smoke_per_day':
                mask = ( self.data[col].isna() &
                       ( self.data['subs_smoking_frequency'].isin( [1, 2, 3, 4, 5] ) ) )
            elif col == 'subs_smoking_stop_year':
                mask = ( self.data[col].isna() &
                       ( self.data['subs_tobacco_use'] == 1 ) &
                       ( self.data['subs_smoke_now'] == 0 ) )
            elif col in ['subs_snuff_use', 'subs_tobacco_chew_use']:
                mask = ( self.data[col].isna() &
                         ( self.data['subs_smokeless_tobacc_use'] == 1 ) )
            elif col in ['subs_snuff_method_use', 'subs_snuff_use_freq']:
                mask = ( self.data[col].isna() &
                         ( self.data['subs_snuff_use'] == 1 ) )
            elif col == 'subs_freq_snuff_use':
                mask = ( self.data[col].isna() &
                         self.data['subs_snuff_use_freq'].isin( [1, 2, 3, 4, 5] ) )
            elif col == 'subs_tobacco_chew_freq':
                mask = ( self.data[col].isna() &
                         ( self.data['subs_tobacco_chew_use'] == 1 ) )
            elif col == 'subs_tobacco_chew_d_freq':
                mask = ( self.data[col].isna() &
                         self.data['subs_tobacco_chew_freq'].isin( [1, 2, 3, 4, 5] ) )
            elif col == 'subs_alcohol_consume_now':
                mask = ( self.data[col].isna() &
                         ( self.data['subs_alcohol_consump'] == 1 ) )
            elif col in['subs_alcohol_consump_freq', 'subs_alcohol_criticize', ' subs_alcohol_guilty', 'subs_alcohol_hangover',
                        'subs_alcoholtype_consumed___1', 'subs_alcoholtype_consumed___2', 'subs_alcoholtype_consumed___3',
                        'subs_alcoholtype_consumed___4', 'subs_alcoholtype_consumed___5']:
                mask = ( self.data[col].isna() &
                       ( ( self.data['subs_alcohol_consume_now'] == 1 ) |
                         ( self.data['subs_alcohol_consump'] == 1 ) ) )
            elif col == 'subs_alcohol_consume_freq':
                mask = ( self.data[col].isna() &
                         self.data['subs_alcohol_consump_freq'].isin( [1, 2, 3, 4, 5] ) )
            elif col == 'subs_alcohol_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['subs_alcoholtype_consumed___5'] == 1 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_general_health(self):
        colNames = self.data.columns[self.data.columns.str.contains('genh_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col in ['genh_breast_cancer_treat', 'genh_bre_cancer_trad_med']:
                mask = ( self.data[col].isna() &
                         ( self.data['genh_breast_cancer'] == 1 ) )
            elif col == 'genh_bre_cancer_treat_now':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_breast_cancer_treat'] == 1 ) )  #check this one
            elif col == 'genh_breast_cancer_meds':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_bre_cancer_treat_now'] == 1 ) )
            elif col == 'genh_cervical_cancer':
                mask = ( self.data[col].isna() &
                         ( self.data['demo_gender'] == 0 ) )
            elif col in ['genh_cer_cancer_treat', 'genh_cer_cancer_trad_med']:
                mask = ( self.data[col].isna() &
                         ( self.data['genh_cervical_cancer'] == 1 ) )
            elif col == 'genh_cer_cancer_treat_now':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_cer_cancer_treat'] == 1 ) )  #check this one
            elif col == 'genh_cervical_cancer_meds':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_cer_cancer_treat_now'] == 1 ) )
            elif col == 'genh_prostate_cancer':
                mask = ( self.data[col].isna() &
                         ( self.data['demo_gender'] == 1 ) )
            elif col in ['genh_pro_cancer_treat', 'genh_pro_cancer_trad_med']:
                mask = ( self.data[col].isna() &
                         ( self.data['genh_prostate_cancer'] == 1 ) )
            elif col == 'genh_pro_cancer_treat_now':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_pro_cancer_treat'] == 1 ) )
            elif col == 'genh_prostate_cancer_meds':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_pro_cancer_treat_now'] == 1 ) )
            elif col in ['genh_oes_cancer_treat', 'genh_oesophageal_trad_med']:
                mask = ( self.data[col].isna() &
                         ( self.data['genh_oesophageal_cancer'] == 1 ) )
            elif col == 'genh_oes_cancer_treat_now':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_oes_cancer_treat'] == 1 ) )
            elif col == 'genh_oes_cancer_meds':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_oes_cancer_treat_now'] == 1 ) )
            elif col in ['genh_cancer_specify_other', 'genh_oth_cancer_trad_med']:
                mask = ( self.data[col].isna() &
                         ( self.data['genh_other_cancers'] == 1 ) )
            elif col == 'genh_oth_cancer_treat_now':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_other_cancer_treat'] == 1 ) )
            elif col == 'genh_other_cancer_meds':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_oth_cancer_treat_now'] == 1 ) )
            elif col in ['genh_starchy_staple_freq', 'genh_staple_servings']:
                mask = ( self.data[col].isna() & ( ( self.data['genh_starchy_staple_food___1'] == 1 ) |
                                                   ( self.data['genh_starchy_staple_food___2'] == 1 ) |
                                                   ( self.data['genh_starchy_staple_food___3'] == 1 ) |
                                                   ( self.data['genh_starchy_staple_food___4'] == 1 ) |
                                                   ( self.data['genh_starchy_staple_food___5'] == 1 ) |
                                                   ( self.data['genh_starchy_staple_food___6'] == 1 ) |
                                                   ( self.data['genh_starchy_staple_food___7'] == 1 ) |
                                                   ( self.data['genh_starchy_staple_food___8'] == 1 ) |
                                                   ( self.data['genh_starchy_staple_food___9'] == 1 ) |
                                                   ( self.data['genh_starchy_staple_food___10'] == 1 ) |
                                                   ( self.data['genh_starchy_staple_food___11'] == 1 ) |
                                                   ( self.data['genh_starchy_staple_food___12'] == 1 ) ) )
            elif col == 'genh_pesticide_years':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_pesticide'] == 1 ) )
            elif col == 'genh_pesticide_list':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_pesticide_type'] == 1 ) )
            elif col == 'genh_cookingplace_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_cooking_place'] == 3 ) )
            elif col == 'genh_energy_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_energy_source_type___6'] == 6 ) )
            elif col == 'genh_smoke_freq_someone':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_smoker_in_your_house'] == 1 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_infection_history(self):
        colNames = self.data.columns[self.data.columns.str.contains('infh_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'infh_malaria_month':
                mask = ( self.data[col].isna() &
                         ( self.data['infh_malaria'] == 1 ) )
            elif col in ['infh_tb_12months', 'infh_tb_treatment', 'infh_tb_meds', 'infh_tb_counselling']:
                mask = ( self.data[col].isna() &
                         ( self.data['infh_tb'] == 1 ) )
            elif col == 'infh_tb_diagnosed':
                mask = ( self.data[col].isna() &
                         ( self.data['infh_tb_12months'] == 1 ) )
            elif col in ['infh_hiv_tested', 'infh_hiv_status', 'infh_hiv_positive']:
                mask = ( self.data[col].isna() &
                         ( self.data['infh_hiv_que_answering'] == 1 ) )
            elif col in ['infh_hiv_diagnosed', 'infh_hiv_medication', 'infh_hiv_traditional_meds', 'infh_painful_feet_hands',
                         'infh_hypersensitivity', 'infh_kidney_problems', 'infh_liver_problems',
                         'infh_change_in_body_shape', 'infh_mental_state_change', 'infh_chol_levels_change']:
                mask = ( self.data[col].isna() &
                         ( self.data['infh_hiv_positive'] == 1 ) )
            elif col in ['infh_hiv_treatment', 'infh_hiv_arv_meds']:
                mask = ( self.data[col].isna() &
                         ( self.data['infh_hiv_medication'] == 1 ) )
            elif col == 'infh_hiv_arv_meds_now':
                mask = ( self.data[col].isna() &
                         ( self.data['infh_hiv_status'] == 1 ) )
            elif col in ['infh_hiv_arv_meds_specify', 'infh_hiv_arv_single_pill']:
                mask = ( self.data[col].isna() &
                         ( self.data['infh_hiv_arv_meds_now'] == 1 ) )
            elif col == 'infh_hiv_pill_size':
                mask = ( self.data[col].isna() &
                         ( self.data['infh_hiv_arv_single_pill'] == 1 ) )
            elif col == 'infh_hiv_counselling':
                mask = ( self.data[col].isna() &
                         ( self.data['infh_hiv_test'] == 1 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_a_cardiometabolic_risk_factors(self):
        colNames = self.data.columns[self.data.columns.str.contains('carf_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col in ['carf_diabetes_12months', 'carf_diabetes_treatment']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes'] == 1 ) )
            elif col == 'carf_diabetes_treat_now':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_treatment'] == 1 ) )
            elif col == 'carf_diabetetreat_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_treat___5'] == 1 ) )
            elif col == 'carf_diabetes_meds_2':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_treat_now'] == 1 ) )
            elif col == 'carf_diabetes_traditional':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_12months'] == 1 ) )
            elif col in ['carf_diabetes_mother', 'carf_diabetes_father', 'carf_diabetes_brother_1','carf_diabetes_brother_2',
                        'carf_diabetes_brother_3',	'carf_diabetes_brother_4', 'carf_diabetes_sister_1', 'carf_diabetes_sister_2',
                        'carf_diabetes_sister_3', 'carf_diabetes_sister_4', 'carf_diabetes_son_1', 'carf_diabetes_son_2',
                        'carf_diabetes_son_3',	'carf_diabetes_son_4', 'carf_daughter_diabetes_1', 'carf_diabetes_daughter_2',
                        'carf_diabetes_daughter_3', 'carf_diabetes_daughter_4', 'carf_diabetes_fam_other']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) )
            elif col == 'carf_diabetes_fam_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_fam_other'] == 1 ) )
            elif col == 'carf_stroke_diagnosed':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_stroke'] == 1 ) )
            elif col in ['carf_angina_treatment', 'carf_angina_treat_now', 'carf_angina_meds', 'carf_angina_traditional', 'carf_pain']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_angina'] == 1 ) )
            elif col in ['carf_pain_action_stopslow', 'carf_relief_standstill']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_pain2'] == 1 ) )
            elif col in ['carf_heartattack_treat', 'carf_heartattack_trad']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_heartattack'] == 1 ) )
            elif col == 'carf_heartattack_meds':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_heartattack_treat'] == 1 ) )
            elif col in ['carf_chf_treatment', 'carf_chf_treatment_now', 'carf_chf_meds']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_congestiv_heart_fail'] == 1 ) )
            elif col in ['carf_hypertension_12mnths', 'carf_hypertension_treat',
                         'carf_hypertension_meds', 'carf_hypertension_medlist']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_hypertension'] == 1 ) )
            elif col in ['carf_chol_treatment', 'carf_chol_medicine']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_h_cholesterol'] == 1 ) )
            elif col == 'carf_chol_treat_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_chol_treatment_now___4'] == 1 ) )
            elif col in ['carf_thyroid_type', 'carf_thyroid_treatment', 'carf_parents_thyroid']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_thyroid'] == 1 ) )
            elif col == 'carf_thryroid_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_thyroid_type'] == 1 ) )
            elif col == 'carf_thyroid_treat_use':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_thyroid_treatment'] == 1 ) )
            elif col == 'carf_thyroidparnt_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_parents_thyroid'] == 1 ) )
            elif col in ['carf_kidney_disease_known', 'carf_kidney_function_low']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_kidney_disease'] == 1 ) )
            elif col == 'carf_kidneydiseas_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_kidney_disease_known'] == 1 ) )
            elif col in ['carf_kidney_family_mother', 'carf_kidney_family_father', 'carf_kidney_family_other']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_kidney_family'] == 1 ) )
            elif col == 'carf_kidney_fam_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_kidney_family_other'] == 1 ) )
            elif col == 'carf_kidney_family_type':
                mask = ( self.data[col].isna() &
                         ( ( self.data['carf_kidney_family_other'] == 1 ) |
                           ( self.data['carf_kidney_family_mother'] == 1 ) |
                           ( self.data['carf_kidney_family_father'] == 1 ) ) )
            elif col == 'carf_kidney_fam_tspecify':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_kidney_family_type'] == 1 ) )
            elif col in ['carf_joints_swollen', 'carf_joints_involved', 'carf_when_they_hurt', 'carf_symptoms_how_long']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_joints_swollen_pain'] == 1 ) )
            # elif col in ['carf_rheumatoid_factor', 'carf_acpa', 'carf_esr_crp']:
            #     mask = ( self.data[col].isna() &
            #              ( self.data['carf_arthritis_results'] == 1 ) )   # TODO Not present for agincourt
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_physical_activity_and_sleep(self):
        colNames = self.data.columns[self.data.columns.str.contains('gpaq_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col in ['gpaq_work_vigorous_days', 'gpaq_work_vigorous_time',
                       'gpaq_work_vigorous_hrs', 'gpaq_work_vigorous_mins']:
                mask = ( self.data[col].isna() &
                         ( self.data['gpaq_work_vigorous'] == 1 ) )
            elif col in ['gpaq_work_moderate_days', 'gpaq_work_moderate_time',
                         'gpaq_work_moderate_hrs', 'gpaq_work_moderate_mins']:
                mask = ( self.data[col].isna() &
                         ( self.data['gpaq_work_moderate'] == 1 ) )
            elif col in ['gpaq_transport_phy_days', 'gpaq_transport_phy_time',
                         'gpaq_transport_phy_hrs', 'gpaq_transport_phy_mins']:
                mask = ( self.data[col].isna() &
                         ( self.data['gpaq_transport_phy'] == 1 ) )
            elif col == 'gpaq_leisurevigorous_days':
                mask = ( self.data[col].isna() &
                         ( ( self.data['gpaq_leisure_phy'] == 1 ) |
                           ( self.data['gpaq_leisure_vigorous'] == 1 ) ) )
            elif col in ['gpaq_leisurevigorous_time', 'gpaq_leisurevigorous_hrs', 'gpaq_leisurevigorous_mins']:
                mask = ( self.data[col].isna() &
                         ( self.data['gpaq_leisurevigorous_days'].notna() ) )
            elif col == 'gpaq_leisuremoderate_days':
                mask = ( self.data[col].isna() &
                         ( self.data['gpaq_leisuremoderate'] == 1 ) )
            elif col in ['gpaq_leisurevigorous_time', 'gpaq_leisurevigorous_hrs', 'gpaq_leisurevigorous_mins']:
                mask = ( self.data[col].isna() &
                         ( self.data['gpaq_leisurevigorous_days'].notna() ) )
            elif col in ['gpaq_leisuremoderate_time', 'gpaq_leisuremoderate_hrs', 'gpaq_leisuremoderate_mins']:
                mask = ( self.data[col].isna() &
                         ( self.data['gpaq_leisuremoderate_days'].notna() ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_anthropometric_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('anth_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_blood_pressure_and_pulse_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('bppm_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_ultrasound_and_dxa_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('ultr_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'ultr_comment':
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_vat_scat_measured'] == 0 ) )
            elif col in ['ultr_technician', 'ultr_visceral_fat', 'ultr_subcutaneous_fat']:
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_vat_scat_measured'] == 1 ) )
            elif col == 'ultr_cimt_comment':
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_cimt'] == 0 ) ) # TODO check
            elif col in ['ultr_cimt_technician', 'ultr_cimt_right_min',	'ultr_cimt_right_max',
                         'ultr_cimt_right_mean', 'ultr_cimt_left_min', 'ultr_cimt_left_max', 'ultr_cimt_left_mean']:
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_cimt'] == 1 ) )
            elif col == 'ultr_plaque_comment':
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_plaque_measured'] == 0 ) )  # TODO ultr_plaque == ultr_plaque_measured?? in Soweto
            elif col in ['ultr_plaque_technician', 'ultr_plaque_present', 'ultr_plaque_right_min', 'ultr_plaque_right_max',
                         'ultr_plaque_right_mean',	'ultr_plaque_left_min',	'ultr_plaque_left_max',	'ultr_plaque_left_mean']:
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_plaque_measured'] == 1 ) )
            elif col == 'ultr_dxa_scan_comment':
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_dxa_scan_completed'] == 0 ) )
            elif col in ['ultr_dxa_measurement_1', 'ultr_dxa_measurement_2', 'ultr_dxa_measurement_3',
                         'ultr_dxa_measurement_4',	'ultr_dxa_measurement_5']:
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_dxa_scan_completed'] == 1 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_a_respiratory_health(self):
        colNames = self.data.columns[self.data.columns.str.contains('resp_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col in ['resp_age_diagnosed', 'resp_asthma_treat']:
                mask = ( self.data[col].isna() &
                         ( self.data['resp_asthma_diagnosed'] == 1 ) )
            elif col == 'resp_asthma_treat_now':
                mask = ( self.data[col].isna() &
                         ( self.data['resp_asthma_treat'] == 1 ) )
            elif col == 'resp_copd_treat':
                mask = ( self.data[col].isna() &
                        ( ( self.data['resp_copd_suffer___1'] == 1 ) |
                          ( self.data['resp_copd_suffer___2'] == 1 ) |
                          ( self.data['resp_copd_suffer___3'] == 1 ) ) )
            elif col in ['resp_medication_list', 'resp_puffs_time', 'resp_puffs_times_day']:
                mask = ( self.data[col].isna() &
                         ( self.data['resp_inhaled_medication'] == 1 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_b_spirometry_eligibility(self):
        colNames = self.data.columns[self.data.columns.str.contains('rspe_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'rspe_chest_pain':
                mask = ( self.data[col].isna() &
                        ( ( self.data['rspe_major_surgery'] == 0 ) |
                          ( self.data['rspe_major_surgery'] == 9 ) ) )
            elif col == 'rspe_coughing_blood':
                mask = ( self.data[col].isna() &
                        ( ( self.data['rspe_chest_pain'] == 0 ) |
                          ( self.data['rspe_chest_pain'] == 9 ) ) )
            elif col == 'rspe_acute_retinal_detach':
                mask = ( self.data[col].isna() &
                        ( ( self.data['rspe_coughing_blood'] == 0 ) |
                          ( self.data['rspe_coughing_blood'] == 9 ) ) )
            elif col == 'rspe_any_pain':
                mask = ( self.data[col].isna() &
                        ( ( self.data['rspe_acute_retinal_detach'] == 0 ) |
                          ( self.data['rspe_acute_retinal_detach'] == 9 ) ) )
            elif col == 'rspe_diarrhea':
                mask = ( self.data[col].isna() &
                        ( ( self.data['rspe_any_pain'] == 0 ) |
                          ( self.data['rspe_any_pain'] == 9 ) ) )
            elif col == 'rspe_high_blood_pressure':
                mask = ( self.data[col].isna() &
                        ( ( self.data['rspe_diarrhea'] == 0 ) |
                          ( self.data['rspe_diarrhea'] == 9 ) ) )
            elif col == 'rspe_tb_diagnosed':
                mask = ( self.data[col].isna() &
                        ( ( self.data['rspe_high_blood_pressure'] == 0 ) |
                          ( self.data['rspe_high_blood_pressure'] == 9 ) ) )
            elif col == 'rspe_tb_treat_past4wks':
                mask = ( self.data[col].isna() &
                         ( self.data['rspe_tb_diagnosed'] == 1 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_c_spirometry_test(self):
        colNames = self.data.columns[self.data.columns.str.contains('spiro_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col in ['spiro_researcher', 'spiro_num_of_blows', 'spiro_num_of_vblows', 'spiro_pass']:
                mask = ( self.data[col].isna() &
                         ( self.data['spiro_eligible'] == 1 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_d_reversibility_test(self):
        colNames = self.data.columns[self.data.columns.str.contains('rspir_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col in ['rspir_salb_time_admin', 'rspir_time_started',
                        'rspir_researcher', 'rspir_num_of_blows', 'rspir_num_of_vblows']:
                mask = ( self.data[col].isna() &
                         ( self.data['rspir_salb_admin'] == 1 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_a_microbiome(self):
        colNames = self.data.columns[self.data.columns.str.contains('micr_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'micr_probiotics_t_period':
                mask = ( self.data[col].isna() &
                         ( self.data['micr_probiotics_taken'] == 1 ) )
            elif col == 'micr_wormintestine_period':
                mask = ( self.data[col].isna() &
                         ( self.data['micr_worm_intestine_treat'] == 1 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_b_blood_collection_c_urine_collection(self):
        colNames = self.data.columns[self.data.columns.str.contains('bloc_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'bloc_last_ate_hrs':
                mask = ( self.data[col].isna() &
                         ( self.data['bloc_last_eat_time'].notna() ) )
            elif col == 'bloc_red_tubes_num':
                mask = ( self.data[col].isna() &
                         ( self.data['bloc_two_red_tubes'] == 0 ) )
            elif col == 'bloc_if_no_purple_tubes':
                mask = ( self.data[col].isna() &
                         ( self.data['bloc_one_purple_tube'] == 0 ) )
            elif col == 'bloc_grey_tubes_no':
                mask = ( self.data[col].isna() &
                         ( self.data['bloc_one_grey_tube'] == 0 ) )
            elif col == 'bloc_specify_reason':
                mask = ( self.data[col].isna() &
                         ( self.data['bloc_urine_collected'] == 0 ) )
            elif col in ['bloc_urcontainer_batchnum', 'bloc_urine_tube_expiry']:
                mask = ( self.data[col].isna() &
                         ( self.data['bloc_urine_collected'] == 1 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_point_of_care_testing(self):
        colNames = self.data.columns[self.data.columns.str.contains('poc_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'poc_comment':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_test_conducted'] == 0 ) )
            elif col in ['poc_instrument_serial_num', 'poc_test_strip_batch_num', 'poc_glucose_test_result', 'poc_chol_results_provided',
                         'poc_teststrip_expiry_date', 'poc_test_date', 'poc_test_time', 'poc_chol_result', 'poc_hiv_strip_batch_num',
                         'poc_hiv_strip_expiry_date']:
                mask = ( self.data[col].isna() &
                         ( self.data['poc_test_conducted'] == 1 ) )
            elif col == 'poc_gluc_results_notes':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_gluc_results_provided'] == 0 ) )
            elif col == 'poc_chol_results_notes':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_chol_results_provided'] == 0 ) )
            elif col == 'poc_hiv_comment':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_hiv_test_conducted'] == 0 ) )
            elif col in ['poc_hiv_pre_test', 'poc_test_kit_serial_num',
                         'poc_hiv_test_date_done', 'poc_technician_name', 'poc_hiv_test_result',
                         'poc_result_provided', 'poc_post_test_counselling']:
                mask = ( self.data[col].isna() &
                         ( self.data['poc_hiv_test_conducted'] == 1 ) )
            elif col == 'poc_pre_test_worker':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_hiv_pre_test'] == 1 ) )
            elif col == 'poc_post_test_worker':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_post_test_counselling'] == 1 ) )
            elif col == 'poc_hivpositive_firsttime':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_hiv_test_result'] == 1 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_trauma(self):
        colNames = self.data.columns[self.data.columns.str.contains('tram_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_completion_of_questionnaire(self):
        colNames = self.data.columns[self.data.columns.str.contains('comp_')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'comp_comment_no_1_13':
                mask = ( self.data[col].isna() &
                         ( self.data['comp_sections_1_13'] == 0 ) )
            else:
                mask = self.data[col].isna()

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)


    def write_report(self):
        df = pd.DataFrame()

        for instrument_name, instrument_checker in self.instrument_dict.items():
            missing_df = instrument_checker(self)
            missing_df['Instrument'] = instrument_name
            df = df.append(missing_df)

        df['Comment'] = ""

        df = df.sort_values(by=['study_id', 'Instrument'])
        df.to_excel(self.excelWriter, sheet_name='Missing Data', startcol=0, startrow=0, index=False)
        self.excelWriter.sheets['Missing Data'].set_column(0, 0 , 20)
        self.excelWriter.sheets['Missing Data'].set_column(1, 1 , 30)
        self.excelWriter.sheets['Missing Data'].set_column(2, 2 , 30)
        self.excelWriter.sheets['Missing Data'].set_column(3, 3 , 30)

    instrument_dict = {
        'a_phase_1_data'                    : check_a_phase_1_data,
        'participant_identification'        : check_participant_identification,
        'ethnolinguistic_information'       : check_ethnolinguistic_information,
        'family_composition'                : check_family_composition,
        'pregnancy_and_menopause'           : check_pregnancy_and_menopause,
        'civil_status_marital_status_education_employment' : check_civil_status_marital_status_education_employment,
        'a_cognition'                       : check_cognition,
        'b_frailty_measurements'            : check_b_frailty_measurements,
        'household_attributes'              : check_household_attributes,
        'substance_use'                     : check_substance_use,
        'general_health'                    : check_general_health,
        'infection_history'                 : check_infection_history,
        'a_cardiometabolic_risk_factors'    : check_a_cardiometabolic_risk_factors,
        'physical_activity_and_sleep'       : check_physical_activity_and_sleep,
        'anthropometric_measurements'       : check_anthropometric_measurements,
        'blood_pressure_and_pulse_measurements' : check_blood_pressure_and_pulse_measurements,
        'ultrasound_and_dxa_measurements'   : check_ultrasound_and_dxa_measurements,
        'a_respiratory_health'              : check_a_respiratory_health,
        'b_spirometry_eligibility'          : check_b_spirometry_eligibility,
        'c_spirometry_test'                 : check_c_spirometry_test,
        'd_reversibility_test'              : check_d_reversibility_test,
        'a_microbiome'                      : check_a_microbiome,
        'b_blood_collection_c_urine_collection' : check_b_blood_collection_c_urine_collection,
        'point_of_care_testing'             : check_point_of_care_testing,
        'trauma'                            : check_trauma,
        'completion_of_questionnaire'       : check_completion_of_questionnaire
        }

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
