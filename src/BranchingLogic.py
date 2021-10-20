import pandas as pd
import csv
import os

class BranchingLogicHandler:
    def __init__(self, data, site):
        self.data = data.set_index('study_id')
        self.site = site

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
                                     (self.data.columns.str.contains('language')) |
                                     (self.data.columns.str.contains('ethnicity')) |
                                     (self.data.columns.str.contains('participant')) ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if (col in self.ignored_cols) or ('phase_1' in col):
                continue
            # elif col == 'gene_uni_site_id_is_correct':
            #     mask = ( self.data[col].isna() &
            #             ~( self.data['phase_1_unique_site_id'].isna() ) )
            # elif col == 'gene_uni_site_id_correct':
            #     mask = ( self.data[col].isna() &
            #            ( self.data['gene_uni_site_id_is_correct'] == 0 ) ) #Missing for DIMAMO

            elif col == 'demo_dob_new':
                mask = ( self.data[col].isna() &
                       ( self.data['demo_dob_is_correct'] == 0 ) )

            elif col == 'demo_gender_correction':
                mask = ( self.data[col].isna() &
                       ( ( self.data['demo_gender_is_correct'] == 0 ) |
                         ( self.data['demo_gender_is_correct'].isna() ) ) )
            elif col == 'demo_gender_is_correct':
                mask = ( self.data[col].isna() &
                       ( self.data['demo_gender_correction'].isna() ) )

            elif col == 'home_language_confirmation':
                mask = ( self.data[col].isna() &
                       ( self.data['home_language'].isna() ) )
            elif col == 'home_language':
                mask = ( self.data[col].isna() &
                       ( ( self.data['home_language_confirmation'] == 0 ) |
                         ( self.data['home_language_confirmation'].isna() ) ) )
            elif col == 'demo_home_language':
                mask = ( self.data[col].isna() &
                       ( self.data['home_language'].isna() ) )
            elif col == 'other_home_language':
                mask = ( self.data[col].isna() &
                       ( self.data['home_language'] == 98 ) )

            elif col == 'ethnicity_confirmation':
                mask = ( self.data[col].isna() &
                       ( self.data['ethnicity'].isna() ) )
            elif col == 'ethnicity':
                mask = ( self.data[col].isna() &
                       ( self.data['ethnicity_confirmation'] == 0 ) )
            elif col == 'other_ethnicity':
                mask = ( self.data[col].isna() &
                       ( self.data['ethnicity'] == 98 ) )

            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['participant_identification_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_ethnolinguistic_information(self):
        colNames = self.data.columns[(self.data.columns.str.contains('ethn_')) |
                                     (self.data.columns.str.contains('ethnolinguistic_information_complete')) ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif 'sa' in col:
                mask = ( self.data[col].isna() &
                       ( self.data['ethnolinguistc_available'] == 0 ) &
                       ( ( self.data['gene_site'] == 2 ) |
                         ( self.data['gene_site'] == 6 ) ) )
            elif ('sa' not in col) and ('ethn_' in col):
                string = col.rsplit('_', 1)[0] + '_sa'

                mask = ( self.data[col].isna() &
                       ( self.data[string] == 98 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['ethnolinguistic_information_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_family_composition(self):
        colNames = self.data.columns[(self.data.columns.str.contains('famc_')) |
                                     (self.data.columns.str.contains('family_composition_complete')) ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col == 'famc_number_of_brothers' or col == 'famc_number_of_sisters':
                mask = ( self.data[col].isna() &
                       ( self.data['famc_siblings'] > 0 ) )
            elif col == 'famc_living_brothers':
                mask = ( self.data[col].isna() &
                       ( self.data['famc_number_of_brothers'] > 0 ) )
            elif col == 'famc_living_sisters':
                mask = ( self.data[col].isna() &
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

            mask = mask & ( self.data['family_composition_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_pregnancy_and_menopause(self):
        colNames = self.data.columns[(self.data.columns.str.contains('preg_')) |
                                     (self.data.columns.str.contains('pregnancy_and_menopause_complete')) ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col == 'preg_pregnant':
                mask = ( self.data[col].isna() &
                       ( self.data['demo_gender_is_correct'] == 1 ) )
            elif col == 'preg_num_of_live_births':
                mask = ( self.data[col].isna() &
                       ( self.data['preg_pregnant'] == 0 ) &
                       ( self.data['preg_num_of_pregnancies'] > 0 ) )
            elif col in ['preg_last_period_mon', 'preg_last_period_mon_2', 'preg_last_period']:
                mask = ( self.data[col].isna() &
                       ( self.data['preg_pregnant'] == 0 ) &
                       ( self.data['preg_last_period_remember'] == 1 ) )
            elif col == 'preg_period_more_than_yr':
                mask = ( self.data[col].isna() &
                       ( self.data['preg_pregnant'] == 0 ) &
                       ( ( self.data['preg_last_period_remember'] == 0 ) |
                         ( self.data['preg_last_period_remember'] == 2 ) ) )
            else:
                mask = ( self.data[col].isna() &
                        ( self.data['preg_pregnant'] == 0 ) )

            mask = ( mask &
                   ( self.data['demo_gender'] == 0 ) &
                   ( self.data['pregnancy_and_menopause_complete'] == 2 ) )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_civil_status_marital_status_education_employment(self):
        colNames = self.data.columns[(self.data.columns.str.contains('mari_')) |
                                     (self.data.columns.str.contains('educ_')) |
                                     (self.data.columns.str.contains('empl_')) |
                                     (self.data.columns.str.contains('civil_')) |
                                     (self.data.columns.str.contains('civil_status_marital_status_education_employment_complete')) ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue
            elif col == 'educ_highest_years' or col == 'educ_formal_years':
                mask = ( self.data[col].isna() &
                         self.data['educ_highest_level'].between(2, 4, inclusive=True) )
            elif col == 'empl_days_work':
                mask = ( self.data[col].isna() &
                         self.data['empl_status'].between(1, 4, inclusive=True) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['civil_status_marital_status_education_employment_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_a_cognition_one(self):
        colNames = self.data.columns[ ( self.data.columns.str.contains('cogn_') &
                                      ~(self.data.columns.str.contains('delayed')) &
                                      ~(self.data.columns.str.contains('word_cog')) &
                                      ~(self.data.columns.str.contains('recognition_score')) &
                                      ~(self.data.columns.str.contains('animals')) &
                                      ~(self.data.columns.str.contains('comments')) ) |
                                     self.data.columns.str.contains('a_cognition_one_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue
            elif col == 'cogn_orientation_score':
                mask = ( self.data[col].isna() &
                         ( self.data['cogn_year'] * 1 >= 0  ) &
                         ( self.data['cogn_what_is_the_month'] * 1 >= 0 ) &
                         ( self.data['cogn_day_of_the_month'] * 1 >= 0 ) &
                         ( self.data['cogn_country_of_residence'] * 1 >= 0 ) &
                         ( self.data['cogn_district_province'] * 1 >= 0 ) &
                         ( self.data['cogn_village_town_city'] * 1 >= 0 ) &
                         ( self.data['cogn_weekdays_forward'] * 1 >= 0 ) &
                         ( self.data['cogn_weekdays_backwards'] * 1 >= 0 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['a_cognition_one_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_b_frailty_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('frai_') |
                                     self.data.columns.str.contains('b_frailty_measurements_complete') ]

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

            mask = mask & ( self.data['b_frailty_measurements_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_c_cognition_two(self):
        colNames = self.data.columns[( self.data.columns.str.contains('cogn_') &
                                     ( self.data.columns.str.contains('delayed') |
                                       self.data.columns.str.contains('word_cog') |
                                       self.data.columns.str.contains('recognition_score') |
                                       self.data.columns.str.contains('animals') |
                                       self.data.columns.str.contains('comments') ) ) |
                                       self.data.columns.str.contains('c_cognition_two_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['c_cognition_two_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_household_attributes(self):
        colNames = self.data.columns[self.data.columns.str.contains('hous_') |
                                     self.data.columns.str.contains('household_attributes_complete') ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col == 'hous_microwave':
                mask = ( self.data[col].isna() &
                         self.data['gene_site'].isin( [1, 2, 3, 6] ) )
            elif col in ['hous_power_generator','hous_telephone','hous_toilet_facilities']:
                mask = ( self.data[col].isna() &
                         self.data['gene_site'].isin( [1, 2, 4, 5, 6] ) )
            elif col in ['hous_washing_machine','hous_computer_or_laptop','hous_internet_by_m_phone']:
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

            mask = mask & ( self.data['household_attributes_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_substance_use(self):
        colNames = self.data.columns[self.data.columns.str.contains('subs_') |
                                     self.data.columns.str.contains('substance_use_complete') ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col in ['subs_smoke_100', 'subs_smoke_now']:
                mask = ( self.data[col].isna() &
                         ( self.data['subs_tobacco_use'] == 1 ) )

            elif col in ['subs_smoke_last_hour', 'subs_smoking_frequency', 'subs_smoking_start_age', 'subs_smoke_cigarettes']:
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

            # elif col in [ 'subs_alcohol_consume_now', 'subs_alcohol_con_past_yr', 'subs_alcohol_cutdown' ]:
            elif col in [ 'subs_alcohol_consume_now', 'subs_alcohol_con_past_yr']:
                mask = ( self.data[col].isna() &
                         ( self.data['subs_alcohol_consump'] == 1 ) )
            # elif col in ['subs_alcohol_consump_freq', 'subs_alcohol_criticize', 'subs_alcohol_guilty', 'subs_alcohol_hangover']:
            elif col in ['subs_alcohol_consump_freq', 'subs_alcohol_criticize',
                         'subs_alcohol_guilty', 'subs_alcohol_hangover', 'subs_alcohol_cutdown']:
                mask = ( self.data[col].isna() &
                       ( self.data['subs_alcohol_consume_now'] == 1 ) )
            elif col == 'subs_alcoholtype_consumed':
                mask = ( self.data[col].isna() &
                       ( ( self.data['subs_alcohol_consump'] == 1 ) |
                         ( self.data['subs_alcohol_consume_now'] == 1 ) ) )
            elif col == 'subs_alcohol_consume_freq':
                mask = ( self.data[col].isna() &
                         self.data['subs_alcohol_consump_freq'].isin( [1, 2, 3, 4, 5] ) )
            elif col == 'subs_alcohol_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['subs_alcoholtype_consumed___5'] == 1 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['substance_use_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_a_general_health_cancer(self):
        colNames = self.data.columns[self.data.columns.str.contains('genh_bre') |
                                     self.data.columns.str.contains('genh_cer') |
                                     self.data.columns.str.contains('genh_pro') |
                                     self.data.columns.str.contains('genh_oes') |
                                     self.data.columns.str.contains('genh_oth') |
                                     self.data.columns.str.contains('a_general_health_cancer_complete') ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col in ['genh_breast_cancer_treat', 'genh_bre_cancer_trad_med']:
                mask = ( self.data[col].isna() &
                         ( self.data['genh_breast_cancer'] == 1 ) )
            elif col == 'genh_bre_cancer_treat_now':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_breast_cancer_treat'] == 1 ) )
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
                         ( self.data['genh_cer_cancer_treat'] == 1 ) )
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

            elif col == 'genh_other_cancer_treat':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_other_cancers'] == 1 ) )
            elif col in ['genh_cancer_specify_other', 'genh_oth_cancer_trad_med']:
                mask = ( self.data[col].isna() &
                         ( self.data['genh_other_cancers'] == 1 ) )
            elif col == 'genh_oth_cancer_treat_now':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_other_cancer_treat'] == 1 ) )
            elif col == 'genh_other_cancer_meds':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_oth_cancer_treat_now'] == 1 ) )

            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['a_general_health_cancer_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_c_general_health_diet(self):
        colNames = self.data.columns[ (self.data.columns.str.contains('genh') & self.data.columns.str.contains('veg') ) |
                                      (self.data.columns.str.contains('genh') & self.data.columns.str.contains('fruit') ) |
                                       self.data.columns.str.contains('genh_staple') |
                                       self.data.columns.str.contains('genh_vendor_meals') |
                                       self.data.columns.str.contains('genh_starchy') |
                                       self.data.columns.str.contains('genh_sugar_drinks') |
                                       self.data.columns.str.contains('genh_juice') |
                                       self.data.columns.str.contains('genh_change_diet') |
                                       self.data.columns.str.contains('genh_lose_weight') |
                                       self.data.columns.str.contains('c_general_health_diet_complete') ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue
            elif col in ['genh_starchy_staple_freq', 'genh_staple_servings']:
                if self.site == 'nanoro':
                    mask = ( self.data[col].isna() & ( ( self.data['genh_starchy_staple_food___2'] == 1 ) |
                                                    ( self.data['genh_starchy_staple_food___3'] == 1 ) |
                                                    ( self.data['genh_starchy_staple_food___13'] == 1 ) |
                                                    ( self.data['genh_starchy_staple_food___14'] == 1 ) |
                                                    ( self.data['genh_starchy_staple_food___15'] == 1 ) |
                                                    ( self.data['genh_starchy_staple_food___16'] == 1 ) ) )
                else:
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
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['c_general_health_diet_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_b_general_health_family_history(self):
        colNames = self.data.columns[ (self.data.columns.str.contains('genh') & self.data.columns.str.contains('mom') ) |
                                      (self.data.columns.str.contains('genh') & self.data.columns.str.contains('dad') ) |
                                       self.data.columns.str.contains('b_general_health_family_history_complete') ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['b_general_health_family_history_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_d_general_health_exposure_to_pesticides_pollutants(self):
        colNames = self.data.columns[(self.data.columns.str.contains('genh_pesticide')) |
                                     (self.data.columns.str.contains('genh_cooking')) |
                                     (self.data.columns.str.contains('genh_energy')) |
                                     (self.data.columns.str.contains('genh_smoke')) |
                                     (self.data.columns.str.contains('genh_insect_repellent_use')) |
                                      self.data.columns.str.contains('d_general_health_exposure_to_pesticides_pollutants_complete') ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col == 'genh_pesticide_years':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_pesticide'] == 1 ) )
            elif col == 'genh_pesticide_list':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_pesticide_type'] == 1 ) )

            elif col == 'genh_cookingplace_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_cooking_place'] == 3 ) )
            elif col == 'genh_cooking_done_inside':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_cooking_place'] == 1 ) )
            elif col == 'genh_energy_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_energy_source_type___6'] == 6 ) )
            elif col == 'genh_smoke_freq_someone':
                mask = ( self.data[col].isna() &
                         ( self.data['genh_smoker_in_your_house'] == 1 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['d_general_health_exposure_to_pesticides_pollutants_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_infection_history(self):
        colNames = self.data.columns[self.data.columns.str.contains('infh_') |
                                     self.data.columns.str.contains('infection_history_complete') ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col == 'infh_malaria_month':
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

            elif col in ['infh_hiv_treatment', 'infh_hiv_arv_meds', 'infh_hiv_arv_meds_now']:
                mask = ( self.data[col].isna() &
                         ( self.data['infh_hiv_medication'] == 1 ) )
            elif col in ['infh_hiv_arv_meds_specify', 'infh_hiv_arv_single_pill']:
                mask = ( self.data[col].isna() &
                         ( self.data['infh_hiv_arv_meds_now'] == 1 ) )
            elif col == 'infh_hiv_pill_size':
                mask = ( self.data[col].isna() &
                         ( self.data['infh_hiv_arv_single_pill'] == 1 ) )
            elif col == 'infh_hiv_counselling':
                mask = ( self.data[col].isna() &
                         ( self.data['infh_hiv_test'] == 1 ) )
            elif col == 'infh_hiv_test':
                mask = ( self.data[col].isna() &
                         ( self.data['infh_hiv_positive'] == 0 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['infection_history_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_a_cardiometabolic_risk_factors_diabetes(self):
        colNames = self.data.columns[self.data.columns.str.contains('carf_diab') |
                                     self.data.columns.str.contains('carf_daughter_diabetes_1') |
                                     self.data.columns.str.contains('carf_blood_sugar') |
                                     self.data.columns.str.contains('a_cardiometabolic_risk_factors_diabetes_complete') ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col in ['carf_diabetes_12months', 'carf_diabetes_treatment']:
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
            elif col in ['carf_diabetes_mother', 'carf_diabetes_father', 'carf_diabetes_fam_other']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) )

            elif col == 'carf_diabetes_brother_1':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_number_of_brothers'] >= 1 ) )
            elif col == 'carf_diabetes_brother_2':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_number_of_brothers'] >= 2 ) )
            elif col == 'carf_diabetes_brother_3':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_number_of_brothers'] >= 3 ) )
            elif col == 'carf_diabetes_brother_4':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_number_of_brothers'] >= 4 ) )

            elif col == 'carf_diabetes_sister_1':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_number_of_sisters'] >= 1 ) )
            elif col == 'carf_diabetes_sister_2':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_number_of_sisters'] >= 2 ) )
            elif col == 'carf_diabetes_sister_3':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_number_of_sisters'] >= 3 ) )
            elif col == 'carf_diabetes_sister_4':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_number_of_sisters'] >= 4 ) )

            elif col == 'carf_diabetes_son_1':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_bio_sons'] >= 1 ) )
            elif col == 'carf_diabetes_son_2':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_bio_sons'] >= 2 ) )
            elif col == 'carf_diabetes_son_3':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_bio_sons'] >= 3 ) )
            elif col == 'carf_diabetes_son_4':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_bio_sons'] >= 4 ) )

            elif col == 'carf_daughter_diabetes_1':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_bio_daughters'] >= 1 ) )
            elif col == 'carf_diabetes_daughter_2':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_bio_daughters'] >= 2 ) )
            elif col == 'carf_diabetes_daughter_3':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_bio_daughters'] >= 3 ) )
            elif col == 'carf_diabetes_daughter_4':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_history'] == 1 ) &
                         ( self.data['famc_bio_daughters'] >= 4 ) )

            elif col == 'carf_diabetes_fam_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_diabetes_fam_other'] == 1 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['a_cardiometabolic_risk_factors_diabetes_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_b_cardiometabolic_risk_factors_heart_conditions(self):
        colNames = self.data.columns[self.data.columns.str.contains('carf_stroke') |
                                     self.data.columns.str.contains('carf_tia') |
                                     self.data.columns.str.contains('carf_weakness') |
                                     self.data.columns.str.contains('carf_numbness') |
                                     self.data.columns.str.contains('carf_blindness') |
                                     self.data.columns.str.contains('carf_half_vision_loss') |
                                     self.data.columns.str.contains('carf_understanding_loss') |
                                     self.data.columns.str.contains('carf_expression_loss') |
                                     self.data.columns.str.contains('carf_angina') |
                                     self.data.columns.str.contains('carf_pain') |
                                     self.data.columns.str.contains('carf_relief_standstill') |
                                     self.data.columns.str.contains('carf_heart') |
                                     self.data.columns.str.contains('carf_congestiv_heart_fail') |
                                     self.data.columns.str.contains('carf_chf_') |
                                     self.data.columns.str.contains('b_cardiometabolic_risk_factors_heart_conditions_complete') ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue
            elif col == 'carf_stroke_diagnosed':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_stroke'] == 1 ) )
            elif col in ['carf_angina_treatment', 'carf_pain_location',
                         'carf_angina_traditional', 'carf_pain', 'carf_pain2']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_angina'] == 1 ) )
            elif col == 'carf_angina_treat_now':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_angina_treatment'] == 1 ) )
            elif col == 'carf_angina_meds':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_angina_treat_now'] == 1 ) )
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
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['b_cardiometabolic_risk_factors_heart_conditions_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_c_cardiometabolic_risk_factors_hypertension_choles(self):
        colNames = self.data.columns[self.data.columns.str.contains('carf_hypertension') |
                                     ( self.data.columns.str.contains('carf_') & self.data.columns.str.contains('chol') ) |
                                     self.data.columns.str.contains('carf_bp_measured') |
                                     self.data.columns.str.contains('c_cardiometabolic_risk_factors_hypertension_choles_complete') ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col in ['carf_hypertension_12mnths', 'carf_hypertension_treat']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_hypertension'] == 1 ) )
            elif col == 'carf_hypertension_meds':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_hypertension_treat'] == 1 ) )
            elif col == 'carf_hypertension_medlist':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_hypertension_meds'] == 1 ) )
            elif col in ['carf_chol_treatment']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_h_cholesterol'] == 1 ) )
            elif col == 'carf_chol_medicine':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_chol_treatment_now___3'] == 1 ) )
            elif col == 'carf_chol_treat_specify':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_chol_treatment_now___4'] == 1 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['c_cardiometabolic_risk_factors_hypertension_choles_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_d_cardiometabolic_risk_factors_kidney_thyroid_ra(self):
        colNames = self.data.columns[self.data.columns.str.contains('carf_kidney') |
                                     ( self.data.columns.str.contains('carf_') & self.data.columns.str.contains('thyroid') ) |
                                     self.data.columns.str.contains('carf_joints') |
                                     self.data.columns.str.contains('carf_when_they_hurt') |
                                     self.data.columns.str.contains('carf_symptoms_how_long') |
                                     self.data.columns.str.contains('carf_arthritis_results') |
                                     self.data.columns.str.contains('carf_rheumatoid_factor') |
                                     self.data.columns.str.contains('carf_acpa') |
                                     self.data.columns.str.contains('carf_esr_crp') |
                                     self.data.columns.str.contains('carf_osteo') |
                                     self.data.columns.str.contains('d_cardiometabolic_risk_factors_kidney_thyroid_ra_complete') ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

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

            elif col in ['carf_joints_swollen', 'carf_joints_involved', 'carf_arthritis_results',
                         'carf_when_they_hurt', 'carf_symptoms_how_long']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_joints_swollen_pain'] == 1 ) )

            elif col in ['carf_acpa', 'carf_esr_crp', 'carf_rheumatoid_factor'] :
                mask = ( self.data[col].isna() &
                         ( self.data['carf_arthritis_results'] == 1 ) )

            elif col in ['carf_osteo_sites', 'carf_osteo_sites___1', 'carf_osteo_sites___2',
                         'carf_osteo_sites___3', 'carf_osteo_sites___4', 'carf_osteo_sites___5',
                         'carf_osteo_sites___6']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_osteo'] == 1 ) )

            elif col == 'carf_osteo_hip_replace':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_osteo'] == 1 ) &
                         ( ( self.data['carf_osteo_sites___1'] == 1 ) |
                           ( self.data['carf_osteo_sites___2'] == 1 ) ) )
            elif col in [ 'carf_osteo_hip_repl_site','carf_osteo_hip_repl_age']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_osteo_hip_replace'] == 1 ) )

            elif col == 'carf_osteo_knee_replace':
                mask = ( self.data[col].isna() &
                         ( self.data['carf_osteo'] == 1 ) &
                         ( ( self.data['carf_osteo_sites___3'] == 1 ) |
                           ( self.data['carf_osteo_sites___4'] == 1 ) ) )
            elif col in [ 'carf_osteo_knee_repl_site', 'carf_osteo_knee_repl_age']:
                mask = ( self.data[col].isna() &
                         ( self.data['carf_osteo_knee_replace'] == 1 ) )

            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['d_cardiometabolic_risk_factors_kidney_thyroid_ra_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_physical_activity_and_sleep(self):
        colNames = self.data.columns[self.data.columns.str.contains('gpaq_') |
                                     self.data.columns.str.contains('physical_activity_and_sleep_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col in ['gpaq_work_vigorous_days', 'gpaq_work_vigorous_time',
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
                         ( self.data['gpaq_leisure_vigorous'] == 1 ) )
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

            mask = mask & ( self.data['physical_activity_and_sleep_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_anthropometric_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('anth_') |
                                     self.data.columns.str.contains('anthropometric_measurements_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['anthropometric_measurements_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_blood_pressure_and_pulse_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('bppm_') |
                                     self.data.columns.str.contains('blood_pressure_and_pulse_measurements_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['blood_pressure_and_pulse_measurements_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_ultrasound_and_dxa_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('ultr_') |
                                     self.data.columns.str.contains('ultrasound_and_dxa_measurements_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col == 'ultr_comment':
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_vat_scat_measured'] == 0 ) )
            elif col in ['ultr_technician', 'ultr_visceral_fat', 'ultr_subcutaneous_fat']:
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_vat_scat_measured'] == 1 ) )

            elif col == 'ultr_cimt_comment':
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_cimt'] == 0 ) )
            elif col in ['ultr_cimt_technician',
                         'ultr_cimt_right_min',	'ultr_cimt_right_max', 'ultr_cimt_right_mean',
                         'ultr_cimt_left_min', 'ultr_cimt_left_max', 'ultr_cimt_left_mean']:
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_cimt'] == 1 ) )

            elif col == 'ultr_plaque_comment':
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_plaque_measured'] == 0 ) )  # TODO ultr_plaque == ultr_plaque_measured?? in Soweto
            elif col in ['ultr_plaque_technician', 'ultr_plaque_right_present' , 'ultr_plaque_left_present']:
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_plaque_measured'] == 1 ) )
            elif col == 'ultr_plaque_right':
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_plaque_right_present'] == 1 ) )
            elif col == 'ultr_plaque_left':
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_plaque_left_present'] == 1 ) )

            elif col == 'ultr_dxa_scan_comment':
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_dxa_scan_completed'] == 0 ) )
            elif col in ['ultr_dxa_measurement_1', 'ultr_dxa_measurement_2', 'ultr_dxa_measurement_3',
                         'ultr_dxa_measurement_4',	'ultr_dxa_measurement_5']:
                mask = ( self.data[col].isna() &
                         ( self.data['ultr_dxa_scan_completed'] == 1 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['ultrasound_and_dxa_measurements_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_a_respiratory_health(self):
        colNames = self.data.columns[self.data.columns.str.contains('resp_') |
                                     self.data.columns.str.contains('a_respiratory_health_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col in ['resp_age_diagnosed', 'resp_asthma_treat']:
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

            mask = mask & ( self.data['a_respiratory_health_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_b_spirometry_eligibility(self):
        colNames = self.data.columns[self.data.columns.str.contains('rspe_') |
                                     self.data.columns.str.contains('b_spirometry_eligibility_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col == 'rspe_chest_pain':
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

            mask = mask & ( self.data['b_spirometry_eligibility_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_c_spirometry_test(self):
        colNames = self.data.columns[self.data.columns.str.contains('spiro_') |
                                     self.data.columns.str.contains('c_spirometry_test_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col in ['spiro_researcher', 'spiro_num_of_blows', 'spiro_num_of_vblows', 'spiro_pass']:
                mask = ( self.data[col].isna() &
                         ( self.data['spiro_eligible'] == 1 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['c_spirometry_test_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_d_reversibility_test(self):
        colNames = self.data.columns[self.data.columns.str.contains('rspir_') |
                                     self.data.columns.str.contains('d_reversibility_test_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols: continue

            if col in ['rspir_salb_time_admin', 'rspir_time_started',
                        'rspir_researcher', 'rspir_num_of_blows', 'rspir_num_of_vblows']:
                mask = ( self.data[col].isna() &
                         ( self.data['rspir_salb_admin'] == 1 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['d_reversibility_test_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_a_microbiome(self):
        colNames = self.data.columns[self.data.columns.str.contains('micr_') |
                                     self.data.columns.str.contains('a_microbiome_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col == 'micr_probiotics_t_period':
                mask = ( self.data[col].isna() &
                         ( self.data['micr_probiotics_taken'] == 1 ) )
            elif col == 'micr_wormintestine_period':
                mask = ( self.data[col].isna() &
                         ( self.data['micr_worm_intestine_treat'] == 1 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['a_microbiome_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_b_blood_collection(self):
        colNames = self.data.columns[self.data.columns.str.contains('bloc_last') |
                                     self.data.columns.str.contains('bloc_hours_last_drink') |
                                     self.data.columns.str.contains('bloc_fasting_confirmed') |
                                     ( self.data.columns.str.contains('bloc_') & self.data.columns.str.contains('tube') & ~self.data.columns.str.contains('ur') ) |
                                     self.data.columns.str.contains('bloc_phlebotomist_name') |
                                     self.data.columns.str.contains('bloc_blood') |
                                     self.data.columns.str.contains('b_blood_collection_complete') ]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col == 'bloc_hours_last_drink':
                mask = ( self.data[col].isna() &
                         ( ( self.data['bloc_last_drink_time'].notna() ) &
                           ( self.data['bloc_last_ate_hrs'] >= 0 ) ) )
            elif col == 'bloc_last_ate_hrs':
                mask = ( self.data[col].isna() &
                         ( ( self.data['bloc_last_eat_time'].notna() ) &
                           ( self.data['bloc_last_ate_hrs'] >= 0 ) ) )
            elif col == 'bloc_red_tubes_num':
                mask = ( self.data[col].isna() &
                         ( self.data['bloc_two_red_tubes'] == 0 ) )
            elif col == 'bloc_if_no_purple_tubes':
                mask = ( self.data[col].isna() &
                         ( self.data['bloc_one_purple_tube'] == 0 ) )
            elif col == 'bloc_grey_tubes_no':
                mask = ( self.data[col].isna() &
                         ( self.data['bloc_one_grey_tube'] == 0 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['b_blood_collection_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_c_urine_collection(self):
        colNames = self.data.columns[self.data.columns.str.contains('bloc_ur') |
                                     self.data.columns.str.contains('bloc_specify_reason') |
                                     self.data.columns.str.contains('c_urine_collection_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col == 'bloc_specify_reason':
                mask = ( self.data[col].isna() &
                         ( self.data['bloc_urine_collected'] == 0 ) )
            elif col in ['bloc_urcontainer_batchnum', 'bloc_urine_tube_expiry',
                         'bloc_urine_collector', 'bloc_urine_taken_date', 'bloc_urinecollection_time']:
                mask = ( self.data[col].isna() &
                         ( self.data['bloc_urine_collected'] == 1 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['c_urine_collection_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_point_of_care_testing(self):
        colNames = self.data.columns[self.data.columns.str.contains('poc_') |
                                     self.data.columns.str.contains('point_of_care_testing_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col == 'poc_comment':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_test_conducted'] == 0 ) )
            elif col in ['poc_instrument_serial_num', 'poc_test_strip_batch_num', 'poc_glucose_test_result', 'poc_chol_results_provided',
                         'poc_teststrip_expiry_date', 'poc_test_date', 'poc_test_time', 'poc_chol_result','poc_gluc_results_provided']:
                mask = ( self.data[col].isna() &
                         ( self.data['poc_test_conducted'] == 1 ) )
            elif col == 'poc_gluc_results_notes':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_gluc_results_provided'] == 0 ) )
            elif col == 'poc_glucresults_discussed':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_gluc_results_provided'] == 1 ) )
            elif col == 'poc_cholresults_discussed':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_chol_results_provided'] == 1 ) )
            elif col == 'poc_chol_results_notes':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_chol_results_provided'] == 0 ) )
            elif col == 'poc_hiv_comment':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_hiv_test_conducted'] == 0 ) )
            elif col in ['poc_hiv_pre_test', 'poc_test_kit_serial_num',
                         'poc_hiv_test_date_done', 'poc_technician_name', 'poc_hiv_test_result',
                         'poc_result_provided', 'poc_post_test_counselling', 'poc_hiv_strip_batch_num',
                         'poc_hiv_strip_expiry_date']:
                mask = ( self.data[col].isna() &
                         ( self.data['poc_hiv_test_conducted'] == 1 ) )
            elif col == 'poc_pre_test_worker':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_hiv_pre_test'] == 1 ) )
            elif col == 'poc_post_test_worker':
                mask = ( self.data[col].isna() &
                         ( self.data['poc_post_test_counselling'] == 1 ) )
            elif col in ['poc_hivpositive_firsttime', 'poc_hiv_seek_advice']:
                mask = ( self.data[col].isna() &
                         ( self.data['poc_hiv_test_result'] == 1 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['point_of_care_testing_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_trauma(self):
        colNames = self.data.columns[self.data.columns.str.contains('tram_') |
                                     self.data.columns.str.contains('trauma_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['trauma_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def check_completion_of_questionnaire(self):
        colNames = self.data.columns[self.data.columns.str.contains('comp_') |
                                     self.data.columns.str.contains('completion_of_questionnaire_complete')]

        df = pd.DataFrame(index=self.data.index)

        for col in sorted(colNames.to_list()):
            if col in self.ignored_cols:
                continue

            elif col == 'comp_comment_no_1_13':
                mask = ( self.data[col].isna() &
                         ( self.data['comp_sections_1_13'] == 0 ) )
            else:
                mask = self.data[col].isna()

            mask = mask & ( self.data['completion_of_questionnaire_complete'] == 2 )

            df[col] = mask.to_frame().values

        return self.get_missing_ids(df)

    def write_missingness_report(self, xlsx_writer):
        df = pd.DataFrame()

        # df2 = pd.DataFrame()

        for instrument_name, instrument_checker in self.instrument_dict.items():
            missing_df = instrument_checker(self)
            missing_df['Instrument'] = instrument_name
            df = df.append(missing_df)

        # df['Comment'] = ""

        # df2 = df2.append(df[df['Data Field'] == 'genh_cervical_cancer_mom'])
        # df2 = df2.append(df[df['Data Field'] == 'genh_breast_cancer_mom'])
        # df2 = df2.append(df[df['Data Field'] == 'genh_prostate_cancer_dad'])
        # df2 = df2.append(df[df['Data Field'] == 'genh_other_cancers_dad'])
        # df2 = df2.append(df[df['Data Field'] == 'genh_oes_cancer_mom'])
        # df2 = df2.sort_values(by=['study_id'])
        # df2 = df2[['study_id', 'Data Field']]
        # df2.to_excel(self.excelWriter, sheet_name='Missing Data', startcol=0, startrow=0, index=False)
        # self.excelWriter.sheets['Missing Data'].set_column(0, 0 , 20)
        # self.excelWriter.sheets['Missing Data'].set_column(1, 1 , 30)
        # self.excelWriter.save()

        df = df.sort_values(by=['study_id', 'Instrument'])
        missing_summary = df['Data Field'].value_counts().reset_index()

        df.to_excel(xlsx_writer, sheet_name='Missing Data', startcol=0, startrow=0, index=False)
        xlsx_writer.sheets['Missing Data'].set_column(0, 0 , 20)
        xlsx_writer.sheets['Missing Data'].set_column(1, 1 , 30)
        xlsx_writer.sheets['Missing Data'].set_column(2, 2 , 30)

        missing_summary['Comments'] = ''
        missing_summary.rename(columns={'index':'Data Field', 'Data Field' : 'Total Missing'}, inplace=True)

        missing_summary.to_excel(xlsx_writer, sheet_name='Missing Data Summary', startcol=0, startrow=0, index=False)
        xlsx_writer.sheets['Missing Data Summary'].set_column(0, 0 , 30)
        xlsx_writer.sheets['Missing Data Summary'].set_column(1, 1 , 20)
        xlsx_writer.sheets['Missing Data Summary'].set_column(2, 2 , 30)

    instrument_dict = {
        # 'a_phase_1_data'                    : check_a_phase_1_data,
        'participant_identification'        : check_participant_identification,
        'ethnolinguistic_information'       : check_ethnolinguistic_information,
        'family_composition'                : check_family_composition,
        'pregnancy_and_menopause'           : check_pregnancy_and_menopause,
        'civil_status_marital_status_education_employment' : check_civil_status_marital_status_education_employment,
        'a_cognition_one'                   : check_a_cognition_one,
        'b_frailty_measurements'            : check_b_frailty_measurements,
        'c_cognition_two'                   : check_c_cognition_two,
        'household_attributes'              : check_household_attributes,
        'substance_use'                     : check_substance_use,
        'a_general_health_cancer'           : check_a_general_health_cancer,
        'c_general_health_diet'             : check_c_general_health_diet,
        'b_general_health_family_history'   : check_b_general_health_family_history,
        'd_general_health_exposure_to_pesticides_pollutants' : check_d_general_health_exposure_to_pesticides_pollutants,
        'infection_history'                 : check_infection_history,
        'a_cardiometabolic_risk_factors_diabetes'    : check_a_cardiometabolic_risk_factors_diabetes,
        'b_cardiometabolic_risk_factors_heart_conditions'    : check_b_cardiometabolic_risk_factors_heart_conditions,
        'c_cardiometabolic_risk_factors_hypertension_choles'    : check_c_cardiometabolic_risk_factors_hypertension_choles,
        'd_cardiometabolic_risk_factors_kidney_thyroid_ra'    : check_d_cardiometabolic_risk_factors_kidney_thyroid_ra,
        'physical_activity_and_sleep'       : check_physical_activity_and_sleep,
        'anthropometric_measurements'       : check_anthropometric_measurements,
        'blood_pressure_and_pulse_measurements' : check_blood_pressure_and_pulse_measurements,
        'ultrasound_and_dxa_measurements'   : check_ultrasound_and_dxa_measurements,
        'a_respiratory_health'              : check_a_respiratory_health,
        'b_spirometry_eligibility'          : check_b_spirometry_eligibility,
        'c_spirometry_test'                 : check_c_spirometry_test,
        'd_reversibility_test'              : check_d_reversibility_test,
        'a_microbiome'                      : check_a_microbiome,
        'b_blood_collection'                : check_b_blood_collection,
        'c_urine_collection'                : check_c_urine_collection,
        'point_of_care_testing'             : check_point_of_care_testing,
        'trauma'                            : check_trauma,
        'completion_of_questionnaire'       : check_completion_of_questionnaire
        }

    ignored_cols = ['gene_uni_site_id_correct',
                    'gene_uni_site_id_is_correct',
                    'ethnolinguistc_available',
                    'a_phase_1_data_complete',
                    'gene_site_id',
                    'demo_dob',
                    'demo_date_of_birth',
                    'demo_approx_dob_is_correct',
                    'demo_dob_is_correct',
                    'demo_date_of_birth_known',
                    'demo_dob_new',
                    'demo_approx_dob_new',
                    'demo_gender',
                    'cogn_comments',
                    'rspe_participation',
                    'rspe_participation_note',
                    'spiro_comment',
                    'rspir_salb_admin',
                    'rspir_comment',
                    # 'ultr_dxa_scan_completed',
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
