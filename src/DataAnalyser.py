import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import requests
from io import StringIO

import xlsxwriter
import ApiKeys

from Instruments import Instruments
from RedcapApiHandler import RedcapApiHandler

class DataAnalyser:
    def __init__(self, resource_dir, data, site):
        self.data = data
        self.instruments = Instruments(data)
        self.resource_dir = resource_dir
        self.site = site

    def plot_histogram(self, data, col, outliers):
        q1 = data.quantile(0.25)
        q3 = data.quantile(0.75)
        median = np.round(data.median(),1)
        iqr = q3 - q1

        std = np.round(data.std(),1)
        mean = np.round(data.mean(),1)

        upper_limit_iqr = q3 + 1.5 * iqr
        lower_limit_iqr = q1 - 1.5 * iqr

        upper_limit_std = mean + std * 3
        lower_limit_std = mean - std * 3

        upper_bound = max(upper_limit_iqr, upper_limit_std)
        lower_bound = min(lower_limit_iqr, lower_limit_std)

        plt.figure(figsize=(16*0.9, 9*0.9), dpi=200)
        plt.title(col)

        ax = data.plot.hist(bins=50)

        # Plot and label the median
        plt.axvline(median, color='k', linestyle='dashed', linewidth=1)
        plt.text(median*1.0001, ax.dataLim.ymax*0.9995, str('Median: ' + str(median)), fontsize=9)

        # Don't plot the lower boundary if it is below 0 and there are no negative values
        if (lower_bound >= 0) or (data[data < 0].size > 0):
            plt.axvline(lower_bound, color='k', linestyle='dashed', linewidth=1)

        plt.axvline(upper_bound, color='k', linestyle='dashed', linewidth=1)

        # Sort the outliers by value
        outliers = outliers.to_frame().sort_values(by=col)

        # Get the histogram bins and the number of values in each bin
        bar_info = pd.cut(data, 50)
        num_vals = bar_info.value_counts().sort_index()

        # This loop is used to write the outlier IDs above the histogram bins
        id_text_height = 0
        last_bin_idx = -1
        for idx, num in outliers.iterrows():
            current_bin_idx = num_vals.index.get_loc(bar_info[idx])

            # Get the number of values of the bin of the current outlier ID
            num_bin_vals = num_vals.values[current_bin_idx]

            # Reset id_text_height if this is a new bin and update last_bin_idx
            if current_bin_idx != last_bin_idx:
                last_bin_idx = current_bin_idx
                id_text_height = 0

            # If there are more than 30 values in the bin, don't write the IDs
            if num_bin_vals > 30:
                continue

            # Write the ID above the bin, incremented by id_text_height,
            # and increment id_text_height by id_step
            plt.text(bar_info[idx].mid, num_bin_vals + id_text_height, idx, fontsize=7, rotation=40)

            id_step = np.round((ax.dataLim.ymax - num_bin_vals) / 30 , 2)
            id_text_height += id_step

        fig_title = self.resource_dir + col + '_hist.png'

        plt.savefig(fig_title, bbox_inches='tight')
        plt.close()

        return fig_title
        #plt.show()

    def instrument_outliers(self, instrument_data, data_frame, instrument_key):
        for col in instrument_data.columns:

            if col in self.ignored_cols:
                continue

            data = instrument_data[col]

            # Skip iteration if all data is NaN
            if data.dropna().size == 0:
                continue

            data = data.dropna()

            # Remove -999 (missing data)
            data = data[data != -999]

            q1 = data.quantile(0.25)
            q3 = data.quantile(0.75)
            mean = np.round(data.mean(),1)
            std = np.round(data.std(),1)
            median = np.round(data.median(),1)
            iqr = q3 - q1

            # Skip iteration if IQR = 0
            if iqr == 0:
                continue

            upper_limit_iqr = q3 + 1.5 * iqr
            lower_limit_iqr = q1 - 1.5 * iqr

            upper_limit_std = mean + std * 3
            lower_limit_std = mean - std * 3

            upper_limit = max(upper_limit_iqr, upper_limit_std)
            lower_limit = min(lower_limit_iqr, lower_limit_std)

            # Find outliers i.e. values outside the range (q1 - 1.5 * iqr, q3 + 1.5 * iqr)
            mask = data.between(lower_limit, upper_limit, inclusive='both')
            outliers = data[~mask].dropna()

            # Skip iteration if there are no outliers
            if outliers.size == 0:
                continue

            # self.plot_histogram(data, col, outliers)

            outliers = outliers.to_frame()

            outliers.rename(columns={col:'Value'}, inplace=True)
            outliers['Data Field'] = col
            outliers['Instrument'] = instrument_key
            # outliers['Median'] = median
            outliers['Lower Limit'] = lower_limit
            outliers['Upper Limit'] = upper_limit

            # outliers['Limit'] = np.where( ( outliers['Value'] >= upper_limit ), upper_limit, lower_limit )
            # outliers['Comment'] = ''

            data_frame = data_frame.append(outliers)

        return data_frame

    def write_outliers_report(self, outliers_xlsx_writer):
        exceptions = RedcapApiHandler(self.site).get_exceptions_from_redcap()

        df = pd.DataFrame()

        for instrument_key, instrument_getter in self.instruments.instrument_dict.items():
            if instrument_key == 'ethnolinguistic_information':
                continue
            instrument_data = instrument_getter(self.instruments)
            instrument_data.set_index(['study_id'], inplace=True)
            instrument_data = instrument_data.select_dtypes(include=np.number)
            df = self.instrument_outliers(instrument_data, df, instrument_key)

        # Remove exceptions from outliers frame
        df = pd.merge(df, exceptions, on=['study_id','Data Field'], how='outer', indicator='source')
        df = df[df['source'] == 'left_only'].drop('source', axis=1)

        df['Is Correct'] = ''
        df['Comment/Updated Value'] = ''
        df = df[['Data Field', 'Instrument', 'Value', 'Lower Limit', 'Upper Limit', 'Is Correct', 'Comment/Updated Value']]
        df = df.sort_values(by=['study_id', 'Instrument'])
        df.reset_index(inplace=True)
        df.to_excel(outliers_xlsx_writer, sheet_name='Outliers', startcol=0, startrow=3, index=False)

        lower_limit_text = 'Lower Limit = min(mean - std * 3, 1st quartile - 1.5 * IQR)'
        upper_limit_text = 'Upper Limit = max(mean + std * 3, 3rd quartile + 1.5 * IQR)'

        outliers_xlsx_writer.sheets['Outliers'].write(0, 0, lower_limit_text)
        outliers_xlsx_writer.sheets['Outliers'].write(1, 0, upper_limit_text)
        outliers_xlsx_writer.sheets['Outliers'].set_column(0, 0 , 15)
        outliers_xlsx_writer.sheets['Outliers'].set_column(1, 1 , 30)
        outliers_xlsx_writer.sheets['Outliers'].set_column(2, 2 , 30)
        outliers_xlsx_writer.sheets['Outliers'].set_column(3, 3 , 10)
        outliers_xlsx_writer.sheets['Outliers'].set_column(4, 4 , 12)
        outliers_xlsx_writer.sheets['Outliers'].set_column(5, 5 , 12)
        outliers_xlsx_writer.sheets['Outliers'].set_column(6, 6 , 20)
        outliers_xlsx_writer.sheets['Outliers'].set_column(7, 7 , 30)

    # Skip all dropdowns/checkboxes/radio buttons
    ignored_cols = ['bloc_hours_last_drink',
                    'bloc_last_ate_hrs',
                    'demo_home_language',
                    'home_language_confirmation',
                    'phase_1_site_id_1',
                    'phase_1_gender',
                    'phase_1_home_language',
                    'phase_1_ethnicity',
                    'gene_site',
                    'demo_gender_correction',
                    'home_language',
                    'ethnicity_confirmation',
                    'ethnicity',
                    'ethn_father_ethn_sa',
                    'ethn_father_lang_sa',
                    'ethn_pat_gfather_ethn_sa',
                    'ethn_pat_gfather_lang_sa',
                    'ethn_pat_gmother_ethn_sa',
                    'ethn_pat_gmother_lang_sa',
                    'ethn_mother_ethn_sa',
                    'ethn_mother_lang_sa',
                    'ethn_mat_gfather_ethn_sa',
                    'ethn_mat_gfather_lang_sa',
                    'ethn_mat_gmother_ethn_sa',
                    'ethn_mat_gmother_lang_sa',
                    'famc_siblings',
                    'famc_children',
                    'preg_pregnant',
                    'preg_birth_control',
                    'preg_hysterectomy',
                    'preg_regular_periods',
                    'preg_last_period_remember',
                    'preg_period_more_than_yr',
                    'mari_marital_status',
                    'educ_highest_level',
                    'empl_status',
                    'empl_days_work',
                    'cogn_read_sentence',
                    'cogn_memory',
                    'cogn_difficulty_remember',
                    'cogn_difficulty_concern',
                    'cogn_learning_new_task',
                    'cogn_words_remember_p1',
                    'cogn_year',
                    'cogn_what_is_the_month',
                    'cogn_day_of_the_month',
                    'cogn_country_of_residence',
                    'cogn_district_province',
                    'cogn_village_town_city',
                    'cogn_weekdays_forward',
                    'cogn_weekdays_backwards',
                    'frai_use_hands',
                    'frai_sit_stands_completed',
                    'frai_non_dominant_hand',
                    'frai_complete_procedure',
                    'frai_need_support',
                    'frai_procedure_walk_comp',
                    'cogn_delayed_recall',
                    'cogn_word_cognition_list',
                    'hous_electricity',
                    'hous_solar_energy',
                    'hous_power_generator',
                    'hous_alter_power_src',
                    'hous_television',
                    'hous_radio',
                    'hous_motor_vehicle',
                    'hous_motorcycle',
                    'hous_bicycle',
                    'hous_refrigerator',
                    'hous_washing_machine',
                    'hous_sewing_machine',
                    'hous_telephone',
                    'hous_mobile_phone',
                    'hous_microwave',
                    'hous_dvd_player',
                    'hous_satellite_tv_or_dstv',
                    'hous_computer_or_laptop',
                    'hous_internet_by_computer',
                    'hous_internet_by_m_phone',
                    'hous_electric_iron',
                    'hous_fan',
                    'hous_electric_gas_stove',
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
                    'subs_tobacco_use',
                    'subs_smoke_100',
                    'subs_smoke_now',
                    'subs_smoke_last_hour',
                    'subs_smoke_cigarettes',
                    'subs_smoking_frequency',
                    'subs_smoke_per_day',
                    'subs_smokeless_tobacc_use',
                    'subs_snuff_use',
                    'subs_snuff_method_use',
                    'subs_snuff_use_freq',
                    'subs_tobacco_chew_use',
                    'subs_freq_snuff_use',
                    'subs_tobacco_chew_freq',
                    'subs_tobacco_chew_d_freq',
                    'subs_alcohol_consump',
                    'subs_alcohol_consume_now',
                    'subs_alcohol_consump_freq',
                    'subs_alcohol_cutdown',
                    'subs_alcohol_criticize',
                    'subs_alcohol_guilty',
                    'subs_alcohol_hangover',
                    'subs_alcohol_con_past_yr',
                    'subs_alcoholtype_consumed',
                    'subs_drugs_use',
                    'subs_drug_use_other',
                    'genh_breast_cancer',
                    'genh_breast_cancer_treat',
                    'genh_bre_cancer_treat_now',
                    'genh_bre_cancer_trad_med',
                    'genh_cervical_cancer',
                    'genh_cer_cancer_treat',
                    'genh_cer_cancer_treat_now',
                    'genh_cer_cancer_trad_med',
                    'genh_prostate_cancer',
                    'genh_pro_cancer_treat',
                    'genh_pro_cancer_treat_now',
                    'genh_pro_cancer_trad_med',
                    'genh_oesophageal_cancer',
                    'genh_oes_cancer_treat',
                    'genh_oes_cancer_treat_now',
                    'genh_oesophageal_trad_med',
                    'genh_other_cancers',
                    'genh_other_cancer_treat',
                    'genh_oth_cancer_treat_now',
                    'genh_oth_cancer_trad_med',
                    'genh_obesity_mom',
                    'genh_h_blood_pressure_mom',
                    'genh_h_cholesterol_mom',
                    'genh_breast_cancer_mom',
                    'genh_cervical_cancer_mom',
                    'genh_oes_cancer_mom',
                    'genh_cancer_other_mom',
                    'genh_asthma_mom',
                    'genh_obesity_dad',
                    'genh_h_blood_pressure_dad',
                    'genh_h_cholesterol_dad',
                    'genh_prostate_cancer_dad',
                    'genh_other_cancers_dad',
                    'genh_asthma_dad',
                    'genh_starchy_staple_food',
                    'genh_change_diet',
                    'genh_lose_weight',
                    'genh_pesticide',
                    'genh_pesticide_region',
                    'genh_pesticide_type',
                    'genh_cooking_place',
                    'genh_cooking_done_inside',
                    'genh_energy_source_type',
                    'genh_smoker_in_your_house',
                    'genh_smoke_freq_someone',
                    'genh_insect_repellent_use',
                    'infh_malaria',
                    'infh_malaria_month',
                    'infh_malaria_area',
                    'infh_tb',
                    'infh_tb_12months',
                    'infh_tb_treatment',
                    'infh_tb_meds',
                    'infh_tb_counselling',
                    'infh_tb_traditional_med',
                    'infh_hiv_tested',
                    'infh_hiv_status',
                    'infh_hiv_positive',
                    'infh_hiv_medication',
                    'infh_hiv_arv_meds_now',
                    'infh_hiv_arv_single_pill',
                    'infh_hiv_pill_size',
                    'infh_hiv_traditional_meds',
                    'infh_painful_feet_hands',
                    'infh_hypersensitivity',
                    'infh_kidney_problems',
                    'infh_liver_problems',
                    'infh_change_in_body_shape',
                    'infh_mental_state_change',
                    'infh_chol_levels_change',
                    'infh_hiv_test',
                    'infh_hiv_counselling',
                    'carf_blood_sugar',
                    'carf_diabetes',
                    'carf_diabetes_12months',
                    'carf_diabetes_treatment',
                    'carf_diabetes_treat_now',
                    'carf_diabetes_treat',
                    'carf_diabetes_traditional',
                    'carf_diabetes_history',
                    'carf_diabetes_mother',
                    'carf_diabetes_father',
                    'carf_diabetes_brother_1',
                    'carf_diabetes_brother_2',
                    'carf_diabetes_brother_3',
                    'carf_diabetes_brother_4',
                    'carf_diabetes_sister_1',
                    'carf_diabetes_sister_2',
                    'carf_diabetes_sister_3',
                    'carf_diabetes_sister_4',
                    'carf_diabetes_son_1',
                    'carf_diabetes_son_2',
                    'carf_diabetes_son_3',
                    'carf_diabetes_son_4',
                    'carf_daughter_diabetes_1',
                    'carf_diabetes_daughter_2',
                    'carf_diabetes_daughter_3',
                    'carf_diabetes_daughter_4',
                    'carf_diabetes_fam_other',
                    'carf_stroke',
                    'carf_tia',
                    'carf_weakness',
                    'carf_numbness',
                    'carf_blindness',
                    'carf_half_vision_loss',
                    'carf_understanding_loss',
                    'carf_expression_loss',
                    'carf_angina',
                    'carf_angina_treatment',
                    'carf_angina_treat_now',
                    'carf_angina_traditional',
                    'carf_pain',
                    'carf_pain2',
                    'carf_pain_action_stopslow',
                    'carf_relief_standstill',
                    'carf_pain_location',
                    'carf_heartattack',
                    'carf_heartattack_treat',
                    'carf_heartattack_trad',
                    'carf_congestiv_heart_fail',
                    'carf_chf_treatment',
                    'carf_chf_treatment_now',
                    'carf_chf_traditional',
                    'carf_bp_measured',
                    'carf_hypertension',
                    'carf_hypertension_12mnths',
                    'carf_hypertension_treat',
                    'carf_hypertension_meds',
                    'carf_hypertension_trad',
                    'carf_cholesterol',
                    'carf_h_cholesterol',
                    'carf_chol_treatment',
                    'carf_chol_treatment_now',
                    'carf_chol_traditional',
                    'carf_thyroid',
                    'carf_thyroid_type',
                    'carf_thyroid_treatment',
                    'carf_thyroid_treat_use',
                    'carf_parents_thyroid',
                    'carf_thyroidparnt_specify',
                    'carf_kidney_disease',
                    'carf_kidney_disease_known',
                    'carf_kidney_function_low',
                    'carf_kidney_family',
                    'carf_kidney_family_mother',
                    'carf_kidney_family_father',
                    'carf_kidney_family_other',
                    'carf_kidney_family_type',
                    'carf_joints_swollen_pain',
                    'carf_joints_swollen',
                    'carf_joints_involved',
                    'carf_when_they_hurt',
                    'carf_symptoms_how_long',
                    'carf_arthritis_results',
                    'carf_rheumatoid_factor',
                    'carf_acpa',
                    'carf_esr_crp',
                    'carf_osteo_sites',
                    'carf_osteo_hip_repl_site',
                    'carf_osteo_knee_repl_site',
                    'gpaq_work_weekend',
                    'gpaq_work_sedentary',
                    'gpaq_work_vigorous',
                    'gpaq_work_vigorous_days',
                    'gpaq_work_moderate',
                    'gpaq_work_moderate_days',
                    'gpaq_transport_phy',
                    'gpaq_transport_phy_days',
                    'gpaq_leisure_phy',
                    'gpaq_leisure_vigorous',
                    'gpaq_leisurevigorous_days',
                    'gpaq_leisuremoderate',
                    'gpaq_leisuremoderate_days',
                    'gpaq_sleep_room_livestock',
                    'gpaq_sleep_on',
                    'gpaq_mosquito_net_use',
                    'gpaq_feel_alert',
                    'gpaq_sleeping_difficulty',
                    'gpaq_difficulty_staysleep',
                    'gpaq_waking_early_problem',
                    'gpaq_waking_up_tired',
                    'gpaq_sleep_pattern_satis',
                    'gpaq_sleep_interfere',
                    'anth_measurementcollector',
                    'bppm_measurementcollector',
                    'ultr_vat_scat_measured',
                    'ultr_cimt',
                    'ultr_plaque_measured',
                    'ultr_plaque_technician',
                    'ultr_plaque_right_present',
                    'ultr_dxa_scan_completed',
                    'resp_breath_shortness',
                    'resp_breath_shortness_ever',
                    'resp_mucus',
                    'resp_breath_too_short',
                    'resp_cough',
                    'resp_wheezing_whistling',
                    'resp_asthma_diagnosed',
                    'resp_asthma_treat',
                    'resp_asthma_treat_now',
                    'resp_copd_suffer',
                    'resp_copd_treat',
                    'resp_inhaled_medication',
                    'resp_measles_suffer',
                    'rspe_major_surgery',
                    'rspe_chest_pain',
                    'rspe_coughing_blood',
                    'rspe_acute_retinal_detach',
                    'rspe_any_pain',
                    'rspe_diarrhea',
                    'rspe_high_blood_pressure',
                    'rspe_tb_diagnosed',
                    'rspe_tb_treat_past4wks',
                    'rspe_infection',
                    'rspe_participation',
                    'rspe_wearing_tightclothes',
                    'rspe_wearing_dentures',
                    'rspe_researcher_question',
                    'spiro_eligible',
                    'spiro_researcher',
                    'spiro_pass',
                    'rspir_salb_admin',
                    'rspir_researcher',
                    'micr_take_antibiotics',
                    'micr_diarrhea_last_time',
                    'micr_worm_intestine_treat',
                    'micr_probiotics_t_period',
                    'micr_wormintestine_period',
                    'micr_probiotics_taken',
                    'bloc_fasting_confirmed',
                    'bloc_two_red_tubes',
                    'bloc_one_purple_tube',
                    'bloc_one_grey_tube',
                    'bloc_phlebotomist_name',
                    'bloc_urcontainer_batchnum',
                    'bloc_urine_tube_expiry',
                    'bloc_urine_collector',
                    'poc_test_conducted',
                    'poc_instrument_serial_num',
                    'poc_test_strip_batch_num',
                    'poc_teststrip_expiry_date',
                    'poc_researcher_name',
                    'poc_gluc_results_provided',
                    'poc_chol_results_provided',
                    'poc_glucresults_discussed',
                    'poc_cholresults_discussed',
                    'poc_seek_advice',
                    'poc_hiv_test_conducted',
                    'poc_hiv_pre_test',
                    'poc_test_kit_serial_num',
                    'poc_hiv_strip_batch_num',
                    'poc_hiv_strip_expiry_date',
                    'poc_technician_name',
                    'poc_hiv_test_result',
                    'poc_result_provided',
                    'poc_post_test_counselling',
                    'poc_post_test_worker',
                    'poc_hivpositive_firsttime',
                    'poc_hiv_seek_advice',
                    'tram_injury_ill_assault',
                    'tram_relative_ill_injured',
                    'tram_deceased',
                    'tram_family_friend_died',
                    'tram_marital_separation',
                    'tram_broke_relationship',
                    'tram_problem_with_friend',
                    'tram_unemployed',
                    'tram_sacked_from_your_job',
                    'tram_financial_crisis',
                    'tram_problems_with_police',
                    'tram_some_valued_lost']