import csv
import pandas as pd
import numpy as np
#import datetime as dt
#from datetime import datetime
import math

class AnalysisClassPhase2:

    def __init__(self, data):
        self.data = data

    def filter_redcap_columns(self, input_data):
        all_cols = ['study_id',
                    'redcap_event_name',
                    'gene_site_id',
                    'gene_uni_site_id_correct',
                    'gene_site',
                    'gene_enrolment_date',
                    'gene_start_time',
                    'gene_end_time',
                    'gene_compensation',
                    'demo_approx_dob_is_correct',
                    'demo_dob_is_correct',
                    'demo_date_of_birth_known',
                    'demo_dob_new',
                    'demo_approx_dob_new',
                    'demo_age_at_collection',
                    'demo_gender_is_correct',
                    'demo_gender_correction',
                    'demo_gender',
                    'home_language_confirmation',
                    'home_language',
                    'other_home_language',
                    'demo_home_language',
                    'ethnicity_confirmation',
                    'ethnicity',
                    'other_ethnicity',
                    'gene_identity_confirmed',
                    'participant_identification_complete',
                    'famc_siblings',
                    'famc_number_of_brothers',
                    'famc_living_brothers',
                    'famc_number_of_sisters',
                    'famc_living_sisters',
                    'famc_children',
                    'famc_bio_sons',
                    'famc_living_bio_sons',
                    'famc_bio_daughters',
                    'famc_living_bio_daughters',
                    'family_composition_complete',
                    'preg_pregnant',
                    #'preg_alert',
                    'preg_num_of_pregnancies',
                    'preg_num_of_live_births',
                    'preg_birth_control',
                    'preg_hysterectomy',
                    'preg_regular_periods',
                    'preg_last_period_remember',
                    'preg_last_period_mon',
                    'preg_last_period_mon_2',
                    'preg_period_more_than_yr',
                    'pregnancy_and_menopause_complete',
                    'mari_marital_status',
                    'educ_highest_level',
                    'educ_highest_years',
                    'educ_formal_years',
                    'empl_status',
                    'empl_days_work',
                    'civil_status_marital_status_education_employment_complete',
                    'cogn_read_sentence',
                    'cogn_memory',
                    'cogn_difficulty_remember',
                    'cogn_difficulty_concern',
                    'cogn_learning_new_task',
                    'cogn_words_remember_p1___1',
                    'cogn_words_remember_p1___2',
                    'cogn_words_remember_p1___3',
                    'cogn_words_remember_p1___4',
                    'cogn_words_remember_p1___5',
                    'cogn_words_remember_p1___6',
                    'cogn_words_remember_p1___7',
                    'cogn_words_remember_p1___8',
                    'cogn_words_remember_p1___9',
                    'cogn_words_remember_p1___10',
                    'cogn_words_remember_p1____8',
                   # 'cogn_words_remember_p1___navu',
                    'cogn_words_remember_p1____999',
                    'cogn_imm_recall_score_p1',
                    'cogn_year',
                    'cogn_what_is_the_month',
                    'cogn_day_of_the_month',
                    'cogn_country_of_residence',
                    'cogn_district_province',
                    'cogn_village_town_city',
                    'cogn_weekdays_forward',
                    'cogn_weekdays_backwards',
                    'cogn_orientation_score',
                    'a_cognition_one_complete',
                    'frai_standing_up_time',
                    'frai_use_hands',
                    'frai_sit_stands_completed',
                    'frai_comment',
                    'frai_non_dominant_hand',
                    'frai_dynometer_force_1',
                    'frai_dynometer_force_2',
                    'frai_dynometer_force_3',
                    'frai_complete_procedure',
                    'frai_comment_why',
                    'frai_turn_walk_back',
                    'frai_need_support',
                    'frai_procedure_walk_comp',
                    'frai_please_comment_why',
                    'b_frailty_measurements_complete',
                    'cogn_delayed_recall___1',
                    'cogn_delayed_recall___2',
                    'cogn_delayed_recall___3',
                    'cogn_delayed_recall___4',
                    'cogn_delayed_recall___5',
                    'cogn_delayed_recall___6',
                    'cogn_delayed_recall___7',
                    'cogn_delayed_recall___8',
                    'cogn_delayed_recall___9',
                    'cogn_delayed_recall___10',
                    'cogn_delayed_recall____8',
                    #'cogn_delayed_recall___navu',
                    'cogn_delayed_recall____999',
                    'cogn_delayed_recall_score',
                    'cogn_word_cognition_list___1',
                    'cogn_word_cognition_list___2',
                    'cogn_word_cognition_list___3',
                    'cogn_word_cognition_list___4',
                    'cogn_word_cognition_list___5',
                    'cogn_word_cognition_list___6',
                    'cogn_word_cognition_list___7',
                    'cogn_word_cognition_list___8',
                    'cogn_word_cognition_list___9',
                    'cogn_word_cognition_list___10',
                    'cogn_word_cognition_list___11',
                    'cogn_word_cognition_list___12',
                    'cogn_word_cognition_list___13',
                    'cogn_word_cognition_list___14',
                    'cogn_word_cognition_list___15',
                    'cogn_word_cognition_list___16',
                    'cogn_word_cognition_list___17',
                    'cogn_word_cognition_list___18',
                    'cogn_word_cognition_list___19',
                    'cogn_word_cognition_list___20',
                    'cogn_word_cognition_list____8',
                    #'cogn_word_cognition_list___navu',
                    'cogn_word_cognition_list____999',
                    'cogn_recognition_score',
                    'cogn_different_animals',
                    'cogn_comments',
                    'c_cognition_two_complete',
                    'hous_household_size',
                    'hous_number_of_rooms',
                    'hous_number_of_bedrooms',
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
                    'household_attributes_complete',
                    'subs_tobacco_use',
                    'subs_smoke_100',
                    'subs_smoke_now',
                    'subs_smoke_last_hour',
                    'subs_smoke_cigarettes___1',
                    'subs_smoke_cigarettes___2',
                    'subs_smoke_cigarettes___3',
                    'subs_smoke_cigarettes___4',
                    'subs_smoke_cigarettes___5',
                    'subs_smoke_cigarettes____8',
                    #'subs_smoke_cigarettes___navu',
                    'subs_smoke_cigarettes____999',
                    'subs_smoke_specify',
                    'subs_smoking_frequency',
                    'subs_smoke_per_day',
                    'subs_smoking_start_age',
                    'subs_smoking_stop_year',
                    'subs_smokeless_tobacc_use',
                    'subs_snuff_use',
                    'subs_snuff_method_use',
                    'subs_snuff_use_freq',
                    'subs_freq_snuff_use',
                    'subs_tobacco_chew_use',
                    'subs_tobacco_chew_freq',
                    'subs_tobacco_chew_d_freq',
                    'subs_alcohol_consump',
                    'subs_alcohol_consume_now',
                    'subs_alcohol_consump_freq',
                    'subs_alcohol_consume_freq',
                    'subs_alcohol_cutdown',
                    'subs_alcohol_criticize',
                    'subs_alcohol_guilty',
                    'subs_alcohol_hangover',
                    'subs_alcohol_con_past_yr',
                    'subs_alcoholtype_consumed___1',
                    'subs_alcoholtype_consumed___2',
                    'subs_alcoholtype_consumed___3',
                    'subs_alcoholtype_consumed___4',
                    'subs_alcoholtype_consumed___5',
                    #'subs_alcoholtype_consumed___navu',
                    'subs_alcoholtype_consumed____999',
                    'subs_alcohol_specify',
                    'subs_drugs_use',
                    'subs_drug_use_other',
                    'substance_use_complete',
                    'genh_breast_cancer',
                    'genh_breast_cancer_treat',
                    'genh_bre_cancer_treat_now',
                    'genh_breast_cancer_meds',
                    'genh_bre_cancer_trad_med',
                    'genh_cervical_cancer',
                    'genh_cer_cancer_treat',
                    'genh_cer_cancer_treat_now',
                    'genh_cervical_cancer_meds',
                    'genh_cer_cancer_trad_med',
                    'genh_prostate_cancer',
                    'genh_pro_cancer_treat',
                    'genh_pro_cancer_treat_now',
                    'genh_prostate_cancer_meds',
                    'genh_pro_cancer_trad_med',
                    'genh_oesophageal_cancer',
                    'genh_oes_cancer_treat',
                    'genh_oes_cancer_treat_now',
                    'genh_oes_cancer_meds',
                    'genh_oesophageal_trad_med',
                    'genh_other_cancers',
                    'genh_cancer_specify_other',
                    'genh_other_cancer_treat',
                    'genh_oth_cancer_treat_now',
                    'genh_other_cancer_meds',
                    'genh_oth_cancer_trad_med',
                    'a_general_health_cancer_complete',
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
                    'b_general_health_family_history_complete',
                    'genh_days_fruit',
                    'genh_fruit_servings',
                    'genh_days_veg',
                    'genh_veg_servings',
                    'genh_starchy_staple_food___1',
                    'genh_starchy_staple_food___2',
                    'genh_starchy_staple_food___3',
                    'genh_starchy_staple_food___4',
                    'genh_starchy_staple_food___5',
                    'genh_starchy_staple_food___6',
                    'genh_starchy_staple_food___7',
                    'genh_starchy_staple_food___8',
                    'genh_starchy_staple_food___9',
                    'genh_starchy_staple_food___10',
                    'genh_starchy_staple_food___11',
                    'genh_starchy_staple_food___12',
                    'genh_starchy_staple_food____8',
                    #'genh_starchy_staple_food___navu',
                    'genh_starchy_staple_food____999',
                    'genh_starchy_staple_freq',
                    'genh_staple_servings',
                    'genh_vendor_meals',
                    'genh_sugar_drinks',
                    'genh_juice',
                    'genh_change_diet',
                    'genh_lose_weight',
                    'c_general_health_diet_complete',
                    'genh_pesticide',
                    'genh_pesticide_years',
                    'genh_pesticide_region',
                    'genh_pesticide_type',
                    'genh_pesticide_list',
                    'genh_cooking_place',
                    'genh_cookingplace_specify',
                    'genh_cooking_done_inside',
                    'genh_smoker_in_your_house',
                    'genh_energy_source_type___1',
                    'genh_energy_source_type___2',
                    'genh_energy_source_type___3',
                    'genh_energy_source_type___4',
                    'genh_energy_source_type___5',
                    'genh_energy_source_type___6',
                    #'genh_energy_source_type___navu',
                    'genh_energy_source_type____999',
                    'genh_energy_specify',
                    'genh_smoker_in_your_house',
                    'genh_smoke_freq_someone',
                    'genh_insect_repellent_use',
                    'd_general_health_exposure_to_pesticides_pollutants_complete',
                    'infh_malaria',
                    'infh_malaria_month',
                    'infh_malaria_area',
                    'infh_tb',
                    'infh_tb_12months',
                    'infh_tb_diagnosed',
                    'infh_tb_treatment',
                    'infh_tb_meds',
                    'infh_tb_counselling',
                    'infh_tb_traditional_med',
                    'infh_hiv_que_answering',
                    'infh_hiv_tested',
                    'infh_hiv_status',
                    'infh_hiv_positive',
                    'infh_hiv_diagnosed',
                    'infh_hiv_medication',
                    'infh_hiv_treatment',
                    'infh_hiv_arv_meds',
                    'infh_hiv_arv_meds_now',
                    'infh_hiv_arv_meds_specify',
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
                    'infection_history_complete',
                    'carf_blood_sugar',
                    'carf_diabetes',
                    'carf_diabetes_12months',
                    'carf_diabetes_treatment',
                    'carf_diabetes_treat_now',
                    'carf_diabetes_treat___1',
                    'carf_diabetes_treat___2',
                    'carf_diabetes_treat___3',
                    'carf_diabetes_treat___4',
                    'carf_diabetes_treat___5',
                    #'carf_diabetes_treat___navu',
                    'carf_diabetes_treat____999',
                    'carf_diabetetreat_specify',
                    'carf_diabetes_meds_2',
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
                    'carf_stroke_diagnosed',
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
                    'carf_angina_meds',
                    'carf_angina_traditional',
                    'carf_pain',
                    'carf_pain2',
                    'carf_pain_action_stopslow',
                    'carf_relief_standstill',
                    'carf_pain_location___1',
                    'carf_pain_location___2',
                    'carf_pain_location___3',
                    'carf_pain_location___4',
                    'carf_pain_location___5',
                    'carf_pain_location___6',
                    'carf_pain_location___7',
                    'carf_pain_location___8',
                    'carf_pain_location___9',
                    'carf_pain_location___10',
                    'carf_pain_location___11',
                    'carf_pain_location___12',
                    'carf_pain_location___13',
                    'carf_pain_location___14',
                    'carf_pain_location___15',
                    'carf_pain_location___16',
                    'carf_pain_location___17',
                    'carf_pain_location___18',
                   # 'carf_pain_location___navu',
                    'carf_pain_location____999',
                    'carf_heartattack',
                    'carf_heartattack_treat',
                    'carf_heartattack_meds',
                    'carf_heartattack_trad',
                    'carf_congestiv_heart_fail',
                    'carf_chf_treatment',
                    'carf_chf_treatment_now',
                    'carf_chf_meds',
                    'carf_chf_traditional',
                    'b_cardiometabolic_risk_factors_heart_conditions_complete',
                    'carf_bp_measured',
                    'carf_hypertension',
                    'carf_h_cholesterol',
                    'carf_hypertension_12mnths',
                    'carf_hypertension_treat',
                    'carf_hypertension_meds',
                    'carf_hypertension_medlist',
                    'carf_hypertension_trad',
                    'carf_cholesterol',
                    'carf_chol_treatment',
                    'carf_chol_treatment_now___1',
                    'carf_chol_treatment_now___2',
                    'carf_chol_treatment_now___3',
                    'carf_chol_treatment_now___4',
                    #'carf_chol_treatment_now___navu',
                    'carf_chol_treatment_now____999',
                    'carf_chol_treat_specify',
                    'carf_chol_medicine',
                    'carf_chol_traditional',
                    'c_cardiometabolic_risk_factors_hypertension_choles_complete',
                    'carf_thyroid',
                    'carf_thyroid_type',
                    'carf_thryroid_specify',
                    'carf_thyroid_treatment',
                    'carf_thyroid_treat_use',
                    'carf_parents_thyroid',
                    'carf_thyroidparnt_specify',
                    'carf_kidney_disease',
                    'carf_kidney_disease_known',
                    'carf_kidneydiseas_specify',
                    'carf_kidney_function_low',
                    'carf_kidney_family',
                    'carf_kidney_family_mother',
                    'carf_kidney_family_father',
                    'carf_kidney_family_other',
                    'carf_kidney_fam_specify',
                    'carf_kidney_family_type',
                    'carf_kidney_fam_tspecify',
                    'carf_joints_swollen_pain',
                    'carf_joints_swollen',
                    'carf_joints_involved',
                    'carf_when_they_hurt',
                    'carf_symptoms_how_long',
                    'carf_arthritis_results',
                    'carf_rheumatoid_factor',
                    'carf_acpa',
                    'carf_esr_crp',
                    'carf_osteo',
                    'carf_osteo_sites___1',
                    'carf_osteo_sites___2',
                    'carf_osteo_sites___3',
                    'carf_osteo_sites___4',
                    'carf_osteo_sites___5',
                    'carf_osteo_sites___6',
                    #'carf_osteo_sites___navu',
                    'carf_osteo_sites____999',
                    'carf_osteo_hip_replace',
                    'carf_osteo_hip_repl_site___1',
                    'carf_osteo_hip_repl_site___2',
                    'carf_osteo_hip_repl_site___3',
                    #'carf_osteo_hip_repl_site___navu',
                    'carf_osteo_hip_repl_site____999',
                    'carf_osteo_hip_repl_age',
                    'carf_osteo_knee_replace',
                    'carf_osteo_knee_repl_site',
                    'carf_osteo_knee_repl_age',
                    'd_cardiometabolic_risk_factors_kidney_thyroid_ra_complete',
                    'gpaq_work_weekend',
                    'gpaq_work_sedentary',
                    'gpaq_work_vigorous',
                    'gpaq_work_vigorous_days',
                    'gpaq_work_vigorous_time',
                    'gpaq_work_vigorous_hrs',
                    'gpaq_work_vigorous_mins',
                    'gpaq_work_moderate',
                    'gpaq_work_moderate_days',
                    'gpaq_work_moderate_time',
                    'gpaq_work_moderate_hrs',
                    'gpaq_work_moderate_mins',
                    'gpaq_work_day_time',
                    'gpaq_work_day_hrs',
                    'gpaq_work_day_mins',
                    'gpaq_leisure_phy',
                    'gpaq_transport_phy',
                    'gpaq_transport_phy_days',
                    'gpaq_transport_phy_time',
                    'gpaq_transport_phy_hrs',
                    'gpaq_transport_phy_mins',
                    'gpaq_leisure_phy',
                    'gpaq_leisure_vigorous',
                    'gpaq_leisurevigorous_days',
                    'gpaq_leisurevigorous_time',
                    'gpaq_leisurevigorous_hrs',
                    'gpaq_leisurevigorous_mins',
                    'gpaq_leisuremoderate',
                    'gpaq_leisuremoderate_days',
                    'gpaq_leisuremoderate_time',
                    'gpaq_leisuremoderate_hrs',
                    'gpaq_leisuremoderate_mins',
                    'gpaq_work_day_stng_time',
                    'gpaq_work_day_stng_hrs',
                    'gpaq_work_day_stng_mins',
                    'gpaq_non_work_day_time',
                    'gpaq_non_work_day_hrs',
                    'gpaq_non_work_day_mins',
                    'gpaq_week_sleep_time',
                    'gpaq_week_wakeup_time',
                    'gpaq_weekend_sleep_time',
                    'gpaq_weekend_wakeup_time',
                    'gpaq_sleep_room_pple_num',
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
                    'physical_activity_and_sleep_complete',
                    'anth_standing_height',
                    'anth_weight',
                    'anth_waist_circumf_1',
                    'anth_waist_circumf_2',
                    'anth_waist_circumf',
                    'anth_hip_circumf_1',
                    'anth_hip_circumf_2',
                    'anth_hip_circumf',
                    'anth_measurementcollector',
                    'anthropometric_measurements_complete',
                    'bppm_systolic_1',
                    'bppm_diastolic_1',
                    'bppm_pulse_1',
                    'bppm_measurement_time_1',
                    'bppm_systolic_2',
                    'bppm_diastolic_2',
                    'bppm_pulse_2',
                    'bppm_measurement_time_2',
                    'bppm_systolic_3',
                    'bppm_diastolic_3',
                    'bppm_pulse_3',
                    'bppm_measurement_time_3',
                    'bppm_measurementcollector',
                    'bppm_systolic_avg',
                    'bppm_diastolic_avg',
                    'bppm_pulse_avg',
                    'blood_pressure_and_pulse_measurements_complete',
                    'ultr_vat_scat_measured',
                    'ultr_comment',
                    'ultr_technician',
                    'ultr_visceral_fat',
                    'ultr_subcutaneous_fat',
                    'ultr_cimt',
                    'ultr_cimt_comment',
                    'ultr_cimt_technician',
                    'ultr_cimt_right_min',
                    'ultr_cimt_right_max',
                    'ultr_cimt_right_mean',
                    'ultr_cimt_left_min',
                    'ultr_cimt_left_max',
                    'ultr_cimt_left_mean',
                    'ultr_plaque_measured',
                    'ultr_plaque_comment',
                    'ultr_plaque_technician',
                    'ultr_plaque_right_present',
                    'ultr_plaque_right',
                    'ultr_plaque_left_present',
                    'ultr_plaque_left',
                    'ultr_dxa_scan_completed',
                    'ultr_dxa_scan_comment',
                    'ultr_dxa_measurement_1',
                    'ultr_dxa_measurement_2',
                    'ultr_dxa_measurement_3',
                    'ultr_dxa_measurement_4',
                    'ultr_dxa_measurement_5',
                    'ultrasound_and_dxa_measurements_complete',
                    'resp_breath_shortness',
                    'resp_breath_shortness_ever',
                    'resp_mucus',
                    'resp_breath_too_short',
                    'resp_cough',
                    'resp_wheezing_whistling',
                    'resp_asthma_diagnosed',
                    'resp_age_diagnosed',
                    'resp_asthma_treat',
                    'resp_asthma_treat_now',
                    'resp_copd_suffer___1',
                    'resp_copd_suffer___2',
                    'resp_copd_suffer___3',
                    'resp_copd_suffer___0',
                    'resp_copd_suffer___9',
                    #'resp_copd_suffer___navu',
                    'resp_copd_suffer____999',
                    'resp_copd_treat',
                    'resp_inhaled_medication',
                    'resp_medication_list',
                    'resp_puffs_time',
                    'resp_puffs_times_day',
                    'resp_measles_suffer___1',
                    'resp_measles_suffer___2',
                    'resp_measles_suffer___0',
                    'resp_measles_suffer___9',
                    #'resp_measles_suffer___navu',
                    'resp_measles_suffer____999',
                    'a_respiratory_health_complete',
                    'rspe_major_surgery',
                    'rspe_chest_pain',
                    'rspe_coughing_blood',
                    'rspe_acute_retinal_detach',
                    'rspe_any_pain',
                    'rspe_diarrhea',
                    'rspe_high_blood_pressure',
                    'rspe_tb_diagnosed',
                    'rspe_tb_treat_past4wks',
                    'rspe_infection___1',
                    'rspe_infection___2',
                    'rspe_infection___3',
                    'rspe_infection___4',
                    'rspe_infection___0',
                    #'rspe_infection___navu',
                    'rspe_infection____999',
                    'rspe_participation',
                    'rspe_wearing_tightclothes',
                    'rspe_wearing_dentures',
                    'rspe_participation_note',
                    'rspe_researcher_question',
                    'b_spirometry_eligibility_complete',
                    'spiro_eligible',
                    'spiro_researcher',
                    'spiro_num_of_blows',
                    'spiro_num_of_vblows',
                    'spiro_pass',
                    'spiro_comment',
                    'c_spirometry_test_complete',
                    'rspir_salb_admin',
                    'rspir_salb_time_admin',
                    'rspir_time_started',
                    'rspir_researcher',
                    'rspir_num_of_blows',
                    'rspir_num_of_vblows',
                    'rspir_comment',
                    'd_reversibility_test_complete',
                    'micr_take_antibiotics',
                    'micr_diarrhea_last_time',
                    'micr_worm_intestine_treat',
                    'micr_probiotics_t_period',
                    'micr_wormintestine_period',
                    'micr_probiotics_taken',
                    'a_microbiome_complete',
                    'bloc_last_eat_time',
                    'bloc_last_ate_hrs',
                    'bloc_last_drink_time',
                    'bloc_hours_last_drink',
                    'bloc_fasting_confirmed',
                    'bloc_two_red_tubes',
                    'bloc_red_tubes_num',
                    'bloc_one_purple_tube',
                    'bloc_if_no_purple_tubes',
                    'bloc_one_grey_tube',
                    'bloc_grey_tubes_no',
                    'bloc_phlebotomist_name',
                    'bloc_blood_taken_date',
                    'bloc_bloodcollection_time',
                    'b_blood_collection_complete',
                    'bloc_urine_collected',
                    'bloc_specify_reason',
                    'bloc_urcontainer_batchnum',
                    'bloc_urine_tube_expiry',
                    'bloc_urine_collector',
                    'bloc_urine_taken_date',
                    'bloc_urinecollection_time',
                    'c_urine_collection_complete',
                    'poc_test_conducted',
                    'poc_comment',
                    'poc_instrument_serial_num',
                    'poc_test_strip_batch_num',
                    'poc_teststrip_expiry_date',
                    'poc_test_date',
                    'poc_test_time',
                    'poc_researcher_name',
                    'poc_glucose_test_result',
                    'poc_chol_result',
                    'poc_gluc_results_provided',
                    'poc_gluc_results_notes',
                    'poc_chol_results_provided',
                    'poc_cholresults_discussed',
                    'poc_seek_advice',
                    'poc_hiv_test_conducted',
                    'poc_hiv_comment',
                    'poc_hiv_pre_test',
                    'poc_pre_test_worker',
                    'poc_test_kit_serial_num',
                    'poc_hiv_strip_batch_num',
                    'poc_hiv_strip_expiry_date',
                    'poc_hiv_test_date_done',
                    'poc_technician_name',
                    'poc_hiv_test_result',
                    'poc_result_provided',
                    'poc_post_test_counselling',
                    'poc_post_test_worker',
                    'poc_hivpositive_firsttime',
                    'poc_hiv_seek_advice',
                    'point_of_care_testing_complete',
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
                    'tram_some_valued_lost',
                    'trauma_complete'
                    ]
        #filter columns
        output_data = input_data[all_cols]
        #replace -999
        #output_data = output_data.replace(-999, np.nan)

        return output_data

    def add_calculated_variables(self, input_data):

        output_data = input_data
        #dates encoding
        output_data['demo_dob_new'] = pd.to_datetime(output_data.demo_dob_new)
        output_data['demo_approx_dob_new'] = pd.to_datetime(output_data.demo_approx_dob_new)
        output_data['gene_enrolment_date'] = pd.to_datetime(output_data.gene_enrolment_date)

        def country(df):

            if ((df['gene_site']==1)  or (df['gene_site']==2)  or (df['gene_site']==6)):
                return 'South Africa'
            elif (df['gene_site']== 3):
                return 'Kenya'
            elif (df['gene_site']== 4):
                return  'Burkina Faso'
            elif (df['gene_site']== 5):
                return 'Ghana'
        output_data['country'] = output_data.apply(country, axis=1)

        def region(df):

            if ((df['gene_site']==1)  or (df['gene_site']==2)  or (df['gene_site']==6)):
                return 'South Africa'
            elif (df['gene_site'] == 3):
                return 'East Africa'
            elif ((df['gene_site']==4) or (df['gene_site']==5)):
                return 'West Africa'

        output_data['region'] = output_data.apply(region, axis=1)

        #site_type
        def site_type(df):

            if ((df['gene_site']==1)  or (df['gene_site']==2)  or (df['gene_site']==4) or (df['gene_site'] == 5)):
                return 1
            elif ((df['gene_site'] == 3) or (df['gene_site']==6)):
                return 2

        output_data['site_type'] = output_data.apply(site_type, axis=1)

        #number of siblings

        output_data['famc_number_of_siblings'] = output_data['famc_number_of_brothers'] + output_data['famc_number_of_sisters']

        # number of children
        output_data['famc_number_of_children'] = output_data['famc_bio_sons'] + output_data['famc_bio_daughters']

        # menopause status calculation

        output_data['day'] = 1
        output_data['year'] = np.floor(output_data['preg_last_period_mon_2']).astype(str).str[:4]
        output_data['month'] = np.floor(output_data['preg_last_period_mon']).astype(str).str[:2]

        def last_period(df):
            if ((pd.isna(df['preg_last_period_mon'])) or (pd.isna(df['preg_last_period_mon_2']))):
                return np.nan
            elif ((df['preg_last_period_mon'] != np.nan) and (df['preg_last_period_mon_2'] != np.nan)):
                return pd.to_datetime(str(df['year']) + '-' + str(df['month']) + '-' + str(df['day']), yearfirst=False)

        output_data['meno_last_period'] = output_data.apply(last_period, axis=1)


        output_data['meno_months_since_last_period'] = (
                    (output_data['gene_enrolment_date'] - output_data['meno_last_period']) / np.timedelta64(1, 'M'))
        output_data['meno_months_since_last_period'] = output_data['meno_months_since_last_period'].astype(int, errors='ignore')
        output_data['meno_months_since_last_period'] = np.floor(output_data['meno_months_since_last_period'])

        def meno(df):

            if (df['preg_regular_periods'] == 1):
                return 1
            elif ((df['preg_regular_periods'] == 0) and (df['preg_period_more_than_yr'] == 0)) or \
                    ((df['preg_regular_periods'] == 0) and (df['meno_months_since_last_period'] <= 12)):
                return 2
            elif (df['preg_period_more_than_yr'] == 1) or (
                    (df['preg_regular_periods'] == 0) and (df['meno_months_since_last_period'] > 12)) or \
                    ((df['demo_age_at_collection']>=55) and (df['demo_gender']==0)):
                return 3

        output_data['preg_menopause_status'] = output_data.apply(meno, axis=1)

        #partnership status calculation

        def partnership_status(df):

            if (df['mari_marital_status']==3):
                return 0
            elif ((df['mari_marital_status']==1) or (df['mari_marital_status']==2)):
                return 1
            elif (df['mari_marital_status']>=4):
                return 2

        output_data['mari_partnership_status'] = output_data.apply(partnership_status, axis=1)

        #frailty calcualtion
        output_data['frai_dynometer_force_max'] = output_data[["frai_dynometer_force_1", "frai_dynometer_force_2",
                                                               'frai_dynometer_force_3']].max(axis=1)

        # people to room density calculation
        output_data['hous_people_to_rooms_density'] = (output_data['hous_household_size']/output_data['hous_number_of_rooms']).round(decimals=2)

        # people to bedroom density calculation
        output_data['hous_people_to_bedrooms_density'] = (output_data['hous_household_size'] / output_data['hous_number_of_bedrooms']).round(decimals=2)

        #house attributes ses calculation
        house_col = ['hous_electricity', 'hous_solar_energy', 'hous_power_generator', 'hous_alter_power_src',  'hous_television',
                    'hous_radio', 'hous_motor_vehicle', 'hous_motorcycle', 'hous_bicycle', 'hous_refrigerator', 'hous_washing_machine',
                    'hous_sewing_machine', 'hous_telephone', 'hous_mobile_phone', 'hous_microwave', 'hous_dvd_player',
                    'hous_satellite_tv_or_dstv', 'hous_computer_or_laptop', 'hous_internet_by_computer', 'hous_internet_by_m_phone',
                    'hous_electric_iron', 'hous_fan', 'hous_electric_gas_stove', 'hous_kerosene_stove', 'hous_plate_gas',
                    'hous_electric_plate', 'hous_torch', 'hous_gas_lamp', 'hous_kerosene_lamp', 'hous_toilet_facilities',
                    'hous_portable_water', 'hous_grinding_mill', 'hous_table', 'hous_sofa', 'hous_wall_clock', 'hous_bed',
                    'hous_mattress', 'hous_blankets', 'hous_cattle', 'hous_other_livestock', 'hous_poultry',
                    'hous_tractor','hous_plough']

        #replace 2 and -8 oprions to 0
        for col in house_col:
            output_data[col] = output_data[col].replace({2: 0, -8: 0})

        #ses site totals
        #to check
        #ses calculation: adding all household attributes per participant

        output_data['hous_participant_ses'] = output_data[house_col].sum(axis=1)

        #ses % calculation
        def ses(df):
            if df['gene_site'] == 6:
                return df['hous_participant_ses']/22
            elif df['gene_site'] == 5:
                return df['hous_participant_ses']/36
            elif df['gene_site'] == 4:
                return df['hous_participant_ses']/30
            elif df['gene_site'] == 3:
                return df['hous_participant_ses']/34
            elif df['gene_site'] == 2:
                return df['hous_participant_ses']/27
            elif df['gene_site'] == 1:
                return df['hous_participant_ses']/27

        output_data['hous_ses_perc'] = output_data.apply(ses, axis=1)

        #ses site quintile calculation
        output_data['hous_ses_quintiles'] = pd.cut(output_data['hous_ses_perc'], 5,labels=[1, 2, 3 ,4, 5])

        #mvpa calculation
        output_data['gpaq_work_vigourous_total_minutes'] = output_data['gpaq_work_vigorous_days'] * output_data['gpaq_work_vigorous_time']
        output_data['gpaq_work_moderate_total_minutes'] = output_data['gpaq_work_moderate_days'] * output_data['gpaq_work_moderate_time']
        output_data['gpaq_transport_phy_total_minutes'] = output_data['gpaq_transport_phy_days'] * output_data['gpaq_transport_phy_time']
        output_data['gpaq_leisure_vigorous_total_minutes'] = output_data['gpaq_leisurevigorous_days'] * output_data['gpaq_leisurevigorous_time']
        output_data['gpaq_leisure_moderate_total_minutes'] = output_data['gpaq_leisuremoderate_days'] * output_data['gpaq_leisuremoderate_time']

        output_data['gpaq_mvpa'] = output_data[['gpaq_work_vigourous_total_minutes', 'gpaq_work_moderate_total_minutes',
                   'gpaq_transport_phy_total_minutes', 'gpaq_leisure_vigorous_total_minutes',
                   'gpaq_leisure_moderate_total_minutes']].sum(axis=1)

        #cancer stautus calculation
        def cancer_status(df):
            if ((df['genh_breast_cancer'] == 0) and (df['genh_cervical_cancer']==0) and
                    (df['genh_prostate_cancer'] == 0) and (df['genh_oesophageal_cancer']==0) and
                    (df['genh_other_cancers']==0)):
                return 0
            elif ((df['genh_breast_cancer'] == 1) or (df['genh_cervical_cancer']==1) or
                    (df['genh_prostate_cancer'] == 1) or (df['genh_oesophageal_cancer']==1) or
                    (df['genh_other_cancers']==1)):
                return 1

        output_data['genh_cancer_status'] = output_data.apply(cancer_status, axis=1)

        #hiv_status calculation
        def hiv(df):
            if (df['infh_hiv_positive'] == df['poc_hiv_test_result']):
                return df['infh_hiv_positive']
            elif ((df['infh_hiv_positive'] == 1) or (df['poc_hiv_test_result'] == 1)):
                return 1
            elif ((df['infh_hiv_positive'] == 2 or pd.isna(df['infh_hiv_positive'])) and (df['poc_hiv_test_result'] == 1)):
                return 1
            elif ((df['infh_hiv_positive'] == 1) and (df['poc_hiv_test_result'] == 2 or pd.isna(df['poc_hiv_test_result']))):
                return 1
            elif ((df['infh_hiv_positive'] == 0) and
                  (pd.isna(df['poc_hiv_test_result']) or df['poc_hiv_test_result'] == -999 or
                    df['poc_hiv_test_result'] == 2)):
                return 0
            elif ((pd.isna(df['infh_hiv_positive']) or df['infh_hiv_positive'] == -999 or
                   df['infh_hiv_positive'] == 2) and (df['poc_hiv_test_result'] == 0)):
                return 0
            elif ((df['infh_hiv_positive'] == 2) and (
                    pd.isna(df['poc_hiv_test_result']) or df['poc_hiv_test_result'] == -999)):
                return 2
            elif ((pd.isna(df['infh_hiv_positive']) or df['infh_hiv_positive'] == -999) and
                  (df['poc_hiv_test_result'] == 2)):
                return 2

        output_data['infh_hiv_status_calculated'] = output_data.apply(hiv, axis=1)

        # diabetes status point of care calculation
        def diabetes_status_poc(df):
            if ((df['carf_diabetes'] ==1) |
                    ((df['bloc_fasting_confirmed']==1) & (df['poc_glucose_test_result']>=7)) |
                    ((df['bloc_fasting_confirmed'] == 0) & (df['poc_glucose_test_result'] >= 11.1))
            ):
                return 1
            elif ((df['carf_diabetes'] ==0) |
                    ((df['bloc_fasting_confirmed']==1) & (df['poc_glucose_test_result']<7)) |
                    ((df['bloc_fasting_confirmed'] == 0) & (df['poc_glucose_test_result'] < 11.1))
            ):
                return 0

        output_data['carf_diabetes_status_poc'] = output_data.apply(diabetes_status_poc, axis=1)

        # diabetes status lab calculation
        def diabetes_status_lab(df):
            if ((df['carf_diabetes'] == 1) |
                    ((df['bloc_fasting_confirmed'] == 1) & (df['glucose'] >= 7)) |
                    ((df['bloc_fasting_confirmed'] == 0) & (df['glucose'] >= 11.1))
            ):
                return 1
            elif ((df['carf_diabetes'] == 0) |
                  ((df['bloc_fasting_confirmed'] == 1) & (df['glucose'] < 7)) |
                  ((df['bloc_fasting_confirmed'] == 0) & (df['glucose'] < 11.1))
            ):
                return 0

        output_data['carf_diabetes_status_lab'] = output_data.apply(diabetes_status_lab, axis=1)

        # hypertension status calculation
        def hypertension(df):

            if ((df['bppm_systolic_avg']>= 140) or (df['bppm_diastolic_avg']>=90) or (df['carf_hypertension'] == 1)):
                return 1
            elif (((df['bppm_systolic_avg']<140) & (df['bppm_diastolic_avg']<90)) and \
                  ((df['carf_hypertension']==0) or (df['carf_hypertension']==2))):
                return 0
        output_data['carf_hypertension_status'] = output_data.apply(hypertension, axis=1)

        #converting hip and wait circumfance to mm
        output_data['anth_waist_circumf'] = output_data['anth_waist_circumf']*10
        output_data['anth_hip_circumf'] = output_data['anth_hip_circumf']*10

        #BMI calculation
        output_data['anth_bmi_c'] = output_data.anth_weight / (output_data.anth_standing_height / 1000) ** 2

        # bmi class encoding
        bins = [0, 18.5, 25, 30, 70]
        # labels = ['<18.5', '18.5-24.9', '25-30 ', '>=30']
        labels = [0, 1, 2, 3]
        output_data['anth_bmi_cat'] = pd.cut(output_data['anth_bmi_c'], bins=bins, labels=labels)

        output_data['anth_waist_hip_ratio'] = output_data['anth_waist_circumf']/output_data['anth_hip_circumf']

        # remember to re-encode the -999
        output_data['lipids_friedewald_ldl'] = np.where(output_data['lipids_triglycerides'] > 4.52, np.nan,
                                                        output_data['lipids_cholesterol'] - (
                                                                    output_data['lipids_triglycerides'] / 2.2) -
                                                        output_data['lipids_hdl'])

        # non_hdl calculation
        def lipids_nonhdl(df):

            if ((df['lipids_cholesterol'] > 0) and (df['lipids_hdl'] > 0)):
                return df['lipids_cholesterol'] - df['lipids_hdl']
            else:
                np.nan

        output_data['lipids_nonhdl'] = output_data.apply(lipids_nonhdl, axis=1)

        # dyslipidemia calculation
        def dyslipidemia(df):

            if ((pd.isna(df['carf_chol_treatment'])) and (pd.isna(df['lipids_cholesterol'])) and \
                    (pd.isna(df['lipids_hdl'])) and (pd.isna(df['lipids_hdl'])) and \
                    (pd.isna(df['lipids_ldl_calculated'])) and (pd.isna(df['lipids_triglycerides']))):
                return np.nan
            elif ((df['carf_chol_treatment'] == 1) or (df['lipids_cholesterol'] >= 5) or \
                  ((df['lipids_hdl'] < 1) and (df['demo_gender'] == 1)) or \
                  ((df['lipids_hdl'] < 1.3) and (df['demo_gender'] == 0)) or (df['lipids_ldl_calculated'] >= 3) or \
                  (df['lipids_triglycerides'] >= 1.7)):
                return 1
            else:
                return 0

        output_data['dyslipidemia'] = output_data.apply(dyslipidemia, axis=1)

        # egrf calculation
        output_data['serum_creatinine_2'] = output_data['serum_creatinine'].replace({-111:np.nan, -999:np.nan})
        output_data['one'] = 1
        output_data['female_cal'] = output_data['serum_creatinine_2']/61.9
        output_data['male_cal'] = output_data['serum_creatinine_2']/79.6

        output_data['female_min'] = output_data[['female_cal', 'one']].min(axis=1)
        output_data['female_max'] = output_data[['female_cal', 'one']].max(axis=1)

        output_data['male_min'] = output_data[['male_cal', 'one']].min(axis=1)
        output_data['male_max'] = output_data[['male_cal', 'one']].max(axis=1)

        def egfr(df):
            if (df['serum_creatinine_2'] == 0):
                return np.nan
            elif (df['demo_gender'] == 0):
                #return (141 * (df['female_min']**(-0.329))*(df['female_max']**(-1.209))*(0.993**df['demo_age_at_collection'])*1.018).real
                return 141 * ((df['female_min'])**(-0.329))*((df['female_max'])**(-1.209))*((0.993)**(df['demo_age_at_collection']))*1.018
            elif (df['demo_gender'] == 1):
                #return (141*(df['male_min']**(-0.411))*(df['male_min']**(-1.209))*(0.993**df['demo_age_at_collection'])).real
                return 141*((df['male_min'])**(-0.411))*((df['male_min'])**(-1.209))*((0.993)**(df['demo_age_at_collection']))
            else:
                return np.nan

        output_data['egfr'] = output_data.apply(egfr, axis=1)

        # ckd calculation
        def ckd(df):

            if ((pd.isna(df['egfr'])) or (pd.isna(df['urine_acr']))):
                return np.nan
            elif ((df['egfr'] < 60) or (df['urine_acr'] > 3)):
                return 1
            elif ((df['egfr'] >= 60) or (df['urine_acr'] <= 33)):
                return 0
            else:
                np.nan

        output_data['ckd'] = output_data.apply(ckd, axis=1)

        return output_data

        
