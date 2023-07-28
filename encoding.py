import csv
import pandas as pd
import numpy as np

class Encodings:

    def __init__(self, data):
        self.data = data
        #self.site = site

    def encoding(self, data):
        # changing encodings

        # sites
        data.gene_site = data.gene_site.replace({1: 'Agincourt', 2: 'DIMAMO', 3: 'Nairobi'
                                                    , 4: 'Nanoro', 5: 'Navrongo', 6: 'Soweto'})

        # age_class calculation
        bins = [0, 40, 50, 55, 60, 65, 70, 75, 80, 85, 90]
        labels = ['age>40', '40<=age<50', '50<=age<55', '55<=age<60', '60<=age<65', '65<=age<70', '70<=age<75',
                  '75<=age<80', '85<=age<85', 'age>85']
        data['age_class'] = pd.cut(data['demo_age_at_collection'], bins=bins, labels=labels)

        # gender encoding
        data.demo_gender = data.demo_gender.replace({0: 'Female', 1: 'Male'})

        # setting -111 for less than the acceptable mimimum values
        data['frai_standing_up_time'] = np.where((data['frai_standing_up_time'] < 6), -111,
                                                 data['frai_standing_up_time'])

        # bmi class encoding
        bins = [0, 18.5, 25, 30, 70]
        # labels = ['<18.5', '18.5-24.9', '25-30 ', '>=30']
        labels = ['Underweight', 'Normal', 'Overweight', 'Obese']
        data['bmi_cat'] = pd.cut(data['anth_bmi_c'], bins=bins, labels=labels)

        # 'yes', 'no', 'dont know', 'Decline at answer' encoding
        col_category = ['famc_siblings', 'famc_children', 'preg_pregnant', 'preg_birth_control',
                'preg_hysterectomy', 'preg_regular_periods', 'preg_last_period_remember',
                'preg_period_more_than_yr' ,'cogn_words_remember_p1___1',
                'cogn_words_remember_p1___2', 'cogn_words_remember_p1___3', 'cogn_words_remember_p1___4',
                'cogn_words_remember_p1___5', 'cogn_words_remember_p1___6', 'cogn_words_remember_p1___7',
                'cogn_words_remember_p1___8', 'cogn_words_remember_p1___9', 'cogn_words_remember_p1___10',
                'cogn_words_remember_p1____8', 'cogn_delayed_recall___1', 'cogn_delayed_recall___2',
                'cogn_delayed_recall___3', 'cogn_delayed_recall___4', 'cogn_delayed_recall___5',
                'cogn_delayed_recall___6', 'cogn_delayed_recall___7', 'cogn_delayed_recall___8',
                'frai_use_hands', 'frai_sit_stands_completed', 'frai_complete_procedure',
                'frai_need_support', 'frai_procedure_walk_comp',
                'cogn_word_cognition_list___1', 'cogn_word_cognition_list___2', 'cogn_word_cognition_list___3',
                'cogn_word_cognition_list___4', 'cogn_word_cognition_list___5', 'cogn_word_cognition_list___6',
                'cogn_word_cognition_list___7', 'cogn_word_cognition_list___8', 'cogn_word_cognition_list___9',
                'cogn_word_cognition_list___10', 'cogn_word_cognition_list___11', 'cogn_word_cognition_list___12',
                'cogn_word_cognition_list___13', 'cogn_word_cognition_list___14', 'cogn_word_cognition_list___15',
                'cogn_word_cognition_list___16', 'cogn_word_cognition_list___17', 'cogn_word_cognition_list___18',
                'cogn_word_cognition_list___19', 'cogn_word_cognition_list___20', 'cogn_word_cognition_list____8',
                'cogn_word_cognition_list____999', 'subs_tobacco_use',  'subs_smoke_100',   'subs_smoke_now',
                'subs_smoke_last_hour', 'subs_smoke_per_day', 'subs_smokeless_tobacc_use',  'subs_snuff_use',
                'subs_tobacco_chew_use', 'subs_alcohol_consump', 'subs_alcohol_consume_now', 'subs_alcohol_cutdown',
                'subs_alcohol_criticize', 'subs_alcohol_guilty', 'subs_alcohol_hangover', 'subs_alcohol_con_past_yr',
                'subs_alcoholtype_consumed___1',  'subs_alcoholtype_consumed___2',  'subs_alcoholtype_consumed___3',
                'subs_alcoholtype_consumed___4', 'subs_alcoholtype_consumed___5',    'subs_alcoholtype_consumed____999',
                'subs_tobacco_use', 'subs_snuff_use', 'subs_drugs_use', 'subs_drug_use_other',
                'genh_breast_cancer', 'genh_breast_cancer_treat', 'genh_bre_cancer_treat_now', 'genh_cervical_cancer',
                'genh_bre_cancer_trad_med', 'genh_cer_cancer_treat', 'genh_cer_cancer_treat_now', 'genh_cer_cancer_trad_med',
                'genh_prostate_cancer', 'genh_pro_cancer_treat', 'genh_pro_cancer_treat_now', 'genh_pro_cancer_trad_med',
                'genh_oes_cancer_treat', 'genh_oes_cancer_treat_now', 'genh_oesophageal_trad_med', 'genh_other_cancers',
                'genh_other_cancer_treat', 'genh_oth_cancer_treat_now', 'genh_oth_cancer_trad_med', 'genh_oesophageal_cancer',
                'genh_obesity_mom', 'genh_h_blood_pressure_mom', 'genh_h_cholesterol_mom', 'genh_breast_cancer_mom',
                'genh_cervical_cancer_mom', 'genh_oes_cancer_mom', 'genh_cancer_other_mom', 'genh_asthma_mom',
                'genh_obesity_dad', 'genh_h_blood_pressure_dad', 'genh_h_cholesterol_dad', 'genh_prostate_cancer_dad',
                'genh_other_cancers_dad', 'genh_asthma_dad', 'genh_pesticide', 'genh_pesticide_region', 'genh_pesticide_type',
                'genh_cooking_done_inside', 'genh_insect_repellent_use','genh_smoker_in_your_house', 'infh_malaria', 'infh_malaria_month', 'infh_malaria_area',
                'infh_tb_12months', 'infh_tb', 'infh_tb_treatment', 'infh_tb_meds', 'infh_tb_counselling', 'infh_tb_traditional_med',
                'infh_hiv_que_answering', 'infh_hiv_tested', 'infh_hiv_medication','infh_hiv_positive',
                'infh_hiv_arv_meds_now', 'infh_hiv_arv_single_pill',
                'infh_hiv_traditional_meds', 'infh_painful_feet_hands', 'infh_hypersensitivity', 'infh_kidney_problems',
                'infh_hiv_status', 'infh_hiv_positive', 'infh_liver_problems', 'infh_change_in_body_shape', 'infh_mental_state_change',
                'infh_chol_levels_change', 'infh_hiv_test', 'infh_hiv_counselling',
                'carf_blood_sugar','carf_diabetes',
                'carf_diabetes_12months', 'carf_diabetes_treatment', 'carf_diabetes_treat_now', 'carf_diabetes_traditional',
                'carf_diabetes_history', 'carf_diabetes_mother', 'carf_diabetes_father', 'carf_diabetes_brother_1', 'carf_diabetes_brother_2',
                'carf_diabetes_brother_3', 'carf_diabetes_brother_4', 'carf_diabetes_sister_1', 'carf_diabetes_son_1',
                'carf_diabetes_son_2', 'carf_diabetes_son_3', 'carf_diabetes_son_4', 'carf_daughter_diabetes_1',
                'carf_diabetes_fam_other', 'carf_stroke', 'carf_tia', 'carf_weakness', 'carf_numbness', 'carf_blindness',
                'carf_half_vision_loss', 'carf_understanding_loss', 'carf_expression_loss', 'carf_angina', 'carf_angina_treatment',
                'carf_angina_treat_now', 'carf_angina_traditional', 'carf_pain', 'carf_pain2', 'carf_relief_standstill',
                'carf_heartattack', 'carf_heartattack_treat', 'carf_heartattack_trad', 'carf_chf_treatment', 'carf_chf_treatment_now',
                'carf_congestiv_heart_fail', 'carf_chf_traditional', 'carf_bp_measured', 'carf_hypertension',
                'carf_hypertension_12mnths', 'carf_hypertension_treat', 'carf_hypertension_meds', 'carf_hypertension_trad',
                'carf_cholesterol', 'carf_h_cholesterol', 'carf_chol_treatment', 'carf_chol_traditional', 'carf_thyroid', 'carf_kidney_disease',
                'carf_thyroid_type', 'carf_thyroid_treatment', 'carf_parents_thyroid', 'carf_kidney_disease_known',
                'carf_kidney_function_low', 'carf_kidney_family', 'carf_kidney_family_mother', 'carf_kidney_family_father',
                'carf_kidney_family_other', 'carf_kidney_family_type', 'carf_joints_swollen_pain', 'carf_joints_involved',
                'carf_arthritis_results',
                'carf_osteo_hip_replace', 'carf_osteo_knee_replace',  'carf_osteo_knee_repl_site',
                'gpaq_work_weekend', 'gpaq_work_sedentary', 'gpaq_work_vigorous', 'gpaq_work_moderate', 'gpaq_transport_phy',
                'gpaq_leisure_phy', 'gpaq_leisure_vigorous', 'gpaq_leisuremoderate', 'gpaq_sleep_room_livestock',
                'gpaq_mosquito_net_use', 'resp_breath_shortness', 'resp_breath_shortness_ever', 'resp_breath_shortness_ever',
                'resp_mucus', 'resp_breath_too_short', 'resp_cough', 'resp_wheezing_whistling', 'resp_asthma_diagnosed',
                'resp_asthma_treat', 'resp_asthma_treat_now', 'resp_copd_treat', 'resp_inhaled_medication', 'rspe_major_surgery',
                'rspe_chest_pain', 'rspe_coughing_blood', 'rspe_acute_retinal_detach', 'rspe_any_pain', 'rspe_diarrhea',
                'rspe_high_blood_pressure', 'rspe_tb_diagnosed', 'rspe_tb_treat_past4wks', 'rspe_participation',
                'rspe_wearing_tightclothes', 'rspe_wearing_dentures',
                'micr_worm_intestine_treat', 'micr_probiotics_taken', 'tram_injury_ill_assault',
                'tram_relative_ill_injured', 'tram_deceased', 'tram_family_friend_died', 'tram_marital_separation',
                'tram_broke_relationship', 'tram_problem_with_friend', 'tram_unemployed', 'tram_sacked_from_your_job',
                'tram_financial_crisis', 'tram_problems_with_police', 'tram_some_valued_lost',
                'carf_osteo',
                'bloc_urine_collected', 'genh_prostate_cancer', 'genh_other_cancers', 'resp_asthma_diagnosed',
                'subs_smokeless_tobacc_use', 'carf_diabetes_mother', 'carf_diabetes_father', 'genh_h_cholesterol_dad', 'bloc_fasting_confirmed']
        for col in col_category:
            data[col] = data[col].replace({0: 'No', 1: 'Yes', 2: 'Dont know', -8: 'Decline to answer', 9:'Dont know'})

        # maritus status encoding
        data.mari_marital_status = data.mari_marital_status.replace({1: 'Married', 2: 'Living together',
                                                                           3: 'Never married or co-habited',
                                                                           4: 'Divorced, and partner is alive',
                                                                           5: 'Separated, and partner is alive',
                                                                           6: 'Partner deceased'
                                                                           ,-8: 'Decline to answer'})

        # employment status encoding

        data.empl_status = data.empl_status.replace({ 1: 'Self-employed', 2: 'Formal full-time',
                                                            3: 'Formal part-time', 4: 'Informal',
                                                            5: 'Unemployed', -8: 'Decline to answer'})

       # education level encoding
        data.educ_highest_level = data.educ_highest_level.replace({1: 'No formal education', 2: 'Primary',
                                                                         3: 'Secondary', 4: 'Tertiary',
                                                                         -8: 'Decline to answer' })

        # cognition encoding
        data.cogn_read_sentence  = data.cogn_read_sentence.replace({1: 'Cannot read at all', 2: 'Able to read part of the sentence',
                                                                          3: 'Able to read whole sentence', 4: 'Blind or severely visually impaired'})


        data.cogn_memory = data.cogn_memory.replace({1: 'Excellent', 2: 'Very good',
                                                           3: 'Fair', 4: 'Poor',
                                                           -8: 'Decline to answer'})

        data.cogn_difficulty_remember = data.cogn_difficulty_remember.replace({1: 'No', 2: 'A little', 3: 'Some',
                                                                                     4: 'Quite a lot', 5: 'I couldnt remember anything',
                                                                                     -8: 'Decline to answer'})
        data.cogn_difficulty_concern = data.cogn_difficulty_concern.replace({1: 'No', 2: 'A little', 3: 'Some',
                                                                                   4: 'Quite a lot', 5: 'I couldnt concentrate',
                                                                                   -8: 'Decline to answer'})

        data.cogn_learning_new_task = data.cogn_learning_new_task.replace({1: 'None', 2: 'Mild', 3: 'Moderate',
                                                                                 4: 'I couldnt learn something new', 5: 'There was no opportunity to learn something new',
                                                                                 -8: 'Decline to answer'})
        data.frai_non_dominant_hand = data.frai_non_dominant_hand.replace({1: 'Left', 2: 'Right'})

        # cogn col
        cogn_cols = ['cogn_year', 'cogn_what_is_the_month', 'cogn_day_of_the_month', 'cogn_country_of_residence',
             'cogn_district_province', 'cogn_village_town_city', 'cogn_weekdays_forward', 'cogn_weekdays_backwards']

        for col in cogn_cols:
            data[col] = data[col].replace({0: 'Incorrect', 1: 'Correct', -8: 'Decline to answer'})

        # mvpa categories
        bins = [0, 150, 300]
        labels = [0, 1]
        data['gpaq_mvpa_categories'] = pd.cut(data['gpaq_mvpa'], bins=bins, labels=labels)

        #poc
        data.poc_hiv_test_result = data.poc_hiv_test_result.replace({0: 'Negative', 1: 'Positive',
                                                                           2: 'Inconclusive'})
        # complete, incomplete and unverified encoding
        complete_col = ['a_respiratory_health_complete', 'c_spirometry_test_complete', 'd_reversibility_test_complete',
                'b_blood_collection_complete']

        for col in complete_col:
            data[col] = data[col].replace({0: 'Incomplete', 1: 'Unverified', 2: 'Complete'})

        output_df = data
        return output_df