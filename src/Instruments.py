import pandas as pd

# This class has functions which return a specific instrument
# An instrument contains variables belonging to the same group
# All instruments were assigned study_id variable for merging purposes


class Instruments:

    # df is data_frame in csv format
    def __init__(self, dataSet=None, csv_link=None):
        # Read the csv file if no data set has been passed in
        if dataSet is None:
            # A list of missing value types
            missing_values = ["n/a", "na", "--"]
            self.data = pd.read_csv(csv_link, na_values=missing_values, index_col=None)
        else:
            self.data = dataSet

    def get_anthropometric_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('anth')]
        colNames = colNames.insert(0,'study_id')
        colNames = colNames.insert(1,'gene_uni_site_id_correct')

        anthropometry = self.data[colNames]
        return anthropometry

    def get_a_phase_1_data(self):
        a_phase_1_data = self.data[['study_id',
                                    'phase_1_site_id_1',
                                    'phase_1_enrolment_date',
                                    'phase_1_gender',
                                    'phase_1_dob_known',
                                    'phase_1_dob',
                                    'phase_1_yob',
                                    'phase_1_age',
                                    'phase_1_unique_site_id',
                                    'phase_1_home_language',
                                    'phase_1_ethnicity',
                                    'ethnolinguistc_available'
                                    ]]
        return a_phase_1_data

    def get_a_cardiometabolic_risk_factors_diabetes(self):
        colNames = self.data.columns[self.data.columns.str.contains('carf_diabetes')]
        colNames = colNames.insert(0,'study_id')
        colNames = colNames.insert(1,'carf_blood_sugar')

        a_cardiometabolic_risk_factors_diabetes = self.data[colNames]
        return a_cardiometabolic_risk_factors_diabetes

    def get_a_cognition_one(self):
        colNames = self.data.columns[self.data.columns.str.contains('cogn_')]
        colNames = colNames.insert(0,'study_id')

        a_cognition_one = self.data[colNames]
        return a_cognition_one

    def get_a_general_health_cancer(self):
        colNames = self.data.columns[(self.data.columns.str.contains('genh')) &
                                     (self.data.columns.str.contains('cancer')) &
                                     ~(self.data.columns.str.contains('mom')) & 
                                     ~(self.data.columns.str.contains('dad')) ]
        colNames = colNames.insert(0,'study_id')

        a_general_health_cancer = self.data[colNames]
        return a_general_health_cancer

    def get_a_microbiome(self):
        colNames = self.data.columns[self.data.columns.str.contains('micr_')]
        colNames = colNames.insert(0,'study_id')

        a_microbiome = self.data[colNames]
        return a_microbiome

    def get_a_respiratory_health(self):
        colNames = self.data.columns[self.data.columns.str.contains('resp_')]
        colNames = colNames.insert(0,'study_id')

        a_respiratory_health = self.data[colNames]
        return a_respiratory_health

    def get_b_blood_collection(self):
        b_blood_collection = self.data[['study_id',
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
                                        'bloc_bloodcollection_time']]
        return b_blood_collection

    def get_b_cardiometabolic_risk_factors_heart_conditions(self):
        b_cardiometabolic_risk_factors_heart_conditions = self.data[['study_id',
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
                                                                     # 'carf_pain_location',
                                                                     'carf_heartattack',
                                                                     'carf_heartattack_treat',
                                                                     'carf_heartattack_meds',
                                                                     'carf_heartattack_trad',
                                                                     'carf_congestiv_heart_fail',
                                                                     'carf_chf_treatment',
                                                                     'carf_chf_treatment_now',
                                                                     'carf_chf_meds',
                                                                     'carf_chf_traditional']]
        return b_cardiometabolic_risk_factors_heart_conditions

    def get_b_frailty_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('frai_')]
        colNames = colNames.insert(0,'study_id')

        b_frailty_measurements = self.data[colNames]
        return b_frailty_measurements

    def get_b_general_health_family_history(self):
        colNames = self.data.columns[(self.data.columns.str.contains('genh')) &
                                     (self.data.columns.str.contains('mom') |
                                     self.data.columns.str.contains('dad'))]
        colNames = colNames.insert(0,'study_id')

        b_general_health_family_history = self.data[colNames]
        return b_general_health_family_history

    def get_b_spirometry_eligibility(self):
        colNames = self.data.columns[self.data.columns.str.contains('rspe_')]
        colNames = colNames.insert(0,'study_id')

        b_spirometry_eligibility = self.data[colNames]
        return b_spirometry_eligibility

    def get_blood_pressure_and_pulse_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('bppm_')]
        colNames = colNames.insert(0,'study_id')

        blood_pressure_and_pulse_measurements = self.data[colNames]
        return blood_pressure_and_pulse_measurements

    def get_c_cardiometabolic_risk_factors_hypertension_choles(self):
        c_cardiometabolic_risk_factors_hypertension_choles = self.data[['study_id',
                                                                        'carf_bp_measured',
                                                                        'carf_hypertension',
                                                                        'carf_hypertension_12mnths',
                                                                        'carf_hypertension_treat',
                                                                        'carf_hypertension_meds',
                                                                        'carf_hypertension_medlist',
                                                                        'carf_hypertension_trad',
                                                                        'carf_cholesterol',
                                                                        'carf_h_cholesterol',
                                                                        'carf_chol_treatment',
                                                                        # 'carf_chol_treatment_now',
                                                                        'carf_chol_treat_specify',
                                                                        'carf_chol_medicine',
                                                                        'carf_chol_traditional']]
        return c_cardiometabolic_risk_factors_hypertension_choles

    def get_c_cognition_two(self):
        c_cognition_two = self.data[['study_id',
                                     # 'cogn_delayed_recall_note',
                                     # 'cogn_delayed_recall',
                                     'cogn_delayed_recall_score',
                                     # 'cogn_word_cognition_note',
                                     # 'cogn_word_cognition_list',
                                     'cogn_recognition_score',
                                     'cogn_different_animals',
                                     'cogn_comments']]
        return c_cognition_two

    def get_c_general_health_diet(self):
        c_general_health_diet = self.data[['study_id',
                                           'genh_days_fruit',
                                           'genh_fruit_servings',
                                           'genh_days_veg',
                                           'genh_veg_servings',
                                           # 'genh_starchy_staple_food',
                                           'genh_starchy_staple_freq',
                                           'genh_staple_servings',
                                           'genh_vendor_meals',
                                           'genh_sugar_drinks',
                                           'genh_juice',
                                           'genh_change_diet',
                                           'genh_lose_weight']]
        return c_general_health_diet

    def get_c_spirometry_test(self):
        colNames = self.data.columns[self.data.columns.str.contains('spiro_')]
        colNames = colNames.insert(0,'study_id')

        c_spirometry_test = self.data[colNames]
        return c_spirometry_test

    def get_c_urine_collection(self):
        c_urine_collection = self.data[['study_id',
                                        'bloc_urine_collected',
                                        'bloc_specify_reason',
                                        'bloc_urcontainer_batchnum',
                                        'bloc_urine_tube_expiry',
                                        'bloc_urine_collector',
                                        'bloc_urine_taken_date',
                                        'bloc_urinecollection_time']]
        return c_urine_collection

    def get_civil_status_marital_status_education_employment(self):
        colNames = self.data.columns[(self.data.columns.str.contains('mari_')) |
                                     (self.data.columns.str.contains('educ_')) |
                                     (self.data.columns.str.contains('empl_')) |
                                     (self.data.columns.str.contains('civil_')) ]
        colNames = colNames.insert(0,'study_id')

        civil_status_marital_status_education_employment = self.data[colNames]
        return civil_status_marital_status_education_employment

    def get_completion_of_questionnaire(self):
        colNames = self.data.columns[self.data.columns.str.contains('comp_')]
        colNames = colNames.insert(0,'study_id')

        completion_of_questionnaire = self.data[colNames]
        return completion_of_questionnaire

    def get_d_cardiometabolic_risk_factors_kidney_thyroid_ra(self):
        d_cardiometabolic_risk_factors_kidney_thyroid_ra = self.data[['study_id',
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
                                                                      'carf_esr_crp']]
        return d_cardiometabolic_risk_factors_kidney_thyroid_ra

    def get_d_general_health_exposure_to_pesticides_pollutants(self):
        d_general_health_exposure_to_pesticides_pollutants = self.data[['study_id',
                                                                        'genh_pesticide',
                                                                        'genh_pesticide_years',
                                                                        'genh_pesticide_region',
                                                                        'genh_pesticide_type',
                                                                        'genh_pesticide_list',
                                                                        'genh_cooking_place',
                                                                        'genh_cookingplace_specify',
                                                                        'genh_cooking_done_inside',
                                                                        # 'genh_energy_source_type',
                                                                        'genh_energy_specify',
                                                                        'genh_smoker_in_your_house',
                                                                        'genh_smoke_freq_someone',
                                                                        'genh_insect_repellent_use']]
        return d_general_health_exposure_to_pesticides_pollutants

    def get_d_reversibility_test(self):
        colNames = self.data.columns[self.data.columns.str.contains('rspir_')]
        colNames = colNames.insert(0,'study_id')

        d_reversibility_test = self.data[colNames]
        return d_reversibility_test

    def get_ethnolinguistic_information(self):
        colNames = self.data.columns[self.data.columns.str.contains('ethn_')]
        colNames = colNames.insert(0,'study_id')

        ethnolinguistic_information = self.data[colNames]
        return ethnolinguistic_information

    def get_family_composition(self):
        colNames = self.data.columns[self.data.columns.str.contains('famc_')]
        colNames = colNames.insert(0,'study_id')

        family_composition = self.data[colNames]
        return family_composition

    def get_household_attributes(self):
        colNames = self.data.columns[self.data.columns.str.contains('hous_')]
        colNames = colNames.insert(0,'study_id')

        household_attributes = self.data[colNames]
        return household_attributes

    def get_infection_history(self):
        colNames = self.data.columns[self.data.columns.str.contains('infh_')]
        colNames = colNames.insert(0,'study_id')

        infection_history = self.data[colNames]
        return infection_history

    def get_participant_identification(self):
        colNames = self.data.columns[(self.data.columns.str.contains('gene_')) |
                                     (self.data.columns.str.contains('demo_')) |
                                     (self.data.columns.str.contains('language')) | 
                                     (self.data.columns.str.contains('ethnicity')) ]
        colNames = colNames.insert(0,'study_id')

        participant_identification = self.data[colNames]
        return participant_identification

    def get_physical_activity_and_sleep(self):
        colNames = self.data.columns[self.data.columns.str.contains('gpaq_')]
        colNames = colNames.insert(0,'study_id')

        physical_activity_and_sleep = self.data[colNames]
        return physical_activity_and_sleep

    def get_point_of_care_testing(self):
        colNames = self.data.columns[self.data.columns.str.contains('poc_')]
        colNames = colNames.insert(0,'study_id')

        point_of_care_testing = self.data[colNames]
        return point_of_care_testing

    def get_pregnancy_and_menopause(self):
        colNames = self.data.columns[self.data.columns.str.contains('preg_')]
        colNames = colNames.insert(0,'study_id')

        pregnancy_and_menopause = self.data[colNames]
        return pregnancy_and_menopause

    def get_substance_use(self):
        colNames = self.data.columns[self.data.columns.str.contains('subs_')]
        colNames = colNames.insert(0,'study_id')

        substance_use = self.data[colNames]
        return substance_use

    def get_trauma(self):
        colNames = self.data.columns[self.data.columns.str.contains('tram_')]
        colNames = colNames.insert(0,'study_id')

        trauma = self.data[colNames]
        return trauma

    def get_ultrasound_and_dxa_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('ultr_')]
        colNames = colNames.insert(0,'study_id')

        ultrasound_and_dxa_measurements = self.data[colNames]
        return ultrasound_and_dxa_measurements

    instrument_getters = {
        'anth' : get_anthropometric_measurements,
        'ultr' : get_ultrasound_and_dxa_measurements,
        'gpaq' : get_physical_activity_and_sleep,
        'bppm' : get_blood_pressure_and_pulse_measurements,
        'cogn' : get_a_cognition_one,
        'bloc_u' : get_c_urine_collection,
        'bloc_b' :get_b_blood_collection,
        'micr' : get_a_microbiome,
        'resp' : get_a_respiratory_health,
        'carf_hyp_chol' : get_c_cardiometabolic_risk_factors_hypertension_choles,
        'carf_kid_thy' : get_d_cardiometabolic_risk_factors_kidney_thyroid_ra,
        'carf_diab' : get_a_cardiometabolic_risk_factors_diabetes,
        'cerf_heart' : get_b_cardiometabolic_risk_factors_heart_conditions,
        'hous' : get_household_attributes,
        'spiro' : get_c_spirometry_test,
        'famc' : get_family_composition,
        'diet' : get_c_general_health_diet,
        'exposure' : get_d_general_health_exposure_to_pesticides_pollutants,
        'famh' : get_b_general_health_family_history,
        'frai' : get_b_frailty_measurements,
        'cancer' : get_a_general_health_cancer,
        'rspe' : get_b_spirometry_eligibility,
        'civil' : get_civil_status_marital_status_education_employment,
        'rspir' : get_d_reversibility_test,
        'infh' : get_infection_history,
        'poc' : get_point_of_care_testing,
        'preg' : get_pregnancy_and_menopause,
        'subs' : get_substance_use,
        'tram' : get_trauma
    }