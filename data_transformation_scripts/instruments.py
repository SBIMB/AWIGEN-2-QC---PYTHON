import pandas as pd

class Instruments:

    def __init__(self, data):
        self.data = data

    def get_a_phase_1_data(self):
        colNames = self.data.columns[(self.data.columns.str.contains('phase_1'))]
        colNames = colNames.insert(0,'study_id')

        a_phase_1_data = self.data[colNames]
        return a_phase_1_data

    def get_participant_identification(self):
        colNames = self.data.columns[(self.data.columns.str.contains('gene_')) |
                                     (self.data.columns.str.contains('demo_')) |
                                     (self.data.columns.str.contains('language')) |
                                     (self.data.columns.str.contains('ethnicity')) |
                                     (self.data.columns.str.contains('participant')) ]
        return colNames

    def get_ethnolinguistic_information(self):
        colNames = self.data.columns[self.data.columns.str.contains('ethn_')]
        colNames = colNames.insert(0,'study_id')

        ethnolinguistic_information = self.data[colNames]
        return ethnolinguistic_information

    def get_family_composition(self):
        colNames = self.data.columns[self.data.columns.str.contains('famc_')]
        family_composition = self.data[colNames]
        return family_composition

    def get_pregnancy_and_menopause(self):
        colNames = self.data.columns[self.data.columns.str.contains('preg_')]
        pregnancy_and_menopause = self.data[colNames]
        return pregnancy_and_menopause

    def get_civil_status_marital_status_education_employment(self):
        colNames = self.data.columns[(self.data.columns.str.contains('mari_')) |
                                     (self.data.columns.str.contains('educ_')) |
                                     (self.data.columns.str.contains('empl_')) |
                                     (self.data.columns.str.contains('civil_')) ]
        civil_status_marital_status_education_employment = self.data[colNames]
        return civil_status_marital_status_education_employment

    def get_a_cognition_one(self):
        colNames = self.data.columns[self.data.columns.str.contains('cogn_') &
                                    ~(self.data.columns.str.contains('delayed')) &
                                    ~(self.data.columns.str.contains('word_cog')) &
                                    ~(self.data.columns.str.contains('recognition_score')) &
                                    ~(self.data.columns.str.contains('animals'))]

        a_cognition_one = self.data[colNames]
        return a_cognition_one

    def get_b_frailty_measurements(self):
        colNames = self.data.columns[(self.data.columns.str.contains('frai_'))]

        b_frailty_measurements = self.data[colNames]
        return b_frailty_measurements

    def get_c_cognition_two(self):
        colNames = self.data.columns[( self.data.columns.str.contains('cogn_') &
                                     ( self.data.columns.str.contains('delayed') |
                                       self.data.columns.str.contains('word_cog') |
                                       self.data.columns.str.contains('recognition_score') |
                                       self.data.columns.str.contains('animals') |
                                       self.data.columns.str.contains('comments') ) )]
        c_cognition_two = self.data[colNames]
        return c_cognition_two

    def get_household_attributes(self):
        colNames = self.data.columns[self.data.columns.str.contains('hous_')]
        #colNames = colNames.insert(0,'study_id')

        household_attributes = self.data[colNames]
        return household_attributes

    def get_substance_use(self):
        colNames = self.data.columns[self.data.columns.str.contains('subs_')]

        substance_use = self.data[colNames]
        return substance_use

    def get_a_general_health_cancer(self):
        colNames = self.data.columns[self.data.columns.str.contains('genh_bre') |
                                     self.data.columns.str.contains('genh_cer') |
                                     self.data.columns.str.contains('genh_pro') |
                                     self.data.columns.str.contains('genh_oes') |
                                     self.data.columns.str.contains('genh_oth') ]

        a_general_health_cancer = self.data[colNames]
        return a_general_health_cancer

    def get_b_general_health_family_history(self):
        colNames = self.data.columns[ (self.data.columns.str.contains('genh') & self.data.columns.str.contains('mom') ) |
                                      (self.data.columns.str.contains('genh') & self.data.columns.str.contains('dad') ) ]

        b_general_health_family_history = self.data[colNames]
        return b_general_health_family_history

    def get_c_general_health_diet(self):
        colNames = self.data.columns[ (self.data.columns.str.contains('genh') & self.data.columns.str.contains('veg') ) |
                                      (self.data.columns.str.contains('genh') & self.data.columns.str.contains('fruit') ) |
                                       self.data.columns.str.contains('genh_staple') |
                                       self.data.columns.str.contains('genh_vendor_meals') |
                                       self.data.columns.str.contains('genh_starchy') |
                                       self.data.columns.str.contains('genh_sugar_drinks') |
                                       self.data.columns.str.contains('genh_juice') |
                                       self.data.columns.str.contains('genh_change_diet') |
                                       self.data.columns.str.contains('genh_lose_weight')]
        #colNames = colNames.insert(0,'study_id')

        c_general_health_diet = self.data[colNames]
        return c_general_health_diet

    def get_d_general_health_exposure_to_pesticides_pollutants(self):
        colNames = self.data.columns[(self.data.columns.str.contains('genh_pesticide')) |
                                     (self.data.columns.str.contains('genh_cooking')) |
                                     (self.data.columns.str.contains('genh_energy')) |
                                     (self.data.columns.str.contains('genh_smoke')) |
                                     (self.data.columns.str.contains('genh_insect_repellent_use'))]

        d_general_health_exposure_to_pesticides_pollutants = self.data[colNames]
        return d_general_health_exposure_to_pesticides_pollutants

    def get_infection_history(self):
        colNames = self.data.columns[self.data.columns.str.contains('infh_')]

        infection_history = self.data[colNames]
        return infection_history

    def get_a_cardiometabolic_risk_factors_diabetes(self):
        colNames = self.data.columns[self.data.columns.str.contains('carf_diab') |
                                     self.data.columns.str.contains('carf_blood_sugar')]

        a_cardiometabolic_risk_factors_diabetes = self.data[colNames]
        return a_cardiometabolic_risk_factors_diabetes

    def get_b_cardiometabolic_risk_factors_heart_conditions(self):
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
                                     self.data.columns.str.contains('carf_chf_')]

        b_cardiometabolic_risk_factors_heart_conditions = self.data[colNames]
        return b_cardiometabolic_risk_factors_heart_conditions

    def get_c_cardiometabolic_risk_factors_hypertension_choles(self):
        colNames = self.data.columns[self.data.columns.str.contains('carf_hypertension') |
                                     ( self.data.columns.str.contains('carf_') & self.data.columns.str.contains('chol') ) |
                                     self.data.columns.str.contains('carf_bp_measured')]

        c_cardiometabolic_risk_factors_hypertension_choles = self.data[colNames]
        return c_cardiometabolic_risk_factors_hypertension_choles

    def get_d_cardiometabolic_risk_factors_kidney_thyroid_ra(self):
        colNames = self.data.columns[self.data.columns.str.contains('carf_kidney') |
                                     ( self.data.columns.str.contains('carf_') & self.data.columns.str.contains('thyroid') ) |
                                     self.data.columns.str.contains('carf_joints') |
                                     self.data.columns.str.contains('carf_when_they_hurt') |
                                     self.data.columns.str.contains('carf_symptoms_how_long') |
                                     self.data.columns.str.contains('carf_arthritis_results') |
                                     self.data.columns.str.contains('carf_rheumatoid_factor') |
                                     self.data.columns.str.contains('carf_acpa') |
                                     self.data.columns.str.contains('carf_esr_crp') |
                                     self.data.columns.str.contains('carf_osteo')]


        d_cardiometabolic_risk_factors_kidney_thyroid_ra = self.data[colNames]
        return d_cardiometabolic_risk_factors_kidney_thyroid_ra

    def get_physical_activity_and_sleep(self):
        colNames = self.data.columns[self.data.columns.str.contains('gpaq_')]
        #colNames = colNames.insert(0,'study_id')

        physical_activity_and_sleep = self.data[colNames]
        return physical_activity_and_sleep

    def get_anthropometric_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('anth')]
        colNames = colNames.insert(0,'study_id')
        colNames = colNames.insert(1,'gene_uni_site_id_correct')

        anthropometry = self.data[colNames]
        return anthropometry

    def get_blood_pressure_and_pulse_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('bppm_')]
        colNames = colNames.insert(0,'study_id')

        blood_pressure_and_pulse_measurements = self.data[colNames]
        return blood_pressure_and_pulse_measurements

    def get_ultrasound_and_dxa_measurements(self):
        colNames = self.data.columns[self.data.columns.str.contains('ultr_')]
        #colNames = colNames.insert(0,'study_id')

        ultrasound_and_dxa_measurements = self.data[colNames]
        return ultrasound_and_dxa_measurements

    def get_a_respiratory_health(self):
        colNames = self.data.columns[self.data.columns.str.contains('resp_')]

        a_respiratory_health = self.data[colNames]
        return a_respiratory_health

    def get_b_spirometry_eligibility(self):
        colNames = self.data.columns[self.data.columns.str.contains('rspe_')]

        b_spirometry_eligibility = self.data[colNames]
        return b_spirometry_eligibility

    def get_c_spirometry_test(self):
        colNames = self.data.columns[self.data.columns.str.contains('spiro_')]
        #colNames = colNames.insert(0,'study_id')

        c_spirometry_test = self.data[colNames]
        return c_spirometry_test

    def get_d_reversibility_test(self):
        colNames = self.data.columns[self.data.columns.str.contains('rspir_')]
        #colNames = colNames.insert(0,'study_id')

        d_reversibility_test = self.data[colNames]
        return d_reversibility_test

    def get_a_microbiome(self):
        colNames = self.data.columns[self.data.columns.str.contains('micr_')]

        a_microbiome = self.data[colNames]
        return a_microbiome

    def get_b_blood_collection(self):
        colNames = self.data.columns[self.data.columns.str.contains('bloc_last') |
                                     self.data.columns.str.contains('bloc_hours_last_drink') |
                                     self.data.columns.str.contains('bloc_fasting_confirmed') |
                                     ( self.data.columns.str.contains('bloc_') & self.data.columns.str.contains('tube') ) |
                                     self.data.columns.str.contains('bloc_phlebotomist_name') |
                                     self.data.columns.str.contains('bloc_blood') |
                                     self.data.columns.str.contains('b_blood_collection_complete')]
        colNames = colNames.insert(0,'study_id')

        b_blood_collection = self.data[colNames]
        return b_blood_collection

    def get_c_urine_collection(self):
        colNames = self.data.columns[self.data.columns.str.contains('bloc_ur') |
                                     self.data.columns.str.contains('bloc_specify_reason')]
        colNames = colNames.insert(0,'study_id')

        c_urine_collection = self.data[colNames]
        return c_urine_collection

    def get_point_of_care_testing(self):
        colNames = self.data.columns[self.data.columns.str.contains('poc_')]
        colNames = colNames.insert(0,'study_id')

        point_of_care_testing = self.data[colNames]
        return point_of_care_testing

    def get_trauma(self):
        colNames = self.data.columns[self.data.columns.str.contains('tram_')]

        trauma = self.data[colNames]
        return trauma

    def get_completion_of_questionnaire(self):
        colNames = self.data.columns[self.data.columns.str.contains('comp_')]
        colNames = colNames.insert(0,'study_id')

        completion_of_questionnaire = self.data[colNames]
        return completion_of_questionnaire

    # def get_c_cognition_two(self):
    #     c_cognition_two = self.data[['study_id',
    #                                  # 'cogn_delayed_recall_note',
    #                                  # 'cogn_delayed_recall',
    #                                  'cogn_delayed_recall_score',
    #                                  # 'cogn_word_cognition_note',
    #                                  # 'cogn_word_cognition_list',
    #                                  'cogn_recognition_score',
    #                                  'cogn_different_animals',
    #                                  'cogn_comments']]
    #     return c_cognition_two

    instrument_dict = {
        'a_phase_1_data'                    : get_a_phase_1_data,
        'participant_identification'        : get_participant_identification,
        'ethnolinguistic_information'       : get_ethnolinguistic_information,
        'family_composition'                : get_family_composition,
        'pregnancy_and_menopause'           : get_pregnancy_and_menopause,
        'civil_status_marital_status_education_employment' : get_civil_status_marital_status_education_employment,
        'a_cognition_one'                   : get_a_cognition_one,
        'b_frailty_measurements'            : get_b_frailty_measurements,
        'c_cognition_two'                   : get_c_cognition_two,
        'household_attributes'              : get_household_attributes,
        'substance_use'                     : get_substance_use,
        'a_general_health_cancer'           : get_a_general_health_cancer,
        'b_general_health_family_history'   : get_b_general_health_family_history,
        'c_general_health_diet'             : get_c_general_health_diet,
        'd_general_health_exposure_to_pesticides_pollutants' : get_d_general_health_exposure_to_pesticides_pollutants,
        'infection_history'                 : get_infection_history,
        'a_cardiometabolic_risk_factors_diabetes'    : get_a_cardiometabolic_risk_factors_diabetes,
        'b_cardiometabolic_risk_factors_heart_conditions'    : get_b_cardiometabolic_risk_factors_heart_conditions,
        'c_cardiometabolic_risk_factors_hypertension_choles'    : get_c_cardiometabolic_risk_factors_hypertension_choles,
        'd_cardiometabolic_risk_factors_kidney_thyroid_ra'    : get_d_cardiometabolic_risk_factors_kidney_thyroid_ra,
        'physical_activity_and_sleep'       : get_physical_activity_and_sleep,
        'anthropometric_measurements'       : get_anthropometric_measurements,
        'blood_pressure_and_pulse_measurements' : get_blood_pressure_and_pulse_measurements,
        'ultrasound_and_dxa_measurements'   : get_ultrasound_and_dxa_measurements,
        'a_respiratory_health'              : get_a_respiratory_health,
        'b_spirometry_eligibility'          : get_b_spirometry_eligibility,
        'c_spirometry_test'                 : get_c_spirometry_test,
        'd_reversibility_test'              : get_d_reversibility_test,
        'a_microbiome'                      : get_a_microbiome,
        'b_blood_collection'                : get_b_blood_collection,
        'c_urine_collection'                : get_c_urine_collection,
        'point_of_care_testing'             : get_point_of_care_testing,
        'trauma'                            : get_trauma,
        'completion_of_questionnaire'       : get_completion_of_questionnaire
        }
