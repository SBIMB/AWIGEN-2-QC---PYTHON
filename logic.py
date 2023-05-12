from instruments import Instruments
import pandas as pd
import numpy as np

class BranchingLogic:

    def __init__(self, data):
        self.data = data

    #branching logic
    def family_composition_logic(self):
        famc_cols = Instruments(self.data).get_family_composition()
        for col in famc_cols:
            if col == 'famc_number_of_brothers' or col == 'famc_number_of_sisters':
                mask = (self.data[col].isna() & ((self.data['famc_siblings']==0) | (self.data['famc_siblings']==2) | (self.data['famc_siblings']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'famc_living_brothers':
                mask = (self.data[col].isna() & ((self.data['famc_number_of_brothers']==0) | (self.data['famc_number_of_brothers']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'famc_living_sisters':
                mask = (self.data[col].isna() & ((self.data['famc_number_of_sisters']== 0) | (self.data['famc_number_of_sisters']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'famc_bio_sons' or col == 'famc_bio_daughters':
                mask = (self.data[col].isna() & ((self.data['famc_children']==0) | (self.data['famc_children']==2) | (self.data['famc_children']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'famc_living_bio_sons':
                mask = (self.data[col].isna() & ((self.data['famc_bio_sons']== 0) | (self.data['famc_bio_sons']== -555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'famc_living_bio_daughters':
                mask = (self.data[col].isna() & ((self.data['famc_bio_daughters']==0) | (self.data['famc_bio_daughters']== -555)))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def pregnancy_and_menopause_logic(self):
        preg_cols = Instruments(self.data).get_pregnancy_and_menopause()
        for col in preg_cols:
            if col in['preg_pregnant', 'preg_num_of_pregnancies', 'preg_birth_control', 'preg_hysterectomy', 'preg_regular_periods', 'preg_last_period_remember']:
                mask = (self.data[col].isna() & self.data['demo_gender']==1)
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'preg_num_of_live_births':
                mask = (self.data[col].isna() & ((self.data['preg_num_of_pregnancies']==0) | (self.data['preg_pregnant'] == -555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['preg_last_period_mon', 'preg_last_period_mon_2', 'preg_last_period']:
                mask = ((self.data[col].isna()) & ((self.data['preg_pregnant'] ==-555) | (self.data['preg_last_period_remember'] == 0) | (self.data['preg_last_period_remember'] == 2) |\
                                                   (self.data['preg_last_period_remember'] == -8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'preg_period_more_than_yr':
                mask = ((self.data[col].isna()) & (self.data['preg_pregnant'] ==-555) & ((self.data['preg_last_period_remember'] == 1) | (self.data['preg_last_period_remember'] == -8)))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def civil_status_marital_status_education_employment_logic(self):
        civil_cols = Instruments(self.data).get_civil_status_marital_status_education_employment()
        for col in civil_cols:
            if col == 'educ_highest_years':
                mask = ((self.data[col].isna()) & ((self.data['educ_highest_level']== 1) | (self.data['educ_highest_level']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'educ_formal_years':
                #becuase of the equation there was an auto completion to 0(zero)
                mask = (((self.data[col].isna()) | (self.data[col] == 0)) & ((self.data['educ_highest_level']== 1) | (self.data['educ_highest_level']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'empl_days_work':
                mask = (self.data[col].isna() & ((self.data['empl_status'] ==5) | (self.data['empl_status'] ==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def frailty_measurements_logic(self):
        frai_cols = Instruments(self.data).get_b_frailty_measurements(self)
        for col in frai_cols:
            if col == 'frai_comment':
                mask = (self.data[col].isna() & ((self.data['frai_sit_stands_completed]'] == 1)| (self.data['frai_sit_stands_completed]']==2) | \
                                                 (self.data['frai_sit_stands_completed]']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'frai_comment_why':
                mask = (self.data[col].isna() & ((self.data['rai_complete_procedure]'] == 1)| (self.data['rai_complete_procedure]']==2) | \
                                                 (self.data['rai_complete_procedure]']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'frai_please_comment_why':
                mask = (self.data[col].isna() & ((self.data['frai_procedure_walk_comp]'] == 1)| (self.data['frai_procedure_walk_comp]']==2) | \
                                                 (self.data['frai_procedure_walk_comp]']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def household_attributes_logic(self):
        hous_col = Instruments(self.data).get_household_attributes()
        for col in hous_col:
            if col in ['hous_power_generator', 'hous_telephone', 'hous_internet_by_computer', 'hous_toilet_facilities', 'hous_tractor', 'hous_plough']:
                mask = (self.data[col].isna() & (self.data['gene_site']==3))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['hous_washing_machine', 'hous_microwave', 'hous_computer_or_laptop', 'hous_internet_by_m_phone']:
                mask = (self.data[col].isna() & (self.data['gene_site'] == 4))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['hous_electric_iron']:
                mask = (self.data[col].isna() & ((self.data['gene_site'] == 1) | (self.data['gene_site'] == 2) |
                            (self.data['gene_site'] == 4) | (self.data['gene_site'] == 6)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['hous_fan', 'hous_table', 'hous_sofa', 'hous_bed', 'hous_mattress', 'hous_blankets']:
                mask = (self.data[col].isna() & ((self.data['gene_site'] == 1) | (self.data['gene_site'] == 2)|
                        (self.data['gene_site'] == 6)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['hous_kerosene_stove', 'hous_electric_plate', 'hous_torch', 'hous_gas_lamp', 'hous_kerosene_lamp', 'hous_wall_clock']:
                mask = (self.data[col].isna() & ((self.data['gene_site'] == 1) | (self.data['gene_site'] == 2) |
                        (self.data['gene_site'] == 5) | (self.data['gene_site'] == 6)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['hous_plate_gas', 'hous_grinding_mill']:
                mask = (self.data[col].isna() & ((self.data['gene_site'] == 1) | (self.data['gene_site'] == 2) |
                        (self.data['gene_site'] == 3) | (self.data['gene_site'] == 6)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['hous_portable_water']:
                mask = (self.data[col].isna() & ((self.data['gene_site'] == 1) | (self.data['gene_site'] == 2) |
                        (self.data['gene_site'] == 3) | (self.data['gene_site'] == 4) | (self.data['gene_site'] == 6)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['hous_cattle', 'hous_other_livestock', 'hous_poultry', 'hous_tractor', 'hous_plough']:
                mask = (self.data[col].isna() & (self.data['gene_site'] == 6))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def substance_use_logic(self):
        subuse_col = Instruments(self.data).get_substance_use()
        for col in subuse_col:
            if col in ['subs_smoke_100', 'subs_smoke_now']:
                mask = (self.data[col].isna() & ((self.data['subs_tobacco_use']==0) | (self.data['subs_tobacco_use']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['subs_smoke_last_hour', 'subs_smoking_frequency', 'subs_smoking_start_age']:
                mask = (self.data[col].isna() & ((self.data['subs_smoke_now'] == 0)|(self.data['subs_smoke_now'] == 2)|
                                                 (self.data['subs_smoke_now'] == -8)| (self.data['subs_smoke_now']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['subs_smoke_cigarettes___1', 'subs_smoke_cigarettes___2', 'subs_smoke_cigarettes___3', 'subs_smoke_cigarettes___4',
                         'subs_smoke_cigarettes___5', 'subs_smoke_cigarettes____8', 'subs_smoke_cigarettes____999']:
                mask = ((self.data[col]==0) & ((self.data['subs_smoke_now'] == 0)|(self.data['subs_smoke_now'] == 2)|
                                                 (self.data['subs_smoke_now'] == -8)| (self.data['subs_smoke_now']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'subs_smoke_specify':
                mask = (self.data[col].isna() & ((self.data['subs_smoke_cigarettes___5'] == 1) | (self.data['subs_smoke_cigarettes___5'] == -555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'subs_smoke_per_day':
                mask = (self.data[col].isna() & ((self.data['subs_smoking_frequency']==-8) | (self.data['subs_smoking_frequency'] == -555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'subs_tobacco_chew_freq':
                mask = (self.data[col].isna() & ((self.data['subs_tobacco_chew_use']==0)|(self.data['subs_tobacco_chew_use']==-8) | (self.data['subs_tobacco_chew_use']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'subs_smoking_stop_year':
                mask = (self.data[col].isna() & ((self.data['subs_tobacco_use'] == 0) | (self.data['subs_tobacco_use'] == 0))\
                        & ((self.data['subs_smoke_now'] == 1) | (self.data['subs_smoke_now'] == 2) |(self.data['subs_smoke_now'] == -8)| (self.data['subs_smoke_now'] == -555)))
            elif col in ['subs_snuff_use', 'subs_tobacco_chew_use']:
                mask = ((self.data[col].isna()) & ((self.data['subs_smokeless_tobacc_use'] == 0)| (self.data['subs_smokeless_tobacc_use'] == -8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['subs_snuff_method_use', 'subs_snuff_use_freq']:
                mask = (self.data[col].isna() &  (self.data['subs_snuff_use']==0)| (self.data['subs_snuff_use'] == -8) | (self.data['subs_snuff_use'] == -555))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'subs_freq_snuff_use':
                mask = (self.data[col].isna() & ((self.data['subs_snuff_use_freq']==-8) | (self.data['subs_snuff_use_freq']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'subs_tobacco_chew_freq':
                mask = (self.data[col].isna() & ((self.data['subs_tobacco_chew_use'] == 0)|(self.data['subs_tobacco_chew_use'] == -8) | (self.data['subs_tobacco_chew_use'] == -555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'subs_tobacco_chew_d_freq':
                mask = (self.data[col].isna() & ((self.data['subs_tobacco_chew_freq']==-8)|(self.data['subs_tobacco_chew_freq']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['subs_alcohol_consume_now', 'subs_alcohol_con_past_yr']:
                mask = (self.data[col].isna() & ((self.data['subs_alcohol_consump']==0)|(self.data['subs_alcohol_consump']== 2) |
                                                 (self.data['subs_alcohol_consump'] == -8) | (self.data['subs_alcohol_consump'] == -555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['subs_alcohol_consump_freq', 'subs_alcohol_criticize', 'subs_alcohol_guilty', 'subs_alcohol_hangover', 'subs_alcohol_cutdown']:
                mask = (self.data[col].isna() & ((self.data['subs_alcohol_consume_now']==0)|(self.data['subs_alcohol_consume_now'] == 2) |
                        (self.data['subs_alcohol_consume_now']==-8)|(self.data['subs_alcohol_consume_now']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['subs_alcoholtype_consumed___1', 'subs_alcoholtype_consumed___2', 'subs_alcoholtype_consumed___3',
                        'subs_alcoholtype_consumed___4', 'subs_alcoholtype_consumed___5', 'subs_alcoholtype_consumed____999']:
                #ist argument ==0 becoz of auto completion of the variables to 0
                mask = (self.data[col]==0 & ((self.data['subs_alcohol_consump']!=1) | (self.data['subs_alcohol_consume_now']!=1)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'subs_alcohol_consume_freq':
                mask = (self.data[col].isna() &  ((self.data['subs_alcohol_consump_freq']==-8)|(self.data['subs_alcohol_consump_freq']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            #elif col == 'subs_alcohol_specify':
            #    mask = (self.data[col].isna() &  (self.data['subs_alcoholtype_consumed___5'] == 1))

        return self.data

    def a_general_health_cancer_logic(self):
        a_general_col = Instruments(self.data).get_a_general_health_cancer()
        for col in a_general_col:
            if col in ['genh_breast_cancer_treat', 'genh_bre_cancer_trad_med']:
                mask = (self.data[col].isna() &((self.data['genh_breast_cancer'] == 0 )| ( self.data['genh_breast_cancer'] == 2 )|
                                                 ( self.data['genh_breast_cancer'] == -8 )))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_bre_cancer_treat_now':
                mask = (self.data[col].isna() & ((self.data['genh_breast_cancer_treat']==0) | (self.data['genh_breast_cancer_treat']==2)|
                                                  (self.data['genh_breast_cancer_treat']==-8)| (self.data['genh_breast_cancer_treat']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_breast_cancer_meds':
                mask = (self.data[col].isna() & ((self.data['genh_bre_cancer_treat_now']==0) | (self.data['genh_bre_cancer_treat_now']==2)|
                                                  (self.data['genh_bre_cancer_treat_now']==-8) | (self.data['genh_bre_cancer_treat_now']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_cervical_cancer':
                mask = ( self.data[col].isna() & (self.data['demo_gender']==1))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['genh_cer_cancer_treat', 'genh_cer_cancer_trad_med']:
                mask = ( self.data[col].isna()&((self.data['genh_cervical_cancer']==0)| ( self.data['genh_cervical_cancer']==2 ) |
                                                ( self.data['genh_cervical_cancer']==-8 ) | ( self.data['genh_cervical_cancer']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_cer_cancer_treat_now':
                mask = ( self.data[col].isna() &((self.data['genh_cer_cancer_treat']==0)| (self.data['genh_cer_cancer_treat']== 2)|
                                                 (self.data['genh_cer_cancer_treat']==-8) | (self.data['genh_cer_cancer_treat']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_cervical_cancer_meds':
                mask = ( self.data[col].isna() & (( self.data['genh_cer_cancer_treat_now']== 0 ) | (self.data['genh_cer_cancer_treat_now']==2)|
                                                  (self.data['genh_cer_cancer_treat_now']==-8)|(self.data['genh_cer_cancer_treat_now']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_prostate_cancer':
                mask = ( self.data[col].isna() & ( self.data['demo_gender']==0))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['genh_pro_cancer_treat', 'genh_pro_cancer_trad_med']:
                mask = (self.data[col].isna() & ((self.data['genh_prostate_cancer']==0) |(self.data['genh_prostate_cancer']==2)|
                                                  ( self.data['genh_prostate_cancer']==-8) |( self.data['genh_prostate_cancer']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_pro_cancer_treat_now':
                mask = (self.data[col].isna() &(( self.data['genh_pro_cancer_treat']==0) |   ( self.data['genh_pro_cancer_treat']==2 ) |
                                                 ( self.data['genh_pro_cancer_treat']==-8 ) | ( self.data['genh_pro_cancer_treat']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_prostate_cancer_meds':
                 mask = ( self.data[col].isna() & ((self.data['genh_pro_cancer_treat_now']==0 )| (self.data['genh_pro_cancer_treat_now']==2)|
                                    (self.data['genh_pro_cancer_treat_now']==-8)| (self.data['genh_pro_cancer_treat_now']==-555)))
                 self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['genh_oes_cancer_treat', 'genh_oesophageal_trad_med']:
                mask = ( self.data[col].isna() &((self.data['genh_oesophageal_cancer']==0) |  ( self.data['genh_oesophageal_cancer']==2) |
                                                 ( self.data['genh_oesophageal_cancer']==-8)| ( self.data['genh_oesophageal_cancer']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_oes_cancer_treat_now':
                mask = ( self.data[col].isna() & ((self.data['genh_oes_cancer_treat']==0)| ( self.data['genh_oes_cancer_treat']==2)|
                                                  ( self.data['genh_oes_cancer_treat']==-8)| ( self.data['genh_oes_cancer_treat']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_oes_cancer_meds':
                mask = ( self.data[col].isna() &((self.data['genh_oes_cancer_treat_now']==0)|( self.data['genh_oes_cancer_treat_now']==2)|
                                        ( self.data['genh_oes_cancer_treat_now']==-8)| ( self.data['genh_oes_cancer_treat_now']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['genh_cancer_specify_other', 'genh_oth_cancer_trad_med']:
                mask = ( self.data[col].isna() &(( self.data['genh_other_cancers']==0)| ( self.data['genh_other_cancers']==2) |
                                                 ( self.data['genh_other_cancers']==-8) | ( self.data['genh_other_cancers']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_oth_cancer_treat_now':
                mask = ( self.data[col].isna() &((self.data['genh_other_cancer_treat']==0)| ( self.data['genh_other_cancer_treat']==2)|
                                                 (self.data['genh_other_cancer_treat']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_other_cancer_meds':
                mask = ( self.data[col].isna() &((self.data['genh_oth_cancer_treat_now']==0)| ( self.data['genh_oth_cancer_treat_now']==2) |
                                                 ( self.data['genh_oth_cancer_treat_now']==-8)| ( self.data['genh_oth_cancer_treat_now']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def c_general_health_diet_logic(self):
        c_general_col = Instruments(self.data).get_c_general_health_diet()
        for col in c_general_col:
            if col in ['genh_starchy_staple_freq', 'genh_staple_servings']:
                #if self.data['gene_site'] == 4:
                #    mask = (self.data[col]==0 & ((self.data['genh_starchy_staple_food___2'] == 0) |
                #                         (self.data['genh_starchy_staple_food___3'] == 0) |
                #                         (self.data['genh_starchy_staple_food___13'] == 0) |
                #                         (self.data['genh_starchy_staple_food___14'] == 0) |
                #                         (self.data['genh_starchy_staple_food___15'] == 0) |
                #                         (self.data['genh_starchy_staple_food___16'] == 0)))
                #    self.data[col] = self.data[col].mask(mask, -555)
                mask = (self.data[col]==0 & ((self.data['genh_starchy_staple_food___1'] == 0) |
                                         (self.data['genh_starchy_staple_food___2'] == 0) |
                                         (self.data['genh_starchy_staple_food___3'] == 0) |
                                         (self.data['genh_starchy_staple_food___4'] == 0) |
                                         (self.data['genh_starchy_staple_food___5'] == 0) |
                                         (self.data['genh_starchy_staple_food___6'] == 0) |
                                         (self.data['genh_starchy_staple_food___7'] == 0) |
                                         (self.data['genh_starchy_staple_food___8'] == 0) |
                                         (self.data['genh_starchy_staple_food___9'] == 0) |
                                         (self.data['genh_starchy_staple_food___10'] == 0) |
                                         (self.data['genh_starchy_staple_food___11'] == 0) |
                                         (self.data['genh_starchy_staple_food___12'] == 0)))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def d_general_health_exposure_to_pesticides_pollutants_logic(self):
        d_general_col = Instruments(self.data).get_d_general_health_exposure_to_pesticides_pollutants()
        for col in d_general_col:
            if col == 'genh_pesticide_years':
                mask = (self.data[col].isna() & ((self.data['genh_pesticide']==0)| (self.data['genh_pesticide']==2)|(self.data['genh_pesticide']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_pesticide_list':
                mask = (self.data[col].isna() & ((self.data['genh_pesticide_type']==0) | (self.data['genh_pesticide_type']==2) |
                                                 (self.data['genh_pesticide_type']==-8) | (self.data['genh_pesticide_type']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_cookingplace_specify':
                mask = (self.data[col].isna() & ((self.data['genh_cooking_place']==1) | (self.data['genh_cooking_place']==2) |
                                                 (self.data['genh_cooking_place']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'genh_smoke_freq_someone':
                mask = ((self.data[col].isna()) & ((self.data['genh_smoker_in_your_house']==0)| (self.data['genh_smoker_in_your_house']==2)|
                                                   (self.data['genh_smoker_in_your_house']==-8) | (self.data['genh_smoker_in_your_house']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def infection_history_logic(self):
        infection_col = Instruments(self.data).get_infection_history()
        for col in infection_col:
            if col == 'infh_malaria_month':
                mask = (self.data[col].isna() & ((self.data['infh_malaria']==0)| (self.data['infh_malaria']==2)| (self.data['infh_malaria']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['infh_tb_12months', 'infh_tb_treatment', 'infh_tb_meds', 'infh_tb_counselling']:
                mask = (self.data[col].isna() &((self.data['infh_tb']==0)| (self.data['infh_tb']==2)| (self.data['infh_tb']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'infh_tb_diagnosed':
                mask = (self.data[col].isna() &((self.data['infh_tb_12months']==0)| (self.data['infh_tb_12months']==2)|
                                                (self.data['infh_tb_12months']==-8)| (self.data['infh_tb_12months']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['infh_hiv_tested', 'infh_hiv_status', 'infh_hiv_positive']:
                mask = (self.data[col].isna() &((self.data['infh_hiv_que_answering']==0)| (self.data['infh_hiv_que_answering']==2)|(self.data['infh_hiv_que_answering']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['infh_hiv_diagnosed', 'infh_hiv_medication', 'infh_hiv_traditional_meds', 'infh_painful_feet_hands',
             'infh_hypersensitivity', 'infh_kidney_problems', 'infh_liver_problems',
             'infh_change_in_body_shape', 'infh_mental_state_change', 'infh_chol_levels_change']:
                mask = (self.data[col].isna() &((self.data['infh_hiv_positive']==0)|(self.data['infh_hiv_positive']==2)|
                                                (self.data['infh_hiv_positive']==-8)| (self.data['infh_hiv_positive']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['infh_hiv_treatment', 'infh_hiv_arv_meds', 'infh_hiv_arv_meds_now']:
                mask = (self.data[col].isna() & ((self.data['infh_hiv_medication']==0)| (self.data['infh_hiv_medication']!=2)|
                                                 (self.data['infh_hiv_medication']==-8) | (self.data['infh_hiv_medication']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['infh_hiv_arv_meds_specify', 'infh_hiv_arv_single_pill']:
                mask = (self.data[col].isna() & ((self.data['infh_hiv_arv_meds_now']==0)| (self.data['infh_hiv_arv_meds_now']==2)|
                                                 (self.data['infh_hiv_arv_meds_now']==-8) | (self.data['infh_hiv_arv_meds_now']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'infh_hiv_pill_size':
                mask = (self.data[col].isna() & ((self.data['infh_hiv_arv_single_pill']==0)| (self.data['infh_hiv_arv_single_pill']!=2)|
                                                 (self.data['infh_hiv_arv_single_pill']==-8) | (self.data['infh_hiv_arv_single_pill']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'infh_hiv_counselling':
                mask = (self.data[col].isna() & ((self.data['infh_hiv_test']==0)| (self.data['infh_hiv_test']==2)|
                                                 (self.data['infh_hiv_test']==-8)| (self.data['infh_hiv_test']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'infh_hiv_test':
                mask = (self.data[col].isna() &(self.data['infh_hiv_positive'] == 0))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def a_cardiometabolic_risk_factors_diabetes_logic(self):
        a_cardiometabolic = Instruments(self.data).get_a_cardiometabolic_risk_factors_diabetes()
        for col in a_cardiometabolic:
            if col in ['carf_diabetes_12months', 'carf_diabetes_treatment']:
                mask = ( self.data[col].isna() & ((self.data['carf_diabetes']==0)|(self.data['carf_diabetes']==2) | (self.data['carf_diabetes']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_diabetes_treat_now':
                mask = ( self.data[col].isna() &(( self.data['carf_diabetes_treatment']==0)| (self.data['carf_diabetes_treatment']==2)|
                                                 ( self.data['carf_diabetes_treatment']==-8)| ( self.data['carf_diabetes_treatment']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['carf_diabetes_treat___1', 'carf_diabetes_treat___2', 'carf_diabetes_treat___3', 'carf_diabetes_treat___4',
                         'carf_diabetes_treat___5', 'carf_diabetes_treat____999', 'carf_diabetes_meds_2']:
                mask = (self.data[col]==0 & ((self.data['carf_diabetes_treat_now'] == 0) | (self.data['carf_diabetes_treat_now'] == 2) |
                            (self.data['carf_diabetes_treat_now'] == -8) | (self.data['carf_diabetes_treat_now'] == -555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['carf_diabetes_meds_2']:
                mask = (self.data[col].isna() & ((self.data['carf_diabetes_treat_now'] == 0) | (self.data['carf_diabetes_treat_now'] == 2) |
                            (self.data['carf_diabetes_treat_now'] == -8) | (self.data['carf_diabetes_treat_now'] == -555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_diabetetreat_specify':
                mask = ( self.data[col].isna() &((self.data['carf_diabetes_treat___5'] == 0 ) | ( self.data['carf_diabetes_treat___5'] == -555 )))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_diabetes_traditional':
                mask = ( self.data[col].isna() &(( self.data['carf_diabetes_12months']==0)| ( self.data['carf_diabetes_12months']==2)|
                                                 ( self.data['carf_diabetes_12months']==-8) | ( self.data['carf_diabetes_12months']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['carf_diabetes_mother', 'carf_diabetes_father', 'carf_diabetes_fam_other', 'carf_diabetes_brother_1',
                         'carf_diabetes_brother_2', 'carf_diabetes_brother_3', 'carf_diabetes_brother_4', 'carf_diabetes_sister_1',
                         'carf_diabetes_sister_2', 'carf_diabetes_sister_3', 'carf_diabetes_sister_4', 'carf_diabetes_son_1',
                         'carf_diabetes_son_2', 'carf_diabetes_son_3', 'carf_diabetes_son_4', 'carf_daughter_diabetes_1',
                         'carf_diabetes_daughter_2', 'carf_diabetes_daughter_3', 'carf_diabetes_daughter_4']:
                mask = ( self.data[col].isna() &((self.data['carf_diabetes_history']==0)| (self.data['carf_diabetes_history']==2)|
                                                 (self.data['carf_diabetes_history']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            #elif col == 'carf_diabetes_brother_1':
            #    mask = ( self.data[col].isna() & (self.data['carf_diabetes_history']==1)&(self.data['famc_number_of_brothers']>=1))
            #elif col == 'carf_diabetes_brother_2':
            #    mask = ( self.data[col].isna()&(self.data['carf_diabetes_history']==1)&(self.data['famc_number_of_brothers'] >= 2 ) )
            #elif col == 'carf_diabetes_brother_3':
            #    mask = ( self.data[col].isna() &(self.data['carf_diabetes_history']==1)&(self.data['famc_number_of_brothers'] >= 3 ) )
            #elif col == 'carf_diabetes_brother_4':
            #    mask = ( self.data[col].isna()&( self.data['carf_diabetes_history'] == 1)&(self.data['famc_number_of_brothers'] >= 4 ) )
            #elif col == 'carf_diabetes_sister_1':
            #    mask = ( self.data[col].isna() &(self.data['carf_diabetes_history'] == 1)&(self.data['famc_number_of_sisters'] >= 1 ) )
            #elif col == 'carf_diabetes_sister_2':
            #    mask = ( self.data[col].isna() &( self.data['carf_diabetes_history'] == 1 ) &( self.data['famc_number_of_sisters'] >= 2 ) )
            #elif col == 'carf_diabetes_sister_3':
                # mask = ( self.data[col].isna() &( self.data['carf_diabetes_history'] == 1 ) & ( self.data['famc_number_of_sisters'] >= 3 ) )
            #elif col == 'carf_diabetes_sister_4':
            #   mask = ( self.data[col].isna() & ( self.data['carf_diabetes_history'] == 1 ) & ( self.data['famc_number_of_sisters'] >= 4 ) )
            #elif col == 'carf_diabetes_son_1':
            #    mask = ( self.data[col].isna() & ( self.data['carf_diabetes_history'] == 1 ) & ( self.data['famc_bio_sons'] >= 1 ) )
            #elif col == 'carf_diabetes_son_2':
            #    mask = ( self.data[col].isna() &(self.data['carf_diabetes_history'] == 1 ) & ( self.data['famc_bio_sons'] >= 2 ) )
            #elif col == 'carf_diabetes_son_3':
                # mask = ( self.data[col].isna() &( self.data['carf_diabetes_history'] == 1 ) & ( self.data['famc_bio_sons'] >= 3 ) )
            #elif col == 'carf_diabetes_son_4':
            #    mask = ( self.data[col].isna() &( self.data['carf_diabetes_history'] == 1 ) & ( self.data['famc_bio_sons'] >= 4 ) )
            #elif col == 'carf_daughter_diabetes_1':
            #    mask = ( self.data[col].isna() &( self.data['carf_diabetes_history'] == 1 ) & ( self.data['famc_bio_daughters'] >= 1 ) )
            #elif col == 'carf_diabetes_daughter_2': mask = ( self.data[col].isna() & ( self.data['carf_diabetes_history'] == 1 ) &
            # ( self.data['famc_bio_daughters'] >= 2 ) )
            #elif col == 'carf_diabetes_daughter_3':
            #    mask = ( self.data[col].isna() & ( self.data['carf_diabetes_history'] == 1 ) &  ( self.data['famc_bio_daughters'] >= 3 ) )
            #elif col == 'carf_diabetes_daughter_4':
            #    mask = ( self.data[col].isna() &( self.data['carf_diabetes_history'] == 1 ) & ( self.data['famc_bio_daughters'] >= 4 ) )
            elif col == 'carf_diabetes_fam_specify':
                mask = ( self.data[col].isna() & ((self.data['carf_diabetes_fam_other']==0)| (self.data['carf_diabetes_fam_other']==2)|
                                                  (self.data['carf_diabetes_fam_other']==-8)|(self.data['carf_diabetes_fam_other']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def b_cardiometabolic_risk_factors_heart_conditions_logic(self):
        b_cardiometabolic = Instruments(self.data).get_b_cardiometabolic_risk_factors_heart_conditions()
        for col in b_cardiometabolic:
            if col == 'carf_stroke_diagnosed':
                mask = (self.data[col].isna() & ((self.data['carf_stroke'] ==0) | (self.data['carf_stroke'] == 2)|
                                                 (self.data['carf_stroke'] == -8)))
                self.data[col] = self.data[col].mask(mask, -555)
            if col in ['carf_angina_treatment', 'carf_pain_location','carf_angina_traditional', 'carf_pain', 'carf_pain2']:
                mask = (self.data[col].isna() & ((self.data['carf_angina']==0)| (self.data['carf_angina']==2)| (self.data['carf_angina']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_angina_treat_now':
                mask = (self.data[col].isna() &((self.data['carf_angina_treatment']==0)| (self.data['carf_angina_treatment']==2)|
                                                (self.data['carf_angina_treatment']==-8)| (self.data['carf_angina_treatment']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_angina_meds':
                mask = (self.data[col].isna() & ((self.data['carf_angina_treat_now']==0)| (self.data['carf_angina_treat_now']==2)|
                                                 (self.data['carf_angina_treat_now']==-8)| (self.data['carf_angina_treat_now']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['carf_pain_action_stopslow', 'carf_relief_standstill']:
                mask = (self.data[col].isna() & ((self.data['carf_pain2']==0)| (self.data['carf_pain2']==2)|(self.data['carf_pain2']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['carf_heartattack_treat', 'carf_heartattack_trad']:
                mask = (self.data[col].isna() & ((self.data['carf_heartattack']==0)| (self.data['carf_heartattack']==2)|(self.data['carf_heartattack']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_heartattack_meds':
                mask = (self.data[col].isna() & ((self.data['carf_heartattack_treat']==0)| (self.data['carf_heartattack_treat']==2)|
                                                 (self.data['carf_heartattack_treat']==-8)| (self.data['carf_heartattack_treat']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['carf_chf_treatment', 'carf_chf_treatment_now', 'carf_chf_meds']:
                mask = (self.data[col].isna() & ((self.data['carf_congestiv_heart_fail']==0)| (self.data['carf_congestiv_heart_fail']==2)|(self.data['carf_congestiv_heart_fail']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def c_cardiometabolic_risk_factors_hypertension_choles_logic(self):
        c_cardiometabolic = Instruments(self.data).get_c_cardiometabolic_risk_factors_hypertension_choles()
        for col in c_cardiometabolic:
            if col in ['carf_hypertension_12mnths', 'carf_hypertension_treat']:
                mask = ( self.data[col].isna() &((self.data['carf_hypertension']==0)|(self.data['carf_hypertension']==2)|
                                                 (self.data['carf_hypertension']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_hypertension_meds':
                mask = ( self.data[col].isna() &((self.data['carf_hypertension_treat']==0)|(self.data['carf_hypertension_treat']==2)|
                                ( self.data['carf_hypertension_treat']==-8)| ( self.data['carf_hypertension_treat']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_hypertension_medlist':
                mask = ( self.data[col].isna() & ((self.data['carf_hypertension_meds']==0)| (self.data['carf_hypertension_meds']==2)|
                                (self.data['carf_hypertension_meds']==-8)| (self.data['carf_hypertension_meds']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['carf_chol_treatment']:
                mask = ( self.data[col].isna() &((self.data['carf_h_cholesterol']==0)| (self.data['carf_h_cholesterol']==2)|
                                                 (self.data['carf_h_cholesterol']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['carf_chol_treatment_now___1', 'carf_chol_treatment_now___2',
                         'carf_chol_treatment_now___3', 'carf_chol_treatment_now___4', 'carf_chol_treatment_now____999']:
                mask = ( self.data[col]==0 &((self.data['carf_h_cholesterol']==0)| (self.data['carf_h_cholesterol']==2)|
                                                 (self.data['carf_h_cholesterol']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_chol_medicine':
                mask = ( self.data[col].isna() &((self.data['carf_chol_treatment_now___3']==0)| (self.data['carf_chol_treatment_now___3']==2)|
                                                 (self.data['carf_chol_treatment_now___3']==-8)| (self.data['carf_chol_treatment_now___3']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_chol_treat_specify':
                mask = ( self.data[col].isna() & ( self.data['carf_chol_treatment_now___4'] == 0) | ( self.data['carf_chol_treatment_now___4'] ==2)|
                         ( self.data['carf_chol_treatment_now___4'] == -8 )| ( self.data['carf_chol_treatment_now___4'] == -555 ))
        return self.data

    def d_cardiometabolic_risk_factors_kidney_thyroid_ra_logic(self):
        d_cardiometabolic = Instruments(self.data).get_d_cardiometabolic_risk_factors_kidney_thyroid_ra()
        for col in d_cardiometabolic:
            if col in ['carf_thyroid_type', 'carf_thyroid_treatment', 'carf_parents_thyroid']:
                mask = ( self.data[col].isna() &((self.data['carf_thyroid']==0)| (self.data['carf_thyroid']==2)|
                                                 (self.data['carf_thyroid']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_thryroid_specify':
                mask = ( self.data[col].isna() & ((self.data['carf_thyroid_type'] == 0)|(self.data['carf_thyroid_type']==2)|
                                (self.data['carf_thyroid_type']==-8)| (self.data['carf_thyroid_type'] == -555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_thyroid_treat_use':
                mask = ( self.data[col].isna() &((self.data['carf_thyroid_treatment']==0)|(self.data['carf_thyroid_treatment']==2)|
                            (self.data['carf_thyroid_treatment']==-8)| (self.data['carf_thyroid_treatment']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_thyroidparnt_specify':
                mask = ( self.data[col].isna() & ((self.data['carf_parents_thyroid']==0)|(self.data['carf_parents_thyroid']==2)|
                                (self.data['carf_parents_thyroid']==-8)| (self.data['carf_parents_thyroid']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['carf_kidney_disease_known', 'carf_kidney_function_low']:
                mask = (self.data[col].isna() & ((self.data['carf_kidney_disease']==0)|
                    (self.data['carf_kidney_disease']==2)|(self.data['carf_kidney_disease']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_kidneydiseas_specify':
                mask = ( self.data[col].isna() & ((self.data['carf_kidney_disease_known']==0)|(self.data['carf_kidney_disease_known']==2)|
                                    (self.data['carf_kidney_disease_known']==-8)|(self.data['carf_kidney_disease_known']==-555)))
            elif col in ['carf_kidney_family_mother', 'carf_kidney_family_father', 'carf_kidney_family_other']:
                mask = ( self.data[col].isna() &((self.data['carf_kidney_family']==0)|(self.data['carf_kidney_family']==2)|
                                ( self.data['carf_kidney_family']==-8)| ( self.data['carf_kidney_family']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_kidney_fam_specify':
                mask = ( self.data[col].isna() & ((self.data['carf_kidney_family_other']==0)|(self.data['carf_kidney_family_other']==2)|
                                (self.data['carf_kidney_family_other']==-8)|(self.data['carf_kidney_family_other']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_kidney_family_type':
                mask = ( self.data[col].isna() &
                         ((self.data['carf_kidney_family_other']!=1)|(self.data['carf_kidney_family_mother']!=1) |
                           (self.data['carf_kidney_family_father']!=1)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_kidney_fam_tspecify':
                mask = ( self.data[col].isna() & (self.data['carf_kidney_family_type']==0))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['carf_joints_swollen', 'carf_joints_involved',
                         'carf_when_they_hurt', 'carf_symptoms_how_long']:
                mask = ( self.data[col].isna() & ((self.data['carf_joints_swollen_pain']==0)|
                    (self.data['carf_joints_swollen_pain']==2)|(self.data['carf_joints_swollen_pain']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['carf_acpa', 'carf_esr_crp', 'carf_rheumatoid_factor'] :
                mask = (self.data[col].isna() & ((self.data['carf_arthritis_results']==0)|
                                (self.data['carf_arthritis_results']==2)|(self.data['carf_arthritis_results']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['carf_osteo_sites___1', 'carf_osteo_sites___2',
                         'carf_osteo_sites___3', 'carf_osteo_sites___4', 'carf_osteo_sites___5',
                         'carf_osteo_sites___6']:
                mask = (self.data[col]==0 & (self.data['carf_osteo']==0))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_osteo_hip_replace':
                mask = ( self.data[col].isna() & (self.data['carf_osteo']==0))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in [ 'carf_osteo_hip_repl_site','carf_osteo_hip_repl_age']:
                mask = ( self.data[col].isna() & (self.data['carf_osteo_hip_replace']==0))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'carf_osteo_knee_replace':
                mask = ( self.data[col].isna() & ( self.data['carf_osteo']==0))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in [ 'carf_osteo_knee_repl_site', 'carf_osteo_knee_repl_age']:
                mask = ( self.data[col].isna()&(self.data['carf_osteo_knee_replace']==0 ))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def physical_activity_and_sleep_logic(self):
        gpaq_col = Instruments(self.data).get_physical_activity_and_sleep()
        for col in gpaq_col:
            if col in ['gpaq_work_vigorous_days', 'gpaq_work_vigorous_time', 'gpaq_work_vigorous_hrs', 'gpaq_work_vigorous_mins']:
                mask = (self.data[col].isna() & ((self.data['gpaq_work_vigorous']==0)|(self.data['gpaq_work_vigorous']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['gpaq_work_moderate_days', 'gpaq_work_moderate_time','gpaq_work_moderate_hrs', 'gpaq_work_moderate_mins']:
                mask = (self.data[col].isna() & (self.data['gpaq_work_moderate']==0)|(self.data['gpaq_work_moderate']==-8))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['gpaq_transport_phy_days', 'gpaq_transport_phy_time','gpaq_transport_phy_hrs', 'gpaq_transport_phy_mins']:
                mask = (self.data[col].isna() & ((self.data['gpaq_transport_phy']==0)|
                        (self.data['gpaq_transport_phy']==2)| (self.data['gpaq_transport_phy']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'gpaq_leisurevigorous_days':
                mask = (self.data[col].isna() & ((self.data['gpaq_leisure_vigorous']==0)| (self.data['gpaq_leisure_vigorous']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['gpaq_leisurevigorous_time', 'gpaq_leisurevigorous_hrs', 'gpaq_leisurevigorous_mins']:
                mask = (self.data[col].isna() & (self.data['gpaq_leisurevigorous_days'].isna()))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'gpaq_leisuremoderate_days':
                mask = (self.data[col].isna() & ((self.data['gpaq_leisuremoderate']==0)|
                                (self.data['gpaq_leisuremoderate']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['gpaq_leisurevigorous_time', 'gpaq_leisurevigorous_hrs', 'gpaq_leisurevigorous_mins']:
                mask = (self.data[col].isna() & (self.data['gpaq_leisurevigorous_days'].isna()))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['gpaq_leisuremoderate_time', 'gpaq_leisuremoderate_hrs', 'gpaq_leisuremoderate_mins']:
                mask = (self.data[col].isna() & (self.data['gpaq_leisuremoderate_days'].isna()))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    #ultrasound_and_dxa_measurements
    def ultrasound_and_dxa_measurements_logic(self):
        ultrasound_and_dxa_measurements_col = Instruments(self.data).get_ultrasound_and_dxa_measurements()
        for col in ultrasound_and_dxa_measurements_col:
            if col in ['ultr_comment']:
                mask = (self.data[col].isna() & self.data['ultr_vat_scat_measured']==1)
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == ['ultr_technician', 'ultr_visceral_fat', 'ultr_subcutaneous_fat']:
                mask = (self.data[col].isna() & self.data['ultr_vat_scat_measured']==0)
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'ultr_cimt_comment':
                mask = (self.data[col].isna() &self.data['ultr_cimt']==1)
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['ultr_cimt_technician', 'ultr_cimt_right_min', 'ultr_cimt_right_max',
                         'ultr_cimt_right_mean', 'ultr_cimt_left_min', 'ultr_cimt_left_max',
                         'ultr_cimt_left_mean']:
                mask = (self.data[col].isna() & (self.data['ultr_cimt'] == 0))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['ultr_plaque_comment']:
                mask = (self.data[col].isna() & (self.data['ultr_plaque_measured'] == 1))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['ultr_plaque_technician', 'ultr_plaque_right_present', 'ultr_plaque_right',
                         'ultr_plaque_left_present', 'ultr_plaque_left']:
                mask = (self.data[col].isna() & (self.data['ultr_plaque_measured']==0))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['ultr_dxa_scan_comment']:
                mask = (self.data[col].isna() & self.data['ultr_dxa_scan_completed'] == 1)
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['ultr_dxa_measurement_1', 'ultr_dxa_measurement_2', 'ultr_dxa_measurement_3',
                         'ultr_dxa_measurement_4', 'ultr_dxa_measurement_5']:
                mask = (self.data[col].isna() & self.data['ultr_dxa_scan_completed'] == 0)
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def a_respiratory_health_logic(self):
        a_respiratory_col = Instruments(self.data).get_a_respiratory_health()
        for col in a_respiratory_col:
            if col in ['resp_age_diagnosed', 'resp_asthma_treat']:
                mask = (self.data[col].isna() & ((self.data['resp_asthma_diagnosed']==0)|
                                                 (self.data['resp_asthma_diagnosed']==9)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'resp_asthma_treat_now':
                mask = (self.data[col].isna() & ((self.data['resp_asthma_treat']==0)|
                         (self.data['resp_asthma_treat']==9)) | (self.data['resp_asthma_treat']==-555))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'resp_copd_treat':
                mask = (self.data[col].isna() &((self.data['resp_copd_suffer___1']==0) |
                (self.data['resp_copd_suffer___2']==0) |(self.data['resp_copd_suffer___3']==0)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col in ['resp_medication_list', 'resp_puffs_time', 'resp_puffs_times_day']:
                mask = (self.data[col].isna() &((self.data['resp_inhaled_medication'] == 0)| (self.data['resp_inhaled_medication'] == 9)))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def b_spirometry_eligibility_logic(self):
        b_spirometry_col = Instruments(self.data).get_b_spirometry_eligibility()
        for col in b_spirometry_col:
            if col == 'rspe_chest_pain':
                mask = (self.data[col].isna() & ((self.data['rspe_major_surgery']==1)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'rspe_coughing_blood':
                mask = (self.data[col].isna() &((self.data['rspe_chest_pain']==1)| (self.data['rspe_chest_pain']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'rspe_acute_retinal_detach':
                mask = (self.data[col].isna() & ((self.data['rspe_coughing_blood']==1) | (self.data['rspe_coughing_blood']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'rspe_any_pain':
                mask = (self.data[col].isna() &((self.data['rspe_acute_retinal_detach']==1)| (self.data['rspe_acute_retinal_detach']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'rspe_diarrhea':
                mask = (self.data[col].isna() & ((self.data['rspe_any_pain']==1)| (self.data['rspe_any_pain']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'rspe_high_blood_pressure':
                mask = (self.data[col].isna() &((self.data['rspe_diarrhea']== 1)| (self.data['rspe_diarrhea']== -555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'rspe_tb_diagnosed':
                mask = (self.data[col].isna() &((self.data['rspe_high_blood_pressure']==1)| (self.data['rspe_high_blood_pressure']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'rspe_tb_treat_past4wks':
                mask = (self.data[col].isna() & ((self.data['rspe_tb_diagnosed']==0)| (self.data['rspe_tb_diagnosed']==9)|
                                                 (self.data['rspe_tb_diagnosed']==-555)))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def c_spirometry_test(self):
        c_spirometry_test_col = Instruments(self.data).get_c_spirometry_test()
        for col in c_spirometry_test_col:
            if col in ['spiro_num_of_blows', 'spiro_num_of_vblows', 'spiro_pass']:
                mask = (self.data[col].isna() & (self.data['spiro_eligible']==0))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def d_reversibility_test(self):
        d_reversibility_test_col = Instruments(self.data).get_d_reversibility_test()
        for col in d_reversibility_test_col:
            if col in ['rspir_salb_time_admin', 'rspir_time_started', 'rspir_researcher',
                       'rspir_num_of_blows', 'rspir_num_of_blows']:
                mask = (self.data[col].isna() & (self.data['rspir_salb_admin']==0))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data

    def a_microbiome_logic(self):
        a_microbiome = Instruments(self.data).get_a_microbiome()
        for col in a_microbiome:
            if col == 'micr_probiotics_t_period':
                mask = (self.data[col].isna() & (self.data['micr_probiotics_taken']==1))
                self.data[col] = self.data[col].mask(mask, -555)
            elif col == 'micr_wormintestine_period':
                mask = (self.data[col].isna() & ((self.data['micr_worm_intestine_treat']==0)|
                        (self.data['micr_worm_intestine_treat']==2)|(self.data['micr_worm_intestine_treat']==-8)))
                self.data[col] = self.data[col].mask(mask, -555)
        return self.data