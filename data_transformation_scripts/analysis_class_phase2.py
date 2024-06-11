import csv
import pandas as pd
import numpy as np
#import datetime as dt
#from datetime import datetime
import math

class AnalysisClassPhase2:

    def __init__(self, data):
        self.data = data

    def add_calculated_variables(self, input_data):

        output_data = input_data
        #dates encoding
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

        output_data['site_type_c'] = output_data.apply(site_type, axis=1)

        #number of siblings

        output_data['famc_number_of_siblings'] = output_data['famc_number_of_brothers'] + output_data['famc_number_of_sisters']

        # number of children
        output_data['famc_number_of_children'] = output_data['famc_bio_sons'] + output_data['famc_bio_daughters']

        # menopause status calculation

        output_data['day'] = 1     
            
        output_data['year']= np.floor(output_data['preg_last_period_mon_2']).astype(str).str[:4]

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

        output_data['meno_months_since_last_period'][output_data['meno_months_since_last_period'] < 0] = -999

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

        output_data = output_data.drop(['day', 'year', 'month'], axis=1)

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
        output_data['hous_people_to_rooms_density'] = np.where( output_data['hous_number_of_rooms']>0,
                            (output_data['hous_household_size']/output_data['hous_number_of_rooms']).round(decimals=2), np.nan)


        # people to bedroom density calculation
        output_data['hous_people_to_bedrooms_density'] = np.where(output_data['hous_number_of_bedrooms']>0, 
                    (output_data['hous_household_size'] / output_data['hous_number_of_bedrooms']).round(decimals=2), np.nan)

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
        #for col in house_col:
        #    output_data[col] = output_data[col].replace({2: 0, -8: 0, -999:0, -555:0})
        #    output_data['hous_participant_ses'] = output_data[house_col].sum(axis=1, skipna=True)

        output_data['hous_participant_ses'] = output_data[house_col].replace({2: 0, -8: 0, -999:0, -555:0}).sum(axis=1, skipna=True)

        #ses site totals
        #to check
        #ses calculation: adding all household attributes per participant

        #ses % calculation
        def ses(df):
            if df['gene_site'] == 6:
                return df['hous_participant_ses']/22
            elif df['gene_site'] == 5:
                return df['hous_participant_ses']/36
            elif df['gene_site'] == 4:
                return df['hous_participant_ses']/36
            elif df['gene_site'] == 3:
                return df['hous_participant_ses']/34
            elif df['gene_site'] == 2:
                return df['hous_participant_ses']/34
            elif df['gene_site'] == 1:
                return df['hous_participant_ses']/27

        output_data['hous_ses_perc'] = output_data.apply(ses, axis=1)

        #ses site quintile calculation
        # bmi class encoding
        bins = [0, 0.2, 0.4, 0.6, 0.8, 1]
        labels = [1, 2, 3 ,4, 5]
        output_data['hous_ses_quintiles'] = pd.cut(output_data['hous_ses_perc'], bins = bins ,labels=labels, include_lowest=True)

        #mvpa calculation

        output_data['gpaq_work_vigorous_time'][output_data['gpaq_work_vigorous_time']<0] = np.nan
        output_data['gpaq_work_moderate_time'][output_data['gpaq_work_moderate_time']<0] = np.nan
        output_data['gpaq_transport_phy_time'][output_data['gpaq_transport_phy_time']<0] = np.nan
        output_data['gpaq_leisurevigorous_time'][output_data['gpaq_leisurevigorous_time']<0] = np.nan
        output_data['gpaq_leisuremoderate_time'][output_data['gpaq_leisuremoderate_time']<0] = np.nan

        output_data['gpaq_work_vigorous_days'][output_data['gpaq_work_vigorous_days']<0] = np.nan
        output_data['gpaq_work_moderate_days'][output_data['gpaq_work_moderate_days']<0] = np.nan
        output_data['gpaq_transport_phy_days'][output_data['gpaq_transport_phy_days']<0] = np.nan
        output_data['gpaq_leisurevigorous_days'][output_data['gpaq_leisurevigorous_days']<0] = np.nan
        output_data['gpaq_leisuremoderate_days'][output_data['gpaq_leisuremoderate_days']<0] = np.nan

        output_data['gpaq_work_vigorous_total_minutes'] = output_data['gpaq_work_vigorous_days'] * output_data['gpaq_work_vigorous_time']
        output_data['gpaq_work_moderate_total_minutes'] = output_data['gpaq_work_moderate_days'] * output_data['gpaq_work_moderate_time']
        output_data['gpaq_transport_phy_total_minutes'] = output_data['gpaq_transport_phy_days'] * output_data['gpaq_transport_phy_time']
        output_data['gpaq_leisure_vigorous_total_minutes'] = output_data['gpaq_leisurevigorous_days'] * output_data['gpaq_leisurevigorous_time']
        output_data['gpaq_leisure_moderate_total_minutes'] = output_data['gpaq_leisuremoderate_days'] * output_data['gpaq_leisuremoderate_time']

        output_data['gpaq_mvpa'] = output_data[['gpaq_work_vigorous_total_minutes', 'gpaq_work_moderate_total_minutes',
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

        #BMI calculation

        output_data['weight_2'] = output_data['anth_weight'].replace({-999: np.nan})
        output_data['height_2'] = output_data['anth_standing_height'].replace({-999:np.nan})
        output_data['anth_bmi_c'] = output_data.weight_2 / (output_data.height_2 / 1000) ** 2

        # bmi class encoding
        bins = [0, 18.5, 25, 30, 70]
        # labels = ['<18.5', '18.5-24.9', '25-30 ', '>=30']
        labels = [0, 1, 2, 3]
        output_data['anth_bmi_cat'] = pd.cut(output_data['anth_bmi_c'], bins=bins, labels=labels)

        output_data['anth_waist_hip_ratio'] = output_data['anth_waist_circumf']/output_data['anth_hip_circumf']

        # remember to re-encode the -999
        #output_data['lipids_friedewald_ldl'] = np.where(output_data['lipids_triglycerides'] > 4.52, np.nan,
        #                                                output_data['lipids_cholesterol'] - (
        #                                                            output_data['lipids_triglycerides'] / 2.2) -
        #                                                output_data['lipids_hdl'])

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
                return 141*((df['male_min'])**(-0.411))*((df['male_max'])**(-1.209))*((0.993)**(df['demo_age_at_collection']))
            else:
                return np.nan

        output_data['egfr'] = output_data.apply(egfr, axis=1)


        output_data = output_data.drop(['one', 'female_cal',	'male_cal',	'female_min', 'hous_ses_perc',
                               'female_max',	'male_min',	'male_max', 'serum_creatinine_2', 'weight_2', 'height_2'], axis=1)


        # ckd calculation
        def ckd(df):

            if ((pd.isna(df['egfr'])) or (pd.isna(df['urine_acr']))):
                return np.nan
            elif ((df['egfr'] < 60) or (df['urine_acr'] > 3)):
                return 1
            elif ((df['egfr'] >= 60) or (df['urine_acr'] <= 3)):
                return 0
            else:
                np.nan

        output_data['ckd'] = output_data.apply(ckd, axis=1)


        output_data['cimt_mean_max'] = (output_data['ultr_qc_rt_t_max']+output_data['ultr_qc_lt_t_max'])/2


        return output_data

        
