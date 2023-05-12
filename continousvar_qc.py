import pandas as pd
import numpy as np
class ContinuousQC:

    def __init__(self, data):
        self.data = data
        #self.site = site

    def phase1_2diff(self, data):

        # create an Empty DataFrame object
        df = pd.DataFrame()

        ##populate it with the difference between awigen 1 and 2 values
        df['study_id'] = data['study_id']

        # participant identification
        df['age_diff'] = data['demo_age_at_collection'] - data['age_c']

        # family composition
        df['sons_diff'] = data['famc_bio_sons'] - data['number_of_sons_qc']
        df['daughters_diff'] = data['famc_bio_daughters'] - data['number_of_daughters_qc']

        # education
        df['education_diff'] = data['educ_highest_years'] - data['years_education_c_qc']

        # pesticide
        df['pesticide_diff'] = data['genh_pesticide_years']- data['years_pesticide_qc']

        # anthropometric measurement
        df['height_diff'] = data['anth_standing_height'] - data['standing_height_qc']
        df['weight_diff'] = data['anth_weight'] - data['weight_qc']
        df['waist_diff'] = data['anth_waist_circumf'] - data['waist_circumference_qc']
        df['hip_diff'] = data['anth_hip_circumf'] - data['hip_circumference_qc']
        df['bmi_diff'] = data['anth_bmi_c'] - data['bmi_c_qc']

        # blood_pressure_and_pulse_measurements
        df['systolic_diff'] = data['bppm_systolic_avg'] - data['bp_sys_average_qc']
        df['diastolic_diff'] = data['bppm_diastolic_avg'] - data['bp_dia_average_qc']
        df['pulse_ave_diff'] = data['bppm_pulse_avg'] - data['pulse_average_qc']

        # #ultrasound_and_dxa_measurements
        df['visceral_diff'] = data['ultr_visceral_fat'] - data['visceral_fat_qc']
        df['subcutaneous_diff'] = data['ultr_subcutaneous_fat'] - data['subcutaneous_fat_qc']
        df['cimt_right_diff'] = data['ultr_cimt_right_mean'] - data['mean_cimt_right_qc']
        df['cimt_left_diff'] = data['ultr_cimt_left_mean'] - data['mean_cimt_left_qc']


        # lipids
        df['glucose_diff'] = data['glucose_y'] - data['glucose_qc']
        df['insulin_diff'] = data['insulin_y'] - data['insulin_qc']
        df['serum_creatinine_diff'] = data['serum_creatinine'] - data['s_creatinine_qc']
        df['hdl_diff'] = data['lipids_hdl'] - data['hdl_qc']
        df['cholesterol_diff'] = data['lipids_cholesterol'] - data['cholesterol_1_qc']
        df['triglycerides_diff'] = data['lipids_triglycerides'] - data['triglycerides_qc']
        df['nonhdl_diff'] = data['lipids_nonhdl'] - data['non_hdl_c_c_qc']
        df['friedewald_ldl_diff'] = data['lipids_ldl_calculated'] - data['friedewald_ldl_c_c_qc']

        # urine
        df['urine_creatinine_diff'] = data['urine_creatinine'] - data['ur_creatinine_qc']
        df['urine_albumin_diff'] = data['urine_albumin'] - data['ur_albumin_qc']
        df['urine_acr_diff'] = data['urine_acr'] - data['acr_qc']
        df['urine_protein_diff'] = data['urine_protein'] - data['ur_protein_qc']

        df.set_index('study_id', inplace=True)

        phase1and2diff = df

        return phase1and2diff



    def outliers_phase1_2(self, phase1and2diff, data):

        cols = phase1and2diff.columns  # one or more
        Q1 = phase1and2diff[cols].quantile(0.25)
        Q3 = phase1and2diff[cols].quantile(0.75)
        IQR = Q3 - Q1

        upper_limit_iqr = Q3 + 1.5 * IQR
        lower_limit_iqr = Q1 - 1.5 * IQR

        upper_limit_std = phase1and2diff.mean() + phase1and2diff.std() * 3
        lower_limit_std = phase1and2diff.mean() - phase1and2diff.std() * 3

        upper_limits = pd.DataFrame({'iqr': upper_limit_iqr, 'std': upper_limit_std})
        lower_limits = pd.DataFrame({'iqr': lower_limit_std, 'std': lower_limit_iqr})

        # filtering outliers
        outliers = phase1and2diff[cols][((phase1and2diff[cols] > upper_limits.max(axis = 1)) | (phase1and2diff[cols] < lower_limits.min(axis = 1)))]
        outliers = outliers.dropna(axis=0, how='all')


        # unpivoting the dataframe by study id
        outliers = outliers.reset_index()
        outliers = pd.melt(outliers, id_vars=['study_id'], var_name='variables', value_name='awigen1_2_diff')
        outliers = outliers.dropna()
        outliers = outliers.merge(data[['study_id', 'site_x']],
                                  on=['study_id', 'study_id'], how='outer').dropna()
        #outliers = outliers.groupby('variables')
        outliers = outliers.sort_values('variables')

        return outliers

    def outliers_writer(self, outliers, data, variables, phase1and2diff):
        variables_list = variables.values.tolist()
        dfs_list = []
        for i in range(len(variables_list)):
            if (phase1and2diff[variables_list[i][2]].isna()).all():
                pass
            else:
                df_list = outliers[outliers['variables'] == variables_list[i][2]].merge(
                    data[['study_id', variables_list[i][0], variables_list[i][1]]], on=['study_id', 'study_id'],
                    how='left').dropna()
            dfs_list.append(df_list)
        outliers_combined = pd.concat(dfs_list, axis=0)

        return outliers_combined