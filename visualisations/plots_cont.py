import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
#import DataFrame

phase2_data = pd.read_csv('./resources/combined_phase2data_encoded.csv', sep=',', low_memory=False)
phase1_data = pd.read_csv('./resources/all_sites_20_12_22.txt', sep = ',', low_memory=False)

# acr and albumin values phase1
phase1_data['ur_albumin_qc']= np.where(phase1_data['ur_albumin']<3, 3, phase1_data['ur_albumin'])
phase1_data['ur_albumin_qc']= np.where(phase1_data['ur_albumin_qc']>400, 400, phase1_data['ur_albumin_qc'])
phase1_data['ur_creatinine_qc']= np.where(phase1_data['ur_creatinine']<3.75, 3, phase1_data['ur_creatinine'])
phase1_data['ur_creatinine_qc']= np.where(phase1_data['ur_creatinine_qc']>550, 550, phase1_data['ur_creatinine_qc'])

phase1_data['acr_qc'] = phase1_data.apply(lambda row: row['ur_albumin_qc']/row['ur_creatinine_qc']\
      if row['ur_albumin_qc']>=0 and row['ur_creatinine_qc']>=0 else np.nan, axis=1)


# acr and albumin values phase2
phase2_data['ur_albumin']= np.where(phase2_data['ur_albumin']<3, 3, phase2_data['ur_albumin'])
phase2_data['ur_albumin']= np.where(phase2_data['ur_albumin']>400, 400, phase2_data['ur_albumin'])
phase2_data['ur_creatinine']= np.where(phase2_data['ur_creatinine']<3, 3, phase2_data['ur_creatinine'])
phase2_data['ur_creatinine']= np.where(phase2_data['ur_creatinine']>550, 550, phase2_data['ur_creatinine'])

phase2_data['acr'] = phase2_data.apply(lambda row: row['ur_albumin']/row['ur_creatinine']\
      if row['ur_albumin']>=0 and row['ur_creatinine']>=0 else np.nan, axis=1)


#age
phase1and2 = pd.read_csv('./redcap_resources/resources/phase1and2data.csv', sep = ',', low_memory=False)
phase1and2 = phase1and2[['study_id', 'age_c']]
phase1_data = phase1_data.merge(phase1and2, on='study_id', how='left')
phase1_data['age_c'] = phase1_data['age_c'].fillna(phase1_data['age_c'])

data = pd.merge(phase1_data, phase2_data, on = 'study_id', how = 'inner' )

data = data.replace(-999,np.nan)

print(data['ur_albumin_y'])

def sub(A, B, C, df):
    mask = (df[A] > 0) & (df[B] > 0)
    df[C] = pd.Series(dtype='float64')  # initialize the column if it doesn't exist
    df.loc[mask, C] = df[A] - df[B]
    return df[C]
   
# change between phase 1 and 2
data['age_change'] = sub('age_y', 'age_c', 'age_change', data)
data['height_change'] = sub('standing_height', 'standing_height_qc', 'height_change', data)
data['weight_change'] = sub('weight', 'weight_qc', 'weight_change', data)
data['waist_circumference_change'] = sub('waist_circumference', 'waist_circumference_qc', 'waist_circumference_change', data)
data['hip_circumference_change'] = sub('hip_circumference', 'hip_circumference_qc', 'hip_circumference_change', data)
data['bmi_change'] = sub('bmi_c_y', 'bmi_c_qc', 'bmi_change', data)
data['glucose_change'] = sub('glucose_result', 'glucose', 'glucose_change', data)
data['ldl_c_change'] = sub('friedewald_ldl_c', 'friedewald_ldl_c_c_qc', 'ldl_c_change', data)
data['hdl_change'] = sub('hdl_y', 'hdl_qc', 'hdl_change', data)
data['cholesterol_change'] = sub('cholesterol_1_y', 'cholesterol_1_qc', 'cholesterol_change', data)
data['triglycerides_change'] = sub('triglycerides_y', 'triglycerides_qc', 'triglycerides_change', data)
data['egfr_change'] = sub('egfr_c_y', 'egfr_c_qc', 'egfr_change', data)
data['insulin_change'] = sub('insulin_result', 'insulin_qc', 'insulin_change', data)
data['ur_creatinine_change'] = sub('ur_creatinine_y', 'ur_creatinine_qc', 'ur_creatinine_change', data)
#data['ur_ulbumin_change'] = sub('ur_ulbumin_y', 'ur_ulbumin_qc', 'ur_ulbumin_change', data)
data['ur_protein_change'] = sub('ur_protein_y', 'ur_protein_qc', 'ur_protein_change', data)
data['s_creatinine_change'] = sub('s_creatinine_y', 's_creatinine_qc', 's_creatinine_change', data)
data['mean_cimt_right_change'] = sub('mean_cimt_right_y', 'mean_cimt_right_qc', 'mean_cimt_right_change', data)
data['mean_cimt_left_change'] = sub('mean_cimt_left_y', 'mean_cimt_left_qc', 'mean_cimt_left_change', data)
data['VAT_change'] = sub('visceral_fat_y', 'visceral_fat_qc', 'VAT_change', data)
data['SCAT_change'] = sub('subcutaneous_fat_y', 'subcutaneous_fat_qc', 'SCAT_change', data)



outliers_height = data[(data.loc[:, 'height_change'] > 50) | (data.loc[:, 'height_change'] < -50)][['site_x', 'height_change']]
print(outliers_height.site_x.value_counts())

variables = pd.read_csv('./visualisations/variables.csv', delimiter=',')

pdfFile = PdfPages('plots.pdf')

variables_list = variables.values.tolist()

#print(data[data['site_x'] == 1][variables_list[0][2]])

for i in range(len(variables_list)):

    fig = plt.figure(figsize=(10, 6))

    # Dictionary to map categories to colors
    colors = {1: 'blue', 2: 'green', 3: 'brown', 4: 'red', 5:'orange', 6:'purple'}

    # define plotting region (2 rows, 2 columns)
    plt.subplot(1, 2, 1)
    # Precompute histogram data for each category
    hist_data = []
    bins = np.arange(min(data[variables_list[i][2]].dropna()), max(data[variables_list[i][2]].dropna()) + 1, 1)
    for cat in data['site_x'].unique():
        hist, _ = np.histogram(data[data['site_x'] == cat][variables_list[i][2]], bins=bins)
        hist_data.append(hist)

    # Plot histogram
    plt.hist([data[data['site_x'] == cat][variables_list[i][2]] for cat in data['site_x'].unique()], 
         bins=bins, color=[colors[cat] for cat in data['site_x'].unique()], 
         label=data['site_x'].unique(), alpha=0.5, align='mid')

    plt.title('The difference between phase 1 and 2 {} values'.format(variables_list[i][0]), fontdict = {'fontsize' : 8})
    plt.xlabel('{} differences'.format(variables_list[i][0]), fontsize=6)
    plt.ylabel('frequency of {} differences'.format(variables_list[i][0]), fontsize=6)
    plt.xticks(fontsize=6)
    plt.yticks(fontsize=6)
    plt.legend(title='site_x')
    plt.tight_layout(pad=4)

    plt.subplot(1, 2, 2)
    for t in data['site_x'].unique():
        plt.scatter(x=data[data['site_x']==t][variables_list[i][0]], y=data[data['site_x']==t][variables_list[i][1]], 
                label = t)
    plt.title('{} of participants phase 1 vs phase 2'.format(variables_list[i][0]), fontdict = {'fontsize' : 8})
    plt.xlabel('phase1', fontsize=6)
    plt.ylabel('phase2', fontsize=6)
    plt.xticks(fontsize=6)
    plt.yticks(fontsize=6)
    plt.legend(title='site')
    # space between the plots
    plt.tight_layout()
    #save plots
    pdfFile.savefig(fig)

pdfFile.close()

