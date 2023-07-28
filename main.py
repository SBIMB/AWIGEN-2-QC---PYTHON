import pandas as pd
import numpy as np
from analysis_class_phase1 import AnalysisClassPhase1
from Age_recalculation import AgeRecalculation
from continousvar_qc import ContinuousQC


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # main()
    outputDir = './resources/'

    #extracted from the database aafter setup
    phase2_data = pd.read_csv(outputDir + "/combined_phase2data.csv", delimiter=",", low_memory=False)

    # phase 1(this will be extracted from sql after setting in up)
    phase1 = pd.read_csv(outputDir + "/all_sites_20_12_22.txt", delimiter=",", low_memory=False)

    # extracting study ids in both phase 1 and phase 2
    phase2_data = phase2_data[phase2_data['study_id'].isin(phase1['study_id'])]
    study_id = phase2_data['study_id']

    # utilising the phase1 analysis class
    phase1 = AnalysisClassPhase1(phase1).phase1_data(phase1).reset_index(drop=True)

    #agincort awigen 1 age correction
    study_id_3 = ['AB2301', 'AB0851', 'AB1454', 'AB0641', 'AB0668', 'AB2192',
                  'AB0569', 'AB1295', 'AB0751', 'AB0736', 'AB0045', 'AB0101', 'AB0989']

    new_age = [53, 61, 69, 60, 42, 61, 54, 68, 65, 55, 63, 46, 70]
    for i in range(len(study_id_3)):
        phase1.loc[phase1.study_id == study_id_3[i], ['age']] = new_age[i]

    # nanoro awigen 1 age correction
    study_id_5 = ['LKL0T',  'LAI0T', 'JVY0T', 'JIZ0W', 'KDA0N', 'IPN0V', 'IMR0T', 'IKS0Q', 'IVC0T']
    new_age1 = [45, 60, 51, 50, 49, 50, 59, 52, 56]

    for i in range(len(study_id_5)):
        phase1.loc[phase1.study_id == study_id_5[i], ['age']] = new_age1[i]

    # phase 2 and phase 1 data
    phase2_data = phase2_data[phase2_data['study_id'].isin(study_id)]
    phase1_data = phase1[phase1['study_id'].isin(study_id)]

    # sort soweto
    phase1_data = phase1_data.sort_values(['study_id'], ascending=True).reset_index(drop=True)
    phase2_data = phase2_data.sort_values(['study_id'], ascending=True).reset_index(drop=True)

    #phase 1 age recalculation
    age_class = AgeRecalculation(phase1_data, phase2_data)
    phase1 = age_class.AgePhase1(phase1_data, phase2_data)

    # replacing -999 and -555 to nas
    phase1_data = phase1_data.replace({'-999': np.nan, -999: np.nan, -555: np.nan, -111: np.nan, -222:np.nan})
    phase2_data = phase2_data.replace({-999:np.nan, -111: np.nan, -222:np.nan})

    #merging phase 1 and phase 2 data
    data = phase1_data.merge(phase2_data, on=['study_id', 'study_id'], how='inner')
    data = data.reset_index(drop=True)

    # continuous variables qc

    data = data[data['gene_site']==5]
    qc_class = ContinuousQC(data)
    phase1and2diff = qc_class.phase1_2diff(data)
    outliers = qc_class.outliers_phase1_2(phase1and2diff, data)

    data.to_csv(outputDir + 'phase1and2data_navrongo.csv')
    phase1and2diff.to_csv(outputDir + 'phase1and2diff_navrongo.csv')
    outliers.to_csv('outliers_navrongo.csv')

    #print(phase1and2diff['site'])

    #outliers.to_csv(outputDir + 'outliers_{}.csv'.format(site))

    #phase1and2diff.to_csv(outputDir + 'phase1and2diff_{}.csv'.format(site))

    #data.to_csv(outputDir + 'phase1and2data_{}.csv'.format(site))

    #   phase2_data.to_csv(outputDir + 'phase2_data_{}_{}.csv'.format(site, datestr))

    #   #outliers_writer

    #    outliers_combined = qc_class.outliers_writer(outliers, data, variables, phase1and2diff)
    #    outliers_combined.to_csv(outputDir + 'outliers_combined_{}.csv'.format(site))


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
