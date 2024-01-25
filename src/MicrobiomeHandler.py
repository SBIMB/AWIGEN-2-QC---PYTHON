import pandas as pd
import numpy as np

outputDir = './resources/Microbiome/'

# xlsx = pd.read_excel(outputDir + 'Nanoro_Stockage des echantillons H3A_AWI-Gen 2 - STOOL PORTOIRE 1-17.xlsx', sheet_name=None)

# all_ids = []
# for name, sheet in xlsx.items():
#     data = sheet.stack().reset_index()
#     data = data.iloc[5:,2]
#     test = data.str.strip('\n').str.split('\n')
#     ids1 = [row[2] for row in test.values]
#     ids2 = [id[3:] for id in ids1]
#     all_ids.append(ids2)
#     asd = 1

# all_ids = sum(all_ids, [])
# all_ids_df = pd.DataFrame(all_ids)
# all_ids_df.to_csv(outputDir + 'nanoro_MICROBIOME.csv')

stored_samples = pd.read_csv(outputDir + 'input_soweto_microbiome.csv', low_memory=False, sep=';')
redcap = pd.read_csv(outputDir + 'AWIGen2DPHRUSoweto_DATA_2022-06-14_1131.csv', low_memory=False, sep='\t')
redcap = redcap[redcap['redcap_event_name'] == 'phase_2_arm_1']

microbiome_data = redcap['study_id'][redcap['a_microbiome_complete'] == 2]

merge = pd.merge(stored_samples, microbiome_data, indicator=True, how='outer', left_on='STUDY ID', right_on='study_id', suffixes=('_left', '_right'))
merge['_merge'] = merge['_merge'].cat.rename_categories({'right_only': 'Metadata Only', 'left_only': 'Sample Only'})
merge.rename(columns={"STUDY ID": "Sample ID", "study_id": "study_id", "_merge": "Sample/Metadata"}, inplace=True)
merge = merge[["Sample ID", "study_id", "Sample/Metadata"]]

merge.to_csv(outputDir + 'soweto_micro_test.csv')