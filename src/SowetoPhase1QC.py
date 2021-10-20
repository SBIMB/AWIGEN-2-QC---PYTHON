import csv
import ApiKeys
import requests

import pandas as pd
from io import StringIO

import numpy as np
import xlsxwriter
from datetime import datetime

# physical activity, completion of questionnaire, respirometry, microbiome

input1 = './resources/SowetoV0/AWIGEN2DPHRUSowetoV0_DATA_2021-07-28_1057.csv'
input2 = './resources/SowetoV0/AWIGen2DPHRUSoweto_DATA_2021-07-28_1058.csv'

v0 = pd.read_csv(input1, sep='\t')
v1 = pd.read_csv(input2, sep='\t')

common_ids = pd.merge(v0, v1, indicator=True, how='outer', left_on='study_id', right_on='study_id',suffixes=('_left', '_right'))

merged = common_ids[common_ids['_merge'] == 'both']
merged.set_index('study_id', inplace=True)
merged.to_csv('./resources/SowetoV0/merged.csv', index=True)

#### SPIROMETRY
# spiro_num_of_blows = merged[merged.columns[merged.columns.str.contains('valid_blow')]].count(axis=1)
# spiro_num_of_vblows = merged[merged.columns[merged.columns.str.contains('valid_blow')]].sum(axis=1)

# spiro_cols = merged[merged.columns[(merged.columns.str.contains('lung_function_') | merged.columns.str.contains('valid_blow'))]]
# spiro_cols = spiro_cols[spiro_cols.columns[spiro_cols.columns.str.contains('_rt') == False]]

# spiro_cols.to_csv('./resources/SowetoV0/spiro_cols.csv', index=True)

# spiro_df = pd.DataFrame(index=spiro_cols.index)
# temp = pd.DataFrame(index=spiro_cols.index)
# temp['spiro_pass'] = 0

# for col in spiro_cols.columns:
#     num = col.split('_')[-1]
#     lung_func_col = 'lung_function_' + num

#     col1 = spiro_cols[col]
#     col2 = spiro_cols[lung_func_col]

#     lung_function = (col1 == 1) & (col2 < 0.7)

#     temp['spiro_pass'] = temp['spiro_pass'] | lung_function

# spiro_df['spiro_num_of_blows'] = spiro_num_of_blows
# spiro_df['spiro_num_of_blows_old'] = merged['spiro_num_of_blows']

# spiro_df['spiro_num_of_vblows'] = spiro_num_of_vblows
# spiro_df['spiro_num_of_vblows_old'] = merged['spiro_num_of_vblows']

# spiro_df['spiro_pass'] = temp['spiro_pass'] * 1
# spiro_df['spiro_pass_old'] = merged['spiro_pass']

# spiro_df.to_csv('./resources/SowetoV0/spiro.csv', index=True)


#### PHYSICAL ACTIVITY
# gpaq_df = pd.DataFrame()

# time = pd.DatetimeIndex(merged['work_vigorous_days'])
# merged['work_vigorous_days'] = (time.hour * 60 + time.minute)

# time = pd.DatetimeIndex(merged['work_vigorous_time'])
# merged['work_vigorous_time'] = (time.hour * 60 + time.minute)

# time = pd.DatetimeIndex(merged['work_moderate_day'])
# merged['work_moderate_day'] = (time.hour * 60 + time.minute)

# time = pd.DatetimeIndex(merged['transport_physical_time'])
# merged['transport_physical_time'] = (time.hour * 60 + time.minute)

# time = pd.DatetimeIndex(merged['leisure_vigorous_hours'])
# merged['leisure_vigorous_hours'] = (time.hour * 60 + time.minute)

# time = pd.DatetimeIndex(merged['leisure_moderate_time'])
# merged['leisure_moderate_time'] = (time.hour * 60 + time.minute)

# time = pd.DatetimeIndex(merged['working_day_sitting'])
# merged['working_day_sitting'] = (time.hour * 60 + time.minute)

# time = pd.DatetimeIndex(merged['non_working_day_sitting'])
# merged['non_working_day_sitting'] = (time.hour * 60 + time.minute)

# gpaq_df['gpaq_work_weekend'] = merged['work_weekend']
# gpaq_df['gpaq_work_sedentary'] = merged['work_sedentary']

# gpaq_df['gpaq_work_vigorous'] = merged['work_vigorous']
# gpaq_df['gpaq_work_vigorous_days'] = np.round(merged['work_vigorous_days'] / merged['work_vigorous_time']).astype(pd.Int32Dtype())
# # gpaq_df['gpaq_work_vigorous_time'] = merged['work_vigorous_time']
# gpaq_df['gpaq_work_vigorous_hrs'] = np.floor(merged['work_vigorous_time'] / 60)
# gpaq_df['gpaq_work_vigorous_mins'] = merged['work_vigorous_time'] % 60

# gpaq_df['gpaq_work_moderate'] = merged['work_moderate']
# gpaq_df['gpaq_work_moderate_days'] = ((merged['moderate_intensity_hours'] * 60) / merged['work_moderate_day'])
# gpaq_df['gpaq_work_moderate_days'] = np.round(gpaq_df['gpaq_work_moderate_days'].astype(float)).astype(pd.Int32Dtype())
# # gpaq_df['gpaq_work_moderate_time'] = merged['work_moderate_day']
# gpaq_df['gpaq_work_moderate_hrs'] = np.floor(merged['work_moderate_day'] / 60)
# gpaq_df['gpaq_work_moderate_mins'] = merged['work_moderate_day'] % 60

# # gpaq_df['gpaq_work_day_time'] = merged['work_day'] * 60
# gpaq_df['gpaq_work_day_hrs'] = np.floor((merged['work_day'] * 60) / 60)
# gpaq_df['gpaq_work_day_mins'] = (merged['work_day'] * 60) % 60

# gpaq_df['gpaq_transport_phy'] = merged['transport_physical']
# gpaq_df['gpaq_transport_phy_days'] = merged['transport_physical_days']
# # gpaq_df['gpaq_transport_phy_time'] = merged['transport_physical_time']
# gpaq_df['gpaq_transport_phy_hrs'] = np.floor(merged['transport_physical_time'] / 60)
# gpaq_df['gpaq_transport_phy_mins'] = merged['transport_physical_time'] % 60

# gpaq_df['gpaq_leisure_phy'] = merged['leisure_physical']

# gpaq_df['gpaq_leisure_vigorous'] = merged['leisure_vigorous']
# gpaq_df['gpaq_leisurevigorous_days'] = merged['leisure_vigorous_days']
# # gpaq_df['gpaq_leisurevigorous_time'] = merged['leisure_vigorous_hours']
# gpaq_df['gpaq_leisurevigorous_hrs'] = np.floor(merged['leisure_vigorous_hours'] / 60)
# gpaq_df['gpaq_leisurevigorous_mins'] = merged['leisure_vigorous_hours'] % 60

# gpaq_df['gpaq_leisuremoderate'] = merged['leisure_moderate']
# gpaq_df['gpaq_leisuremoderate_days'] = merged['leisure_moderate_days']
# # gpaq_df['gpaq_leisuremoderate_time'] = merged['leisure_moderate_time']
# gpaq_df['gpaq_leisuremoderate_hrs'] = np.floor(merged['leisure_moderate_time'] / 60)
# gpaq_df['gpaq_leisuremoderate_mins'] = merged['leisure_moderate_time'] % 60

# # gpaq_df['gpaq_work_day_stng_time'] = merged['working_day_sitting']
# gpaq_df['gpaq_work_day_stng_hrs'] = np.floor(merged['working_day_sitting'] / 60)
# gpaq_df['gpaq_work_day_stng_mins'] = merged['working_day_sitting'] % 60

# # gpaq_df['gpaq_non_work_day_time'] = merged['non_working_day_sitting']
# gpaq_df['gpaq_non_work_day_hrs'] = np.floor(merged['non_working_day_sitting'] / 60)
# gpaq_df['gpaq_non_work_day_mins'] = merged['non_working_day_sitting'] % 60

# gpaq_df['gpaq_week_sleep_time'] = merged['sleep_time_week']
# gpaq_df['gpaq_week_wakeup_time'] = merged['wakeup_time_week']
# gpaq_df['gpaq_weekend_sleep_time'] = merged['sleep_time_weekend']
# gpaq_df['gpaq_weekend_wakeup_time'] = merged['wakeup_time_weekend']
# gpaq_df['gpaq_sleep_room_pple_num'] = merged['people_sleep_in_room']
# gpaq_df['gpaq_sleep_room_livestock'] = merged['livestock_sleep_in_room']
# gpaq_df['gpaq_sleep_on'] = merged['what_do_you_sleep_on']
# gpaq_df['gpaq_mosquito_net_use'] = merged['sleep_in_mosquito_net']
# gpaq_df['gpaq_feel_alert'] = merged['feel_alert']
# gpaq_df['gpaq_sleeping_difficulty'] = merged['difficulty_falling_asleep']
# gpaq_df['gpaq_difficulty_staysleep'] = merged['difficulty_staying_asleep']
# gpaq_df['gpaq_waking_early_problem'] = merged['problems_waking_up_early']
# gpaq_df['gpaq_waking_up_tired'] = merged['waking_up_tired']
# gpaq_df['gpaq_sleep_pattern_satis'] = merged['satisfied_sleep_pattern']
# gpaq_df['gpaq_sleep_interfere'] = merged['sleep_interfere']

# gpaq_df['redcap_event_name'] = 'phase_2_arm_1'
# gpaq_df['physical_activity_and_sleep_complete'] = 2

# gpaq_df.to_csv('./resources/SowetoV0/gpaq_for_upload2.csv', index=True)



#### COMPLETION OF QUESTIONNAIRE
# comp_df = pd.DataFrame()
# comp_df['comp_sections_1_13'] = merged['sections_1_13']
# comp_df['comp_comment_no_1_13']  = merged['comment_no_1_13']
# comp_df['comp_section_14'] = merged['section_14']
# comp_df['comp_comment_no_14'] = merged['comment_no_14']
# comp_df['comp_section_15'] = merged['section_15']
# comp_df['comp_comment_no_15'] = merged['comment_no_15']
# comp_df['comp_section_16'] = merged['section_16']
# comp_df['comp_comment_no_16'] = merged['comment_no_16']
# comp_df['comp_section_17'] = merged['section_17']
# comp_df['comp_comment_no_17'] = merged['comment_no_17']
# comp_df['comp_section_18'] = merged['section_18']
# comp_df['comp_comment_no_18'] = merged['comment_no_18']
# comp_df['comp_section_19'] = merged['section_19']
# comp_df['comp_comment_no_19'] = merged['comment_no_19']
# comp_df['comp_section_20'] = merged['section_20']
# comp_df['comp_comment_no_20'] = merged['comment_no_20']
# comp_df['completion_of_questionnaire_complete'] = merged['completion_of_questionnaire_complete_left']

# comp_df['redcap_event_name'] = 'phase_2_arm_1'

# comp_df.to_csv('./resources/SowetoV0/comp_questionnaire_for_upload.csv', index=True)


#### MICROBIOME
microbiome_df = pd.DataFrame()
microbiome_df['micr_take_antibiotics'] = merged['take_antibiotics']
microbiome_df['micr_diarrhea_last_time'] = merged['when_last_have_diarrhea']
microbiome_df['micr_worm_intestine_treat'] = merged['treated_for_worms']
microbiome_df['micr_wormintestine_period'] = merged['medication_for_worms']
microbiome_df['micr_probiotics_taken'] = merged['treated_for_worms_2']
microbiome_df['micr_probiotics_t_period'] = merged['medication_for_worms_2']
microbiome_df['a_microbiome_complete'] = merged['general_health_complete']

microbiome_df['redcap_event_name'] = 'phase_2_arm_1'

microbiome_df = microbiome_df[microbiome_df['a_microbiome_complete'] == 2]

microbiome_df.to_csv('./resources/SowetoV0/microbiome_for_upload.csv', index=True)

asd = 1
