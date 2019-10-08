import pandas as pd
import numpy as np
import math

data = pd.read_csv("../resources/data.csv", index_col=0)

f = open("../resources/testing.txt", "a+")

ignored_cols = ['ethnolinguistc_available', 'a_phase_1_data_complete', 'gene_site_id',
                'demo_approx_dob_is_correct', 'demo_dob_is_correct', 'demo_date_of_birth_known',
                'demo_dob_new', 'demo_approx_dob_new']

ethnicity_cols = ['ethn_father_ethn_sa',
                    'ethn_father_ethn_ot',
                    'ethn_father_lang_sa',
                    'ethn_father_lang_ot',
                    'ethn_pat_gfather_ethn_sa',
                    'ethn_pat_gfather_ethn_ot',
                    'ethn_pat_gfather_lang_sa',
                    'ethn_pat_gfather_lang_ot',
                    'ethn_pat_gmother_ethn_sa',
                    'ethn_pat_gmother_ethn_ot',
                    'ethn_pat_gmother_lang_sa',
                    'ethn_pat_gmother_lang_ot',
                    'ethn_mother_ethn_sa',
                    'ethn_mother_ethn_ot',
                    'ethn_mother_lang_sa',
                    'ethn_mother_lang_ot',
                    'ethn_mat_gfather_ethn_sa',
                    'ethn_mat_gfather_ethn_ot',
                    'ethn_mat_gfather_lang_sa',
                    'ethn_mat_gfather_lang_ot',
                    'ethn_mat_gmother_ethn_sa',
                    'ethn_mat_gmother_ethn_ot',
                    'ethn_mat_gmother_lang_sa',
                    'ethn_mat_gmother_lang_ot']

pregnancy_cols = ['preg_pregnant',
                    'preg_num_of_pregnancies',
                    'preg_num_of_live_births',
                    'preg_birth_control',
                    'preg_hysterectomy',
                    'preg_regular_periods',
                    'preg_last_period_remember',
                    'preg_last_period_mon',
                    'preg_last_period_mon_2',
                    'preg_period_more_than_yr']

for i, j in data.iterrows():
    for col in data.columns:
        if col == 'demo_age_at_collection':
            if pd.isnull(j[col]) and pd.notna(j['demo_dob_new']):
                pass
            elif pd.notna(j[col]):
                pass
            else:
                print(i, col, "demo_date_of_birth_issue")

        elif col == 'demo_gender':
            if pd.isnull(j[col]):
                print(i, col, "missing demo_gender")
            else:
                pass

        elif col == 'demo_home_language' or col == 'other_home_language':
            if pd.isnull(j['demo_home_language']):
                print(i, col, "missing home language")
            elif pd.isnull('other_home_language') and j['demo_home_language'] == 98:
                print(i, col, "provide other home language")
            else:
                pass

        elif col == 'ethnicity':
            if pd.isnull(j[col]) and j['ethnicity_confirmation'] == 1:
                pass
            elif pd.notna(j[col]) and j['ethnicity_confirmation'] == 0:
                pass
            else:
                print(i, col, 'missing ethnicity')

        elif col == 'other_ethnicity':
            if pd.isnull(j[col]) and j['ethnicity'] == 98:
                print(i, col, "missing other_ethnicity")
            else:
                pass

        elif col in ethnicity_cols:
            if pd.isnull(j[col]) and (j['gene_site'] == 2 or j['gene_site'] == 6):
                pass
            elif pd.notna(j[col]):
                pass
            else:
                print(i, col, "missing ethnicity value")

        elif col == 'famc_siblings':
            if pd.isnull(j[col]):
                print(i, col, "missing famc_siblings value")
            else:
                pass

        elif col == 'famc_number_of_brothers':
            if pd.isnull(j[col]) and (j['famc_siblings'] == 0 or pd.isnull(j['famc_siblings'])):
                pass
            elif j[col] >= 0 and j['famc_siblings']==1:
                pass
            else:
                print(i, col, "missing brothers")

        elif col == 'famc_living_brothers':
            if pd.isnull(j[col]) and (pd.isnull(j['famc_number_of_brothers']) or j['famc_number_of_brothers']==0):
                pass
            elif j[col] >= 0:
                pass
            else:
                print(i, col, "missing famc_living_brothers value")

        elif col == 'famc_number_of_sisters':
            if pd.isnull(j[col]) and (j['famc_siblings'] == 0 or pd.isnull(j['famc_siblings'])):
                pass
            elif j[col] >= 0:
                pass
            else:
                print(i, col, "missing value")

        elif col == 'famc_living_sisters':
            if pd.isnull(j[col]) and (j['famc_siblings'] == 0 or pd.isnull(j['famc_siblings']) or j['famc_number_of_sisters']==0):
                pass
            elif j[col] >= 0:
                pass
            else:
                print(i, col, "missing value")

        elif col == 'famc_children':
            if pd.isnull(j[col]):
                print(i, col, "missing value")
            else:
                pass

        elif col in ['famc_bio_sons', 'famc_bio_daughters']:
            if pd.isnull(j[col]) and (j['famc_children'] == 0 or pd.isnull(j['famc_children'])):
                pass
            elif j[col] >= 0:
                pass
            else:
                print(i, col, "missing value")

        elif col in ['famc_living_bio_sons', 'famc_living_bio_daughters']:
            if (col == 'famc_living_bio_sons' and pd.isnull(j[col])) and (j['famc_bio_sons'] == 0 or pd.isnull(j['famc_bio_sons'])):
                pass
            elif (col == 'famc_living_bio_daughters' and pd.isnull(j[col])) and (j['famc_bio_daughters'] == 0 or pd.isnull(j['famc_bio_daughters'])):
                pass
            elif j[col] >= 0:
                pass
            else:
                print(i, col, "missing value")

        elif col in pregnancy_cols:
            if pd.isnull(j[col]) and (j['demo_gender'] == 1 or pd.isnull(j['demo_gender'])):
                pass
            elif pd.isnull(j[col]) and j['preg_pregnant'] == 0:
                if col == 'preg_num_of_pregnancies':
                    print(i, col, "missing value")
                elif col == 'preg_num_of_live_births' and j['preg_num_of_pregnancies'] > 0:
                    print(i, col, "missing vlaue")
                elif (col == 'preg_last_period_mon' or col == 'preg_last_period_mon_2') and j['preg_last_period_remember'] == 1:
                    print(i, col, "misssing value")
                elif col == 'preg_period_more_than_yr' and (j['preg_last_period_remember'] == 0 or j['preg_last_period_remember'] == 2):
                    print(i, col, "missing value")
            else:
                pass

        elif col in ['educ_highest_years', 'educ_formal_years', 'empl_days_work']:
            if col == 'educ_highest_years' and pd.isnull(j[col]) and j['educ_highest_level'] in [2, 3, 4]:
                print(i, col, "missing value")
            elif col == 'educ_formal_years' and pd.isnull(j[col]) and j['educ_highest_level'] in [2, 3, 4]:
                print(i, col, "missing value")
            elif col == 'empl_days_work' and pd.isnull(j[col]) and j['empl_status'] in [2, 3, 4]:
                print(i, col, "missing value")
            else:
                pass

        elif col == 'frai_comment':
            if pd.isnull(j[col]) and j['frai_sit_stands_completed'] == 0:
                print(i, col, "missing value")
            else:
                pass

        elif col == 'frai_comment_why':
            if pd.isnull(j[col]) and j['frai_complete_procedure'] == 0:
                print(i, col, "missing value")
            else:
                pass

        elif col == 'frai_please_comment_why':
            if pd.isnull(j[col]) and j['frai_procedure_walk_comp'] == 0:
                print(i, col, "missing value")
            else:
                pass


        else:
            if col not in ignored_cols and pd.isnull(j[col]):
                print(i, col, "missing values")

    print("*********************************\n")
f.close()

# if pd.isnull(j[col]):
#     f.write(str(i) + " " + str(col) + " " + str(j[col]) + "\n")
#     print()