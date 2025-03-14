import pandas as pd
import psycopg2
import numpy as np
import sys
import psycopg2
import matplotlib.pyplot as plt

#insert md5 hash key
#md5_hask key = a9f3eaab19db91b53c4ce2b4ee359419

from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))
sys.path.append(d)
from postgres_db_config import config

#upload height outliers
height_outliers = pd.read_csv('./data_qc/height_difference_5cm_indv.txt', sep = '\t')
phase1_data = pd.read_csv(r'/home/theomathema/database_versions/phase1_versions/data/all_sites_phase1.csv', sep = ',', low_memory=False)

#upload phase2 data from the database
params_ = config()
# Create a cursor object to interact with the database
conn = psycopg2.connect( **params_)
cur = conn.cursor()

# Execute an SQL query: Theo to provide VIEW name
query = "SELECT * FROM all_sites_phase2 "
cur.execute(query)

# Fetch the query results (you can use fetchone(), fetchall(), etc.)
phase2_data  = cur.fetchall()
colnames = [desc[0] for desc in cur.description]

phase2_data =  pd.DataFrame(phase2_data, columns= colnames)

print(phase2_data[phase2_data['standing_height']==-999].shape)

#print(height_outliers['Site'].value_counts())

height_ids = height_outliers['ID']

#merge phase 1 and phase 2 data
height_merged = pd.merge(phase1_data[['study_id', 'standing_height_qc', 'bmi_c_qc']], phase2_data[['study_id', 'standing_height', 'weight',
<<<<<<< HEAD
                        's_creatinine', 'sex', 'age', 'acr']], on = 'study_id', how='right')
=======
                        's_creatinine', 'sex', 'age']], on = 'study_id', how='right')
>>>>>>> master

#update the standing height values with phase 1 standing height values for participants with changes >50mm
height_merged.loc[height_merged['study_id'].isin(height_outliers['ID']), 'standing_height'] = height_merged['standing_height_qc']

#checking number of missing data
print(height_merged[height_merged['standing_height']==-999].shape)

#bmi recalculation
height_merged['bmi_c'] = height_merged.apply(lambda row: float(row['weight'])/(float(row['standing_height']/1000)**2) if row['weight'] > 0 and row['standing_height'] > 0 else np.nan, axis=1)

#bmi class encoding recalculation
bins = [0, 18.5, 25, 30, 70]
labels = [0, 1, 2, 3]
height_merged['bmi_cat_c'] = pd.cut(height_merged['bmi_c'], bins=bins, labels=labels)

# egrf calculation
height_merged['serum_creatinine_2'] = height_merged['s_creatinine'].replace({-111:np.nan, -999:np.nan})
height_merged['one'] = 1
height_merged['female_cal'] = height_merged['serum_creatinine_2'].astype('float')/61.9
height_merged['male_cal'] = height_merged['serum_creatinine_2'].astype('float')/79.6

height_merged['female_min'] = height_merged[['female_cal', 'one']].min(axis=1, skipna=False)
height_merged['female_max'] = height_merged[['female_cal', 'one']].max(axis=1, skipna=False)

height_merged['male_min'] = height_merged[['male_cal', 'one']].min(axis=1, skipna=False)
height_merged['male_max'] = height_merged[['male_cal', 'one']].max(axis=1, skipna=False)

def egfr(df):
     if (df['s_creatinine'] == 0):
        return np.nan
     elif (df['sex'] == 0):
        #return (141 * (df['female_min']**(-0.329))*(df['female_max']**(-1.209))*(0.993**df['demo_age_at_collection'])*1.018).real
        return 141 * ((df['female_min'])**(-0.329))*((df['female_max'])**(-1.209))*((0.993)**(df['age']))*1.018
     elif (df['sex'] == 1):
        #return (141*(df['male_min']**(-0.411))*(df['male_min']**(-1.209))*(0.993**df['demo_age_at_collection'])).real
        return 141*((df['male_min'])**(-0.411))*((df['male_max'])**(-1.209))*((0.993)**(df['age']))
     else:
        return np.nan

height_merged['egfr_c'] = height_merged.apply(egfr, axis=1)

<<<<<<< HEAD
# ckd calculation
def ckd(df):

    if ((df['egfr_c']==-999) or (df['acr']==-999)):
        return -999
    elif ((df['egfr_c'] < 60) or (df['acr'] > 3)):
        return 1
    elif ((df['egfr_c'] >= 60) or (df['acr'] <= 3)):
        return 0
    else:
        -999

height_merged['ckd_c'] = height_merged.apply(ckd, axis=1)

print(phase2_data['ckd_c'].value_counts(dropna=False))
print(height_merged['ckd_c'].value_counts(dropna=False))

=======
>>>>>>> master
print(height_merged.shape)


#height_merged[['bmi_c', 'bmi_c_qc']] = height_merged[['bmi_c', 'bmi_c_qc']].replace(-999, np.nan)

#bmi_diff = height_merged['bmi_c'].astype(float)-height_merged['bmi_c_qc'].astype(float)
#height_diff.to_csv('height_checks.csv')

#plt.hist(bmi_diff)
#plt.savefig('updated_bmi_histogram.png', dpi=300) 
#plt.show()


#preparing the data for the database.
<<<<<<< HEAD
height_merged=height_merged[['study_id', 'standing_height', 'bmi_c', 'egfr_c', 'bmi_cat_c', 'ckd_c']]
=======
height_merged=height_merged[['study_id', 'standing_height', 'bmi_c', 'egfr_c', 'bmi_cat_c']]
>>>>>>> master
data = height_merged

# create the database    
params_ = config()

#insert the data
conn = None
cur = None

try: 
    conn = psycopg2.connect( **params_)

    cur = conn.cursor()
    

    # Assuming you already have a Pandas DataFrame named 'your_dataframe' with updated data
    # Replace 'your_table_name' with the actual table name in your database
    table_name = 'all_sites_phase2'

        # Iterate through rows in the DataFrame and update corresponding rows in the database
    for index, row in data.iterrows():
        # Assuming 'condition_column' is the column used to identify the row to be updated
        condition_column_value = row['study_id']
    
        # Assuming 'column1' and 'column2' are columns to be updated
    
        new_standing_height_values = row['standing_height']
        new_bmi_values = row['bmi_c']
        new_egfr_c_values = row['egfr_c']
<<<<<<< HEAD
        new_ckd_c_values = row['ckd_c']
=======
>>>>>>> master
        

        # Define the UPDATE query
        update_query = """
            UPDATE {} 
<<<<<<< HEAD
            SET standing_height = %s, bmi_c = %s, egfr_c = %s, ckd_c = %s
=======
            SET standing_height = %s, bmi_c = %s, egfr_c = %s
>>>>>>> master
            WHERE study_id = %s
            """.format(table_name)

        # Execute the UPDATE query with the provided values
<<<<<<< HEAD
        cur.execute(update_query, (new_standing_height_values, new_bmi_values, new_egfr_c_values,  new_ckd_c_values,              
=======
        cur.execute(update_query, (new_standing_height_values, new_bmi_values, new_egfr_c_values,                
>>>>>>> master
                    condition_column_value))

    conn.commit()

except Exception as error:
    print(error)
    
<<<<<<< HEAD
#finally:
=======
finally:
>>>>>>> master
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()






