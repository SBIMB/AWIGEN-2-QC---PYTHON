import pandas as pd
import psycopg2
import numpy as np
import sys
import psycopg2
import matplotlib.pyplot as plt

#insert md5 hash key
#md5_hask key = 

from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))
sys.path.append(d)
from postgres_db_config import config

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

#ultrsound data
agincourt_assets_data = pd.read_csv('/home/theomathema/AWI-Gen/phase2/Awigen_Asset_1_agincourt.csv')

#print(agincourt_assets_data.head())
#extraxt phase 2 data only
agincourt_assets_data = agincourt_assets_data[agincourt_assets_data['h'].isin(phase2_data['study_id'])]
print(agincourt_assets_data.shape)

#extractphase 2 data only
phase2_data = phase2_data[phase2_data['study_id'].isin(agincourt_assets_data['h'])]

#update the assets data
phase2_data['toilet_facilities'] = np.where(phase2_data['study_id'].isin(agincourt_assets_data['h']),
                                agincourt_assets_data['toilet_facilities'], phase2_data['toilet_facilities'])

phase2_data['cattle'] = np.where(phase2_data['study_id'].isin(agincourt_assets_data['h']),
                                agincourt_assets_data['cattle'], phase2_data['cattle'])

phase2_data['other_livestock'] = np.where(phase2_data['study_id'].isin(agincourt_assets_data['h']),
                                agincourt_assets_data['other_livestock'], phase2_data['other_livestock'])

phase2_data['poultry'] = np.where(phase2_data['study_id'].isin(agincourt_assets_data['h']),
                                agincourt_assets_data['poultry'], phase2_data['poultry'])

phase2_data['tractor'] = np.where(phase2_data['study_id'].isin(agincourt_assets_data['h']),
                                agincourt_assets_data['tractor'], phase2_data['tractor'])

print(phase2_data.ses_site_quintile_c.value_counts())

#house attributes ses calculation
house_col = ['electricity', 'solar_energy', 'power_generator', 'alter_power_src',  'television',
             'radio', 'motor_vehicle', 'motorcycle', 'bicycle', 'refrigerator', 'washing_machine',
             'sewing_machine', 'telephone', 'mobile_phone', 'microwave', 'dvd_player',
             'satellite_tv_or_dstv', 'computer_or_laptop', 'internet_by_computer', 'internet_by_mobile_phone',
             'electric_iron', 'fan', 'electric_or_gas_stove', 'kerosene_stove', 'plate_gas',
             'electric_plate', 'torch', 'gas_lamp', 'kerosene_lamp_with_glass', 'toilet_facilities',
             'portable_water', 'grinding_mill', 'table_kitchen', 'sofa_set', 'wall_clock', 'bed',
             'mattress', 'blankets', 'cattle', 'other_livestock', 'poultry', 'tractor','plough']

phase2_data['ses_c'] = phase2_data[house_col].replace({2: 0, -8: 0, -999:0, -555:0}).sum(axis=1, skipna=True)

#ses site totals
#ses calculation: adding all household attributes per participant

#ses % calculation
def ses(df):
    if df['site'] == 1:
        return df['ses_c']/27

phase2_data['ses_perc'] = phase2_data.apply(ses, axis=1)

#ses site quintile calculation
bins = [0, 0.2, 0.4, 0.6, 0.8, 1]
labels = [1, 2, 3 ,4, 5]
phase2_data['ses_site_quintile_c'] = pd.cut(phase2_data['ses_perc'], bins = bins ,labels=labels, include_lowest=True)

print(phase2_data['ses_site_quintile_c'].value_counts())

phase2_data.drop(columns=['ses_perc'], axis=1, inplace=True)

print(phase2_data.columns)

#preparing the data for the database.

data = phase2_data.replace(np.nan,-999)

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
    
        new_toilet_facilities_values = row['toilet_facilities']
        new_cattle_values = row['cattle']
        new_other_livestock_values = row['other_livestock']
        new_poultry_values = row['poultry']
        new_tractor_values = row['tractor']
        new_ses_c_values = row['ses_c']
        new_ses_site_quintile_c_values = row['ses_site_quintile_c']
        

        # Define the UPDATE query
        update_query = """
            UPDATE {} 
            SET toilet_facilities = %s, cattle = %s, other_livestock = %s,
            poultry = %s, tractor = %s, ses_c = %s, ses_site_quintile_c =%s
            WHERE study_id = %s
            """.format(table_name)

        # Execute the UPDATE query with the provided values
        cur.execute(update_query, ( new_toilet_facilities_values, new_cattle_values,
                                    new_other_livestock_values, new_poultry_values, new_tractor_values,
                                    new_ses_c_values, new_ses_site_quintile_c_values,
                                    condition_column_value))

    conn.commit()

except Exception as error:
    print(error)
    
#finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()



