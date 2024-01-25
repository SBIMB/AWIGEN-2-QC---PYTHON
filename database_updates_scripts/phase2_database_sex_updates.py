import pandas as pd
import psycopg2
from postgres_db_config import config
import numpy as np



#md5_hask key = d853aa97bdd0c52103ccd742078311

phase2_data = pd.read_csv('./resources/combined_phase2data_encoded.csv',
                     delimiter=',', low_memory=False)

ids = ['AB2298', 'VAT0G']

data = phase2_data[phase2_data['study_id'].isin(ids)]

#update the sex of the participants
data.sex=1

#update all the other sex realted variables
#updating the pregnancy variables
data_preg = ['pregnant', 'number_of_pregnancies', 'number_of_live_births', 'birth_control', 'hysterectomy',
    'regular_periods', 'last_period_remember', 'last_period_mon', 'last_period_yr', 'period_more_than_yr',
    'last_period_c', 'months_last_period_c', 'menopause_status_c']

data[data_preg] = -555

#updating the breast cancer variables
data_breast_cancer = ['breast_cancer', 'breast_cancer_treat_ever', 'breast_cancer_treat_current',
                    'breast_cancer_meds_list', 'breast_cancer_trad_med'
]

data[data_breast_cancer] = -555

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
    
        new_sex_values = row['sex']
        new_pregnant_values = row['pregnant']
        new_number_pregnancies_values = row['number_of_pregnancies']
        new_number_live_births_values = row['number_of_live_births']
        new_birth_control_values = row['birth_control']
        new_hysterectory_values = row['hysterectomy']
        new_regular_periods_values = row['regular_periods']
        new_last_periods_remember_values = row['last_period_remember']
        new_last_period_mon_values = row['last_period_mon']
        new_last_period_yr_values = row['last_period_yr']
        new_period_more_than_yr_values = row['period_more_than_yr']
        new_last_period_c_values = row['last_period_c']
        new_months_last_period_c = row['months_last_period_c']
        new_menopause_status_c_values = row['menopause_status_c']
        new_breast_cancer_values = row['breast_cancer']
        new_breast_cancer_treat_ever_values = row['breast_cancer_treat_ever']
        new_breast_cancer_treat_current_values = row['breast_cancer_treat_current']
        new_breast_cancer_meds_list_values = row['breast_cancer_meds_list']
        new_breast_cancer_trad_med = row['breast_cancer_trad_med']
        

        print(new_pregnant_values)
        

        # Define the UPDATE query
        update_query = """
            UPDATE {} 
            SET sex = %s, pregnant = %s, number_of_pregnancies = %s, number_of_live_births = %s,
            birth_control = %s, hysterectomy = %s, regular_periods = %s, last_period_remember = %s,
            last_period_mon = %s, last_period_yr = %s, period_more_than_yr = %s, last_period_c = %s,
            months_last_period_c = %s, menopause_status_c = %s, breast_cancer = %s, breast_cancer_treat_ever = %s,
            breast_cancer_treat_current = %s, breast_cancer_meds_list = %s, breast_cancer_trad_med = %s
            WHERE study_id = %s
            """.format(table_name)

        # Execute the UPDATE query with the provided values
        cur.execute(update_query, (new_sex_values, new_pregnant_values, new_number_pregnancies_values,
                    new_number_live_births_values, new_birth_control_values, new_hysterectory_values, 
                    new_regular_periods_values, new_last_periods_remember_values, new_last_period_mon_values,
                    new_last_period_yr_values, new_period_more_than_yr_values, new_last_period_c_values, 
                    new_months_last_period_c, new_menopause_status_c_values, new_breast_cancer_values,
                    new_breast_cancer_treat_ever_values,  new_breast_cancer_treat_current_values,
                    new_breast_cancer_meds_list_values,  new_breast_cancer_trad_med,                            
                    condition_column_value))

    conn.commit()

except Exception as error:
    print(error)
    
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()






