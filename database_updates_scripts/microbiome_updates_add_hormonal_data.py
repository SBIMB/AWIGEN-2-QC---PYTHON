import pandas as pd
import json
import psycopg2
import sys

from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))

sys.path.append(d)

from postgres_db_config import config
import numpy as np

params_ = config()
# Create a cursor object to interact with the database
conn = psycopg2.connect( **params_)
cur = conn.cursor()

# Execute an SQL query: Theo to provide VIEW name
query = "SELECT * FROM microbiome_phenotype"
cur.execute(query)

# Fetch the query results (you can use fetchone(), fetchall(), etc.)
microbiome_phenotype  = cur.fetchall()
colnames = [desc[0] for desc in cur.description]

microbiome_phenotype =  pd.DataFrame(microbiome_phenotype, columns= colnames)
print(microbiome_phenotype.shape)

#hormonal data from Raylton
hormonal_data = pd.read_csv('/home/theomathema/AWI-Gen/hormone_data/AWIGen2Microbiome.csv')
print(hormonal_data.shape)
print(hormonal_data['study_id'])

#merge hormonal and microbiome data
merge_data = pd.merge(hormonal_data, microbiome_phenotype, on='study_id', how='left')
print(merge_data.shape)

#extract only the hormonal variables
merge_data = merge_data[['study_id', 'ALB_2', 'E2_2', 'FSH_2', 'SHBG_2', 'TESTO_2']]


print(merge_data.describe())

data = merge_data

#alter table and add the new columns

#conn = None
#cur = None
#
#try:

    # Create a cursor object to interact with the database
#    conn = psycopg2.connect( **params_)
#    cur = conn.cursor()

    # Execute an SQL query to alter the table and add a new column

#    cur.execute("ALTER TABLE microbiome_phenotype ADD COLUMN ALB_2 numeric;")
#    cur.execute("ALTER TABLE microbiome_phenotype ADD COLUMN E2_2 numeric;")
#    cur.execute("ALTER TABLE microbiome_phenotype ADD COLUMN FSH_2 numeric;")
#    cur.execute("ALTER TABLE microbiome_phenotype ADD COLUMN SHBG_2 numeric;")
#    cur.execute("ALTER TABLE microbiome_phenotype ADD COLUMN TESTO_2 numeric;")
#    print("Columns added successfully.")

    # Commit the transaction to make the changes permanent
#    conn.commit()

#except psycopg2.Error as e:
#    print("Error:", e)
#    conn.rollback()  # Rollback changes if an error occurs

#cur.close()
#conn.close()


# add data to the new column

#insert the data to the columns added
#conn = None
#cur = None

#try:

    # Create a connection and a cursor object to interact with the database
#    conn = psycopg2.connect(**params_)
#    cur = conn.cursor()

#    table_name = 'microbiome_phenotype'

    #Iterate through rows in the DataFrame and update corresponding rows in the database
#    for index, row in data.iterrows():
        #'condition_column' i.e study_id is the column used to identify the row to be updated
#        condition_column_value = row['study_id']
    
        # ALB_2, E2_2, FSH_2, SHBG_2, TESTO_2 are columns to be updated
#        new_ALB_2_value = row['ALB_2']
#        new_E2_2_value = row['E2_2']
#        new_FSH_2_value = row['FSH_2']
#        new_SHBG_2_value = row['SHBG_2']
#        new_TESTO_2_value = row['TESTO_2']

        # Define the UPDATE query
#        update_query = """
#            UPDATE {} 
#            SET ALB_2 = %s, E2_2 = %s, FSH_2= %s, SHBG_2=%s, TESTO_2=%s
#            WHERE study_id = %s """.format(table_name)


#        cur.execute(update_query, (new_ALB_2_value, new_E2_2_value, new_FSH_2_value, new_SHBG_2_value, new_TESTO_2_value, condition_column_value))
#        print("Data added to the new column successfully.")

     # Commit the transaction to make the changes permanent
#    conn.commit()

#except psycopg2.Error as e:
#    print("Error:", e)
#    conn.rollback()  # Rollback changes if an error occurs

#    cur.close()
#    conn.close()