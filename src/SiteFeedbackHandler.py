from functools import reduce
import pandas as pd
from ExportData import ExportData


class SiteFeedbackHandler:

    @staticmethod
    def handle_outlier_feedback(csv):
        df = pd.read_excel(csv, skiprows=3, index_col='study_id')

        # We only want rows where 'Is Correct' or 'Comment' has been filled in
        mask = df['Is Correct'].notna() | df['Comment'].notna()
        df = df[mask]

        # Set all 'Is Correct' to 1 for REDCap
        df.loc[df['Is Correct'].notna(),'Is Correct'] = 1

        # Create 'record_id' column
        df = df.reset_index()
        df.index.rename('record_id', inplace=True)

        df = df[['study_id', 'Data Field', 'Value', 'Is Correct', 'Comment']]

        # Get comments with text
        comments = df['Comment'].where(df['Comment'].str.isalpha().notna())

        # Get new values from the Comment columt
        new_vals = pd.to_numeric(df['Comment'], errors='coerce')

        # Set site ID based on the CSV name
        site_ids = {'agincourt' : 1, 'dimamo' : 2, 'nairobi' : 3, 'nanoro' : 4, 'navrongo' : 5, 'soweto' : 6}
        site = [key for key, value in site_ids.items() if key in csv.lower()][0]
        site_id = site_ids[site]
        df['site'] = site_id

        # Rename columns for REDCap
        df.rename(columns={'Data Field':'data_field', 'Value':'old_value',
                           'Is Correct':'is_correct', 'Comment':'comment'}, inplace=True)

        df['comment'] = comments
        df['new_value'] = new_vals

        # Convert to CSV to upload to REDCap
        csvString = df.to_csv()

        ExportData().set_records(csvString, site)

