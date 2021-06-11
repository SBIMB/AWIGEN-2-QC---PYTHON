from functools import reduce
import pandas as pd
from ExportData import ExportData


class SiteFeedbackHandler:

    @staticmethod
    def handle_outlier_feedback(csv):
        df = pd.read_excel(csv, skiprows=3, index_col='study_id')

        # We only want rows where 'Is Correct' or 'Comment/Updated Value' has been filled in
        mask = df['Is Correct'].notna() | df['Comment/Updated Value'].notna()
        df = df[mask]

        # Set all 'Is Correct' for REDCap
        df.loc[df['Is Correct'].str.lower() == 'yes','Is Correct'] = 1
        df.loc[df['Is Correct'] != 1,'Is Correct'] = 0

        # Create 'record_id' column
        df = df.reset_index()
        df.index.rename('record_id', inplace=True)

        df = df[['study_id', 'Data Field', 'Value', 'Is Correct', 'Comment/Updated Value']]

        # Get new values from the Comment column
        new_vals = pd.to_numeric(df['Comment/Updated Value'], errors='coerce')

        # Get comments with text
        comments = df['Comment/Updated Value'].where(new_vals.isna())

        # Set site ID based on the CSV name
        site_ids = {'agincourt' : 1, 'dimamo' : 2, 'nairobi' : 3, 'nanoro' : 4, 'navrongo' : 5, 'soweto' : 6}
        site = [key for key, value in site_ids.items() if key in csv.lower()][0]
        site_id = site_ids[site]
        df['site'] = site_id

        # Rename columns for REDCap
        df.rename(columns={'Data Field':'data_field', 'Value':'old_value',
                           'Is Correct':'is_correct', 'Comment/Updated Value':'comment'}, inplace=True)

        df['new_value'] = new_vals
        df['new_value'][df['new_value'] == 999] = -999
        df['comment'] = comments

        # Convert to CSV to upload to REDCap
        csvString = df.to_csv()

        ExportData().set_records(csvString, site)

        # test_writer = pd.ExcelWriter('test.xlsx', engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
        # df.to_excel(test_writer)
        # test_writer.save()

