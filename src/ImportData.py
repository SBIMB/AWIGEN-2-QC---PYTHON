import os
import requests
import pandas as pd
from io import StringIO


class ImportData:
    # Connection requirements --unique for every data set
    def __init__(self, csv):
        self.token = os.environ.get('SOWETO_REDCAP_TOKEN')
        self.url = 'https://redcap.core.wits.ac.za/redcap/api/'

        self.csv = csv
        # specify the token and report id for report content
        self.data = {
            'token': self.token,
            'content': 'report',
            'format': 'csv',
            'report_id': '15960',
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'returnFormat': 'json'
        }

        self.get_records()

    def get_records(self):
        r = requests.post(self.url, self.data)
        # save records in a csv file --file name: data.csv
        pd.read_csv(StringIO(r.text)).to_csv(self.csv, index=False)
