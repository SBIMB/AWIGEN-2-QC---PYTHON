import os
import requests
import pandas as pd
from io import StringIO


class ImportData:
    # Connection requirements --unique for every data set
    def __init__(self, csv):
        self.token = os.environ.get('SOWETO_REDCAP_TOKEN')
        self.url = 'https://redcap.core.wits.ac.za/redcap/api/'

        report_ids = {'soweto' : 22490, 'dimamo' : 21889, 'agincourt' : 22498, 'nairobi' : 24168}    #TODO Update with other sites

        site = [key for key, value in report_ids.items() if key in csv.lower()][0]
        report_id = report_ids[site]

        self.csv = csv
        # specify the token and report id for report content
        self.data = {
            'token': self.token,
            'content': 'report',
            'format': 'csv',
            'report_id': report_id,
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'returnFormat': 'json'
        }

        self.get_records()

    def get_records(self):
        r = requests.post(self.url, self.data)
        pd.read_csv(StringIO(r.text)).to_csv(self.csv, index=False)
