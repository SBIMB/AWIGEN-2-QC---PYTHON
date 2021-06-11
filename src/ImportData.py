import os
import requests
import pandas as pd
from io import StringIO
import ApiKeys


class ImportData:
    # Connection requirements --unique for every data set
    def __init__(self, csv):
        report_ids = {'soweto': 22490, 'dimamo': 21889,
                      'agincourt': 22498, 'nairobi': 24168,
                      'nanoro': 1, 'navrongo': 1}

        site = [key for key, value in report_ids.items() if key in csv.lower()][0]
        api_key = ApiKeys.GetApiKey(site)
        report_id = report_ids[site]

        if site in ['soweto', 'dimamo', 'agincourt', 'nairobi']:
            self.url = 'https://redcap.core.wits.ac.za/redcap/api/'
        elif site == 'nanoro':
            self.url = 'http://awigen-nanoro.does-it.net:2280/api/'
        elif site == 'navrongo':
            self.url = 'http://awigen-navrongo.does-it.net:2280/redcap/api/'

        self.csv = csv
        # specify the token and report id for report content
        self.data = {
            'token': api_key,
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
