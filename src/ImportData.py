import os
import requests
import pandas as pd
from io import StringIO


class ImportData:

    # constructor
    # Connection requirements --unique for every data set
    def __init__(self):
        self.token = os.environ.get('REDCAP_TOKEN')
        self.url = 'https://redcap.core.wits.ac.za/redcap/api/'

        # specify the token and report id for report content
        self.data = {
            'token': '848A14BA0F69E7394EC22D7BC810BDFE',
            'content': 'report',
            'format': 'csv',
            'report_id': '15960',
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'returnFormat': 'json'
        }

    def get_records(self):
        r = requests.post(self.url, self.data)
        # save records in a csv file --file name: data.csv
        pd.read_csv(StringIO(r.text)).to_csv("../resources/data.csv", index=False)
        return '../resources/data.csv'
