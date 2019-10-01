import os
import requests
import pandas as pd
from io import StringIO


class ImportData:
    # Connection requirements --unique for every data set
    token = os.environ.get('REDCAP_TOKEN')
    url = 'https://redcap.core.wits.ac.za/redcap/api/'

    # specify the token and report id for report content
    data = {
        'token': token,
        'content': 'report',
        'report_id': '15960',
        'format': 'csv',
        'type': 'flat',
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }

    def get_records(self):
        r = requests.post(self.url, self.data)
        # save records in a csv file --file name: data.csv
        pd.read_csv(StringIO(r.text)).to_csv("../resources/data.csv", index=False)
        return '../resources/data.csv'
