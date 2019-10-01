import os
import requests
import pandas as pd
from io import StringIO


class ImportData:
    # Connection requirements --unique for every data set
    token = os.environ.get('REDCAP_TOKEN')
    url = 'https://redcap.core.wits.ac.za/redcap/api/'

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

    r = requests.post(url, data)
    # save records in a csv file
    pd.read_csv(StringIO(r.text)).to_csv("../resources/data.csv", index=False)

    def get_records(self):
        return pd.read_csv(StringIO(self.r.text))
