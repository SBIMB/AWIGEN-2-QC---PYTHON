import os
import requests
import pandas as pd
from io import StringIO


class ExportData:
    # Connection requirements --unique for every data set
    def __init__(self):
        self.token = os.environ.get('REDCAP_EXCEPTION_TOKEN')
        self.url = 'https://redcap.core.wits.ac.za/redcap/api/'


        # r = requests.post(self.url, self.data)
        # self.get_records()

    def set_records(self, csv_data):
        # specify the token and report id for report content
        data = {
            'token': self.token,
            'content': 'record',
            'format': 'csv',
            'type': 'flat',
            'report_id': '23219',
            'overwriteBehavior': 'normal',
            'forceAutoNumber': 'true',
            'data': csv_data,
            'returnContent': 'count',
            'returnFormat': 'json'
        }

        r = requests.post(self.url, data)
        print('HTTP Status: ' + str(r.status_code))
        print(r.text)