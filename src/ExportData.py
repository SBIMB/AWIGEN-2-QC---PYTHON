import os
import requests
import pandas as pd
from io import StringIO
import ApiKeys

class ExportData:
    def set_records(self, csv_data, site):
        report_ids = {'soweto': 23621, 'agincourt': 25317,
                      'dimamo': 25318, 'nairobi': 25813,
                      'nanoro': 28100, 'navrongo': 28735}

        api_key = ApiKeys.GetApiKey('exceptions')
        url = 'https://redcap.core.wits.ac.za/redcap/api/'

        data = {
            'token': api_key,
            'content': 'record',
            'format': 'csv',
            'type': 'flat',
            'report_id': report_ids[site],
            'overwriteBehavior': 'normal',
            'forceAutoNumber': 'true',
            'data': csv_data,
            'returnContent': 'count',
            'returnFormat': 'json'
        }

        r = requests.post(url, data)
        print('HTTP Status: ' + str(r.status_code))
        print(r.text)