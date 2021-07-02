import os
import requests
import pandas as pd
from io import StringIO
import ApiKeys

class RedcapApiHandler:
    def __init__(self, site):
        self.site = site

        if site in ['soweto', 'dimamo', 'agincourt', 'nairobi']:
            self.url = 'https://redcap.core.wits.ac.za/redcap/api/'
        elif site == 'nanoro':
            self.url = 'http://awigen-nanoro.does-it.net:2280/api/'
        elif site == 'navrongo':
            self.url = 'http://awigen-navrongo.does-it.net:2280/redcap/api/'

    def export_from_redcap(self, csv=None):
        report_ids = {'soweto': 22490, 'dimamo': 21889,
                      'agincourt': 22498, 'nairobi': 24168,
                      'nanoro': 1, 'navrongo': 1}

        api_key = ApiKeys.GetApiKey(self.site)
        report_id = report_ids[self.site]

        api_request = {
            'token': api_key,
            'content': 'report',
            'format': 'csv',
            'report_id': report_id,
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'returnFormat': 'json'
        }

        r = requests.post(self.url, api_request)
        df = pd.read_csv(StringIO(r.text), low_memory=False)
        
        if csv:
            df.to_csv(csv, index=False)

        return df

    def upload_exceptions_to_redcap(self, csv_data):
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
            'report_id': report_ids[self.site],
            'overwriteBehavior': 'normal',
            'forceAutoNumber': 'true',
            'data': csv_data,
            'returnContent': 'count',
            'returnFormat': 'json'
        }

        r = requests.post(url, data)
        print('HTTP Status: ' + str(r.status_code))
        print(r.text)

    def get_exceptions_from_redcap(self):
        report_ids = {'soweto': 23621, 'agincourt': 25317,
                      'dimamo': 25318, 'nairobi': 25813,
                      'nanoro': 28100, 'navrongo': 28735}

        site_ids = {'agincourt': 1, 'dimamo': 2, 'nairobi': 3,
                    'nanoro': 4, 'navrongo': 5, 'soweto': 6}

        report_id = report_ids[self.site]
        site_id = site_ids[self.site]

        api_key = ApiKeys.GetApiKey('exceptions')

        url = 'https://redcap.core.wits.ac.za/redcap/api/'

        # specify the token and report id for report content
        data = {
            'token': api_key,
            'content': 'report',
            'format': 'csv',
            'report_id': report_id,
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'returnFormat': 'json'
        }

        r = requests.post(url, data)

        if r.text == '\n':
            df = pd.DataFrame(columns = ['study_id','Data Field'])
            df.set_index('study_id', inplace=True)
        else:
            df = pd.read_csv(StringIO(r.text),index_col='study_id')
            df = df[df['is_correct'].notna() | df['comment'].notna() | df['new_value'].notna()]
            df = df[df['site'] == site_id]
            df = df[['data_field']]
            df.rename(columns={'data_field': 'Data Field'}, inplace=True)

        return df