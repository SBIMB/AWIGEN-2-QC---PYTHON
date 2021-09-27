from RedcapApiHandler import RedcapApiHandler

import ApiKeys
import requests

import pandas as pd
from io import StringIO

import numpy as np
import xlsxwriter
from datetime import datetime
import csv

# REDCap Settings
#   - Report setting - Enable Missing Data Codes in place of blank values
#   - Project Setting -> Additional Customisations -> Missing Data codes -> -999, Missing

def main():
    site = 'soweto'
    fields = ['spiro_researcher']

    redcap_data = RedcapApiHandler(site).export_from_redcap()

    redcap_data.set_index('study_id',inplace=True)

    df_out = pd.DataFrame()
    df_out['redcap_event_name'] = redcap_data['redcap_event_name']

    for field in fields:
        df_out[field] = redcap_data[field]
        mask = redcap_data[field].isna()
        df_out[field][mask] = -999

        df_out[field] = df_out[field].astype(pd.Int32Dtype())

    # test_writer = pd.ExcelWriter('test.xlsx', engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
    # df_out.to_excel(test_writer)
    # test_writer.save()

    csv_data = df_out.to_csv(quoting=csv.QUOTE_NONE, sep='\t')

    RedcapApiHandler(site).upload_to_redcap(csv_data)

if __name__ == '__main__':
    main()