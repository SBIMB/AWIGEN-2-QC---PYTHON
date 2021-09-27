import ApiKeys
import requests
from RedcapApiHandler import RedcapApiHandler

import pandas as pd
from io import StringIO

import numpy as np
import xlsxwriter
from datetime import datetime

def handle_blood_results(csv):
    ### Handle Soweto H3A IDs
    # api_key = ApiKeys.GetApiKey('soweto')
    # data = {
    #         'token': api_key,
    #         'content': 'report',
    #         'format': 'csv',
    #         'report_id': 20410,
    #         'rawOrLabel': 'raw',
    #         'rawOrLabelHeaders': 'raw',
    #         'exportCheckboxLabel': 'false',
    #         'returnFormat': 'json'
    #     }
    # r = requests.post('https://redcap.core.wits.ac.za/redcap/api/', data)
    # phase_1_data = pd.read_csv(StringIO(r.text))    
    # phase_1_ids = pd.merge(df, phase_1_data, indicator=True, how='left', left_on='SID', right_on='phase_1_unique_site_id')
    # mask = df.SID.str.contains("H3A")
    # df.SID[mask] = phase_1_ids.study_id[mask]

    ### Handle Agincourt Barcodes
    # api_key = ApiKeys.GetApiKey('agincourt')
    # data = {
    #         'token': api_key,
    #         'content': 'report',
    #         'format': 'csv',
    #         'report_id': 18787,
    #         'rawOrLabel': 'raw',
    #         'rawOrLabelHeaders': 'raw',
    #         'exportCheckboxLabel': 'false',
    #         'returnFormat': 'json'
    #     }
    # r = requests.post('https://redcap.core.wits.ac.za/redcap/api/', data)
    # agincourt_barcodes = pd.read_csv(StringIO(r.text))  
    # agincourt_barcodes['aawg'] = agincourt_barcodes['bscan']
    # mask = agincourt_barcodes['bscan'].isna()
    # agincourt_barcodes['aawg'][mask] = agincourt_barcodes['barcode_confirm'][mask]
    # agincourt_barcodes['aawg'] = agincourt_barcodes['aawg'].str.upper()
    # phase_1_ids = pd.merge(df, agincourt_barcodes, indicator=True, how='outer', left_on='SID', right_on='aawg')
    # df.SID = phase_1_ids.study_id

    # # agincourt_barcodes['aawg'][16] = 'AAWG0458'
    # # agincourt_barcodes['aawg'][49] = 'AAWG0808'

    df = pd.read_excel(csv)
    df.set_index('SID',inplace=True)

    df_out = pd.DataFrame(index=df.index)
    df_out.index.rename('awigen_id', inplace=True)

    df_out['glucose_test_date'] = df['Glucose dates']
    df_out['glucose_test_date'] = pd.to_datetime(df_out['glucose_test_date'], dayfirst=True).dt.date

    df_out['glucose'] = df['Glucose mmol/l']
    df_out['glucose'] = pd.to_numeric(df_out['glucose'], errors='coerce')

    df_out['plasma_results_complete'] = 2

    df_out['serum_test_date'] = df['Lipids, Ins and creat dates']
    df_out['serum_test_date'] = pd.to_datetime(df_out['serum_test_date'], dayfirst=True).dt.date

    df_out['insulin'] = df['Insulin uIU/mL']
    # df_out['insulin'] = pd.to_numeric(df_out['insulin'], errors='coerce')

    df_out['serum_creatinine'] = df['Creatinine umol/L']
    df_out['serum_creatinine'] = pd.to_numeric(df_out['serum_creatinine'], errors='coerce')

    df_out['lipids_cholesterol'] = df['Cholesterol mmol/L']
    df_out['lipids_cholesterol'] = pd.to_numeric(df_out['lipids_cholesterol'], errors='coerce')

    df_out['lipids_triglycerides'] = df['Triglycerides mmol/L']
    df_out['lipids_triglycerides'] = pd.to_numeric(df_out['lipids_triglycerides'], errors='coerce')

    df_out['lipids_hdl'] = df['HDL-C mmol/L']
    df_out['lipids_hdl'] = pd.to_numeric(df_out['lipids_hdl'], errors='coerce')

    df_out['lipids_ldl_calculated'] = df['LDL-C calculated'].round(3)
    df_out['lipids_ldl_calculated'] = pd.to_numeric(df_out['lipids_ldl_calculated'], errors='coerce')

    df_out['lipids_ldl_measured'] = df['LDL-C measured mmol/L']
    df_out['lipids_ldl_measured'] = pd.to_numeric(df_out['lipids_ldl_measured'], errors='coerce')

    df_out['serum_results_complete'] = 2

    df_out = df_out.sort_index()

    # test_writer = pd.ExcelWriter('blood.xlsx', engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
    # df_out.to_excel(test_writer)
    # test_writer.save()

    csvString = df_out.to_csv()

    RedcapApiHandler('soweto').upload_biomarkers_to_redcap(csvString)

def handle_urine_results(csv):
    ### Handle Soweto H3A IDs
    # api_key = ApiKeys.GetApiKey('soweto')
    # data = {
    #         'token': api_key,
    #         'content': 'report',
    #         'format': 'csv',
    #         'report_id': 20410,
    #         'rawOrLabel': 'raw',
    #         'rawOrLabelHeaders': 'raw',
    #         'exportCheckboxLabel': 'false',
    #         'returnFormat': 'json'
    #     }
    # r = requests.post('https://redcap.core.wits.ac.za/redcap/api/', data)
    # phase_1_data = pd.read_csv(StringIO(r.text))
    # phase_1_ids = pd.merge(df, phase_1_data, indicator=True, how='left', left_on='PatientName', right_on='phase_1_unique_site_id')
    # mask = df.PatientName.str.contains("H3A")
    # df.PatientName[mask] = phase_1_ids.study_id[mask]

    df = pd.read_excel(csv)
    df.set_index('PatientName',inplace=True)

    ### First populate participant data
    # Set AWIGen ID
    df_out = pd.DataFrame(index=df.index.drop_duplicates(keep='first'))
    df_out.index.rename('awigen_id', inplace=True)

    # Set site ID
    site_ids = {'agincourt' : 1, 'dimamo' : 2, 'nairobi' : 3, 'nanoro' : 4, 'navrongo' : 5, 'soweto' : 6}

    site = [key for key, value in site_ids.items() if key in csv.lower()][0]
    df_out['site'] = site_ids[site]

    # Set sex
    df_out['sex'] = df.SEX.groupby([df.index]).first()
    df_out['sex'][df_out['sex'].str.lower().str.contains('f')] = 0
    df_out['sex'][df_out['sex'] != 0] = 1

    df_out['participant_data_complete'] = 2

    ### Then handle urine data
    # Set batch and box num
    df_out['urine_batch'] = df.LBLOCC.groupby([df.index]).first()
    df_out['urine_box'] = df.LBNUM.groupby([df.index]).first()

    # Set received date
    df_out['urine_date_received'] = df.ReceivedDate.groupby([df.index]).first() #TODO: Is this when SBIMB received the sample or when the lab received the sample
    df_out['urine_date_received'] = pd.to_datetime(df_out['urine_date_received']).dt.date

    #### CVW0T appears twice

    # Creatinine
    ur_creatinine = df[df['LBTEST'] == 'Urine Creatinine (CLS)']
    ur_creatinine = ur_creatinine.groupby([ur_creatinine.index]).first()
    df_out['urine_creatinine'] = pd.to_numeric(ur_creatinine.LBORRES, errors='coerce')
    # df_out['urine_creatinine_test_date'] = ur_creatinine.ResultDate    #for soweto
    df_out['urine_creatinine_test_date'] = ur_creatinine.TestedDate
    df_out['urine_creatinine_test_date'] = pd.to_datetime(df_out['urine_creatinine_test_date']).dt.date

    # Albumin
    ur_albumin = df[df['LBTEST'] == 'Urine Albumin']
    ur_albumin = ur_albumin.groupby([ur_albumin.index]).first()
    df_out['urine_albumin'] = pd.to_numeric(ur_albumin.LBORRES, errors='coerce')
    df_out['urine_albumin_test_date'] = ur_albumin.TestedDate
    df_out['urine_albumin_test_date'] = pd.to_datetime(df_out['urine_albumin_test_date']).dt.date

    # Protein
    ur_protein = df[df['LBTEST'] == 'U-Protein']
    ur_protein = ur_protein.groupby([ur_protein.index]).first()
    df_out['urine_protein'] = pd.to_numeric(ur_protein.LBORRES, errors='coerce')
    df_out['urine_protein_test_date'] = ur_protein.TestedDate
    df_out['urine_protein_test_date'] = pd.to_datetime(df_out['urine_protein_test_date']).dt.date

    df_out['urine_results_complete'] = 2

    df_out = df_out.sort_index()

    # test_writer = pd.ExcelWriter('urine.xlsx', engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
    # df_out.to_excel(test_writer)
    # test_writer.save()

    csvString = df_out.to_csv()

    RedcapApiHandler('soweto').upload_biomarkers_to_redcap(csvString)

def main():
    ### NB: Urine CSV is used to populate the participant data
    urine_csv = 'P1566_030321_agincourt_biomarkers_urine.xlsx'
    handle_urine_results(urine_csv)

    # blood_csv = 'AWI-GenIIresultsSowetoconsolidatedwithdates_edit.xlsx'
    blood_csv = 'AWI-GenIIresultsBushbuckridgeconsolidated.xlsx'
    handle_blood_results(blood_csv)

if __name__ == '__main__':
    main()
