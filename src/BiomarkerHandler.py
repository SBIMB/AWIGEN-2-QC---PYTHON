import ApiKeys
import requests
from RedcapApiHandler import RedcapApiHandler

import pandas as pd
from io import StringIO

import numpy as np
import xlsxwriter
from datetime import datetime

def handle_ultrasound_qc_results(csv):
    df = pd.read_csv(csv, low_memory=False, sep=';', decimal=",")
    df.set_index('STUDY ID',inplace=True)

    df.index.rename('awigen_id', inplace=True)

    df.rename(columns={'DATE': "ultr_qc_date",
                       'TIME': 'ultr_qc_time',
                       'IMAGES': 'ultr_qc_num_images',
                       'COMMENT': 'ultr_qc_comment',
                       'VALID IMT': 'ultr_qc_imt_valid',
                       'VALID BIFURCATION': 'ultr_qc_bifurcation_valid',
                       'RT POINTS': 'ultr_qc_rt_points',
                       'RT T MIN': 'ultr_qc_rt_t_min',
                       'RT T MAX': 'ultr_qc_rt_t_max',
                       'RT T MEAN': 'ultr_qc_rt_t_mean',
                       'LT POINTS': 'ultr_qc_lt_points',
                       'LT T MIN': 'ultr_qc_lt_t_min',
                       'LT T MAX': 'ultr_qc_lt_t_max',
                       'LT T MEAN': 'ultr_qc_lt_t_mean',
                       'SCAT' : 'ultr_qc_scat',
                       'VAT': 'ultr_qc_vat',
                       'VISCERAL COMMENT': 'ultr_qc_visceral_comment'}, inplace=True)

    df['ultr_qc_date'] = pd.to_datetime(df['ultr_qc_date'], dayfirst=True).dt.strftime('%Y-%m-%d')
    df['ultr_qc_time'] = pd.to_datetime(df['ultr_qc_time']).dt.strftime('%H:%M')
    df['ultr_qc_num_images'] = pd.to_numeric(df['ultr_qc_num_images'], errors='coerce').astype(str).apply(lambda x: x.replace('.0','')).apply(lambda x: x.replace('nan',''))

    df['ultr_qc_imt_valid'] = df['ultr_qc_imt_valid'].str.upper()
    df['ultr_qc_imt_valid'][df['ultr_qc_imt_valid'] == 'NO'] = 0
    df['ultr_qc_imt_valid'][df['ultr_qc_imt_valid'] == 'YES'] = 1
    df['ultr_qc_imt_valid'][df['ultr_qc_imt_valid'] == 'PARTIAL'] = 2
    df['ultr_qc_imt_valid'] = pd.to_numeric(df['ultr_qc_imt_valid'], errors='coerce').astype(str).apply(lambda x: x.replace('.0','')).apply(lambda x: x.replace('nan',''))

    df['ultr_qc_bifurcation_valid'] = df['ultr_qc_bifurcation_valid'].str.upper()
    df['ultr_qc_bifurcation_valid'][df['ultr_qc_bifurcation_valid'] == 'NO'] = 0
    df['ultr_qc_bifurcation_valid'][df['ultr_qc_bifurcation_valid'] == 'YES'] = 1
    df['ultr_qc_bifurcation_valid'][df['ultr_qc_bifurcation_valid'] == 'PARTIAL'] = 2
    df['ultr_qc_bifurcation_valid'] = pd.to_numeric(df['ultr_qc_bifurcation_valid'], errors='coerce').astype(str).apply(lambda x: x.replace('.0','')).apply(lambda x: x.replace('nan',''))

    df['ultr_qc_rt_points'] = pd.to_numeric(df['ultr_qc_rt_points'], errors='coerce').astype(str).apply(lambda x: x.replace('.0','')).apply(lambda x: x.replace('nan',''))
    df['ultr_qc_rt_t_min'] = pd.to_numeric(df['ultr_qc_rt_t_min'], errors='coerce').astype(float, errors='ignore')
    df['ultr_qc_rt_t_max'] = pd.to_numeric(df['ultr_qc_rt_t_max'], errors='coerce').astype(float, errors='ignore')
    df['ultr_qc_rt_t_mean'] = pd.to_numeric(df['ultr_qc_rt_t_mean'], errors='coerce').astype(float, errors='ignore')
    df['ultr_qc_lt_points'] = pd.to_numeric(df['ultr_qc_lt_points'], errors='coerce').astype(str).apply(lambda x: x.replace('.0','')).apply(lambda x: x.replace('nan',''))
    df['ultr_qc_lt_t_min'] = pd.to_numeric(df['ultr_qc_lt_t_min'], errors='coerce').astype(float, errors='ignore')
    df['ultr_qc_lt_t_max'] = pd.to_numeric(df['ultr_qc_lt_t_max'], errors='coerce').astype(float, errors='ignore')
    df['ultr_qc_lt_t_mean'] = pd.to_numeric(df['ultr_qc_lt_t_mean'], errors='coerce').astype(float, errors='ignore')
    df['ultr_qc_scat'] = pd.to_numeric(df['ultr_qc_scat'], errors='coerce').astype(float, errors='ignore')
    df['ultr_qc_vat'] = pd.to_numeric(df['ultr_qc_vat'], errors='coerce').astype(float, errors='ignore')

    df['ultrasound_qc_results_complete'] = 2

    # test_writer = pd.ExcelWriter('ultrasound.xlsx', engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
    # df.to_excel(test_writer)
    # test_writer.save()

    csvString = df.to_csv()

    RedcapApiHandler('soweto').upload_biomarkers_to_redcap(csvString)

def handle_blood_results(csv):
    df = pd.read_csv(csv, sep=',')

    ########## Handle Soweto H3A IDs ##################
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
    ###################################################

    ########### Handle Agincourt Barcodes ##############
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

    # agincourt_barcodes.to_csv('agincourt_barcodes.csv', index=False)

    # agincourt_barcodes['aawg'] = agincourt_barcodes['bar_code_scanner']
    # agincourt_barcodes['aawg'] = agincourt_barcodes['aawg'].str.upper()

    # phase_1_ids = pd.merge(df, agincourt_barcodes, indicator=True, how='left', left_on='SID', right_on='aawg')
    # mask = df.SID.str.contains("AAWG")

    # df.SID[mask] = phase_1_ids.study_id[mask]
    ####################################################

    # Generate blood biomarker CSV for upload to REDCap
    # df = pd.read_excel(csv)
    df.set_index('SID',inplace=True)

    df_out = pd.DataFrame(index=df.index)
    df_out.index.rename('awigen_id', inplace=True)

    df_out['glucose_test_date'] = df['Glucose dates']
    df_out['glucose_test_date'] = pd.to_datetime(df_out['glucose_test_date'], dayfirst=False).dt.date

    df_out['glucose'] = df['Glucose mmol/l']
    df_out['glucose'] = pd.to_numeric(df_out['glucose'], errors='coerce')

    df_out['plasma_results_complete'] = 2
    df_out['plasma_results_complete'][df_out['glucose_test_date'].isna()] = 0

    df_out['serum_test_date'] = df['Lipids, Ins and creat dates']
    df_out['serum_test_date'] = pd.to_datetime(df_out['serum_test_date'], dayfirst=False).dt.date

    df_out['insulin'] = df['Insulin uIU/mL'].astype(str).apply(lambda x: x.replace(',','.')).apply(lambda x: x.replace('nan',''))
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
    df_out['serum_results_complete'][df_out['lipids_hdl'].isna()] = 0

    df_out = df_out.sort_index()
    # df_out.drop_duplicates(['index'], keep=False, inplace=True)
    # df_out = df_out.reset_index().drop_duplicates(subset='awigen_id', keep=False).set_index('awigen_id')

    # test_writer = pd.ExcelWriter('blood.xlsx', engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
    # df_out.to_excel(test_writer)
    # test_writer.save()

    csvString = df_out.to_csv()

    RedcapApiHandler('soweto').upload_biomarkers_to_redcap(csvString)

def handle_urine_results(csv):
    ########## Handle Soweto H3A IDs ##################
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
    ###################################################

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
    # urine_csv = 'P1566_030321_agincourt_biomarkers_urine.xlsx'
    # handle_urine_results(urine_csv)

    ultrasound_csv = './resources/Biomarkers/ultrasound_results_1.csv'
    # handle_ultrasound_qc_results(ultrasound_csv)

    blood_csv = './resources/Biomarkers/AWI-GenIIresultsNairobiconsolidatedwithdates_removed_HME0D.csv'
    handle_blood_results(blood_csv)

if __name__ == '__main__':
    main()
