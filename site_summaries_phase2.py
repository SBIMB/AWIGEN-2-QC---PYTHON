import pandas as pd
from instruments import Instruments
from encoding import Encodings
from logic import BranchingLogic

if __name__ == '__main__':

    path = './resources/'

    phase2_data = pd.read_csv(path + "/combined_phase2data.csv", delimiter=",",  low_memory=False)

    # branching logic
    #phase2_data = Encodings(phase2_data).encoding(phase2_data)

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter('site_analysis_phase2.xlsx', engine='xlsxwriter')

    # participant_identification
    participant_identification = []
    # = Instruments(phase2_data).get_participant_identification()
    participant_col = ['gene_site', 'demo_gender', 'demo_home_language', 'ethnicity']
    for col in participant_col:
        df = phase2_data.groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        participant_identification.append(df.T)

    participant_identification[0].to_excel(writer, sheet_name='Participant_identificatio')
    participant_identification[1].to_excel(writer, sheet_name='Participant_identificatio', startrow=10)
    participant_identification[2].to_excel(writer, sheet_name='Participant_identificatio', startrow=15)
    participant_identification[3].to_excel(writer, sheet_name='Participant_identificatio', startrow=53)
    #participant_identification[4].to_excel(writer, sheet_name='Participant_identificatio', startrow=80)

    # family_composition
    family_composition = []
    fam_col = Instruments(phase2_data).get_family_composition()
    for col in fam_col:
            df = BranchingLogic(phase2_data).family_composition_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
            family_composition.append(df.T)

    family_composition[0].to_excel(writer, sheet_name='family_composition')
    family_composition[1].to_excel(writer, sheet_name='family_composition', startrow=6)
    family_composition[2].to_excel(writer, sheet_name='family_composition', startrow=47)
    family_composition[3].to_excel(writer, sheet_name='family_composition', startrow=79)
    family_composition[4].to_excel(writer, sheet_name='family_composition', startrow=117)
    family_composition[5].to_excel(writer, sheet_name='family_composition', startrow=150)
    family_composition[6].to_excel(writer, sheet_name='family_composition', startrow=156)
    family_composition[7].to_excel(writer, sheet_name='family_composition', startrow=180)
    family_composition[8].to_excel(writer, sheet_name='family_composition', startrow=203)
    family_composition[9].to_excel(writer, sheet_name='family_composition', startrow=224)

    #Pregnancy and Menopause
    pregnancy_menopause = []
    preg_col = Instruments(phase2_data).get_pregnancy_and_menopause()
    for col in preg_col:
        df = BranchingLogic(phase2_data).pregnancy_and_menopause_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        pregnancy_menopause.append(df.T)

    pregnancy_menopause[0].to_excel(writer, sheet_name='pregnancy_menopause')
    pregnancy_menopause[1].to_excel(writer, sheet_name='pregnancy_menopause', startrow=6)
    pregnancy_menopause[2].to_excel(writer, sheet_name='pregnancy_menopause', startrow=29)
    pregnancy_menopause[3].to_excel(writer, sheet_name='pregnancy_menopause', startrow=50)
    pregnancy_menopause[4].to_excel(writer, sheet_name='pregnancy_menopause', startrow=58)
    pregnancy_menopause[5].to_excel(writer, sheet_name='pregnancy_menopause', startrow=67)
    pregnancy_menopause[6].to_excel(writer, sheet_name='pregnancy_menopause', startrow=74)
    pregnancy_menopause[7].to_excel(writer, sheet_name='pregnancy_menopause', startrow=81)
    pregnancy_menopause[8].to_excel(writer, sheet_name='pregnancy_menopause', startrow=96)
    pregnancy_menopause[9].to_excel(writer, sheet_name='pregnancy_menopause', startrow=138)

    #civil_status
    civil_status = []
    civil_col = Instruments(phase2_data).get_civil_status_marital_status_education_employment()
    for col in civil_col:
        df = BranchingLogic(phase2_data).civil_status_marital_status_education_employment_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        civil_status.append(df.T)

    civil_status[0].to_excel(writer, sheet_name='civil_status')
    civil_status[1].to_excel(writer, sheet_name='civil_status', startrow=10)
    civil_status[2].to_excel(writer, sheet_name='civil_status', startrow=18)
    civil_status[3].to_excel(writer, sheet_name='civil_status', startrow=41)
    civil_status[4].to_excel(writer, sheet_name='civil_status', startrow=78)
    civil_status[5].to_excel(writer, sheet_name='civil_status', startrow=87)
    civil_status[6].to_excel(writer, sheet_name='civil_status', startrow=98)

    # a_cognition_one
    a_cognition_one = []
    a_cogn_col = Instruments(phase2_data).get_a_cognition_one()
    for col in a_cogn_col:
        df = phase2_data.groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        a_cognition_one.append(df.T)

    a_cognition_one[0].to_excel(writer, sheet_name='cognition_one')
    a_cognition_one[1].to_excel(writer, sheet_name='cognition_one', startrow=7)
    a_cognition_one[2].to_excel(writer, sheet_name='cognition_one', startrow=16)
    a_cognition_one[3].to_excel(writer, sheet_name='cognition_one', startrow=26)
    a_cognition_one[4].to_excel(writer, sheet_name='cognition_one', startrow=34)
    a_cognition_one[5].to_excel(writer, sheet_name='cognition_one', startrow=43)
    a_cognition_one[6].to_excel(writer, sheet_name='cognition_one', startrow=47)
    a_cognition_one[7].to_excel(writer, sheet_name='cognition_one', startrow=51)
    a_cognition_one[8].to_excel(writer, sheet_name='cognition_one', startrow=55)
    a_cognition_one[9].to_excel(writer, sheet_name='cognition_one', startrow=59)
    a_cognition_one[10].to_excel(writer, sheet_name='cognition_one', startrow=63)
    a_cognition_one[11].to_excel(writer, sheet_name='cognition_one', startrow=67)
    a_cognition_one[12].to_excel(writer, sheet_name='cognition_one', startrow=71)
    a_cognition_one[13].to_excel(writer, sheet_name='cognition_one', startrow=75)
    a_cognition_one[14].to_excel(writer, sheet_name='cognition_one', startrow=79)
    a_cognition_one[15].to_excel(writer, sheet_name='cognition_one', startrow=83)
    a_cognition_one[16].to_excel(writer, sheet_name='cognition_one', startrow=87)
    a_cognition_one[17].to_excel(writer, sheet_name='cognition_one', startrow=92)
    a_cognition_one[18].to_excel(writer, sheet_name='cognition_one', startrow=106)
    a_cognition_one[19].to_excel(writer, sheet_name='cognition_one', startrow=111)
    a_cognition_one[20].to_excel(writer, sheet_name='cognition_one', startrow=117)
    a_cognition_one[21].to_excel(writer, sheet_name='cognition_one', startrow=122)
    a_cognition_one[22].to_excel(writer, sheet_name='cognition_one', startrow=127)
    a_cognition_one[23].to_excel(writer, sheet_name='cognition_one', startrow=132)
    a_cognition_one[24].to_excel(writer, sheet_name='cognition_one', startrow=137)
    a_cognition_one[25].to_excel(writer, sheet_name='cognition_one', startrow=142)

    # b_cognition_one
    c_cognition_two = []
    c_cogn_col = Instruments(phase2_data).get_c_cognition_two()

    for col in c_cogn_col:
        df = phase2_data.groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        c_cognition_two.append(df.T)

    c_cognition_two[0].to_excel(writer, sheet_name='cognition_two')
    c_cognition_two[1].to_excel(writer, sheet_name='cognition_two', startrow=4)
    c_cognition_two[2].to_excel(writer, sheet_name='cognition_two', startrow=8)
    c_cognition_two[3].to_excel(writer, sheet_name='cognition_two', startrow=12)
    c_cognition_two[4].to_excel(writer, sheet_name='cognition_two', startrow=16)
    c_cognition_two[5].to_excel(writer, sheet_name='cognition_two', startrow=20)
    c_cognition_two[6].to_excel(writer, sheet_name='cognition_two', startrow=24)
    c_cognition_two[7].to_excel(writer, sheet_name='cognition_two', startrow=28)
    c_cognition_two[8].to_excel(writer, sheet_name='cognition_two', startrow=32)
    c_cognition_two[9].to_excel(writer, sheet_name='cognition_two', startrow=48)
    c_cognition_two[10].to_excel(writer, sheet_name='cognition_two', startrow=52)
    c_cognition_two[11].to_excel(writer, sheet_name='cognition_two', startrow=56)
    c_cognition_two[12].to_excel(writer, sheet_name='cognition_two', startrow=60)
    c_cognition_two[13].to_excel(writer, sheet_name='cognition_two', startrow=74)
    c_cognition_two[14].to_excel(writer, sheet_name='cognition_two', startrow=78)
    c_cognition_two[15].to_excel(writer, sheet_name='cognition_two', startrow=82)
    c_cognition_two[16].to_excel(writer, sheet_name='cognition_two', startrow=86)
    c_cognition_two[17].to_excel(writer, sheet_name='cognition_two', startrow=90)
    c_cognition_two[18].to_excel(writer, sheet_name='cognition_two', startrow=94)
    c_cognition_two[19].to_excel(writer, sheet_name='cognition_two', startrow=98)
    c_cognition_two[20].to_excel(writer, sheet_name='cognition_two', startrow=102)
    c_cognition_two[21].to_excel(writer, sheet_name='cognition_two', startrow=106)
    c_cognition_two[22].to_excel(writer, sheet_name='cognition_two', startrow=110)
    c_cognition_two[23].to_excel(writer, sheet_name='cognition_two', startrow=114)
    c_cognition_two[24].to_excel(writer, sheet_name='cognition_two', startrow=118)
    c_cognition_two[25].to_excel(writer, sheet_name='cognition_two', startrow=122)
    c_cognition_two[26].to_excel(writer, sheet_name='cognition_two', startrow=126)
    c_cognition_two[27].to_excel(writer, sheet_name='cognition_two', startrow=130)
    c_cognition_two[28].to_excel(writer, sheet_name='cognition_two', startrow=134)
    c_cognition_two[29].to_excel(writer, sheet_name='cognition_two', startrow=138)
    c_cognition_two[30].to_excel(writer, sheet_name='cognition_two', startrow=142)
    c_cognition_two[31].to_excel(writer, sheet_name='cognition_two', startrow=146)

    # b_frailty_measurements
    b_frailty_measurements = []
    frai_col = ['frai_use_hands', 'frai_sit_stands_completed', 'frai_non_dominant_hand',
                'frai_complete_procedure', 'frai_need_support', 'frai_procedure_walk_comp']
    for col in frai_col:
        df = phase2_data.groupby('gene_site')[col].value_counts(dropna = False).to_frame().unstack()
        b_frailty_measurements.append(df.T)

    b_frailty_measurements[0].to_excel(writer, sheet_name='frailty')
    b_frailty_measurements[1].to_excel(writer, sheet_name='frailty', startrow=5)
    b_frailty_measurements[2].to_excel(writer, sheet_name='frailty', startrow=11)
    b_frailty_measurements[3].to_excel(writer, sheet_name='frailty', startrow=16)
    b_frailty_measurements[4].to_excel(writer, sheet_name='frailty', startrow=21)
    b_frailty_measurements[5].to_excel(writer, sheet_name='frailty', startrow=27)

    # household_attributes
    household_attributes = []
    hous_col = Instruments(phase2_data).get_household_attributes()

    for col in hous_col:
        df = BranchingLogic(phase2_data).household_attributes_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        household_attributes.append(df.T)

    household_attributes[0].to_excel(writer, sheet_name='household_attributes')
    household_attributes[1].to_excel(writer, sheet_name='household_attributes', startrow=54)
    household_attributes[2].to_excel(writer, sheet_name='household_attributes', startrow=93)
    household_attributes[13].to_excel(writer, sheet_name='household_attributes', startrow=133)
    household_attributes[14].to_excel(writer, sheet_name='household_attributes', startrow=141)
    household_attributes[15].to_excel(writer, sheet_name='household_attributes', startrow=148)
    household_attributes[16].to_excel(writer, sheet_name='household_attributes', startrow=153)
    household_attributes[17].to_excel(writer, sheet_name='household_attributes', startrow=160)
    household_attributes[18].to_excel(writer, sheet_name='household_attributes', startrow=167)
    household_attributes[19].to_excel(writer, sheet_name='household_attributes', startrow=174)
    household_attributes[20].to_excel(writer, sheet_name='household_attributes', startrow=181)
    household_attributes[21].to_excel(writer, sheet_name='household_attributes', startrow=188)
    household_attributes[22].to_excel(writer, sheet_name='household_attributes', startrow=195)
    household_attributes[23].to_excel(writer, sheet_name='household_attributes', startrow=202)
    household_attributes[24].to_excel(writer, sheet_name='household_attributes', startrow=208)
    household_attributes[25].to_excel(writer, sheet_name='household_attributes', startrow=213)
    household_attributes[27].to_excel(writer, sheet_name='household_attributes', startrow=220)
    household_attributes[28].to_excel(writer, sheet_name='household_attributes', startrow=227)
    household_attributes[29].to_excel(writer, sheet_name='household_attributes', startrow=234)
    household_attributes[30].to_excel(writer, sheet_name='household_attributes', startrow=241)
    household_attributes[31].to_excel(writer, sheet_name='household_attributes', startrow=248)
    household_attributes[32].to_excel(writer, sheet_name='household_attributes', startrow=255)
    household_attributes[33].to_excel(writer, sheet_name='household_attributes', startrow=262)
    household_attributes[34].to_excel(writer, sheet_name='household_attributes', startrow=269)
    household_attributes[35].to_excel(writer, sheet_name='household_attributes', startrow=275)
    household_attributes[36].to_excel(writer, sheet_name='household_attributes', startrow=280)
    household_attributes[37].to_excel(writer, sheet_name='household_attributes', startrow=287)
    household_attributes[38].to_excel(writer, sheet_name='household_attributes', startrow=294)
    household_attributes[39].to_excel(writer, sheet_name='household_attributes', startrow=311)
    household_attributes[40].to_excel(writer, sheet_name='household_attributes', startrow=318)
    household_attributes[41].to_excel(writer, sheet_name='household_attributes', startrow=325)
    household_attributes[42].to_excel(writer, sheet_name='household_attributes', startrow=332)
    household_attributes[3].to_excel(writer, sheet_name='household_attributes', startrow=343)
    household_attributes[4].to_excel(writer, sheet_name='household_attributes', startrow=350)
    household_attributes[5].to_excel(writer, sheet_name='household_attributes', startrow=357)
    household_attributes[6].to_excel(writer, sheet_name='household_attributes', startrow=364)
    household_attributes[7].to_excel(writer, sheet_name='household_attributes', startrow=372)
    household_attributes[8].to_excel(writer, sheet_name='household_attributes', startrow=379)
    household_attributes[9].to_excel(writer, sheet_name='household_attributes', startrow=386)
    household_attributes[10].to_excel(writer, sheet_name='household_attributes', startrow=393)
    household_attributes[11].to_excel(writer, sheet_name='household_attributes', startrow=400)
    household_attributes[12].to_excel(writer, sheet_name='household_attributes', startrow=407)
    household_attributes[26].to_excel(writer, sheet_name='household_attributes', startrow=414)
    #substance_use
    substance_use = []
    sub_col = Instruments(phase2_data).get_substance_use()
    for col in sub_col:
        df = BranchingLogic(phase2_data).substance_use_logic().groupby('gene_site')[col].value_counts(dropna = False).to_frame().unstack()
        substance_use.append(df.T)

    substance_use[0].to_excel(writer, sheet_name='substance_use')
    substance_use[1].to_excel(writer, sheet_name='substance_use', startrow=7)
    substance_use[2].to_excel(writer, sheet_name='substance_use', startrow =14)
    substance_use[3].to_excel(writer, sheet_name='substance_use', startrow =21)
    substance_use[4].to_excel(writer, sheet_name='substance_use', startrow =28)
    substance_use[5].to_excel(writer, sheet_name='substance_use', startrow =35)
    substance_use[6].to_excel(writer, sheet_name='substance_use', startrow =42)
    substance_use[7].to_excel(writer, sheet_name='substance_use', startrow =49)
    substance_use[8].to_excel(writer, sheet_name='substance_use', startrow =56)
    substance_use[9].to_excel(writer, sheet_name='substance_use', startrow =63)
    substance_use[10].to_excel(writer, sheet_name='substance_use', startrow =70)
    substance_use[11].to_excel(writer, sheet_name='substance_use', startrow =77)
    substance_use[12].to_excel(writer, sheet_name='substance_use', startrow =88)
    substance_use[13].to_excel(writer, sheet_name='substance_use', startrow=97)
    substance_use[21].to_excel(writer, sheet_name='substance_use', startrow=106)
    substance_use[16].to_excel(writer, sheet_name='substance_use', startrow=114)
    substance_use[17].to_excel(writer, sheet_name='substance_use', startrow=121)
    substance_use[18].to_excel(writer, sheet_name='substance_use', startrow=128)
    substance_use[19].to_excel(writer, sheet_name='substance_use', startrow=135)
    substance_use[20].to_excel(writer, sheet_name='substance_use', startrow=144)
    substance_use[22].to_excel(writer, sheet_name='substance_use', startrow=154)
    substance_use[23].to_excel(writer, sheet_name='substance_use', startrow=163)
    substance_use[24].to_excel(writer, sheet_name='substance_use', startrow=170)
    substance_use[25].to_excel(writer, sheet_name='substance_use', startrow=176)
    substance_use[26].to_excel(writer, sheet_name='substance_use', startrow=183)
    substance_use[28].to_excel(writer, sheet_name='substance_use', startrow=194)
    substance_use[29].to_excel(writer, sheet_name='substance_use', startrow=201)
    substance_use[30].to_excel(writer, sheet_name='substance_use', startrow=210)
    substance_use[31].to_excel(writer, sheet_name='substance_use', startrow=217)
    substance_use[14].to_excel(writer, sheet_name='substance_use', startrow=225)
    substance_use[15].to_excel(writer, sheet_name='substance_use', startrow=281)
    substance_use[27].to_excel(writer, sheet_name='substance_use', startrow=350)
    substance_use[32].to_excel(writer, sheet_name='substance_use', startrow=369)
    substance_use[33].to_excel(writer, sheet_name='substance_use', startrow=377)
    substance_use[34].to_excel(writer, sheet_name='substance_use', startrow=384)
    substance_use[35].to_excel(writer, sheet_name='substance_use', startrow=391)
    substance_use[36].to_excel(writer, sheet_name='substance_use', startrow=397)
    substance_use[37].to_excel(writer, sheet_name='substance_use', startrow=404)
    substance_use[38].to_excel(writer, sheet_name='substance_use', startrow=411)
    #substance_use[39].to_excel(writer, sheet_name='substance_use', startrow=418)
    substance_use[40].to_excel(writer, sheet_name='substance_use', startrow=425)
    substance_use[41].to_excel(writer, sheet_name='substance_use', startrow=437)
    #substance_use[42].to_excel(writer, sheet_name='substance_use', startrow=444)

    #a_general_health_cancer
    a_general_health_cancer = []
    a_general_col = Instruments(phase2_data).get_a_general_health_cancer()

    for col in a_general_col:
        df = BranchingLogic(phase2_data).a_general_health_cancer_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        a_general_health_cancer.append(df.T)

    a_general_health_cancer[0].to_excel(writer, sheet_name='a_general_health_cancer')
    a_general_health_cancer[1].to_excel(writer, sheet_name='a_general_health_cancer', startrow=7)
    a_general_health_cancer[2].to_excel(writer, sheet_name='a_general_health_cancer', startrow=15)
    a_general_health_cancer[3].to_excel(writer, sheet_name='a_general_health_cancer', startrow=21)
    a_general_health_cancer[4].to_excel(writer, sheet_name='a_general_health_cancer', startrow=28)
    a_general_health_cancer[5].to_excel(writer, sheet_name='a_general_health_cancer', startrow=35)
    a_general_health_cancer[6].to_excel(writer, sheet_name='a_general_health_cancer', startrow=42)
    a_general_health_cancer[7].to_excel(writer, sheet_name='a_general_health_cancer', startrow=49)
    a_general_health_cancer[8].to_excel(writer, sheet_name='a_general_health_cancer', startrow=56)
    a_general_health_cancer[9].to_excel(writer, sheet_name='a_general_health_cancer', startrow=63)
    a_general_health_cancer[10].to_excel(writer, sheet_name='a_general_health_cancer', startrow=70)
    a_general_health_cancer[11].to_excel(writer, sheet_name='a_general_health_cancer', startrow=77)
    a_general_health_cancer[12].to_excel(writer, sheet_name='a_general_health_cancer', startrow=84)
    a_general_health_cancer[13].to_excel(writer, sheet_name='a_general_health_cancer', startrow=90)
    a_general_health_cancer[14].to_excel(writer, sheet_name='a_general_health_cancer', startrow=103)
    a_general_health_cancer[15].to_excel(writer, sheet_name='a_general_health_cancer', startrow=110)
    a_general_health_cancer[16].to_excel(writer, sheet_name='a_general_health_cancer', startrow=117)
    a_general_health_cancer[17].to_excel(writer, sheet_name='a_general_health_cancer', startrow=124)
    a_general_health_cancer[18].to_excel(writer, sheet_name='a_general_health_cancer', startrow=131)
    a_general_health_cancer[19].to_excel(writer, sheet_name='a_general_health_cancer', startrow=138)
    a_general_health_cancer[20].to_excel(writer, sheet_name='a_general_health_cancer', startrow=145)
    a_general_health_cancer[21].to_excel(writer, sheet_name='a_general_health_cancer', startrow=152)
    a_general_health_cancer[22].to_excel(writer, sheet_name='a_general_health_cancer', startrow=159)
    a_general_health_cancer[23].to_excel(writer, sheet_name='a_general_health_cancer', startrow=166)
    a_general_health_cancer[24].to_excel(writer, sheet_name='a_general_health_cancer', startrow=190)

    # b_general_health_family_history
    b_general_health_family_history = []
    b_general_health = Instruments(phase2_data).get_b_general_health_family_history()

    for col in b_general_health:
        df = phase2_data.groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        b_general_health_family_history.append(df.T)

    b_general_health_family_history[0].to_excel(writer, sheet_name='b_general_health_family_history')
    b_general_health_family_history[1].to_excel(writer, sheet_name='b_general_health_family_history', startrow=7)
    b_general_health_family_history[2].to_excel(writer, sheet_name='b_general_health_family_history', startrow=15)
    b_general_health_family_history[3].to_excel(writer, sheet_name='b_general_health_family_history', startrow=21)
    b_general_health_family_history[4].to_excel(writer, sheet_name='b_general_health_family_history', startrow=28)
    b_general_health_family_history[5].to_excel(writer, sheet_name='b_general_health_family_history', startrow=35)
    b_general_health_family_history[6].to_excel(writer, sheet_name='b_general_health_family_history', startrow=42)
    b_general_health_family_history[7].to_excel(writer, sheet_name='b_general_health_family_history', startrow=49)
    b_general_health_family_history[8].to_excel(writer, sheet_name='b_general_health_family_history', startrow=56)
    b_general_health_family_history[9].to_excel(writer, sheet_name='b_general_health_family_history', startrow=63)
    b_general_health_family_history[10].to_excel(writer, sheet_name='b_general_health_family_history', startrow=70)
    b_general_health_family_history[11].to_excel(writer, sheet_name='b_general_health_family_history', startrow=77)
    b_general_health_family_history[12].to_excel(writer, sheet_name='b_general_health_family_history', startrow=84)
    b_general_health_family_history[11].to_excel(writer, sheet_name='b_general_health_family_history', startrow=91)
    b_general_health_family_history[12].to_excel(writer, sheet_name='b_general_health_family_history', startrow=98)


    #c_general_health_diet)
    c_general_health_diet = []
    c_general_health = Instruments(phase2_data).get_c_general_health_diet()

    for col in c_general_health:
        df = BranchingLogic(phase2_data).c_general_health_diet_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        c_general_health_diet.append(df.T)

    c_general_health_diet[0].to_excel(writer, sheet_name = 'c_general_health_diet')
    c_general_health_diet[1].to_excel(writer, sheet_name='c_general_health_diet', startrow=13)
    c_general_health_diet[2].to_excel(writer, sheet_name='c_general_health_diet', startrow=36)
    c_general_health_diet[3].to_excel(writer, sheet_name='c_general_health_diet', startrow=49)
    c_general_health_diet[4].to_excel(writer, sheet_name='c_general_health_diet', startrow=80)
    c_general_health_diet[5].to_excel(writer, sheet_name='c_general_health_diet', startrow=87)
    c_general_health_diet[6].to_excel(writer, sheet_name='c_general_health_diet', startrow=94)
    c_general_health_diet[7].to_excel(writer, sheet_name='c_general_health_diet', startrow=103)
    c_general_health_diet[8].to_excel(writer, sheet_name='c_general_health_diet', startrow=110)
    c_general_health_diet[9].to_excel(writer, sheet_name='c_general_health_diet', startrow=117)
    c_general_health_diet[10].to_excel(writer, sheet_name='c_general_health_diet', startrow=124)
    c_general_health_diet[11].to_excel(writer, sheet_name='c_general_health_diet', startrow=133)
    c_general_health_diet[12].to_excel(writer, sheet_name='c_general_health_diet', startrow=140)
    c_general_health_diet[13].to_excel(writer, sheet_name='c_general_health_diet', startrow=147)
    c_general_health_diet[14].to_excel(writer, sheet_name='c_general_health_diet', startrow=154)
    c_general_health_diet[15].to_excel(writer, sheet_name='c_general_health_diet', startrow=163)
    c_general_health_diet[16].to_excel(writer, sheet_name='c_general_health_diet', startrow=170)
    c_general_health_diet[17].to_excel(writer, sheet_name='c_general_health_diet', startrow=175)
    c_general_health_diet[18].to_excel(writer, sheet_name='c_general_health_diet', startrow=181)
    c_general_health_diet[19].to_excel(writer, sheet_name='c_general_health_diet', startrow=193)
    c_general_health_diet[21].to_excel(writer, sheet_name='c_general_health_diet', startrow=222)
    c_general_health_diet[22].to_excel(writer, sheet_name='c_general_health_diet', startrow=265)
    c_general_health_diet[23].to_excel(writer, sheet_name='c_general_health_diet', startrow=290)
    c_general_health_diet[24].to_excel(writer, sheet_name='c_general_health_diet', startrow=297)
    c_general_health_diet[22].to_excel(writer, sheet_name='c_general_health_diet', startrow=305)
    c_general_health_diet[23].to_excel(writer, sheet_name='c_general_health_diet', startrow=330)
    c_general_health_diet[24].to_excel(writer, sheet_name='c_general_health_diet', startrow=337)
    c_general_health_diet[20].to_excel(writer, sheet_name='c_general_health_diet', startrow=344)

    #d_general_health
    d_general_health_exposure_to_pesticides_pollutants = []
    d_general_health = Instruments(phase2_data).get_d_general_health_exposure_to_pesticides_pollutants()

    for col in d_general_health:
        df = BranchingLogic(phase2_data).d_general_health_exposure_to_pesticides_pollutants_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        d_general_health_exposure_to_pesticides_pollutants.append(df.T)

    d_general_health_exposure_to_pesticides_pollutants[0].to_excel(writer, sheet_name='d_general_health_exposure')
    d_general_health_exposure_to_pesticides_pollutants[1].to_excel(writer, sheet_name='d_general_health_exposure', startrow=7)
    d_general_health_exposure_to_pesticides_pollutants[2].to_excel(writer, sheet_name='d_general_health_exposure', startrow=50)
    d_general_health_exposure_to_pesticides_pollutants[3].to_excel(writer, sheet_name='d_general_health_exposure', startrow=57)
    d_general_health_exposure_to_pesticides_pollutants[4].to_excel(writer, sheet_name='d_general_health_exposure', startrow=190)
    d_general_health_exposure_to_pesticides_pollutants[6].to_excel(writer, sheet_name='d_general_health_exposure', startrow=64)
    d_general_health_exposure_to_pesticides_pollutants[7].to_excel(writer, sheet_name='d_general_health_exposure', startrow=71)
    d_general_health_exposure_to_pesticides_pollutants[8].to_excel(writer, sheet_name='d_general_health_exposure', startrow=78)
    d_general_health_exposure_to_pesticides_pollutants[5].to_excel(writer, sheet_name='d_general_health_exposure', startrow=85)
    d_general_health_exposure_to_pesticides_pollutants[9].to_excel(writer, sheet_name='d_general_health_exposure', startrow=92)
    d_general_health_exposure_to_pesticides_pollutants[10].to_excel(writer, sheet_name='d_general_health_exposure', startrow=99)
    d_general_health_exposure_to_pesticides_pollutants[11].to_excel(writer, sheet_name='d_general_health_exposure', startrow=106)
    d_general_health_exposure_to_pesticides_pollutants[12].to_excel(writer, sheet_name='d_general_health_exposure', startrow=113)
    d_general_health_exposure_to_pesticides_pollutants[13].to_excel(writer, sheet_name='d_general_health_exposure', startrow=120)
    d_general_health_exposure_to_pesticides_pollutants[14].to_excel(writer, sheet_name='d_general_health_exposure', startrow=127)
    d_general_health_exposure_to_pesticides_pollutants[16].to_excel(writer, sheet_name='d_general_health_exposure', startrow=134)
    d_general_health_exposure_to_pesticides_pollutants[17].to_excel(writer, sheet_name='d_general_health_exposure', startrow=141)
    d_general_health_exposure_to_pesticides_pollutants[18].to_excel(writer, sheet_name='d_general_health_exposure', startrow=148)
    d_general_health_exposure_to_pesticides_pollutants[15].to_excel(writer, sheet_name='d_general_health_exposure', startrow=155)

    # infection_history
    infection_history = []
    infection_history_col = Instruments(phase2_data).get_infection_history()

    for col in infection_history_col:
        df = BranchingLogic(phase2_data).infection_history_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        infection_history.append(df.T)

    infection_history[0].to_excel(writer, sheet_name='infection_history')
    infection_history[1].to_excel(writer, sheet_name='infection_history', startrow=7)
    infection_history[2].to_excel(writer, sheet_name='infection_history', startrow=14)
    infection_history[3].to_excel(writer, sheet_name='infection_history', startrow=21)
    infection_history[4].to_excel(writer, sheet_name='infection_history', startrow=28)
    infection_history[6].to_excel(writer, sheet_name='infection_history', startrow=42)
    infection_history[7].to_excel(writer, sheet_name='infection_history', startrow=49)
    infection_history[8].to_excel(writer, sheet_name='infection_history', startrow=56)
    infection_history[9].to_excel(writer, sheet_name='infection_history', startrow=63)
    infection_history[10].to_excel(writer, sheet_name='infection_history', startrow=70)
    infection_history[11].to_excel(writer, sheet_name='infection_history', startrow=77)
    infection_history[12].to_excel(writer, sheet_name='infection_history', startrow=84)
    infection_history[13].to_excel(writer, sheet_name='infection_history', startrow=91)
    infection_history[15].to_excel(writer, sheet_name='infection_history', startrow=98)
    #infection_history[14].to_excel(writer, sheet_name='infection_history', startrow=115)
    #infection_history[17].to_excel(writer, sheet_name='infection_history', startrow=122)
    infection_history[18].to_excel(writer, sheet_name='infection_history', startrow=128)
    #infection_history[19].to_excel(writer, sheet_name='infection_history', startrow=135)
    infection_history[20].to_excel(writer, sheet_name='infection_history', startrow=142)
    infection_history[21].to_excel(writer, sheet_name='infection_history', startrow=149)
    infection_history[22].to_excel(writer, sheet_name='infection_history', startrow=156)
    infection_history[23].to_excel(writer, sheet_name='infection_history', startrow=173)
    infection_history[24].to_excel(writer, sheet_name='infection_history', startrow=180)
    infection_history[25].to_excel(writer, sheet_name='infection_history', startrow=187)
    infection_history[26].to_excel(writer, sheet_name='infection_history', startrow=194)
    infection_history[27].to_excel(writer, sheet_name='infection_history', startrow=203)
    infection_history[28].to_excel(writer, sheet_name='infection_history', startrow=210)
    infection_history[29].to_excel(writer, sheet_name='infection_history', startrow=217)
    infection_history[30].to_excel(writer, sheet_name='infection_history', startrow=224)
    infection_history[31].to_excel(writer, sheet_name='infection_history', startrow=233)
    infection_history[32].to_excel(writer, sheet_name='infection_history', startrow=240)
    infection_history[16].to_excel(writer, sheet_name='infection_history', startrow=247)
    infection_history[5].to_excel(writer, sheet_name='infection_history', startrow=600)

    # a_cardiometabolic_risk_factors_diabetes
    a_cardiometabolic_risk_factors_diabetes = []
    a_cardiometabolic = Instruments(phase2_data).get_a_cardiometabolic_risk_factors_diabetes()

    for col in a_cardiometabolic:
        df = BranchingLogic(phase2_data).a_cardiometabolic_risk_factors_diabetes_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        a_cardiometabolic_risk_factors_diabetes.append(df.T)

    a_cardiometabolic_risk_factors_diabetes[0].to_excel(writer, sheet_name='a_cardiometabolic')
    a_cardiometabolic_risk_factors_diabetes[1].to_excel(writer, sheet_name='a_cardiometabolic', startrow=7)
    a_cardiometabolic_risk_factors_diabetes[2].to_excel(writer, sheet_name='a_cardiometabolic', startrow=14)
    a_cardiometabolic_risk_factors_diabetes[3].to_excel(writer, sheet_name='a_cardiometabolic', startrow=21)
    a_cardiometabolic_risk_factors_diabetes[4].to_excel(writer, sheet_name='a_cardiometabolic', startrow=28)
    a_cardiometabolic_risk_factors_diabetes[5].to_excel(writer, sheet_name='a_cardiometabolic', startrow=35)
    a_cardiometabolic_risk_factors_diabetes[6].to_excel(writer, sheet_name='a_cardiometabolic', startrow=42)
    a_cardiometabolic_risk_factors_diabetes[7].to_excel(writer, sheet_name='a_cardiometabolic', startrow=49)
    a_cardiometabolic_risk_factors_diabetes[8].to_excel(writer, sheet_name='a_cardiometabolic', startrow=56)
    a_cardiometabolic_risk_factors_diabetes[9].to_excel(writer, sheet_name='a_cardiometabolic', startrow=63)
    a_cardiometabolic_risk_factors_diabetes[10].to_excel(writer, sheet_name='a_cardiometabolic', startrow=70)
    a_cardiometabolic_risk_factors_diabetes[11].to_excel(writer, sheet_name='a_cardiometabolic', startrow=77)
    a_cardiometabolic_risk_factors_diabetes[13].to_excel(writer, sheet_name='a_cardiometabolic', startrow=91)
    a_cardiometabolic_risk_factors_diabetes[14].to_excel(writer, sheet_name='a_cardiometabolic', startrow=98)
    a_cardiometabolic_risk_factors_diabetes[15].to_excel(writer, sheet_name='a_cardiometabolic', startrow=105)
    a_cardiometabolic_risk_factors_diabetes[16].to_excel(writer, sheet_name='a_cardiometabolic', startrow=112)
    a_cardiometabolic_risk_factors_diabetes[17].to_excel(writer, sheet_name='a_cardiometabolic', startrow=119)
    a_cardiometabolic_risk_factors_diabetes[18].to_excel(writer, sheet_name='a_cardiometabolic', startrow=126)
    a_cardiometabolic_risk_factors_diabetes[19].to_excel(writer, sheet_name='a_cardiometabolic', startrow=133)
    a_cardiometabolic_risk_factors_diabetes[20].to_excel(writer, sheet_name='a_cardiometabolic', startrow=140)
    a_cardiometabolic_risk_factors_diabetes[21].to_excel(writer, sheet_name='a_cardiometabolic', startrow=147)
    a_cardiometabolic_risk_factors_diabetes[22].to_excel(writer, sheet_name='a_cardiometabolic', startrow=153)
    a_cardiometabolic_risk_factors_diabetes[23].to_excel(writer, sheet_name='a_cardiometabolic', startrow=160)
    a_cardiometabolic_risk_factors_diabetes[24].to_excel(writer, sheet_name='a_cardiometabolic', startrow=177)
    a_cardiometabolic_risk_factors_diabetes[25].to_excel(writer, sheet_name='a_cardiometabolic', startrow=183)
    a_cardiometabolic_risk_factors_diabetes[26].to_excel(writer, sheet_name='a_cardiometabolic', startrow=190)
    a_cardiometabolic_risk_factors_diabetes[27].to_excel(writer, sheet_name='a_cardiometabolic', startrow=197)
    a_cardiometabolic_risk_factors_diabetes[28].to_excel(writer, sheet_name='a_cardiometabolic', startrow=203)
    a_cardiometabolic_risk_factors_diabetes[29].to_excel(writer, sheet_name='a_cardiometabolic', startrow=210)
    a_cardiometabolic_risk_factors_diabetes[30].to_excel(writer, sheet_name='a_cardiometabolic', startrow=217)
    a_cardiometabolic_risk_factors_diabetes[31].to_excel(writer, sheet_name='a_cardiometabolic', startrow=223)
    a_cardiometabolic_risk_factors_diabetes[32].to_excel(writer, sheet_name='a_cardiometabolic', startrow=230)
    a_cardiometabolic_risk_factors_diabetes[33].to_excel(writer, sheet_name='a_cardiometabolic', startrow=237)
    a_cardiometabolic_risk_factors_diabetes[34].to_excel(writer, sheet_name='a_cardiometabolic', startrow=243)
    a_cardiometabolic_risk_factors_diabetes[12].to_excel(writer, sheet_name='a_cardiometabolic', startrow=250)

    #b_cardiometabolic_risk_factors_heart_conditions
    b_cardiometabolic_risk_factors_heart_conditions = []
    b_cardiometabolic = Instruments(phase2_data).get_b_cardiometabolic_risk_factors_heart_conditions()

    for col in b_cardiometabolic:
        df = BranchingLogic(phase2_data).b_cardiometabolic_risk_factors_heart_conditions_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        b_cardiometabolic_risk_factors_heart_conditions.append(df.T)

    b_cardiometabolic_risk_factors_heart_conditions[0].to_excel(writer, sheet_name='b_cardiometabolic')
    b_cardiometabolic_risk_factors_heart_conditions[2].to_excel(writer, sheet_name='b_cardiometabolic', startrow=14)
    b_cardiometabolic_risk_factors_heart_conditions[3].to_excel(writer, sheet_name='b_cardiometabolic', startrow=21)
    b_cardiometabolic_risk_factors_heart_conditions[4].to_excel(writer, sheet_name='b_cardiometabolic', startrow=28)
    b_cardiometabolic_risk_factors_heart_conditions[5].to_excel(writer, sheet_name='b_cardiometabolic', startrow=35)
    b_cardiometabolic_risk_factors_heart_conditions[6].to_excel(writer, sheet_name='b_cardiometabolic', startrow=42)
    b_cardiometabolic_risk_factors_heart_conditions[7].to_excel(writer, sheet_name='b_cardiometabolic', startrow=49)
    b_cardiometabolic_risk_factors_heart_conditions[8].to_excel(writer, sheet_name='b_cardiometabolic', startrow=56)
    b_cardiometabolic_risk_factors_heart_conditions[9].to_excel(writer, sheet_name='b_cardiometabolic', startrow=63)
    b_cardiometabolic_risk_factors_heart_conditions[10].to_excel(writer, sheet_name='b_cardiometabolic', startrow=70)
    b_cardiometabolic_risk_factors_heart_conditions[11].to_excel(writer, sheet_name='b_cardiometabolic', startrow=77)
    b_cardiometabolic_risk_factors_heart_conditions[13].to_excel(writer, sheet_name='b_cardiometabolic', startrow=84)
    b_cardiometabolic_risk_factors_heart_conditions[14].to_excel(writer, sheet_name='b_cardiometabolic', startrow=99)
    b_cardiometabolic_risk_factors_heart_conditions[15].to_excel(writer, sheet_name='b_cardiometabolic', startrow=108)
    b_cardiometabolic_risk_factors_heart_conditions[16].to_excel(writer, sheet_name='b_cardiometabolic', startrow=115)
    b_cardiometabolic_risk_factors_heart_conditions[17].to_excel(writer, sheet_name='b_cardiometabolic', startrow=122)
    b_cardiometabolic_risk_factors_heart_conditions[18].to_excel(writer, sheet_name='b_cardiometabolic', startrow=129)
    b_cardiometabolic_risk_factors_heart_conditions[19].to_excel(writer, sheet_name='b_cardiometabolic', startrow=136)
    b_cardiometabolic_risk_factors_heart_conditions[20].to_excel(writer, sheet_name='b_cardiometabolic', startrow=144)
    b_cardiometabolic_risk_factors_heart_conditions[21].to_excel(writer, sheet_name='b_cardiometabolic', startrow=151)
    b_cardiometabolic_risk_factors_heart_conditions[22].to_excel(writer, sheet_name='b_cardiometabolic', startrow=158)
    b_cardiometabolic_risk_factors_heart_conditions[23].to_excel(writer, sheet_name='b_cardiometabolic', startrow=164)
    b_cardiometabolic_risk_factors_heart_conditions[24].to_excel(writer, sheet_name='b_cardiometabolic', startrow=173)
    b_cardiometabolic_risk_factors_heart_conditions[25].to_excel(writer, sheet_name='b_cardiometabolic', startrow=180)
    b_cardiometabolic_risk_factors_heart_conditions[26].to_excel(writer, sheet_name='b_cardiometabolic', startrow=187)
    b_cardiometabolic_risk_factors_heart_conditions[27].to_excel(writer, sheet_name='b_cardiometabolic', startrow=191)
    b_cardiometabolic_risk_factors_heart_conditions[28].to_excel(writer, sheet_name='b_cardiometabolic', startrow=198)
    b_cardiometabolic_risk_factors_heart_conditions[29].to_excel(writer, sheet_name='b_cardiometabolic', startrow=203)
    b_cardiometabolic_risk_factors_heart_conditions[30].to_excel(writer, sheet_name='b_cardiometabolic', startrow=210)
    b_cardiometabolic_risk_factors_heart_conditions[31].to_excel(writer, sheet_name='b_cardiometabolic', startrow=217)
    b_cardiometabolic_risk_factors_heart_conditions[32].to_excel(writer, sheet_name='b_cardiometabolic', startrow=224)
    b_cardiometabolic_risk_factors_heart_conditions[33].to_excel(writer, sheet_name='b_cardiometabolic', startrow=231)
    b_cardiometabolic_risk_factors_heart_conditions[34].to_excel(writer, sheet_name='b_cardiometabolic', startrow=238)
    b_cardiometabolic_risk_factors_heart_conditions[35].to_excel(writer, sheet_name='b_cardiometabolic', startrow=244)
    b_cardiometabolic_risk_factors_heart_conditions[36].to_excel(writer, sheet_name='b_cardiometabolic', startrow=251)
    b_cardiometabolic_risk_factors_heart_conditions[37].to_excel(writer, sheet_name='b_cardiometabolic', startrow=258)
    b_cardiometabolic_risk_factors_heart_conditions[38].to_excel(writer, sheet_name='b_cardiometabolic', startrow=271)
    b_cardiometabolic_risk_factors_heart_conditions[40].to_excel(writer, sheet_name='b_cardiometabolic', startrow=278)
    b_cardiometabolic_risk_factors_heart_conditions[41].to_excel(writer, sheet_name='b_cardiometabolic', startrow=284)
    b_cardiometabolic_risk_factors_heart_conditions[42].to_excel(writer, sheet_name='b_cardiometabolic', startrow=291)
    b_cardiometabolic_risk_factors_heart_conditions[43].to_excel(writer, sheet_name='b_cardiometabolic', startrow=298)
    b_cardiometabolic_risk_factors_heart_conditions[45].to_excel(writer, sheet_name='b_cardiometabolic', startrow=304)
    b_cardiometabolic_risk_factors_heart_conditions[1].to_excel(writer, sheet_name='b_cardiometabolic', startrow=311)
    b_cardiometabolic_risk_factors_heart_conditions[12].to_excel(writer, sheet_name='b_cardiometabolic', startrow=343)
    b_cardiometabolic_risk_factors_heart_conditions[39].to_excel(writer, sheet_name='b_cardiometabolic', startrow=410)
    b_cardiometabolic_risk_factors_heart_conditions[44].to_excel(writer, sheet_name='b_cardiometabolic', startrow=437)

    # c_cardiometabolic_risk_factors_hypertension_choles
    c_cardiometabolic_risk_factors_hypertension_choles = []
    c_cardiometabolic = Instruments(phase2_data).get_c_cardiometabolic_risk_factors_hypertension_choles()

    for col in c_cardiometabolic:
        df = BranchingLogic(phase2_data).c_cardiometabolic_risk_factors_hypertension_choles_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        c_cardiometabolic_risk_factors_hypertension_choles.append(df.T)

    c_cardiometabolic_risk_factors_hypertension_choles[0].to_excel(writer, sheet_name='c_cardiometabolic')
    c_cardiometabolic_risk_factors_hypertension_choles[1].to_excel(writer, sheet_name='c_cardiometabolic', startrow=7)
    c_cardiometabolic_risk_factors_hypertension_choles[2].to_excel(writer, sheet_name='c_cardiometabolic', startrow=14)
    c_cardiometabolic_risk_factors_hypertension_choles[3].to_excel(writer, sheet_name='c_cardiometabolic', startrow=21)
    c_cardiometabolic_risk_factors_hypertension_choles[4].to_excel(writer, sheet_name='c_cardiometabolic', startrow=28)
    c_cardiometabolic_risk_factors_hypertension_choles[5].to_excel(writer, sheet_name='c_cardiometabolic', startrow=35)
    c_cardiometabolic_risk_factors_hypertension_choles[7].to_excel(writer, sheet_name='c_cardiometabolic', startrow=49)
    c_cardiometabolic_risk_factors_hypertension_choles[8].to_excel(writer, sheet_name='c_cardiometabolic', startrow=56)
    c_cardiometabolic_risk_factors_hypertension_choles[9].to_excel(writer, sheet_name='c_cardiometabolic', startrow=63)
    c_cardiometabolic_risk_factors_hypertension_choles[10].to_excel(writer, sheet_name='c_cardiometabolic', startrow=70)
    c_cardiometabolic_risk_factors_hypertension_choles[11].to_excel(writer, sheet_name='c_cardiometabolic', startrow=77)
    c_cardiometabolic_risk_factors_hypertension_choles[12].to_excel(writer, sheet_name='c_cardiometabolic', startrow=84)
    c_cardiometabolic_risk_factors_hypertension_choles[13].to_excel(writer, sheet_name='c_cardiometabolic', startrow=91)
    c_cardiometabolic_risk_factors_hypertension_choles[14].to_excel(writer, sheet_name='c_cardiometabolic', startrow=98)
    c_cardiometabolic_risk_factors_hypertension_choles[17].to_excel(writer, sheet_name='c_cardiometabolic', startrow=105)
    c_cardiometabolic_risk_factors_hypertension_choles[18].to_excel(writer, sheet_name='c_cardiometabolic', startrow=112)
    c_cardiometabolic_risk_factors_hypertension_choles[15].to_excel(writer, sheet_name='c_cardiometabolic', startrow=119)
    c_cardiometabolic_risk_factors_hypertension_choles[6].to_excel(writer, sheet_name='c_cardiometabolic', startrow=140)


    #d_cardiometabolic_risk_factors_kidney_thyroid_ra
    d_cardiometabolic_risk_factors_kidney_thyroid_ra = []
    d_cardiometabolic = Instruments(phase2_data).get_d_cardiometabolic_risk_factors_kidney_thyroid_ra()

    for col in d_cardiometabolic:
        df = BranchingLogic(phase2_data).d_cardiometabolic_risk_factors_kidney_thyroid_ra_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        d_cardiometabolic_risk_factors_kidney_thyroid_ra.append(df.T)

    d_cardiometabolic_risk_factors_kidney_thyroid_ra[0].to_excel(writer, sheet_name='d_cardiometabolic')
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[1].to_excel(writer, sheet_name='d_cardiometabolic', startrow=7)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[2].to_excel(writer, sheet_name='d_cardiometabolic', startrow=14)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[3].to_excel(writer, sheet_name='d_cardiometabolic', startrow=21)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[4].to_excel(writer, sheet_name='d_cardiometabolic', startrow=28)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[5].to_excel(writer, sheet_name='d_cardiometabolic', startrow=35)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[6].to_excel(writer, sheet_name='d_cardiometabolic', startrow=42)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[7].to_excel(writer, sheet_name='d_cardiometabolic', startrow=49)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[9].to_excel(writer, sheet_name='d_cardiometabolic', startrow=63)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[10].to_excel(writer, sheet_name='d_cardiometabolic', startrow=70)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[11].to_excel(writer, sheet_name='d_cardiometabolic', startrow=77)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[12].to_excel(writer, sheet_name='d_cardiometabolic', startrow=84)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[13].to_excel(writer, sheet_name='d_cardiometabolic', startrow=91)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[16].to_excel(writer, sheet_name='d_cardiometabolic', startrow=97)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[17].to_excel(writer, sheet_name='d_cardiometabolic', startrow=119)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[18].to_excel(writer, sheet_name='d_cardiometabolic', startrow=126)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[19].to_excel(writer, sheet_name='d_cardiometabolic', startrow=133)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[20].to_excel(writer, sheet_name='d_cardiometabolic', startrow=140)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[21].to_excel(writer, sheet_name='d_cardiometabolic', startrow=147)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[22].to_excel(writer, sheet_name='d_cardiometabolic', startrow=154)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[23].to_excel(writer, sheet_name='d_cardiometabolic', startrow=160)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[24].to_excel(writer, sheet_name='d_cardiometabolic', startrow=168)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[25].to_excel(writer, sheet_name='d_cardiometabolic', startrow=175)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[26].to_excel(writer, sheet_name='d_cardiometabolic', startrow=182)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[27].to_excel(writer, sheet_name='d_cardiometabolic', startrow=190)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[28].to_excel(writer, sheet_name='d_cardiometabolic', startrow=197)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[29].to_excel(writer, sheet_name='d_cardiometabolic', startrow=204)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[30].to_excel(writer, sheet_name='d_cardiometabolic', startrow=221)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[31].to_excel(writer, sheet_name='d_cardiometabolic', startrow=228)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[32].to_excel(writer, sheet_name='d_cardiometabolic', startrow=235)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[33].to_excel(writer, sheet_name='d_cardiometabolic', startrow=242)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[34].to_excel(writer, sheet_name='d_cardiometabolic', startrow=249)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[35].to_excel(writer, sheet_name='d_cardiometabolic', startrow=256)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[36].to_excel(writer, sheet_name='d_cardiometabolic', startrow=263)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[37].to_excel(writer, sheet_name='d_cardiometabolic', startrow=270)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[38].to_excel(writer, sheet_name='d_cardiometabolic', startrow=277)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[39].to_excel(writer, sheet_name='d_cardiometabolic', startrow=281)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[40].to_excel(writer, sheet_name='d_cardiometabolic', startrow=291)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[41].to_excel(writer, sheet_name='d_cardiometabolic', startrow=298)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[8].to_excel(writer, sheet_name='d_cardiometabolic', startrow=305)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[15].to_excel(writer, sheet_name='d_cardiometabolic', startrow=345)
    d_cardiometabolic_risk_factors_kidney_thyroid_ra[14].to_excel(writer, sheet_name='d_cardiometabolic', startrow=370)

    #physical_activity_and_sleep
    physical_activity_and_sleep = []
    physical_activity_col = Instruments(phase2_data).get_physical_activity_and_sleep()

    for col in physical_activity_col:
        df = BranchingLogic(phase2_data).physical_activity_and_sleep_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        physical_activity_and_sleep.append(df.T)

    physical_activity_and_sleep[0].to_excel(writer, sheet_name='physical_activity_and_sleep')
    physical_activity_and_sleep[1].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=7)
    physical_activity_and_sleep[2].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=14)
    physical_activity_and_sleep[3].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=20)
    #physical_activity_and_sleep[5].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=38)
    #physical_activity_and_sleep[6].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=44)
    physical_activity_and_sleep[7].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=38)
    physical_activity_and_sleep[8].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=44)
    #physical_activity_and_sleep[10].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=74)
    #physical_activity_and_sleep[11].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=80)
    #physical_activity_and_sleep[13].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=58)
    #physical_activity_and_sleep[14].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=65)
    physical_activity_and_sleep[15].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=58)
    physical_activity_and_sleep[16].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=65)
    physical_activity_and_sleep[17].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=72)
    #physical_activity_and_sleep[19].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=142)
    #physical_activity_and_sleep[20].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=152)
    physical_activity_and_sleep[21].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=85)
    physical_activity_and_sleep[22].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=91)
    physical_activity_and_sleep[23].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=98)
    physical_activity_and_sleep[27].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=110)
    physical_activity_and_sleep[28].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=117)
    physical_activity_and_sleep[42].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=128)
    physical_activity_and_sleep[43].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=145)
    physical_activity_and_sleep[44].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=152)
    physical_activity_and_sleep[45].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=166)
    physical_activity_and_sleep[46].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=176)
    physical_activity_and_sleep[47].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=186)
    physical_activity_and_sleep[48].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=196)
    physical_activity_and_sleep[49].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=206)
    physical_activity_and_sleep[50].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=216)
    physical_activity_and_sleep[51].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=226)
    physical_activity_and_sleep[52].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=236)
    physical_activity_and_sleep[4].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=246)
    physical_activity_and_sleep[9].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=335)
    physical_activity_and_sleep[12].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=484)
    physical_activity_and_sleep[18].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=649)
    physical_activity_and_sleep[24].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=759)
    physical_activity_and_sleep[29].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=860)
    physical_activity_and_sleep[32].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=952)
    physical_activity_and_sleep[35].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=1111)
    physical_activity_and_sleep[38].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=1258)
    physical_activity_and_sleep[39].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=1530)
    physical_activity_and_sleep[40].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=1780)
    physical_activity_and_sleep[58].to_excel(writer, sheet_name='physical_activity_and_sleep', startrow=2057)

    #ultrasound_and_dxa_measurements
    ultrasound_and_dxa_measurements = []
    ultrasound_and_dxa_measurements_col = Instruments(phase2_data).get_ultrasound_and_dxa_measurements()

    for col in ultrasound_and_dxa_measurements_col:
        df = BranchingLogic(phase2_data).ultrasound_and_dxa_measurements_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        ultrasound_and_dxa_measurements.append(df.T)

    ultrasound_and_dxa_measurements[0].to_excel(writer, sheet_name='ultrasound_and_dxa_measurements')
    #ultrasound_and_dxa_measurements[1].to_excel(writer, sheet_name='ultrasound_and_dxa_measurements', startrow=7)
    ultrasound_and_dxa_measurements[5].to_excel(writer, sheet_name='ultrasound_and_dxa_measurements', startrow=14)
    ultrasound_and_dxa_measurements[14].to_excel(writer, sheet_name='ultrasound_and_dxa_measurements', startrow=21)
    ultrasound_and_dxa_measurements[17].to_excel(writer, sheet_name='ultrasound_and_dxa_measurements', startrow=28)
    ultrasound_and_dxa_measurements[19].to_excel(writer, sheet_name='ultrasound_and_dxa_measurements', startrow=35)
    #ultrasound_and_dxa_measurements[20].to_excel(writer, sheet_name='ultrasound_and_dxa_measurements', startrow=42)
    ultrasound_and_dxa_measurements[21].to_excel(writer, sheet_name='ultrasound_and_dxa_measurements', startrow=42)

    # a_respiratory_health
    a_respiratory_health = []
    a_respiratory_col = Instruments(phase2_data).get_a_respiratory_health()

    for col in a_respiratory_col:
        df = BranchingLogic(phase2_data).a_respiratory_health_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        a_respiratory_health.append(df.T)

    a_respiratory_health[0].to_excel(writer, sheet_name='a_respiratory_health')
    a_respiratory_health[1].to_excel(writer, sheet_name='a_respiratory_health', startrow=7)
    a_respiratory_health[2].to_excel(writer, sheet_name='a_respiratory_health', startrow=14)
    a_respiratory_health[3].to_excel(writer, sheet_name='a_respiratory_health', startrow=21)
    a_respiratory_health[4].to_excel(writer, sheet_name='a_respiratory_health', startrow=31)
    a_respiratory_health[5].to_excel(writer, sheet_name='a_respiratory_health', startrow=36)
    a_respiratory_health[6].to_excel(writer, sheet_name='a_respiratory_health', startrow=45)
    a_respiratory_health[7].to_excel(writer, sheet_name='a_respiratory_health', startrow=51)
    a_respiratory_health[8].to_excel(writer, sheet_name='a_respiratory_health', startrow=119)
    a_respiratory_health[9].to_excel(writer, sheet_name='a_respiratory_health', startrow=126)
    a_respiratory_health[10].to_excel(writer, sheet_name='a_respiratory_health', startrow=133)
    a_respiratory_health[11].to_excel(writer, sheet_name='a_respiratory_health', startrow=140)
    a_respiratory_health[12].to_excel(writer, sheet_name='a_respiratory_health', startrow=147)
    a_respiratory_health[13].to_excel(writer, sheet_name='a_respiratory_health', startrow=154)
    a_respiratory_health[14].to_excel(writer, sheet_name='a_respiratory_health', startrow=161)
    a_respiratory_health[15].to_excel(writer, sheet_name='a_respiratory_health', startrow=168)
    a_respiratory_health[16].to_excel(writer, sheet_name='a_respiratory_health', startrow=175)
    a_respiratory_health[17].to_excel(writer, sheet_name='a_respiratory_health', startrow=182)
    a_respiratory_health[18].to_excel(writer, sheet_name='a_respiratory_health', startrow=189)
    a_respiratory_health[19].to_excel(writer, sheet_name='a_respiratory_health', startrow=196)
    a_respiratory_health[20].to_excel(writer, sheet_name='a_respiratory_health', startrow=203)
    a_respiratory_health[21].to_excel(writer, sheet_name='a_respiratory_health', startrow=271)
    a_respiratory_health[22].to_excel(writer, sheet_name='a_respiratory_health', startrow=278)
    a_respiratory_health[23].to_excel(writer, sheet_name='a_respiratory_health', startrow=285)
    a_respiratory_health[24].to_excel(writer, sheet_name='a_respiratory_health', startrow=292)
    a_respiratory_health[25].to_excel(writer, sheet_name='a_respiratory_health', startrow=298)

    # b_spirometry_eligibility
    b_spirometry_eligibility = []
    b_spirometry_col = Instruments(phase2_data).get_b_spirometry_eligibility()

    for col in b_spirometry_col:
        df = BranchingLogic(phase2_data).b_spirometry_eligibility_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        b_spirometry_eligibility.append(df.T)

    b_spirometry_eligibility[0].to_excel(writer, sheet_name='b_spirometry_eligibility')
    b_spirometry_eligibility[1].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=7)
    b_spirometry_eligibility[2].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=14)
    b_spirometry_eligibility[3].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=21)
    b_spirometry_eligibility[4].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=31)
    b_spirometry_eligibility[5].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=36)
    b_spirometry_eligibility[6].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=45)
    b_spirometry_eligibility[7].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=51)
    b_spirometry_eligibility[8].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=57)
    b_spirometry_eligibility[9].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=63)
    b_spirometry_eligibility[10].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=70)
    b_spirometry_eligibility[11].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=77)
    b_spirometry_eligibility[12].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=84)
    b_spirometry_eligibility[13].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=91)
    b_spirometry_eligibility[14].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=98)
    b_spirometry_eligibility[15].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=104)
    b_spirometry_eligibility[16].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=121)
    b_spirometry_eligibility[17].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=128)
    b_spirometry_eligibility[18].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=134)
    b_spirometry_eligibility[19].to_excel(writer, sheet_name='b_spirometry_eligibility', startrow=146)

    # a_microbiome
    a_microbiome = []
    a_microbiome_col = Instruments(phase2_data).get_a_microbiome()

    for col in a_microbiome_col:
        df = BranchingLogic(phase2_data).a_microbiome_logic().groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        a_microbiome.append(df.T)

    a_microbiome[0].to_excel(writer, sheet_name='a_microbiome')
    a_microbiome[1].to_excel(writer, sheet_name='a_microbiome', startrow=13)
    a_microbiome[2].to_excel(writer, sheet_name='a_microbiome', startrow=26)
    a_microbiome[3].to_excel(writer, sheet_name='a_microbiome', startrow=35)
    a_microbiome[4].to_excel(writer, sheet_name='a_microbiome', startrow=44)
    a_microbiome[5].to_excel(writer, sheet_name='a_microbiome', startrow=55)

    # trauma
    trauma = []
    trauma_col = Instruments(phase2_data).get_trauma()

    for col in trauma_col:
        df = phase2_data.groupby('gene_site')[col].value_counts(dropna=False).to_frame().unstack()
        trauma.append(df.T)

    trauma[0].to_excel(writer, sheet_name='trauma')
    trauma[1].to_excel(writer, sheet_name='trauma', startrow=7)
    trauma[2].to_excel(writer, sheet_name='trauma', startrow=14)
    trauma[3].to_excel(writer, sheet_name='trauma', startrow=21)
    trauma[4].to_excel(writer, sheet_name='trauma', startrow=28)
    trauma[5].to_excel(writer, sheet_name='trauma', startrow=35)
    trauma[6].to_excel(writer, sheet_name='trauma', startrow=42)
    trauma[7].to_excel(writer, sheet_name='trauma', startrow=49)
    trauma[8].to_excel(writer, sheet_name='trauma', startrow=54)
    trauma[10].to_excel(writer, sheet_name='trauma', startrow=63)
    trauma[11].to_excel(writer, sheet_name='trauma', startrow=70)


    bmi_col = ['anth_standing_height', 'anth_weight', 'anth_waist_circumf',
                 'anth_hip_circumf', 'anth_bmi_c']

    bmi_stats = phase2_data.groupby('gene_site')[bmi_col].describe()

    bmi_stats.T.to_excel(writer, sheet_name='bmi_stats')

    bp_stats = phase2_data.groupby('gene_site')[['bppm_systolic_avg', 'bppm_diastolic_avg', 'bppm_pulse_avg']].describe()

    bp_stats.T.to_excel(writer, sheet_name='bp_stats')

    ultra_col = ['ultr_visceral_fat', 'ultr_subcutaneous_fat', 'ultr_cimt_right_mean', 'ultr_cimt_left_mean',
                'ultr_plaque_right', 'ultr_plaque_left']

    ultra_stats = phase2_data.groupby('gene_site')[ultra_col].describe()

    ultra_stats.T.to_excel(writer, sheet_name='ultra_stats')

    frai_col2 = ['frai_standing_up_time', 'frai_dynometer_force_max', 'frai_turn_walk_back', 'cogn_different_animals']

    frai_stats = phase2_data.groupby('gene_site')[frai_col2].describe()

    frai_stats.T.to_excel(writer, sheet_name='frailty_stats')

    writer.save()



