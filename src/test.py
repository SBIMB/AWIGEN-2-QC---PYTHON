import pandas as pd
import numpy as np
import math

data = pd.read_csv("../resources/data.csv", index_col=0)

null_cols = data.columns[data.isnull().any()]
col_totals = data[null_cols].isnull().sum()
f = open("../resources/testing.txt", "a+")

branch_cols = ['carf_diabetes_treatment', 'rspe_infection___0']

for i, j in data.iterrows():
    for col in data.columns:
        if col == 'carf_diabetes_treatment':
            if pd.isnull(j[col]) and j['carf_diabetes_treat___1']!=0 and j['carf_diabetes_treat___2']!=0 and j['carf_diabetes_treat___3']!=0 and j['carf_diabetes_treat___4']!=0 and j['carf_diabetes_treat___5']!=0:
                print(i, "One is not 0")
            else:
                print("hi")
        elif col == 'rspe_infection___0':
            if j[col] == 1 and j['rspe_infection___1']==0 and j['rspe_infection___2']==0 and j['rspe_infection___3']==0 and j['rspe_infection___4']==0:
                print(i, col, "correct entry")
            elif j[col] == 0 and :
                print(i, "help")
        else:
            if pd.isnull(j[col]):
                pass
                # print(i, col, j[col])
    print("*********************************")
f.close()

# if pd.isnull(j[col]):
#     f.write(str(i) + " " + str(col) + " " + str(j[col]) + "\n")
#     print()