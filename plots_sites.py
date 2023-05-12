import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
#import DataFrame

if __name__ == '__main__':

    path = './resources/'
    sites = ['agincourt', 'soweto', 'nairobi', 'dimamo', 'navrongo', 'nanoro']
    for site in sites:
        csv_data = path + 'phase1and2data_{}.csv'.format(site)
        csv_df = path + 'phase1and2diff_{}.csv'.format(site)
        data = pd.read_csv(csv_data)
        df = pd.read_csv(csv_df)
        variables = pd.read_csv('variables.csv', delimiter=';')

        pdfFile = PdfPages('plots_{}.pdf'.format(site))

        variables_list = variables.values.tolist()

        for i in range(len(variables_list)):

            if (df[variables_list[i][2]].isna()).all():
                pass
            else:

                fig = plt.figure(figsize=(10, 6))

                # define plotting region (2 rows, 2 columns)
                plt.subplot(1, 2, 1)
                plt.hist(x=df[variables_list[i][2]], bins=np.arange(min(df[variables_list[i][2]].dropna()), max(df[variables_list[i][2]].dropna()) + 1, 1))
                plt.title('The difference between phase 1 and 2 {} values_{}'.format(variables_list[i][0], site), fontdict = {'fontsize' : 8})
                plt.xlabel('{} differences'.format(variables_list[i][0]), fontsize=6)
                plt.ylabel('frequency of {} differences'.format(variables_list[i][0]), fontsize=6)
                plt.xticks(fontsize=6)
                plt.yticks(fontsize=6)
                plt.tight_layout(pad=4)

                plt.subplot(1, 2, 2)
                plt.scatter(x=data[variables_list[i][0]], y=data[variables_list[i][1]])
                plt.title('{} of participants phase 1 vs phase 2_{}'.format(variables_list[i][0], site), fontdict = {'fontsize' : 8})
                #plt.plot([30, 40, 50, 60, 70, 80, 90], [35, 45, 55, 65, 75, 85, 95], c='black', label='y=x+5')
                plt.xlabel('phase1', fontsize=6)
                plt.ylabel('phase2', fontsize=6)
                plt.xticks(fontsize=6)
                plt.yticks(fontsize=6)
                # space between the plots
                plt.tight_layout()
                #save plots
                pdfFile.savefig(fig)
        pdfFile.close()