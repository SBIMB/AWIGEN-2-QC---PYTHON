import EmailHandler
#import DatabasePopulator
from Instruments import Instruments
from MergeHandler import MergeHandler
from DataAnalyser import DataAnalyser
from BranchingLogic import BranchingLogicHandler
import ExportData
from ImportData import ImportData
from SiteFeedbackHandler import SiteFeedbackHandler

import pandas as pd

import xlsxwriter
from datetime import datetime

def main():
    outputDir = './resources/'

    # Process site feedback
    # SiteFeedbackHandler.handle_outlier_feedback(outputDir + 'outliers_agincourt_20210604_ret.xlsx')

    datestr = datetime.today().strftime('%Y%m%d')
    sites = ['agincourt', 'dimamo', 'soweto', 'nairobi', 'nanoro', 'navrongo']

    # sites = ['navrongo']

    for site in sites:
        csv_link = outputDir + 'data_{}_{}.csv'.format(site, datestr)
        print(csv_link)

        ImportData(csv_link)

        # Generate outlier report
        instruments = Instruments(csv_link)
        outliers_writer = pd.ExcelWriter(outputDir + 'outliers_{}_{}.xlsx'.format(site, datestr), engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
        DataAnalyser(outputDir, instruments, outliers_writer).outliers()
        outliers_writer.save()

        # Generate missing report
        missing_writer = pd.ExcelWriter(outputDir + 'missing_{}_{}.xlsx'.format(site, datestr), engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
        BranchingLogicHandler(outputDir, csv_link, missing_writer).write_report()
        missing_writer.save()

if __name__ == '__main__':
    main()
