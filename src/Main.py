from Instruments import Instruments
from DataAnalyser import DataAnalyser
from BranchingLogic import BranchingLogicHandler
from SiteFeedbackHandler import SiteFeedbackHandler
from RedcapApiHandler import RedcapApiHandler

import pandas as pd

import xlsxwriter
from datetime import datetime

def main():
    outputDir = './resources/'

    # Process site feedback
    # SiteFeedbackHandler.handle_outlier_feedback(outputDir + 'outliers_dimamo_20220605_ret.xlsx')

    datestr = datetime.today().strftime('%Y%m%d')
    # sites = ['dimamo', 'nanoro', 'navrongo', 'agincourt', 'soweto', 'nairobi']

    sites = ['dimamo', 'nanoro', 'navrongo']

    for site in sites:
        csv = outputDir + 'data_{}_{}.csv'.format(site, datestr)
        print(csv)

        data = RedcapApiHandler(site).export_from_redcap(csv)
        data = data[data['redcap_event_name'] == 'phase_2_arm_1']

        # Generate outlier report
        outliers_writer = pd.ExcelWriter(outputDir + 'outliers_{}_{}.xlsx'.format(site, datestr), engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
        DataAnalyser(outputDir, data, site).write_outliers_report(outliers_writer)
        outliers_writer.save()

        # Generate missing report
        missing_writer = pd.ExcelWriter(outputDir + 'missing_{}_{}.xlsx'.format(site, datestr), engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
        BranchingLogicHandler(data, site).write_missingness_report(missing_writer)
        missing_writer.save()

if __name__ == '__main__':
    main()
