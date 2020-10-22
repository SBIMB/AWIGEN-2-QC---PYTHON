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
    datestr = datetime.today().strftime('%Y%m%d')
    siteStr = 'nairobi'
    outputDir = './resources/'

    csv_link = outputDir + 'data_{}_{}.csv'.format(siteStr, datestr)

    # 1     fetch data
    # takes some time
    ImportData(csv_link)

    # Process site feedback
    # SiteFeedbackHandler.handle_outlier_feedback(outputDir + 'outliers_soweto_20200818_returned.xlsx')

    # 2     populate the database
    # populateDatabase = DatabasePopulator.PopulateDatabase(dataset)
    # populateDatabase.add_records_to_database()

    # Generate outlier report
    instruments = Instruments(csv_link)
    outliers_writer = pd.ExcelWriter(outputDir + 'outliers_{}_{}.xlsx'.format(siteStr, datestr), engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
    DataAnalyser(outputDir, instruments, outliers_writer).outliers()
    outliers_writer.save()

    # Generate missing report
    missing_writer = pd.ExcelWriter(outputDir + 'missing_{}_{}.xlsx'.format(siteStr, datestr), engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
    BranchingLogicHandler(outputDir, csv_link, missing_writer).write_report()
    missing_writer.save()

    # 7    list of email addresses. Appended more contacts
    # contacts = ['jajawandera@gmail.com', 'u17253129@tuks.co.za']

    # # 8    list of attachments initialized with the report
    # attachments = [dataAnalyser.get_report(), pair_plot]

    # # 9     add all the jpeg files to the list
    # for plot in dataAnalyser.get_visualizations():
    #     attachments.append(plot)

    # # 10  write general report.csv file
    # attachments.append(report_link)

    # 11     sending the email
    # emailHandler = EmailHandler.EmailHandler(contacts, attachments)
    # emailHandler.send_email()


if __name__ == '__main__':
    main()
