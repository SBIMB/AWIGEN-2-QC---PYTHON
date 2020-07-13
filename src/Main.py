import EmailHandler
#import DatabasePopulator
import Instruments
from MergeHandler import MergeHandler
from DataAnalyser import DataAnalyser
from BranchingLogic import BranchingLogicHandler
import ImportData

import pandas as pd

import xlsxwriter


def main():
    # 1     fetch data
    # takes some time

    #importData = ImportData.ImportData()
    #csv_link = importData.get_records()

    csv_link = './resources/data.csv'
    # 2     populate the database
    # populateDatabase = DatabasePopulator.PopulateDatabase(dataset)
    # populateDatabase.add_records_to_database()

    dataSet = pd.read_csv(csv_link, na_values=["n/a", "na", "--"], index_col=None)

    # 4    merge instruments
    #merge_data = MergeHandler(anthropometry, health_diet)
    #data = merge_data.join_data_frames()

    # 3    specify the instrument
    instruments = Instruments.Instruments(dataSet)

    # Create Excel Writer to write to xlsx file
    excelWriter = pd.ExcelWriter('./resources/' + 'outliers.xlsx', engine='xlsxwriter') # pylint: disable=abstract-class-instantiated
    workbook  = excelWriter.book
    
    # 5    data analysis
    for instrument_key, instrument_getter in instruments.instrument_getters.items():
        instrument_data = instrument_getter(instruments)
        DataAnalyser(instrument_data, instrument_key, './resources/', excelWriter).outliers()

    # Save xlsx file
    excelWriter.save()

    # 7    list of email addresses. Appended more contacts
    # contacts = ['jajawandera@gmail.com', 'u17253129@tuks.co.za']

    # # 8    list of attachments initialized with the report
    # attachments = [dataAnalyser.get_report(), pair_plot]

    # # 9     add all the jpeg files to the list
    # for plot in dataAnalyser.get_visualizations():
    #     attachments.append(plot)

    # # 10  write general report.csv file
    # # add report to attachments
    # branchingLogicHandler = BranchingLogicHandler(csv_link)
    # report_link = branchingLogicHandler.write_report()
    # branchingLogicHandler.get_report_summary()
    # attachments.append(report_link)

    # 11     sending the email
    # emailHandler = EmailHandler.EmailHandler(contacts, attachments)
    # emailHandler.send_email()


if __name__ == '__main__':
    main()
