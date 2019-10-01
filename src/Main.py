import EmailHandler
import DatabasePopulator
import Instruments
from MergeHandler import MergeHandler
from DataAnalyser import DataAnalyser
from ImportData import ImportData
import pandas as pd


def main():
    #     populate the database
    # dataset = ImportData.get_records()
    dataset = pd.read_csv("../resources/data.csv")
    populateDatabase = DatabasePopulator.PopulateDatabase(dataset)
    #     populateDatabase.add_records_to_database()

    #     specify the instrument
    instruments = Instruments.Instruments("../resources/data.csv")
    anthropometry = instruments.get_anthropometric_measurements()
    health_diet = instruments.get_c_general_health_diet()

    #     merge instruments
    merge_data = MergeHandler(anthropometry, health_diet)
    data = merge_data.join_data_frames()

    # data analysis
    dataAnalyser = DataAnalyser(anthropometry)

    #     specify contacts and files to attach
    contacts = ['jajawandera@gmail.com', 'u17253129@tuks.co.za']
    attachments = [dataAnalyser.get_report(),
                   dataAnalyser.get_box_plot(),
                   dataAnalyser.get_heat_map()]

    for plot in dataAnalyser.get_pairplot():
        attachments.append(plot)

    #   sending the email
    emailHandler = EmailHandler.EmailHandler(contacts, attachments)
    emailHandler.send_email()


if __name__ == '__main__':
    main()
