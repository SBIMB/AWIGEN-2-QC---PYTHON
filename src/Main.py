import EmailHandler
import DatabasePopulator
import Instruments
from MergeHandler import MergeHandler
from DataAnalyser import DataAnalyser
import pandas as pd
import ImportData


def main():
    # 1     fetch data
    importData = ImportData.ImportData()
    csv_link = importData.get_records()

    # 2     populate the database
    dataset = pd.read_csv(csv_link)
    populateDatabase = DatabasePopulator.PopulateDatabase(dataset)
    # populateDatabase.add_records_to_database()

    # 3    specify the instrument
    instruments = Instruments.Instruments(csv_link)
    anthropometry = instruments.get_anthropometric_measurements()
    health_diet = instruments.get_c_general_health_diet()

    # 4    merge instruments
    merge_data = MergeHandler(anthropometry, health_diet)
    data = merge_data.join_data_frames()

    # 5    data analysis
    dataAnalyser = DataAnalyser(anthropometry)

    # 6    add columns to plot pairwise relationships in a dataset
    # pair_plot = dataAnalyser.set_pair_plot('genh_days_fruit', 'anth_weight', 'anth_hip_circumf_1')

    # 7    list of email addresses. Appended more contacts
    contacts = ['jajawandera@gmail.com', 'u17253129@tuks.co.za']

    # 8    list of attachments initialized with the report
    attachments = [dataAnalyser.get_report()]

    # 9     add all the jpeg files to the list
    for plot in dataAnalyser.get_visualizations():
        attachments.append(plot)

    # 10     sending the email
    emailHandler = EmailHandler.EmailHandler(contacts, attachments)
    emailHandler.send_email()


if __name__ == '__main__':
    main()
