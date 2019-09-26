import EmailHandler
import DatabasePopulator


def main():
    # populateDatabase = DatabasePopulator.PopulateDatabase()
    # populateDatabase.addRecordsToDatabase()

    contacts = ['jajawandera@gmail.com', 'u17253129@tuks.co.za']

    emailHandler = EmailHandler.EmailHandler(contacts)
    emailHandler.sendEmail()


if __name__ == '__main__':
    main()
