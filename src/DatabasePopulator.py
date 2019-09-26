import ImportData
from sqlalchemy import create_engine


class PopulateDatabase:
    # get data from ImportData class
    import_data = ImportData.ImportData()

    def __init__(self):
        self.data = self.import_data.readCsv()

    def addRecordsToDatabase(self):
        # connect to the database
        engine = create_engine('mysql://root:root@localhost:3306/soweto')
        with engine.connect() as conn, conn.begin():
            # push data to mysql database soweto and create table data
            self.data.to_sql('data', conn, if_exists='replace', index=False)
