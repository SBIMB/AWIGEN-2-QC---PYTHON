from sqlalchemy import create_engine
import os

class PopulateDatabase:

    def __init__(self, data):
        self.data = data
        self.password = os.environ.get('MYSQL_PASS')
        self.db_link = 'mysql://root:'+self.password+'@localhost:3306/soweto'

    def add_records_to_database(self):
        # connect to the database
        print(self.db_link)
        engine = create_engine(self.db_link)
        with engine.connect() as conn, conn.begin():
            # push data to mysql database soweto and create table data
            self.data.to_sql('data', conn, if_exists='replace', index=False)
