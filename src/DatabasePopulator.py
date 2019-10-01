from sqlalchemy import create_engine


class PopulateDatabase:

    def __init__(self, data):
        self.data = data

    def add_records_to_database(self):
        # connect to the database
        engine = create_engine('mysql://root:root@localhost:3306/soweto')
        with engine.connect() as conn, conn.begin():
            # push data to mysql database soweto and create table data
            self.data.to_sql('data', conn, if_exists='replace', index=False)
