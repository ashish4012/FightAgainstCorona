from datetime import datetime
from sqlalchemy import create_engine, func
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Date, DateTime, BigInteger, and_, or_
from fac import config

# Global Variables
SQLITE = 'sqlite'

# Table Names
STATES = 'states'
DEMOGRAPHICS = 'demographics'


class FacDB:
    DB_ENGINE = {
        SQLITE: config.SQLALCHEMY_DATABASE_URI
    }

    # Main DB Connection Ref Obj
    db_engine = None

    def __init__(self, dbtype, username='', password='', dbname=''):
        dbtype = dbtype.lower()
        if dbtype in self.DB_ENGINE.keys():
            engine_url = self.DB_ENGINE[dbtype].format(DB=dbname)
            self.db_engine = create_engine(engine_url)
            print(self.db_engine)
        else:
            print("DBType is not found in DB_ENGINE")

    def create_db_tables(self):
        metadata = MetaData()
        states = Table(STATES, metadata,
                       Column('id', Integer, primary_key=True, autoincrement=True),
                       Column('state', String)
                       )
        demographics = Table(DEMOGRAPHICS, metadata,
                             Column('id', Integer, primary_key=True, autoincrement=True),
                             Column('date', Date),
                             Column('state_id', None, ForeignKey('states.id')),
                             Column('affected_nationals', Integer),
                             Column('affected_foreigner', Integer),
                             Column('cured', Integer),
                             Column('death', Integer),
                             Column('last_updated', DateTime)
                             )
        try:
            metadata.create_all(self.db_engine)
            print("Tables created")
        except Exception as e:
            print("Error occurred during Table creation!")
            print(e)

    def get_table(self, table_name):
        metadata = MetaData(bind=self.db_engine, reflect=True)
        return metadata.tables[table_name]

    def insert_state(self, state_name):
        db_state = self.get_table('states')
        query = db_state.insert().values(state=state_name)
        result_proxy = self.db_engine.execute(query)
        print(result_proxy)

    def get_state_id(self, state_name):
        db_state = self.get_table('states')
        query = db_state.select().where(db_state.columns.state == state_name)
        result = self.db_engine.execute(query).fetchall()
        if len(result) > 0:
            return result[0].id
        else:
            return None

    def get_demographics_data(self, state_id, date):
        db_demographics = self.get_table('demographics')
        query = db_demographics.select().where(and_(db_demographics.columns.state_id == state_id,
                                                    func.DATE(db_demographics.columns.date) == func.DATE(date)))
        result = self.db_engine.execute(query).fetchall()
        if len(result) > 0:
            return result[0].id
        else:
            return None

    def insert_demographics_data(self, data_dict):
        data_dict['date'] = datetime.today()
        data_dict['last_updated'] = datetime.now()
        db_demographics = self.get_table('demographics')
        if self.get_demographics_data(state_id=data_dict['state_id'], date=data_dict['date']) is None:
            query = db_demographics.insert().values(data_dict)
            print("Inserting New Records")
        else:
            query = db_demographics.update().values(data_dict).where(and_(
                db_demographics.columns.state_id == data_dict['state_id'],
                func.DATE(db_demographics.columns.date) == func.DATE(data_dict['date'])))
            print("Updating Existing Records")
        result_proxy = self.db_engine.execute(query)
        print(result_proxy)

    def get_state_demographics_date(self, state):
        state_id = self.get_state_id(state)
        db_demographics = self.get_table('demographics')
        query = db_demographics.select().where(db_demographics.columns.state_id == state_id)
        result = self.db_engine.execute(query).fetchall()
        return result
