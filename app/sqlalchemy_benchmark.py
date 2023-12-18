from config import SQLALCHEMY_BACKEND, DEBUG
from app.core import measure
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


def sqlalchemy_run(df):
    try:
        print("SQLAlchemy:")
        print("Сonnecting to the database...")
        engine = create_engine("sqlite+pysqlite:///:memory:")
        Base = declarative_base()
        Session = sessionmaker(bind=engine)

        class Data(Base):
            __tablename__ = 'data'

            id = Column(Integer, primary_key=True, name='Unnamed: 0')
            VendorID = Column(Integer)
            tpep_pickup_datetime = Column(DateTime)
            tpep_dropoff_datetime = Column(DateTime)
            passenger_count = Column(Float)
            trip_distance = Column(Float)
            RatecodeID = Column(Float)
            store_and_fwd_flag = Column(String)
            PULocationID = Column(Integer)
            DOLocationID = Column(Integer)
            payment_type = Column(Integer)
            fare_amount = Column(Float)
            extra = Column(Float)
            mta_tax = Column(Float)
            tip_amount = Column(Float)
            tolls_amount = Column(Float)
            improvement_surcharge = Column(Float)
            total_amount = Column(Float)
            congestion_surcharge = Column(Float)
            airport_fee = Column(Float)
            Airport_fee2 = Column(Float)

        Base.metadata.create_all(engine)

        print("Importing a dataset...")
        df.to_sql('data', con=engine, if_exists='replace', index=False)

        session = Session()
        queries = [session.query(Data.VendorID, func.count('*')).group_by(Data.VendorID),
                   session.query(Data.passenger_count, func.avg(Data.total_amount)).group_by(Data.passenger_count),
                   session.query(Data.passenger_count, func.extract('year', Data.tpep_pickup_datetime),
                                 func.count('*')).group_by(Data.passenger_count,
                                                           func.extract('year', Data.tpep_pickup_datetime)),
                   session.query(Data.passenger_count, func.extract('year', Data.tpep_pickup_datetime),
                                 func.round(Data.trip_distance), func.count('*')). \
                       group_by(Data.passenger_count, func.extract('year', Data.tpep_pickup_datetime),
                                func.round(Data.trip_distance)). \
                       order_by(func.extract('year', Data.tpep_pickup_datetime), func.count('*').desc())]


        measure(queries, backend=SQLALCHEMY_BACKEND)

        print("Disconnecting from a database....")
    except Exception as e:
        print("Unknown error during pandas operation:", e)
        if DEBUG:
            raise e
