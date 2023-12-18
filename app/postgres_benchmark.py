from sqlalchemy import create_engine
import psycopg2
from config import POSTGRES_CONFIG, POSTGRES_URL, DEFAULT_QUERIES, DEBUG
from app.core import measure

def postgres_import(df):
    try:
        print("Postgres:")
        print("Сonnecting to the database...")
        engine = create_engine(POSTGRES_URL)
        print("Importing a dataset...")
        df.to_sql('data', engine, if_exists='replace', index=False)
        print("DIsconnecting from a database....")
    except psycopg2.Error as e:
        print("Unknown error during postgres operation:", e)
        if DEBUG:
            raise e


def postgres_run():
    try:
        print("Postgres:")
        print("Сonnecting to the database...")
        connection = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = connection.cursor()
        measure(DEFAULT_QUERIES, cursor)
        print("Disconnecting from a database....")
        cursor.close()
        connection.close()
    except psycopg2.Error as e:
        print("Unknown error during postgres operation:", e)