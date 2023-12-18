import sqlite3
from config import SQLITE_QUERIES, DEBUG
from app.core import measure


def sqlite_run(df):
    try:
        print("SQLite:")
        print("Сonnecting to the database...")
        with sqlite3.connect(':memory:') as conn:
            cursor = conn.cursor()
            print("Importing a dataset...")
            df.to_sql('data', conn, if_exists='replace', index=False)
            measure(SQLITE_QUERIES, cursor)
            print("Disconnecting from a database....")
            conn.commit()
    except Exception as e:
        print("Unknown error during sqlite operation:", e)
        if DEBUG:
            raise e
