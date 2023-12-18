import duckdb
from config import DEFAULT_QUERIES, CSV_FILEPATH, DEBUG
from app.core import measure


def duckdb_run():
    try:
        print("DuckDB:")
        print("Сonnecting to the database...")
        conn = duckdb.connect()
        print("Importing a dataset...")
        conn.execute(f'CREATE TABLE data AS FROM "{CSV_FILEPATH}";')
        measure(DEFAULT_QUERIES, conn)
        print("Disconnecting from a database...")
        conn.close()
    except Exception as e:
        print("Unknown error during duckdb operation:", e)
        if DEBUG:
            raise e
