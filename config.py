# Required for the first run. After may turn it off to save time
POSTGRES_IMPORT = False

POSTGRES_RUN = True
SQLITE_RUN = True
DUCKDB_RUN = True
PANDAS_RUN = True
SQLALCHEMY_RUN = True

# String queries for Postgres and DuckDB
DEFAULT_QUERIES = [
    'SELECT "VendorID", count(*) FROM data GROUP BY 1;',
    'SELECT passenger_count, avg(total_amount) FROM data GROUP BY 1;',
    'SELECT passenger_count, extract(year from tpep_pickup_datetime::date), count(*) FROM data GROUP BY 1, 2;',
    'SELECT passenger_count, extract(year from tpep_pickup_datetime::date), round(trip_distance), count(*) FROM data GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;']

# String queries for SQLite
SQLITE_QUERIES = [
    'SELECT "VendorID", count(*) FROM data GROUP BY 1;',
    'SELECT passenger_count, avg(total_amount) FROM data GROUP BY 1;',
    '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) AS "year", count(*) FROM data GROUP BY 1, 2;''',
    '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) AS "year", round(trip_distance), count(*) FROM data GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;''']

# Queries for Pandas and SQLAlchemy are implemented by code inside the benchmark

# Dataset configuration
CSV_FILEPATH = "datasets/nyc_yellow_tiny.csv"


# Postgres configuration
POSTGRES_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

POSTGRES_URL = 'postgresql://postgres:postgres@localhost:5432/postgres'

# Measurement configuration
DEFAULT_BACKEND = 0
PANDAS_BACKEND = 1
SQLALCHEMY_BACKEND = 2

LAUNCH_NUMBER = 1

# Global debug option. Disables exception raising if True
DEBUG = False
