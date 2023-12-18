import pandas as pd
from config import CSV_FILEPATH, POSTGRES_IMPORT, POSTGRES_RUN, SQLITE_RUN, DUCKDB_RUN, PANDAS_RUN, SQLALCHEMY_RUN
from app.postgres_benchmark import postgres_import, postgres_run
from app.sqlite_benchmark import sqlite_run
from app.duckdb_benchmark import duckdb_run
from app.pandas_benchmark import pandas_run
from app.sqlalchemy_benchmark import sqlalchemy_run

print("Reading dataset to a Pandas dataframe...")
df = pd.read_csv(CSV_FILEPATH)

if POSTGRES_IMPORT:
    postgres_import(df)

if POSTGRES_RUN:
    postgres_run()

if SQLITE_RUN:
    sqlite_run(df)

if DUCKDB_RUN:
    duckdb_run()

if PANDAS_RUN:
    pandas_run(df)

if SQLALCHEMY_RUN:
    sqlalchemy_run(df)
