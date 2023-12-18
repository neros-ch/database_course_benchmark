from app.core import measure
from config import DEBUG, PANDAS_BACKEND


def pandas_run(df):
    try:
        print("Pandas:")
        pandas_helper = Pandas_helper(df)
        queries = [pandas_helper.query1, pandas_helper.query2, pandas_helper.query3, pandas_helper.query4]
        measure(queries, backend=PANDAS_BACKEND)
    except Exception as e:
        print("Unknown error during pandas operation:", e)
        if DEBUG:
            raise e


class Pandas_helper:
    def __init__(self, df):
        self.df = df

    def query1(self):
        pulled_df = self.df[['VendorID']].copy()
        # Grouping strings is a lot slower, than converting to categorical series:
        pulled_df['VendorID'] = pulled_df['VendorID'].astype('category')
        grouped_df = pulled_df.groupby('VendorID', observed=False)
        grouped_df.size().reset_index()

    def query2(self):
        pulled_df = self.df[['passenger_count', 'total_amount']]
        grouped_df = pulled_df.groupby('passenger_count')
        grouped_df.mean().reset_index()

    def query3(self):
        # We copy the view, to be able to modify it
        pulled_df = self.df[['passenger_count', 'tpep_pickup_datetime']].copy()
        pulled_df = self._replace_with_years(pulled_df, 'tpep_pickup_datetime')

        grouped_df = pulled_df.groupby(['passenger_count', 'year'])
        grouped_df.size().reset_index()

    def query4(self):
        # We copy the view, to be able to modify it
        pulled_df = self.df[[
            'passenger_count',
            'tpep_pickup_datetime',
            'trip_distance',
        ]].copy()
        pulled_df['trip_distance'] = pulled_df['trip_distance'].round().astype(int)
        pulled_df = self._replace_with_years(pulled_df, 'tpep_pickup_datetime')

        grouped_df = pulled_df.groupby([
            'passenger_count',
            'year',
            'trip_distance',
        ])
        final_df = grouped_df.size().reset_index()
        final_df = final_df.rename(columns={final_df.columns[-1]: 'counts'})
        final_df.sort_values(
            ['year', 'counts'],
            ascending=[True, False],
        )

    def _replace_with_years(self, df, column_name: str):
        df['year'] = df[column_name].astype('datetime64[s]').dt.year
        df.drop(columns=[column_name])
        return df
