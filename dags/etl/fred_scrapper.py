from io import StringIO

import pandas as pd
import requests


class FredScapper:
    def __init__(self):
        self.url = 'https://fred.stlouisfed.org/graph/fredgraph.csv'

    def get_fred_data(self, fred_id):
        """
        Returns pandas dataframe of historical data from FRED
        """
        params = {
            'id': fred_id,
        }
        r = requests.get(self.url, params=params)
        if r.status_code == 200:
            df = pd.read_csv(StringIO(str(r.content, 'utf-8')))
            return df

    def get_fred_gdp(self, start_date='2000-01-01', end_date='2020-01-01') -> pd.DataFrame:
        """
        Returns quarterly US GDP data in billions of dollars
        """
        df = self.get_fred_data('GDPC1')
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df[(df['DATE'] >= start_date) & (df['DATE'] <= end_date)]
        df.rename({'DATE': 'date', 'GDPC1': 'price'}, axis=1, inplace=True)
        return df

    def get_fred_cpi(self, start_date='2000-01-01', end_date='2020-01-01') -> pd.DataFrame:
        """
        Returns US CPI data
        """
        df = self.get_fred_data('CPALTT01USM657N')
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df[(df['DATE'] >= start_date) & (df['DATE'] <= end_date)]
        df.rename({'DATE': 'date', 'CPALTT01USM657N': 'price'}, axis=1, inplace=True)
        return df

    def get_fred_unrate(self, start_date='2000-01-01', end_date='2020-01-01') -> pd.DataFrame:
        """
        Returns US unemployment rate
        """
        df = self.get_fred_data('UNRATE')
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df[(df['DATE'] >= start_date) & (df['DATE'] <= end_date)]
        df.rename({'DATE': 'date', 'UNRATE': 'price'}, axis=1, inplace=True)
        return df

    def get_fred_interest_rate(self, start_date='2000-01-01', end_date='2020-01-01') -> pd.DataFrame:
        """
        Returns US interest rates
        """
        df = self.get_fred_data('DFF')
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df[(df['DATE'] >= start_date) & (df['DATE'] <= end_date)]
        df.rename({'DATE': 'date', 'DFF': 'price'}, axis=1, inplace=True)
        return df
