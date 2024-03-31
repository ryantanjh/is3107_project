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
