import pandas as pd

from fred import FredScapper


def get_fred_gdp(start_date='2000-01-01', end_date='2020-01-01') -> pd.DataFrame:
    """
    Returns quarterly US GDP data in billions of dollars
    """
    scrapper = FredScapper()
    gdp_data = scrapper.get_fred_data('GDP')
    gdp_data.set_index('DATE', inplace=True)
    gdp_data.index = pd.to_datetime(gdp_data.index)
    gdp_data = gdp_data[(gdp_data.index >= start_date) & (gdp_data.index <= end_date)]
    return gdp_data
