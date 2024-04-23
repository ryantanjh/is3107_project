import pandas as pd
import requests_cache
import yfinance as yf
import numpy as np


class YFinanceScrapper:
    def get_data_for_ticker(self, ticker, start_date='2015-01-01', end_date='2024-02-28') -> pd.DataFrame:
        """Scrapes y finance api for given ticker"""
        session = requests_cache.CachedSession(cache_name='cache', backend='sqlite')
        session.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
                           'Accept': 'application/json;charset=utf-8'}
        data = yf.download(ticker, start=start_date, end=end_date, session=session)
        data['Prev Close'] = data['Adj Close'].shift(1)
        data['log_return'] = np.log(data['Adj Close'] / data['Prev Close'])
        data = data.iloc[1:].fillna(0)
        data = data.reset_index()
        return data

    def get_multiple_tickers_log_returns(self, tickers, start_date='2015-01-01', end_date='2024-02-28'):
        """Returns dataframe with adj close data for tickers"""
        combined_df = pd.DataFrame()
        for ticker in tickers:
            df = YFinanceScrapper().get_data_for_ticker(ticker, start_date, end_date)
            df = (df[['Date', 'log_return']]).rename({'log_return': ticker, 'Date': 'date'}, axis=1)
            if combined_df.empty:
                combined_df = df
            else:
                combined_df = combined_df.merge(df, on='date')
        return combined_df

