import pandas as pd
import requests_cache
import yfinance as yf


class YFinanceScrapper:
    def get_data_for_ticker(self, ticker, start_date='2015-01-01', end_date='2024-02-28') -> pd.DataFrame:
        """Scrapes y finance api for given ticker"""
        session = requests_cache.CachedSession(cache_name='cache', backend='sqlite')
        session.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
                           'Accept': 'application/json;charset=utf-8'}
        data = yf.download(ticker, start=start_date, end=end_date, session=session)
        data.insert(0, 'Ticker', ticker)
        data = data.reset_index()
        return data
