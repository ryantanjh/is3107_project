from yfinance_scrapper import YFinanceScrapper


def get_sp500_prices(start_date='2015-01-01', end_date='2024-02-28'):
    """Returns dataframe containing sp500 adj close historical prices"""
    data = YFinanceScrapper().get_data_for_ticker('^GSPC', start_date, end_date)
    data.set_index('Date', inplace=True)
    data = (data[['Adj Close']]).rename({'Adj Close': 'SP500'}, axis=1)
    return data
