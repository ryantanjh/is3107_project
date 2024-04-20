import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from helpers.yfinance_scrapper import YFinanceScrapper
from helpers.fred_scrapper import FredScapper

with DAG('finance_data_etl',
         start_date=datetime(2024, 4, 8),
         schedule_interval='0 0 1 2,5,8,11 *',
         catchup=False) as dag:
    """
    Updates finance data every quarter
    """
    from sqlalchemy import create_engine
    import pandas as pd

    fred_scrapper = FredScapper()
    yfinances_scrapper = YFinanceScrapper()
    connection_string = f'postgresql+psycopg2://postgres:password@host.docker.internal:5432/is3107_project'
    engine = create_engine(connection_string)

    START_DATE = datetime(datetime.now().year, 1, 1).strftime('%Y-%m-%d')
    END_DATE = datetime.now().strftime('%Y-%m-%d')


    @task()
    def gdp_etl():
        df = fred_scrapper.get_fred_gdp(start_date=START_DATE, end_date=END_DATE).tail(1)
        df.to_sql('gdp', con=engine, schema='finance_data', if_exists='append', index=False)

    @task()
    def capm_data_etl():
        tickers = ["XLK", "XLE", "XLV", "XLF", "XLY", "XLI"]
        df = yfinances_scrapper.get_multiple_tickers_price(tickers, start_date=START_DATE, end_date=END_DATE).tail(1)
        df_long = pd.melt(df, id_vars=['date'], var_name='ticker', value_name='log_return')
        df_long.to_sql('ticker_log_returns', con=engine, schema='finance_data', if_exists='append', index=False)


    gdp_etl() >> capm_data_etl()
