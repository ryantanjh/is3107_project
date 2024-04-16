import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from etl.yfinance_scrapper import YFinanceScrapper
from etl.fred_scrapper import FredScapper

with DAG('init_finance_data',
         start_date=datetime(2024, 4, 8),
         schedule_interval=None,
         catchup=False) as dag:
    """
    Initializes financial data from 2000 to current datae
    """
    from sqlalchemy import create_engine

    fred_scrapper = FredScapper()
    yfinances_scrapper = YFinanceScrapper()
    connection_string = f'postgresql+psycopg2://postgres:password@host.docker.internal:5432/is3107_project'
    engine = create_engine(connection_string)
    create_table = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres',
        sql="""
            CREATE SCHEMA IF NOT EXISTS finance_data; 
            CREATE TABLE IF NOT EXISTS finance_data.sp500 (
                id SERIAL PRIMARY KEY,
                date DATE UNIQUE,
                price FLOAT
            );
             CREATE TABLE IF NOT EXISTS finance_data.gdp (
                id SERIAL PRIMARY KEY,
                date DATE UNIQUE,
                price FLOAT
            );
        """
    )

    START_DATE = '2000-01-01'
    END_DATE = datetime.now().strftime('%Y-%m-%d')


    @task()
    def sp500_etl():
        df = yfinances_scrapper.get_sp500_prices(start_date=START_DATE, end_date=END_DATE)
        df.to_sql('sp500', con=engine, schema='finance_data', if_exists='append', index=False)


    @task()
    def gdp_etl():
        df = fred_scrapper.get_fred_gdp(start_date=START_DATE, end_date=END_DATE)
        df.to_sql('gdp', con=engine, schema='finance_data', if_exists='append', index=False)


    create_table >> [sp500_etl(), gdp_etl()]