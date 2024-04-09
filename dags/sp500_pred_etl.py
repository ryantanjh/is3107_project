import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from etl.yfinance_scrapper import YFinanceScrapper
from etl.fred_scrapper import FredScapper

with DAG('sp500_pred_etl',
         start_date=datetime(2024, 4, 8),
         schedule_interval=None,
         catchup=False) as dag:
    from sqlalchemy import create_engine

    fred_scrapper = FredScapper()
    yfinances_scrapper = YFinanceScrapper()
    connection_string = f'postgresql+psycopg2://postgres:password@host.docker.internal:5432/is3107_project'
    engine = create_engine(connection_string)
    create_table = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres',
        sql="""
            CREATE SCHEMA IF NOT EXISTS sp500_pred; 
            CREATE TABLE IF NOT EXISTS sp500_pred.sp500 (
                id SERIAL PRIMARY KEY,
                date DATE,
                price FLOAT
            );
             CREATE TABLE IF NOT EXISTS sp500_pred.gdp (
                id SERIAL PRIMARY KEY,
                date DATE,
                price FLOAT
            );
             CREATE TABLE IF NOT EXISTS sp500_pred.interest_rate (
                id SERIAL PRIMARY KEY,
                date DATE,
                price FLOAT
            );
             CREATE TABLE IF NOT EXISTS sp500_pred.unrate (
                id SERIAL PRIMARY KEY,
                date DATE,
                price FLOAT
            );
             CREATE TABLE IF NOT EXISTS sp500_pred.cpi (
                id SERIAL PRIMARY KEY,
                date DATE,
                price FLOAT
            );
        """
    )


    @task()
    def sp500_etl():
        df = yfinances_scrapper.get_sp500_prices()
        df.to_sql('sp500', con=engine, schema='sp500_pred', if_exists='append', index=False)


    @task()
    def gdp_etl():
        df = fred_scrapper.get_fred_gdp()
        df.to_sql('gdp', con=engine, schema='sp500_pred', if_exists='append', index=False)


    @task()
    def interest_rate_etl():
        df = fred_scrapper.get_fred_interest_rate()
        df.to_sql('interest_rate', con=engine, schema='sp500_pred', if_exists='append', index=False)


    @task()
    def unrate_etl():
        df = fred_scrapper.get_fred_unrate()
        df.to_sql('unrate', con=engine, schema='sp500_pred', if_exists='append', index=False)


    @task()
    def cpi_etl():
        df = fred_scrapper.get_fred_cpi()
        df.to_sql('cpi', con=engine, schema='sp500_pred', if_exists='append', index=False)


    create_table >> [sp500_etl(), gdp_etl(), interest_rate_etl(), unrate_etl(), cpi_etl()]
