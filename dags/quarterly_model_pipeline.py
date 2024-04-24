import os
import sys
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from airflow.decorators import task

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from helpers.yfinance_scrapper import YFinanceScrapper
from helpers.fred_scrapper import FredScapper
from helpers.capm import get_capm_weights

with DAG('quarterly_model_pipeline',
         start_date=datetime(2024, 4, 8),
         schedule_interval='0 0 1 2,5,8,11 *',
         catchup=False) as dag:
    """
    Quarterly updates for financial data and retrains capm and arima models
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
    def update_gdp_data():
        df = fred_scrapper.get_fred_gdp(start_date=START_DATE, end_date=END_DATE).tail(1)
        df.to_sql('gdp', con=engine, schema='finance_data', if_exists='append', index=False)

    @task()
    def update_returns_data():
        tickers = ["XLK", "XLE", "XLV", "XLF", "XLY", "XLI"]
        df = yfinances_scrapper.get_multiple_tickers_log_returns(tickers, start_date=START_DATE, end_date=END_DATE).tail(1)
        df_long = pd.melt(df, id_vars=['date'], var_name='ticker', value_name='log_return')
        df_long.to_sql('ticker_log_returns', con=engine, schema='finance_data', if_exists='append', index=False)

    from sqlalchemy import create_engine
    import pandas as pd

    create_results_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS finance_data.gdp_forecasts (
                id SERIAL PRIMARY KEY,
                date DATE UNIQUE,
                price FLOAT
            );
            DELETE FROM finance_data.gdp_forecasts; 
            CREATE TABLE IF NOT EXISTS finance_data.capm_weights (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(100),
                weight FLOAT
            );
            DELETE FROM finance_data.capm_weights; 
        """
    )


    @task()
    def run_capm_model():
        with engine.connect() as conn:
            res = conn.execute("""
            SELECT * FROM finance_data.ticker_log_returns       
            """)
            rows = res.fetchall()
            df = pd.DataFrame(rows)
            df.drop('id', axis=1, inplace=True)
            df = df.pivot(index='date', columns='ticker', values='log_return')
        weights_df = get_capm_weights(df)
        weights_df.to_sql('capm_weights', con=engine, schema='finance_data', if_exists='append', index=False)

    @task()
    def run_arima_model():
        import statsmodels.api as sm
        with engine.connect() as conn:
            res = conn.execute("""
            SELECT * FROM finance_data.gdp
            ORDER BY date DESC
            LIMIT 120;        
            """)
            rows = res.fetchall()
            df = pd.DataFrame(rows)
            df['date'] = pd.to_datetime(df['date'])
            df_ascending = df.sort_values(by='date', ascending=True)
        best_aic = float('inf')
        q = 0
        p = 0
        for i in range(1, 5):
            for k in range(1, 5):
                arima_model = sm.tsa.ARIMA(df_ascending['price'], order=(i, 1, k))
                arima_result = arima_model.fit()
                results_summary = arima_result.summary()
                results_df1 = pd.read_html(results_summary.tables[0].as_html(), header=0, index_col=0)[0]
                aic = results_df1.iloc[1, 2]
                if aic <= best_aic:
                    best_aic = aic
                    q = i
                    p = k
        final_arima_model = sm.tsa.ARIMA(df_ascending['price'], order=(q, 1, p))
        final_arima_result = final_arima_model.fit()
        forecasts = final_arima_result.forecast(steps=4)

        # generate new dates
        last_date = df['date'].iloc[0]
        next_quarter_start = last_date + pd.DateOffset(months=3)
        forecast_dates = pd.date_range(start=next_quarter_start, periods=4, freq='Q')
        adjusted_dates = forecast_dates - pd.DateOffset(months=2)
        forecast_df = pd.DataFrame(columns=['date', 'price'])
        forecasts = forecasts.reset_index(drop=True)
        forecast_df['date'] = adjusted_dates
        forecast_df['price'] = forecasts

        forecast_df.to_sql('gdp_forecasts', con=engine, schema='finance_data', if_exists='append', index=False)


    [update_returns_data(), update_gdp_data()] >> create_results_tables >> [run_capm_model(), run_arima_model()]
