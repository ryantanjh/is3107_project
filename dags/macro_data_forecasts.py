from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG('macro_data_forecasts',
         start_date=datetime(2024, 4, 8),
         schedule_interval='0 0 1 3,6,9,12 *',
         catchup=False) as dag:
    """
    Runs ARIMA model on GDP data and saves forecasts
    """

    from sqlalchemy import create_engine
    import pandas as pd

    create_table = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS finance_data.gdp_forecasts (
                id SERIAL PRIMARY KEY,
                date DATE UNIQUE,
                price FLOAT
            );
            DELETE FROM finance_data.gdp_forecasts; 
        """
    )

    connection_string = f'postgresql+psycopg2://postgres:password@host.docker.internal:5432/is3107_project'
    engine = create_engine(connection_string)
    # Retrieve the last 30 years of data
    with engine.connect() as conn:
        res = conn.execute("""
        SELECT * FROM finance_data.gdp
        ORDER BY date DESC
        LIMIT 120;        
        """)
        rows = res.fetchall()
        df = pd.DataFrame(rows)


    @task()
    def run_arima_model():
        import statsmodels.api as sm
        best_aic = float('inf')
        q = 0
        p = 0
        for i in range(1, 5):
            for k in range(1, 5):
                arima_model = sm.tsa.ARIMA(df['price'], order=(i, 1, k))
                arima_result = arima_model.fit()
                results_summary = arima_result.summary()
                results_df1 = pd.read_html(results_summary.tables[0].as_html(), header=0, index_col=0)[0]
                aic = results_df1.iloc[1, 2]
                if aic <= best_aic:
                    best_aic = aic
                    q = i
                    p = k
        final_arima_model = sm.tsa.ARIMA(df['price'], order=(q, 1, p))
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


    create_table >> run_arima_model()
