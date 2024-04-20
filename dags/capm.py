from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from helpers.capm import get_capm_weights

with DAG('capm',
         start_date=datetime(2024, 4, 8),
         schedule_interval='0 0 1 3,6,9,12 *',
         catchup=False) as dag:
    """
    Runs CAPM model to get portfolio weights
    """

    from sqlalchemy import create_engine
    import pandas as pd

    create_table = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS finance_data.capm_weights (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(100),
                weight FLOAT
            );
            DELETE FROM finance_data.capm_weights; 
        """
    )

    connection_string = f'postgresql+psycopg2://postgres:password@host.docker.internal:5432/is3107_project'
    engine = create_engine(connection_string)

    with engine.connect() as conn:
        res = conn.execute("""
        SELECT * FROM finance_data.ticker_log_returns       
        """)
        rows = res.fetchall()
        df = pd.DataFrame(rows)
        df.drop('id', axis=1, inplace=True)
        df = df.pivot(index='date', columns='ticker', values='log_return')

    @task()
    def run_capm():
        weights_df = get_capm_weights(df)
        weights_df.to_sql('capm_weights', con=engine, schema='finance_data', if_exists='append', index=False)


    create_table >> run_capm()
