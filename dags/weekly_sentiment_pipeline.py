from datetime import datetime, timedelta
import pandas as pd

from airflow.decorators import task, dag

from sqlalchemy import create_engine

connection_string = f'postgresql+psycopg2://postgres:bren1014@:5432'
engine = create_engine(connection_string)

"""
Generates rolling weekly sentiment score by aggregating daily sentiment score of the past week
"""

@dag(dag_id='weekly_sentiment_score_etl_pipeline', start_date = datetime(2024,4,15), schedule_interval= '0 0 * * *', catchup=False) # Runs once every day at midnight
def weekly_sentiment_score_etl_pipeline():
	@task
	def create_table():
		create_table_sql = """
			CREATE SCHEMA IF NOT EXISTS sentiment_data;
            CREATE TABLE IF NOT EXISTS sentiment_data.weekly_sentiment (
            	id SERIAL PRIMARY KEY,
                start_date DATE,
                end_date DATE,
                sentiment_score FLOAT,
                sentiment VARCHAR
            );
        """
		with engine.connect() as connection:
			connection.execute(create_table_sql)

	@task
	def calculate_weekly_sentiment():
		start_of_week = datetime.date(datetime.now() - timedelta(days=7))
		end_of_week = start_of_week + timedelta(days=6)

		sql_check_count_query = f"SELECT COUNT(*) FROM sentiment_data.daily_sentiment WHERE date BETWEEN '{start_of_week}' AND '{end_of_week}'"
		total_count = pd.read_sql(sql_check_count_query, engine).iloc[0, 0]

		if total_count == 7:
			sql_query = f"SELECT sentiment_score FROM sentiment_data.daily_sentiment WHERE date BETWEEN '{start_of_week}' AND {end_of_week}"
			df = pd.read_sql(sql_query,engine)

			weekly_sentiment_score = df['sentiment_score'].mean()

			if weekly_sentiment_score > 0.05:
				weekly_sentiment = "POSITIVE"
			elif weekly_sentiment_score < -0.05:
				weekly_sentiment = "NEGATIVE"
			else:
				weekly_sentiment = "NEUTRAL"
			
		new_row_in_df = pd.DataFrame([[start_of_week,end_of_week,weekly_sentiment_score,weekly_sentiment]],columns=['start_date','end_date','sentiment_score','sentiment'])
		
		new_row_in_df.to_sql('weekly_sentiment',con=engine,schema='sentiment_data',if_exists='append',index=False)


	create_table()
	calculate_weekly_sentiment()

run_dag = weekly_sentiment_score_etl_pipeline()