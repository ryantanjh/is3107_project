from datetime import datetime, timedelta
import pandas as pd

from airflow.decorators import task, dag

from sqlalchemy import create_engine

connection_string = f'postgresql+psycopg2://postgres:password@host.docker.internal:5432/is3107_project'
engine = create_engine(connection_string)

"""
Generates rolling weekly sentiment score by aggregating daily sentiment score of the past week
"""

@dag(dag_id='weekly_sentiment_score_etl_pipeline', start_date = datetime(2024,4,15), schedule_interval= '15 0 * * *', catchup=False) # Runs once every day at 12:15am (buffer time for daily_sentiment_score to be calculated)
def weekly_sentiment_score_etl_pipeline():
	@task
	def create_table():
		create_table_sql = """
			CREATE SCHEMA IF NOT EXISTS sentiment_data;
            CREATE TABLE IF NOT EXISTS sentiment_data.weekly_sentiment (
                start_date DATE,
                end_date DATE PRIMARY KEY,
                sentiment_score FLOAT,
                sentiment VARCHAR
            );
        """

		#cumulative mean table
		create_table_sql_2 = """
			CREATE SCHEMA IF NOT EXISTS sentiment_data;
            CREATE TABLE IF NOT EXISTS sentiment_data.cumulative_mean_sentiment (
            	id SERIAL PRIMARY KEY,
                mean_daily_sentiment FLOAT,
				num_days_daily_mean INTEGER,
                mean_weekly_sentiment FLOAT,
				num_days_weekly_mean INTEGER 
            );
        """

		with engine.connect() as connection:
			connection.execute(create_table_sql)
			connection.execute(create_table_sql_2)

	@task
	def calculate_weekly_sentiment():
		start_of_week = datetime.date(datetime.now() - timedelta(days=7))
		end_of_week = start_of_week + timedelta(days=6)

		sql_check_count_query = f"SELECT COUNT(*) FROM sentiment_data.daily_sentiment WHERE date BETWEEN '{start_of_week}' AND '{end_of_week}'"
		total_count = pd.read_sql(sql_check_count_query, engine).iloc[0, 0]

		if total_count == 7:
			sql_query = f"SELECT sentiment_score FROM sentiment_data.daily_sentiment WHERE date BETWEEN '{start_of_week}' AND '{end_of_week}'"
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

		#remove old entries if more than 3
		with engine.connect() as connection:
			connection.execute(
				"""
				DELETE FROM sentiment_data.weekly_sentiment
				WHERE end_date IN (
					SELECT end_date
					FROM sentiment_data.weekly_sentiment
					ORDER BY end_date DESC
					LIMIT 3 OFFSET 3
				)
				"""
			)

	@task
	def update_mean_sentiment_score():

		get_latest_sentiment_query = """
		SELECT sentiment_score
		FROM sentiment_data.weekly_sentiment
		ORDER BY end_date DESC
		LIMIT 1
		"""
		latest_sentiment_score = pd.read_sql(get_latest_sentiment_query,engine).iloc[0,0]

		get_latest_cume_query = """
		SELECT *
		FROM sentiment_data.cumulative_mean_sentiment
		"""
		get_cume_data = pd.read_sql(get_latest_cume_query,engine)

		updated_cume_data = {
			'mean_daily_sentiment': get_cume_data['mean_daily_sentiment'],
			'num_days_daily_mean': get_cume_data['num_days_daily_mean'],
			'mean_weekly_sentiment': ((get_cume_data['mean_weekly_sentiment']*get_cume_data['num_days_weekly_mean'])+latest_sentiment_score)/(get_cume_data['num_days_weekly_mean'] + 1),
			'num_days_weekly_mean': get_cume_data['num_days_weekly_mean'] + 1,
		}

		df = pd.DataFrame(updated_cume_data)

		df.to_sql('cumulative_mean_sentiment',con=engine,schema='sentiment_data',if_exists='append',index=False)

		with engine.connect() as connection:
			connection.execute(
				"""
				DELETE FROM sentiment_data.cumulative_mean_sentiment
				WHERE id NOT IN (
					SELECT id
					FROM sentiment_data.cumulative_mean_sentiment
					ORDER BY id DESC
					LIMIT 1
				)
				"""
			)

	create_table_task = create_table()
	calculate_weekly_sentiment_task = calculate_weekly_sentiment()
	update_mean_sentiment_score_task = update_mean_sentiment_score()

	create_table_task >> calculate_weekly_sentiment_task >> update_mean_sentiment_score_task

run_dag = weekly_sentiment_score_etl_pipeline()