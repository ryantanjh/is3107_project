from datetime import datetime, timedelta
import praw
import pandas as pd
import os

from airflow.decorators import task, dag

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.stem import WordNetLemmatizer
import re

from sqlalchemy import create_engine

connection_string = f'postgresql+psycopg2://postgres:password@host.docker.internal:5432/is3107_project'
engine = create_engine(connection_string)

"""
Scraps Reddit headlines daily, and generate an aggregate sentiment score for that day
"""

@dag(dag_id='daily_sentiment_score_etl_pipeline', start_date = datetime(2024,4,15),schedule_interval= '0 0 * * *', catchup=False) #runs once a day at midnight
def daily_sentiment_score_etl_pipeline():
	@task
	def create_table():
		create_table_sql = """
        	CREATE SCHEMA IF NOT EXISTS sentiment_data;
            CREATE TABLE IF NOT EXISTS sentiment_data.daily_sentiment (
                date DATE PRIMARY KEY,
                sentiment_score FLOAT,
                sentiment VARCHAR
            );
        """

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
	def fetch_reddit_data():
		reddit = praw.Reddit(
	        client_id='MwjT-kwEq6x48sV78Tcw7w',
	        client_secret='eTgMh4hzhXOnoGp-5j1Rc_b2H6XyRA',
	        user_agent='IS3107'
	    )

		listofsubreddits = ['wallstreetbets','investing','stocks','worldnews']

		posts_data = []

		for subreddit in listofsubreddits:
			current_subreddit = reddit.subreddit(subreddit)

		for submission in current_subreddit.hot(limit = 25):
			posts_data.append([submission.title])

		df = pd.DataFrame(posts_data, columns=['Title'])

		folder_path = os.path.join(os.getcwd(),'intermediate_files')
		os.makedirs(folder_path,exist_ok=True)
		csv_file_path = os.path.join(folder_path,'reddit_headlines.csv')

		df.to_csv(csv_file_path, index=False)
		return csv_file_path

	@task
	def generate_and_load_sentiment_score(file_path):
		def remove_stopwords(sentence):
			sentence = re.sub(r'https?://\S+|www\.\S+', '',sentence)
			sentence = re.sub(r'<.*?>', '', sentence)
			sentence = re.sub(r'[^a-zA-Z\s]', '', sentence)
			sentence = sentence.lower()
			word_tokens = nltk.tokenize.word_tokenize(sentence)
			filtered_sentence = [w for w in word_tokens if w.lower() not in stopwords]

			lemmatizer = WordNetLemmatizer()
			filtered_sentence = [lemmatizer.lemmatize(w) for w in filtered_sentence]
			return ' '.join(filtered_sentence)

		def analyze_sentiment(sentence):
			sentiment_score = sia.polarity_scores(sentence)
			return sentiment_score['compound']

		def classify_sentiment(sentiment):
			if sentiment > 0.05:
				return "POSITIVE"
			elif sentiment < -0.05:
				return "NEGATIVE"
			else:
				return "NEUTRAL"

		df = pd.read_csv(file_path)

		stopwords = nltk.corpus.stopwords.words("english")
		sia = SentimentIntensityAnalyzer()

		df['Title'] = df['Title'].apply(remove_stopwords)
		df['Sentiment'] = df['Title'].apply(analyze_sentiment)

		yesterday_date = datetime.today() - timedelta(days=1)
		new_row = [yesterday_date.date(),df['Sentiment'].mean(),classify_sentiment(df['Sentiment'].mean())]

		new_row_in_df = pd.DataFrame([new_row],columns=['date','sentiment_score','sentiment'])

		new_row_in_df.to_sql('daily_sentiment',con=engine,schema='sentiment_data',if_exists='append',index=False)

		#remove old entries if more than 10
		with engine.connect() as connection:
			connection.execute(
				"""
				DELETE FROM sentiment_data.daily_sentiment
				WHERE date NOT IN (
					SELECT date
					FROM sentiment_data.daily_sentiment
					ORDER BY date
					LIMIT 10
				)
				"""
			)

	@task
	def update_mean_sentiment_score():

		get_latest_sentiment_query = """
		SELECT sentiment_score
		FROM sentiment_data.daily_sentiment
		ORDER BY date DESC
		LIMIT 1
		"""
		latest_sentiment_score = pd.read_sql(get_latest_sentiment_query,engine).iloc[0,0]

		get_latest_cume_query = """
		SELECT *
		FROM sentiment_data.cumulative_mean_sentiment
		"""
		get_cume_data = pd.read_sql(get_latest_cume_query,engine)

		updated_mean_daily_sentiment = ((get_cume_data['mean_daily_sentiment']*get_cume_data['num_days_daily_mean'])+latest_sentiment_score)/(get_cume_data['num_days_daily_mean'] + 1)
		updated_num_days_daily_mean = get_cume_data['num_days_daily_mean'] + 1

		updated_cume_data = {
			'mean_daily_sentiment': updated_mean_daily_sentiment,
			'num_days_daily_mean': updated_num_days_daily_mean,
			'mean_weekly_sentiment': get_cume_data['mean_weekly_sentiment'],
			'num_days_weekly_mean': get_cume_data['num_days_weekly_mean']
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
	reddit_data_file_path = fetch_reddit_data()
	generate_and_load_sentiment_score_task = generate_and_load_sentiment_score(reddit_data_file_path)
	update_mean_sentiment_score_task = update_mean_sentiment_score()

	create_table_task >> reddit_data_file_path >> generate_and_load_sentiment_score_task >> update_mean_sentiment_score_task

run_dag = daily_sentiment_score_etl_pipeline()


