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

		with engine.connect() as connection:
			connection.execute(create_table_sql)

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

	create_table()
	reddit_data_path = fetch_reddit_data()
	generate_and_load_sentiment_score(reddit_data_path)

run_dag = daily_sentiment_score_etl_pipeline()

