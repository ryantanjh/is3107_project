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
For historical data
Initialises SQL tables, and get historical Reddit headlines, and generates daily aggregate sentiment score.
"""

@dag(dag_id='historical_sentiment_score_etl_pipeline', start_date = datetime(2023,3,31),schedule= None, catchup=False) #once off run to get historical data
def historical_sentiment_score_etl_pipeline():
	@task
	def create_tables():
		#daily sentiment table
		create_table_sql_1 = """
        	CREATE SCHEMA IF NOT EXISTS sentiment_data;
            CREATE TABLE IF NOT EXISTS sentiment_data.daily_sentiment (
                date DATE PRIMARY KEY,
                sentiment_score FLOAT,
                sentiment VARCHAR
            );
        """

        #weekly sentiment table
		create_table_sql_2 = """
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
			connection.execute(create_table_sql_1)
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
			top_posts = []
			
			for submission in current_subreddit.top(time_filter="month",limit=1000):
				top_posts.append([submission.created_utc,submission.title])
				
			for i in range(2,8):
				date = datetime.now().date() - timedelta(days=i)
				start_timestamp = int(datetime(date.year,date.month,date.day,0,0,0).timestamp())
				end_timestamp = start_timestamp + 86400
				count = 0
				for post in top_posts:
					if count == 25:
						break
					if start_timestamp <= post[0] <= end_timestamp:
						count += 1
						posts_data.append(post)
						
		df = pd.DataFrame(posts_data, columns=['Date','Title'])
		folder_path = os.path.join(os.getcwd(),'intermediate_files')
		os.makedirs(folder_path,exist_ok=True)
		csv_file_path = os.path.join(folder_path,'historical_reddit_headlines.csv')
		df.to_csv(csv_file_path, index=False)
		
		return csv_file_path

	@task
	def generate_and_load_sentiment_scores(file_path):
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
		
		df['Date'] = df['Date'].apply(lambda x: datetime.fromtimestamp(x).date())
		df['Title'] = df['Title'].apply(remove_stopwords)
		df['sentiment_score'] = df['Title'].apply(analyze_sentiment)
		
		average_sentiment_per_date = df.groupby('Date')['sentiment_score'].mean().reset_index()
		average_sentiment_per_date.columns = ['date', 'sentiment_score']
		average_sentiment_per_date['sentiment'] = average_sentiment_per_date['sentiment_score'].apply(classify_sentiment)
		average_sentiment_per_date.to_sql('daily_sentiment',con=engine,schema='sentiment_data',if_exists='append',index=False)

	create_tables()
	reddit_data_path = fetch_reddit_data()
	generate_and_load_sentiment_scores(reddit_data_path)

run_dag = historical_sentiment_score_etl_pipeline()