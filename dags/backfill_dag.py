import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from helpers.yfinance_scrapper import YFinanceScrapper
from helpers.fred_scrapper import FredScapper
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.stem import WordNetLemmatizer
import re
import praw

with DAG('backfill_dag',
         start_date=datetime(2024, 4, 8),
         schedule_interval=None,
         catchup=False) as dag:
    """
    Creates all tables and initializes historical data
    """
    from sqlalchemy import create_engine
    import pandas as pd

    fred_scrapper = FredScapper()
    yfinances_scrapper = YFinanceScrapper()
    connection_string = f'postgresql+psycopg2://postgres:password@host.docker.internal:5432/is3107_project'
    engine = create_engine(connection_string)
    create_table = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres',
        sql="""
            CREATE SCHEMA IF NOT EXISTS finance_data; 
            CREATE SCHEMA IF NOT EXISTS sentiment_data;
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
            CREATE TABLE IF NOT EXISTS finance_data.ticker_log_returns (
                id SERIAL PRIMARY KEY,
                date DATE,
                ticker VARCHAR(100),
                log_return FLOAT
            );
            CREATE TABLE IF NOT EXISTS sentiment_data.daily_sentiment (
                date DATE PRIMARY KEY,
                sentiment_score FLOAT,
                sentiment VARCHAR
            );
            CREATE TABLE IF NOT EXISTS sentiment_data.weekly_sentiment (
                start_date DATE,
                end_date DATE PRIMARY KEY,
                sentiment_score FLOAT,
                sentiment VARCHAR
            );
            CREATE TABLE IF NOT EXISTS sentiment_data.cumulative_mean_sentiment (
            	id SERIAL PRIMARY KEY,
                mean_daily_sentiment FLOAT,
				num_days_daily_mean INTEGER,
                mean_weekly_sentiment FLOAT,
				num_days_weekly_mean INTEGER
            );
        """
    )

    START_DATE = '1980-01-01'
    END_DATE = '2023-12-31'


    @task()
    def backfill_ticker_returns_data():
        tickers = ["XLK", "XLE", "XLV", "XLF", "XLY", "XLI"]
        df = yfinances_scrapper.get_multiple_tickers_log_returns(tickers, start_date=START_DATE, end_date=END_DATE)
        df_long = pd.melt(df, id_vars=['date'], var_name='ticker', value_name='log_return')
        df_long.to_sql('ticker_log_returns', con=engine, schema='finance_data', if_exists='append', index=False)


    @task()
    def backfill_gdp_data():
        df = fred_scrapper.get_fred_gdp(start_date=START_DATE, end_date=END_DATE)
        df.to_sql('gdp', con=engine, schema='finance_data', if_exists='append', index=False)

    @task
    def backfill_reddit_data():
        reddit = praw.Reddit(
            client_id='MwjT-kwEq6x48sV78Tcw7w',
            client_secret='eTgMh4hzhXOnoGp-5j1Rc_b2H6XyRA',
            user_agent='IS3107'
        )

        listofsubreddits = ['wallstreetbets', 'investing', 'stocks', 'worldnews']
        posts_data = []

        for subreddit in listofsubreddits:
            current_subreddit = reddit.subreddit(subreddit)
            top_posts = []

            for submission in current_subreddit.top(time_filter="month", limit=1000):
                top_posts.append([submission.created_utc, submission.title])

            for i in range(2, 8):
                date = datetime.now().date() - timedelta(days=i)
                start_timestamp = int(datetime(date.year, date.month, date.day, 0, 0, 0).timestamp())
                end_timestamp = start_timestamp + 86400
                count = 0
                for post in top_posts:
                    if count == 25:
                        break
                    if start_timestamp <= post[0] <= end_timestamp:
                        count += 1
                        posts_data.append(post)

        df = pd.DataFrame(posts_data, columns=['Date', 'Title'])
        folder_path = os.path.join(os.getcwd(), 'intermediate_files')
        os.makedirs(folder_path, exist_ok=True)
        csv_file_path = os.path.join(folder_path, 'historical_reddit_headlines.csv')
        df.to_csv(csv_file_path, index=False)

        return csv_file_path

    @task
    def generate_and_load_sentiment_scores(file_path):
        def remove_stopwords(sentence):
            sentence = re.sub(r'https?://\S+|www\.\S+', '', sentence)
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
        average_sentiment_per_date['sentiment'] = average_sentiment_per_date['sentiment_score'].apply(
            classify_sentiment)
        average_sentiment_per_date.to_sql('daily_sentiment', con=engine, schema='sentiment_data',
                                          if_exists='append', index=False)

        cume_average_sentiment = average_sentiment_per_date['sentiment_score'].mean()
        num_days = 6

        updated_cume_values = pd.DataFrame({
            'mean_daily_sentiment': [cume_average_sentiment],
            'num_days_daily_mean': [num_days],
            'mean_weekly_sentiment': [0],
            'num_days_weekly_mean': [0]
        })
        updated_cume_values.to_sql('cumulative_mean_sentiment', con=engine, schema='sentiment_data',
                                   if_exists='append', index=False)


    reddit_data_file_path = backfill_reddit_data()
    generate_and_load_sentiment_scores_task = generate_and_load_sentiment_scores(reddit_data_file_path)
    create_table >> [backfill_ticker_returns_data(), backfill_gdp_data()]
    create_table >> reddit_data_file_path >> generate_and_load_sentiment_scores_task