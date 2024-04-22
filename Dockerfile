FROM apache/airflow:2.8.4
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN python -m nltk.downloader stopwords vader_lexicon punkt wordnet