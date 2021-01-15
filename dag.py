import os
import pandas as pd
import tweepy
from textblob import TextBlob
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 14)
}

dag = DAG(
    'ms3',
    default_args=default_args,
    description='',
    schedule_interval='@hourly'
)


def ms3():
    auth = tweepy.OAuthHandler(os.environ["CONSUMER_KEY"], os.environ["CONSUMER_SECRET"])
    auth.set_access_token(os.environ["ACCESS_TOKEN"], os.environ["ACCESS_TOKEN_SECRET"])
    api = tweepy.API(auth)

    happy_country_name = 'Finland'
    happy_country = api.geo_search(
        query=happy_country_name, granularity='country')
    happy_country_id = happy_country[0].id
    happy_tweets = api.search(q='place:{}'.format(happy_country_id), count=100)
    happy_tweets_scores = []
    for tweet in happy_tweets:
        text = TextBlob(tweet.text)
        polarity = text.sentiment.polarity
        if polarity != 0:
            happy_tweets_scores.append(polarity)
    happy_scores_df_path = '/c/users/peter/airflowhome/dags/data/{}.csv'.format(
        happy_country_name)
    happy_scores_df = pd.read_csv(happy_scores_df_path)
    happy_tweets_scores_df = pd.DataFrame(
        happy_tweets_scores, columns=['score'])
    happy_scores_df = pd.concat(
        [happy_scores_df, happy_tweets_scores_df], ignore_index=True)
    happy_scores_df.to_csv(happy_scores_df_path, index=False)

    sad_country_name = 'Afghanistan'
    sad_country = api.geo_search(query=sad_country_name, granularity='country')
    sad_country_id = sad_country[0].id
    sad_tweets = api.search(q='place:{}'.format(sad_country_id), count=100)
    sad_tweets_scores = []
    for tweet in sad_tweets:
        text = TextBlob(tweet.text)
        polarity = text.sentiment.polarity
        if polarity != 0:
            sad_tweets_scores.append(polarity)
    sad_scores_df_path = '/c/users/peter/airflowhome/dags/data/{}.csv'.format(
        sad_country_name)
    sad_scores_df = pd.read_csv(sad_scores_df_path)
    sad_tweets_scores_df = pd.DataFrame(sad_tweets_scores, columns=['score'])
    sad_scores_df = pd.concat(
        [sad_scores_df, sad_tweets_scores_df], ignore_index=True)
    sad_scores_df.to_csv(sad_scores_df_path, index=False)


t = PythonOperator(
    task_id='ms3',
    python_callable=ms3,
    dag=dag,
)

t
