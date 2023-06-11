import tweepy
import json
import pandas as pd
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, StructField, StructType, DateType
from pyspark.sql.functions import to_utc_timestamp
from pyspark.sql.functions import count


spark = SparkSession.builder.getOrCreate()# Abra o arquivo contendo as credenciais
with open(r'/Users/tuanymariah/Documents/pipeline_twitter/keys.json') as file:
    data = json.load(file)

TWITTER_CONSUMER_KEY = data['TWITTER_CONSUMER_KEY']
TWITTER_CONSUMER_SECRET = data['TWITTER_CONSUMER_SECRET']
TWITTER_ACCESS_TOKEN = data['TWITTER_ACCESS_TOKEN']
TWITTER_ACCESS_TOKEN_SECRET = data['TWITTER_ACCESS_TOKEN_SECRET']


class TwitterAPI:
      

    def _auth(self):
        auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
        auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)

        api = tweepy.API(auth, wait_on_rate_limit=True)
        return api


    def extract_data(self, q: str, lang: str, count: int, geocode=None) -> DataFrame :
        apitwitter = self._auth()
        tweet_data = []

        tweets = apitwitter.search_tweets(q=q, lang=lang, count=count)

        for tweet in tweets:
            created_at = tweet.created_at
            text = tweet.text
            user = tweet.user.name
            retweet_count = tweet.retweet_count
            favorite_count = tweet.favorite_count

            tweet_data.append((created_at, text, user, retweet_count, favorite_count))

        schema = StructType([
            StructField("created_at", DateType(), True),
            StructField("text", StringType(), True),
            StructField("user", StringType(), True),
            StructField("retweet_count", StringType(), True),
            StructField("favorite_count", StringType(), True)
        ])
        df = spark.createDataFrame(tweet_data, schema)
        df = df.withColumn("created_at", df["created_at"].cast("string"))
        #df.write.mode("overwrite").option("header", "true").csv("/Users/tuanymariah/Documents/pipeline_twitter/data/etl.csv")
        return df
 

if __name__ == '__main__':
    tw = TwitterAPI()
    df = tw.extract_data(q='lula', lang='pt', count=100)

