import tweepy
import sys
print(sys.path)
sys.path.append('/Users/tuanymariah/Documents/pipeline_twitter')

import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, StructField, StructType, DateType, BooleanType, TimestampType
from lib.read_write import Read_Write

spark = SparkSession.builder.getOrCreate()# Abra o arquivo contendo as credenciais

with open(r'keys.json') as file:
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
            source = tweet.source
            favorited = tweet.favorited
            retweeted = tweet.retweeted

            tweet_data.append((created_at, text, user,retweet_count, favorite_count, source, favorited, retweeted))

        schema = StructType([
            StructField("created_at", DateType(), True),
            StructField("text", StringType(), True),
            StructField("user", StringType(), True),
            StructField("retweet_count", StringType(), True),
            StructField("favorite_count", StringType(), True),
            StructField("os", StringType(), True),
            StructField("favorite", BooleanType(), True),
            StructField("retweeted", BooleanType(), True)
        ])
        df = spark.createDataFrame(tweet_data, schema)
        df = df.withColumn("created_at", df["created_at"].cast("string"))
        Read_Write().write_parquet(df, f'./data/{lang}/raw/')
        return df
 

if __name__ == '__main__':
    tw = TwitterAPI()
    df = tw.extract_data(q='lula', lang='en', count=20)
    print(df.show(n=10, truncate=False))
