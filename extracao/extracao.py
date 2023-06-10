import tweepy
import json

# Abra o arquivo contendo as credenciais
with open(r'C:\Users\clara\pipeline_twitter\keys.json') as file:
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
    
    def extract_data(self, q:str, lang:str, count:int, geocode = None):
        apitwitter = self._auth()
        tweet_data = []
        try:
            tweets = apitwitter.search_tweets(q=q, lang=lang, count=count )

            for tweet in tweets:
                created_at = tweet.created_at
                text = tweet.text
                user = tweet.user
                retweet_count = tweet.retweet_count
                favorite_count = tweet.favorite_count
                tweet_data.append((created_at, text, user, retweet_count, favorite_count))
        except tweepy.TweepyException as te:
            print(f"Erro ao fazer a solicitação à API do Twitter: {str(te)}")

        

if __name__ == '__main__':
    tw = TwitterAPI()
    print(tw.extract_data(q='lula', lang='pt', count=1))
