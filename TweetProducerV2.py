#https://gist.github.com/Chancylin/ebe115ebd58c3161949e33ce957334de
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
import time
from configparser import ConfigParser

#Initialize the config parser 
config=ConfigParser()
file='./config.ini'
config.read(file)


consumer_key = config['AUTH']['consumer_key']
consumer_secret = config['AUTH']['consumer_key']
access_token = config['AUTH']['access_token']
access_token_secret = config['AUTH']['access_secret']




import twitter_credentials
from helper.extract_tweet_info import extract_tweet_info
from config import stream_name


class TweetStreamListener(StreamListener):
    # on success
    def on_data(self, data):
        """Overload this method to have some processing on the data before putting it into kiensis data stream
        """
        tweet = json.loads(data)

        try:
            payload = extract_tweet_info(tweet)
            # only put the record when message is not None
            if (payload):
                # print(payload)
                # note that payload is a list
                put_response = kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=payload,
                    PartitionKey=str(tweet['user']['screen_name'])
                )

            return True
        except (AttributeError, Exception) as e:
            print(e)


    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # create kinesis client connection
    session = boto3.Session(profile_name='your_aws_profile')

    # create the kinesis client
    kinesis_client = session.client('kinesis', region_name='us-east-1')

    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    while True:
        try:
            print('Twitter streaming...')

            # create instance of the tweet stream listener
            myStreamlistener = TweetStreamListener()

            # create instance of the tweepy stream
            stream = Stream(auth=auth, listener=myStreamlistener)

            # search twitter for the keyword
            stream.filter(track=["#AI", "#MachineLearning"], languages=['en'], stall_warnings=True)
        except Exception as e:
            print(e)
            print('Disconnected...')
            time.sleep(5)
            continue