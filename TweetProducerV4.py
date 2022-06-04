#https://github.com/fepereiro/FirehoseWithComprehend/blob/master/twitter-streaming.py
import boto3
import random
import time
import json
#from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from configparser import ConfigParser

#Initialize the config parser 
config=ConfigParser()
file='./config.ini'
config.read(file)


consumer_key = config['AUTH']['consumer_key']
consumer_secret = config['AUTH']['consumer_secret']
access_token = config['AUTH']['access_token']
access_token_secret = config['AUTH']['access_secret']

#aws_key_id = 
#aws_key =  

DeliveryStreamName = 'twitter_to_firehose'

client = boto3.client('firehose', region_name='us-east-1',
                          #aws_access_key_id=aws_key_id,
                          #aws_secret_access_key=aws_key
                          )

#This is a basic listener that just prints received tweets and put them into the stream.
class StdOutListener(Stream):

    def on_data(self, data):

        client.put_record(DeliveryStreamName=DeliveryStreamName,Record={'Data': json.loads(data)["text"]})

        print(json.loads(data)["text"])

        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=['realmadrid'])