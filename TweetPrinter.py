import tweepy
import boto3
import json
from configparser import ConfigParser

#Initialize the config parser 
config=ConfigParser()
file='./config.ini'
config.read(file)



consumer_key = config['AUTH']['consumer_key']
consumer_secret = config['AUTH']['consumer_secret']
access_token = config['AUTH']['access_token']
access_token_secret = config['AUTH']['access_secret']



DeliveryStreamName = 'twitter_to_firehose'

client = boto3.client('firehose', region_name='us-east-1')


#This is a basic listener that just prints received tweets and put them into the stream.
class StdOutListener(tweepy.Stream):

    def on_data(self, data):
        try:
            client.put_record(DeliveryStreamName=DeliveryStreamName,Record={'Data': json.loads(data)["text"]})
            print(json.loads(data)["text"])
            return True
        except: 
            pass
    def on_error(self, status):
        print(status)


# Subclass Stream to print IDs of Tweets received
class IDPrinter(tweepy.Stream):

    def on_status(self, status):
        print(status.id)


stream_to_firehose = StdOutListener(
    consumer_key, consumer_secret,
    access_token, access_token_secret
    )

stream_to_firehose.filter(track=["ECB","euro zone", "euro inflation", "supply disruption", "energy shortage", "inflation", "central bank policies", "CPI", "wage inflation", "food price" ])


