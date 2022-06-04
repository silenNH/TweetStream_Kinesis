#https://dwbi.org/pages/236
import boto3
import json
import os
import tweepy
import uuid
from configparser import ConfigParser

#Initialize the config parser 
config=ConfigParser()
file='./config.ini'
config.read(file)


consumer_key = config['AUTH']['consumer_key']
consumer_secret = config['AUTH']['consumer_key']
access_token = config['AUTH']['access_token']
access_token_secret = config['AUTH']['access_secret']

kinesis_stream_name = 'twitter_kinesis_stream'
twitter_filter_tag = config['TOPIC']['topic']


class StreamingTweets(tweepy.Stream):
    def on_status(self, status):
        print("Hello")
        data = {
            'tweet_text': status.text,
            'created_at': str(status.created_at),
            'user_id': status.user.id,
            'user_name': status.user.name,
            'user_screen_name': status.user.screen_name,
            'user_description': status.user.description,
            'user_location': status.user.location,
            'user_followers_count': status.user.followers_count,
            'tweet_body': json.dumps(status._json)
        }
        print(data)
        response = kinesis_client.put_record(
            StreamName=kinesis_stream_name,
            Data=json.dumps(data),
            PartitionKey=partition_key)

        print('Status: ' +
              json.dumps(response['ResponseMetadata']['HTTPStatusCode']))

    def on_error(self, status):
        print(status)


session = boto3.Session()
print(session)
kinesis_client = session.client('kinesis', region_name='us-east-1')
partition_key = str(uuid.uuid4())

stream = StreamingTweets(
    consumer_key, consumer_secret,
    access_token, access_token_secret
)

stream.filter(track=[twitter_filter_tag])


# Tweepy & boto3 needs to be installed 