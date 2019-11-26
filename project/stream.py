from tweepy import Stream
from tweepy import StreamListener
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from kafka import KafkaProducer

import pickle
import argparse


HASH_TAG = []
TOPIC = ''

IP = 'localhost'
PORT = '9092'
producer = None

ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''


def set_kafka():
    global producer
    producer = KafkaProducer(bootstrap_servers=['%s:%s' % (IP, PORT)])


def credentials_load():
    # twitter app token key and consumer key
    data = pickle.load(open("creds.p", 'rb'))
    global ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET
    ACCESS_TOKEN = data['ACCESS_TOKEN']
    ACCESS_SECRET = data['ACCESS_SECRET']
    CONSUMER_KEY = data['CONSUMER_KEY']
    CONSUMER_SECRET = data['CONSUMER_SECRET']


def authenticate():
    credentials_load()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    return auth


class MyStreamListener(StreamListener):
    def __int__(self):
        pass

    def on_data(self, data):
        # Producer produces data for consumer
        producer.send(TOPIC, data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        # Status printing
        print(status)
        return True


def stream():
    set_kafka()
    my_stream = Stream(auth=authenticate(), listener=MyStreamListener())
    my_stream.filter(languages=['en'], track=HASH_TAG)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process tweets from twitter and stream them through kafka topic.')
    parser.add_argument('--hashtag', default='#traffic', dest='hashtag',
                        help='hashtag of the twitter app to retrieve tweets')
    parser.add_argument('--topic', default='twitter', dest='topic',
                        help='topic where kakfa can publish messages')

    args = parser.parse_args()
    HASH_TAG.append(args.hashtag)
    HASH_TAG.append('#accidents')
    HASH_TAG.append('#collision')
    HASH_TAG.append('#crash')
    TOPIC = args.topic
    stream()
