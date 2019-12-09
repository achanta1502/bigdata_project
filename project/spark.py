from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.streaming.kafka import KafkaUtils
import ml_model
from elasticsearch import Elasticsearch

import argparse
import json
import geopy
import pandas as pd

IP = 'localhost'
PORT = '9092'
LOG_LEVEL = 'WARN'
TOPIC = ''

def connection():
    # Pyspark
    # create spark configuration
    conf = SparkConf()
    conf.setAppName('TwitterApp')
    conf.setMaster('local[2]')

    conf.set("spark.network.timeout", "4200s")
    conf.set("spark.executor.heartbeatInterval", "4000s")
    # create spark context with the above configuration
    sc = SparkContext(conf=conf)
    sc.setLogLevel(LOG_LEVEL)
    sqlcontext = SQLContext(sc)
    spark = SparkSession(sc)
    # create the Streaming Context from spark context with interval size 2 seconds
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("checkpoint_TwitterApp")
    return ssc


def get_sql_context_instance(sparkContext):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def get_spark_session_instance(sparkContext):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession(sparkContext)
    return globals()['sparkSessionSingletonInstance']


def elastic_search():
    return Elasticsearch([{'host': 'localhost', 'port': 9200}])


def send_loc_to_es(data):
    es = elastic_search()
    if not es.indices.exists(index="location1"):
        datatype = {
            "mappings": {
                "request-info": {
                    "properties": {
                        "tweet": {
                          "type": "text"
                        },
                        "location": {
                            "type": "geo_point"
                        },
                        "analysis": {
                            "type": "text"
                        }
                    }
                }
            }
        }
        es.indices.create(index="location", body=datatype)
    es.index(index="location", doc_type="request-info", body=data)


def process(rdd):
    try:
        #process the rdd
        tweets = list(rdd)
        for tweet in tweets:
            lat = None
            lon = None
            # print(tweet.get('text').encode("ascii", "ignore"))
            tweet_text = tweet.get('text').encode("ascii", "ignore")
            df = pd.DataFrame({'text': str(tweet_text)}, index=[0])
            # print(pd_df)
            acc = ml_model.pipeline(df)
            loc = tweet.get('user').get('location')
            # print(loc)
            if loc is not None:
                locator = geopy.Nominatim(user_agent="MyGeocoder")
                location = locator.geocode(loc)
                lat = location.latitude
                lon = location.longitude
            # print("location points " + str(location.latitude) + ", " + str(location.longitude))
            data = {
                "tweet": tweet_text,
                "location": {
                    "lat": lat,
                    "lon": lon
                },
                "analysis": acc
            }
            print(data)
            if lat is not None and acc == 'YES':
                print("sending data to kibana")
                send_loc_to_es(data)
    except Exception as e:
        # print(e)
        pass

def start():
    # create config and start streaming
    ssc = connection()
    kafka_stream = KafkaUtils.createDirectStream(ssc, [TOPIC], {"metadata.broker.list": "localhost:9092"})
    ml_model.model_start()
    data = kafka_stream.map(lambda x: json.loads(x[1]))
    data.foreachRDD(lambda x: x.foreachPartition(lambda y: process(y)))
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='subscribe to topic and analyze the data')
    parser.add_argument('--topic', default='twitter', dest='topic',
                        help='topic where kakfa can publish messages')

    args = parser.parse_args()
    TOPIC = args.topic
    start()
