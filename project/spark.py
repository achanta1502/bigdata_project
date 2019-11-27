from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.streaming.kafka import KafkaUtils
from model import pipeline
from elasticsearch import Elasticsearch

import argparse
import json
import geopy

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
    if not es.indices.exists(index="location"):
        datatype = {
            "mappings": {
                "request-info": {
                    "properties": {
                        "tweet": {
                          "type": "text"
                        },
                        "location": {
                            "type": "geo_point"
                        }
                    }
                }
            }
        }
        es.indices.create(index="location", body=datatype)
    es.index(index="location", doc_type="request-info", body=data)


def process(rdd):
    print("entered")
    try:
        #process the rdd
        tweets = list(rdd)
        for tweet in tweets:
            print(tweet.get('text').encode("ascii", "ignore"))
            tweet_text = tweet.get('text').encode("ascii", "ignore")
            # tweet_df = tweet_text.toDF()
            # pd_df = tweet_df.toPandas()
            # pd_df.columns = ["text"]
            # print(pd_df)
            #acc = pipeline(pd_df)
            loc = tweet.get('user').get('location')
            print(loc)
            if loc is None:
                continue
            locator = geopy.Nominatim(user_agent="MyGeocoder")
            location = locator.geocode(loc)
            print("location points " + str(location.latitude) + ", " +str(location.longitude))
            data = {
                "tweet": tweet_text,
                "location": {
                    "lat": location.latitude,
                    "lon": location.longitude
                }
            }
            print(data)
            send_loc_to_es(data)
            # print(i.get('text').encode('ascii', 'ignore'))
        # pd_df.columns = ["label", "review"]
        # acc = pipeline(pd_df)
        # send_acc_to_es(acc)
    except Exception as e:
        print(e)
        pass

def start():
    # create config and start streaming
    ssc = connection()
    kafka_stream = KafkaUtils.createDirectStream(ssc, [TOPIC], {"metadata.broker.list": "localhost:9092"})
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
