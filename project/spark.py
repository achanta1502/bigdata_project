from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.streaming.kafka import KafkaUtils
from model import pipeline
from es import send_acc_to_es, send_loc_to_es

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


def process(rdd):
    print("entered")
    try:
        #process the rdd
        tweets = list(rdd)
        for i in tweets:
            print(i.get('text').encode("ascii", "ignore"))
            loc = i.get('user').get('location')
            print(loc)
            locator = geopy.Nominatim(user_agent="MyGeocoder")
            location = locator.geocode(loc)
            print("location points " + str(location.latitude) + ", " +str(location.longitude))
            # print(i.get('text').encode('ascii', 'ignore'))
        # df = rdd.toDF()
        # pd_df = df.toPandas()
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
