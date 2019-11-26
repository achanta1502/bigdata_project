from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch

import argparse

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
    # creating an elastic search port for data to be sent to kibana
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    return ssc, es


def get_sql_context_instance(sparkContext):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def get_spark_session_instance(sparkContext):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession(sparkContext)
    return globals()['sparkSessionSingletonInstance']


def process(time, rdd):
    print("===========%s=========" % str(time))
    try:
        #process the rdd
        pass
    except Exception as e:
        print(e)
        pass

def start():
    # create config and start streaming
    ssc, es = connection()
    kafka_stream = KafkaUtils.createDirectStream(ssc, [TOPIC], {"metadata.broker.list": "localhost:9092"})
    kafka_stream.foreachRDD(process)
    # your processing here
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='subscribe to topic and analyze the data')
    parser.add_argument('--topic', default='twitter', dest='topic',
                        help='topic where kakfa can publish messages')

    args = parser.parse_args()
    TOPIC = args.topic
    start()
