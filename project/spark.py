from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

import json
import re
from textblob import TextBlob
from elasticsearch import Elasticsearch
from datetime import datetime

TCP_IP = 'localhost'
TCP_PORT = 9001


def connection():
    # Pyspark
    # create spark configuration
    conf = SparkConf()
    conf.setAppName('TwitterApp')
    conf.setMaster('local[2]')

    conf.set("spark.network.timeout","4200s")
    conf.set("spark.executor.heartbeatInterval","4000s")
    # create spark context with the above configuration
    sc = SparkContext(conf=conf)
    # create the Streaming Context from spark context with interval size 2 seconds
    ssc = StreamingContext(sc, 4)
    ssc.checkpoint("checkpoint_TwitterApp")
    # creating an elastic search port for data to be sent to kibana
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    data_stream = ssc.socketTextStream(TCP_IP, TCP_PORT)
    return ssc, es, data_stream


def start():
    # create config and start streaming
    ssc, es, data_stream = connection()
    # your processing here
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    start()
