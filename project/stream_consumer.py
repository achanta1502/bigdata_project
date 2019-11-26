"""
python stream_consumer.py
"""
from kafka import KafkaConsumer
import csv
import argparse
import json
import geopy

TOPIC = ''
IP = 'localhost'
PORT = '9092'

def data_from_kafka_consumer():
    consumer = KafkaConsumer(TOPIC,
                             bootstrap_servers=['%s:%s' % (IP, PORT)],
                             auto_offset_reset='earliest'
                             )
    with open('/home/achanta/Desktop/twitter.csv', mode='a') as test_file:
        writer = csv.writer(test_file)
        for msg in consumer:
            decoded = json.loads(msg.value)
            # cols = decoded.split("||")
            # asciidata = cols[1].encode("ascii", "ignore")
            # writer.writerow([asciidata, cols[0]])
            print(decoded.get('text'))
            loc = decoded.get('user').get('location')
            print(loc)
            locator = geopy.Nominatim(user_agent="MyGeocoder")
            location = locator.geocode(loc)
            print("location points " + str(location.latitude) + str(location.longitude))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='subscribe the topic and save them to file')
    parser.add_argument('--topic', default='twitter', dest='topic',
                        help='topic where kakfa can publish messages')

    args = parser.parse_args()
    TOPIC = args.topic
    data_from_kafka_consumer()