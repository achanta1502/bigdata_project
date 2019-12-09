"""
python stream_consumer.py
"""
from kafka import KafkaConsumer
import csv
import argparse
import json
import geopy
import re

TOPIC = ''
IP = 'localhost'
PORT = '9092'

def data_from_kafka_consumer():
    consumer = KafkaConsumer(TOPIC,
                             bootstrap_servers=['%s:%s' % (IP, PORT)],
                             auto_offset_reset='earliest'
                             )
    with open('/home/achanta/Desktop/twitter2.csv', mode='a') as test_file:
        writer = csv.writer(test_file, delimiter=';')
        try:
            for msg in consumer:
                try:
                    decoded = json.loads(msg.value)
                    text = decoded.get('text')
                    print(text)
                    text = ''.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) (\w+:\ / \ / \S+)", "", text))
                    if text is None:
                        continue
                    text = text.encode("ascii", "ignore")
                    user = decoded.get('user')
                    loc = None
                    if user is not None:
                        loc = user.get('location')
                    writer.writerow([text, loc])
                    print(text)
                    print(loc)
                # print(loc.get('location', "nowhere"))
                # locator = geopy.Nominatim(user_agent="MyGeocoder")
                # location = locator.geocode(loc)
                # print("location points " + str(location.latitude) + str(location.longitude))
                except Exception as e:
                    print(str(e))
                    continue
        except Exception as e:
            print(str(e))
            pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='subscribe the topic and save them to file')
    parser.add_argument('--topic', default='twitter', dest='topic',
                        help='topic where kakfa can publish messages')

    args = parser.parse_args()
    TOPIC = args.topic
    data_from_kafka_consumer()
