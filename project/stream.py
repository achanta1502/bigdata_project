import tweepy
import socket
import sys

# twitter app token key and consumer key
ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

HASHTAG = ''

TCP_IP = 'localhost'
TCP_PORT = 9001

# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()


class MyStreamListener(tweepy.StreamListener):

    @classmethod
    def on_status(cls, status):
        print(status.text)
        conn.send(status.text.encode('utf-8'))

    @classmethod
    def on_error(cls, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)


def stream():
    my_stream = tweepy.Stream(auth=auth, listener=MyStreamListener())

    my_stream.filter(track=[HASHTAG])


if __name__ == '__main__':
    if len(sys.argv) > 0:
        if len(sys.argv) != 6:
            print("Arguments are not correctly defined")
            exit(2)
        ACCESS_TOKEN = sys.argv[1]
        ACCESS_SECRET = sys.argv[2]
        CONSUMER_KEY = sys.argv[3]
        CONSUMER_SECRET = sys.argv[4]
        HASHTAG = sys.argv[5]
    stream()
