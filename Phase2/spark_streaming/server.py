from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import time
import Logger
import Constants
import credentials.twitter_credentials as tc
import random


def set_up_tcp_connection(log):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a socket object
    host = Constants.TCP_IP  # Get local machine name
    port = Constants.port_number  # Reserve a port for your service.
    s.bind((host, port))  # Bind to the port
    log.info("Listening on port: %s" % str(port))
    return s


def set_twitter_authentication(log):
    log.info("Twitter Authentication has been setup successfully")
    consumer_key = tc.consumerKey
    consumer_secret = tc.consumerSecret
    access_token = tc.accessToken
    access_secret = tc.accessTokenSecret
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    return auth


class TweetsListener(StreamListener):

    def __init__(self, csocket):
        super().__init__()
        self.client_socket = csocket

    def on_data(self, data):
        try:
            s = self.client_socket
            s.listen(5)  # Now wait for client connection.
            c, addr = s.accept()  # Establish connection with client.

            print("Received request from: " + str(addr))

            msg = json.loads(data)
            user = json.loads(json.dumps(msg['user']))

            place_type = " "
            if msg['place'] is not None:
                if msg['place']['country_code'] is not None:
                    place_type = msg['place']['country_code']
                    print(place_type)

            lat_long = " "
            if msg['place'] is not None:
                print(msg['place'])

            hashtags = " "
            if msg['entities'] is not None:
                if msg['entities']['hashtags'] is not None:
                    for hashtag in msg['entities']['hashtags']:
                        hashtags = hashtags + " " + hashtag['text']

            timeZone = " "
            if msg['user'] is not None:
                if msg['user']['time_zone'] is not None:
                    timeZone = msg['user']['time_zone']

            # location = json.loads(
            #     str(msg['place']['bounding_box']) if msg['place']['bounding_box'] is not None else '{}')
            s_data = msg['id_str'] + ' ~@ ' + msg['text'].replace('\n', '') + ' ~@ ' + \
                     (str(user['id']) if user['id'] is not None else 'None') + ' ~@ ' + \
                     str(random.choice(['US', 'IND', 'BR', 'UK', 'CAN', 'AUS'])) + '~@ ' + \
                     str(random.choice([0, 1])) + ' ~@ ' + \
                     str(random.choice(["Business & Finance", "Criminal Justice", "Health Care",
                                        "Policy and Politics", "Science & Health"])) + ' ~@ ' + \
                     str(hashtags) + ' ~@ ' + \
                     str(timeZone) + ' ~@ ' + \
                     str(random.choice(['cancer', 'flu', 'aids', 'cholera', 'diabetes']))

            print(s_data.encode('utf-8'))

            c.send(s_data.encode('utf-8'))
            c.close()
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def get_track_list(log, filepath):
    log.info("Preparing the track list")
    track_list = []
    with open(filepath) as fp:
        line = fp.readline()
        while line:
            line = fp.readline()
            track_list.append(line.strip('\n'))
    return track_list


if __name__ == "__main__":
    start_time = time.time()
    log_file = Logger.LoggerFile()
    logger = log_file.set_logger_file('Twitter Streaming to TCP',
                                      Constants.spark_content_classification_log_file_name)

    socket_object = set_up_tcp_connection(logger)
    twitter_auth = set_twitter_authentication(logger)

    twitter_stream = Stream(twitter_auth, TweetsListener(socket_object))
    twitter_stream.filter(track=get_track_list(logger, Constants.stop_words_path))
