import socket
import requests
import requests_oauthlib
import json

import credentials.twitter_credentials as tc
import Constants
import Logger


def authenticate_twitter(consumer_key, consumer_secret, access_token, access_token_secret):
    auth = None
    try:
        logger.info("Twitter authentication has been setup successfully")
        auth = requests_oauthlib.OAuth1(consumer_key, consumer_secret, access_token, access_token_secret)
    except Exception as e:
        logger.error(e)
    return auth


def set_tcp_connection():
    conn = None
    addr = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((Constants.TCP_IP, Constants.port_number))
        s.listen(Constants.twitter_streaming_time)
        logger.info("Waiting for TCP connection...")
        conn, addr = s.accept()
        logger.info("Connected... Starting getting tweets.")
    except Exception as e:
        logger.error(e)
    return conn, addr


def get_tweets():
    response = None
    try:
        url = 'https://stream.twitter.com/1.1/statuses/filter.json'
        query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
        query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
        response = requests.get(query_url, auth=my_auth, stream=True)
        print(query_url, response)
    except Exception as e:
        logger.error(e)
    return response


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = str(full_tweet['text'].encode("utf-8"))
            tweet_data = bytes(tweet_text + "\t", 'utf-8')
            tcp_connection.send(tweet_data)
        except Exception as e:
            logger.error(e)


if __name__ == '__main__':
    log_file = Logger.LoggerFile()
    logger = log_file.set_logger_file('Twitter Streaming to TCP', Constants.tcp_streaming_log_file_name)

    my_auth = authenticate_twitter(tc.consumerKey, tc.consumerSecret, tc.accessToken, tc.accessTokenSecret)
    connection, address = set_tcp_connection()
    resp = get_tweets()
    send_tweets_to_spark(resp, connection)
