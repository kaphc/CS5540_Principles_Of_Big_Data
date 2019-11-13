import socket
import requests
import requests_oauthlib
import json

from pyspark.ml import PipelineModel
import pyspark as ps
from pyspark.sql import SQLContext

import credentials.twitter_credentials as tc
import Constants
import Logger
from pyspark.ml.classification import LogisticRegressionModel


def create_spark_context(log):
    try:
        conf = ps.SparkConf().setAll([('spark.executor.memory', '16g'), ('spark.driver.memory', '16g')])
        sc = ps.SparkContext(conf=conf)
        sql_c = SQLContext(sc)
        log.info("Created a Spark Context")
        return sql_c, sc
    except ValueError as e:
        log.error(e)


def authenticate_twitter(consumer_key, consumer_secret, access_token, access_token_secret):
    auth = None
    try:
        logger.info("Twitter authentication has been setup successfully")
        auth = requests_oauthlib.OAuth1(consumer_key, consumer_secret, access_token, access_token_secret)
    except Exception as e:
        logger.error(e)
    return auth


def set_tcp_connection():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((Constants.TCP_IP, Constants.port_number))
        s.listen(Constants.twitter_streaming_time)
        logger.info("Waiting for TCP connection...")
        conn, addr = s.accept()
        logger.info("Connected... Starting getting tweets.")
        return conn, addr
    except Exception as e:
        logger.error(e)


def get_tweets(auth):
    try:
        url = 'https://stream.twitter.com/1.1/statuses/filter.json'
        query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
        query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
        response = requests.get(query_url, auth=auth, stream=True)
        # print(query_url, response)
        return response
    except Exception as e:
        logger.error(e)


def send_tweets_to_spark(http_resp, tcp_connection, sql_c, sentiment_tf_idf, senti_model):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            new_df = sql_c.createDataFrame([("1", full_tweet['text'])], ["id", "text"])

            sentiment_tf_idf_df = sentiment_tf_idf.transform(new_df)
            # content_tf_idf_df = content_tf_idf.transform(new_df)

            sentiment_df = senti_model.transform(sentiment_tf_idf_df)
            # content_df = cont_model.transform(content_tf_idf_df)

            print(sentiment_df.show())
            # print(content_df.show())

            df_in_str = sentiment_df.collect()
            tweet_text = full_tweet['text']
            print("--------------------------------------------------------------------------------------------")
            tweet_data = bytes(str(full_tweet['id_str']) + "\t" + str(tweet_text) + "\t" + str(full_tweet['user']['id'])
                               + "\t" + str(full_tweet['place']) + "\t" + str(full_tweet['entities']['hashtags'])
                               + "\t" + str(df_in_str[0].prediction), 'utf-8')

            print(str(full_tweet['id_str']) + "\t" + str(tweet_text) + "\t" + str(full_tweet['user']['id'])
                  + "\t" + str(full_tweet['place']) + "\t" + str(full_tweet['entities']['hashtags'])
                  + "\t" + str(df_in_str[0].prediction))

            tcp_connection.send(tweet_data)
        except Exception as e:
            logger.error(e)


if __name__ == '__main__':
    log_file = Logger.LoggerFile()
    logger = log_file.set_logger_file('Twitter Streaming to TCP', Constants.tcp_streaming_log_file_name)

    sql_context, spark_context = create_spark_context(logger)

    my_auth = authenticate_twitter(tc.consumerKey, tc.consumerSecret, tc.accessToken, tc.accessTokenSecret)
    connection, address = set_tcp_connection()
    resp = get_tweets(my_auth)

    sentiment_analysis_tf_idf = PipelineModel.load(Constants.sentiment_tf_idf_model_path)
    # content_classification_tf_idf = PipelineModel.load(Constants.content_tf_idf_model_path)

    sentiment_model = LogisticRegressionModel.load(Constants.sentiment_analysis_model_path)
    # content_model = LogisticRegressionModel.load(Constants.content_classification_model_path)

    # send_tweets_to_spark(resp, connection, sql_context, sentiment_analysis_tf_idf, content_classification_tf_idf,
    #                      sentiment_model, content_model)

    send_tweets_to_spark(resp, connection, sql_context, sentiment_analysis_tf_idf,
                         sentiment_model)
