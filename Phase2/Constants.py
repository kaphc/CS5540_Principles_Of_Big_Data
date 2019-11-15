# twitter_data_path = "../data/twitter_data.csv"
#
# stop_words_data_path = "../data/stopwords.txt"
# vox_news_data_path = "../data/dsjVoxArticles.tsv"
# vox_news_cleaned_data_path = "../data/vox_news_cleaned_data.csv"
#
# twitter_data_pre_processing_log_file_name = '../log_files/twitter_data_pre_processing.log'
# vox_news_data_pre_processing_log_file_name = '../log_files/vox_news_data_pre_processing.log'
#
# sentiment_analysis_log_file_name = '../log_files/sentiment_analysis.log'
#
# content_classification_log_file_name = '../log_files/content_classification.log'
#
#
# sentiment_analysis_model_path = '../models/sentiment_analysis.h5'
#
# encoding_type = 'ISO-8859-1'
# remove_regex = r'@[A-Za-z0-9]+' + r'https?://[A-Za-z0-9./]+'
#
# twitter_data_column_names = ['sentiment', 'tweet_id', 'date', 'flag', 'user', 'text']
# twitter_select_column_names = ['text', 'sentiment']
# twitter_drop_column_names = ['tweet_id', 'date', 'flag', 'user']
# twitter_target_column_name = 'sentiment'
# twitter_feature_column_name = 'text'
#
# vox_data_column_names = ['title', 'author', 'category', 'published_date', 'updated_on', 'slug', 'blurb', 'body']
# vox_select_column_names = ['title', 'category']
# vox_drop_column_names = ['author', 'published_date', 'updated_on', 'slug', 'blurb', 'body']
# vox_target_column_name = 'category'
# vox_feature_column_name = 'title'
# vox_categories = ['Business & Finance', 'Health Care', 'Science & Health', 'Politics & Policy', 'Criminal Justice']
#
# max_features = 2000
# embed_dim = 128
# lstm_out = 196
# # test_size = 0.25
# random_state = 42
# batch_size = 128
# number_of_epochs = 5
#
#
# conn = None

# Spark Sentiment Analysis
spark_sentiment_analysis_log_file_name = '../log_files/spark_sentiment_analysis.log'
twitter_cleaned_data_path = "../data/twitter_cleaned_data.csv"
sentiment_analysis_model_path = "../models/spark_models/sentiment_analysis"
sentiment_tf_idf_model_path = "../models/spark_models/sentiment_tf_idf"

# Spark content classification
spark_content_classification_log_file_name = '../log_files/spark_sentiment_analysis.log'
vox_news_cleaned_data_path = "../data/twitter_cleaned_data.csv"
content_classification_model_path = "../models/spark_models/content_classification"
content_tf_idf_model_path = "../models/spark_models/content_tf_idf"

reading_format = 'com.databricks.spark.csv'
read_header = 'true'
infer_schema = 'true'

train_size = 0.98
val_size = 0.01
test_size = 0.01
seed_value = 2000

feature_column_name = "text"
target_column_name = "sentiment"

number_of_features = 2 ** 16
document_frequency = 5
max_iter = 1

# twitter streaming -> tcp
tcp_streaming_log_file_name = "../log_files/tcp_streaming.log"
stop_words_path = "../data/stopwords.txt"

TCP_IP = "localhost"
port_number = 9009
twitter_streaming_time = 1

# tcp -> spark streaming
spark_streaming_log_file_name = '../log_files/spark_streaming_log_file_name.log'
spark_streaming_time = 2

