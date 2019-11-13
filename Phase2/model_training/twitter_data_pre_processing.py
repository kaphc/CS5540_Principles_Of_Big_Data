import pandas as pd
import re
import time
from bs4 import BeautifulSoup
from nltk.tokenize import WordPunctTokenizer

import Constants
import Logger


def read_data(twitter_data_path, data_column_names, encoding_type):
    try:
        data_frame = pd.read_csv(twitter_data_path, header=None, names=data_column_names,
                                 encoding=encoding_type)
        logger.info('Twitter Dataset has been loaded successfully')
        logger.info(data_frame.head(5))
        return data_frame
    except Exception as e:
        logger.error(e)


def count_sentiment_types(data_frame):
    logger.info('Sentiment count in the twitter dataset:')
    logger.info(data_frame.sentiment.value_counts())


def drop_columns(data_frame, drop_column_names):
    data_frame.drop(drop_column_names, axis=1, inplace=True)
    logger.info(str(drop_column_names) + ' columns are dropped')
    logger.info(data_frame.head(5))
    return data_frame


def modify_label_in_feature(data_frame, target):
    data_frame[target] = data_frame[target].map({0: 'Negative', 4: 'Positive'})
    logger.info(target + ' column is modified')
    logger.info(data_frame.head(5))
    return data_frame


def tweet_cleaner(text, tok, remove_regex):
    soup = BeautifulSoup(text, 'lxml')
    souped = soup.get_text()
    stripped = re.sub(remove_regex, '', souped)
    try:
        clean = stripped.encode("utf-8").decode("utf-8-sig").replace(u"\ufffd", "?")
    except Exception as e:
        logger.error(e)
        clean = stripped
    letters_only = re.sub("[^a-zA-Z]", " ", clean)
    lower_case = letters_only.lower()
    words = tok.tokenize(lower_case)
    return (" ".join(words)).strip()


if __name__ == '__main__':
    start_time = time.time()
    log_file = Logger.LoggerFile()
    logger = log_file.set_logger_file('Twitter Data Pre Processing', Constants.twitter_data_pre_processing_log_file_name)

    twitter_data_frame = read_data(Constants.twitter_data_path, Constants.twitter_data_column_names, Constants.encoding_type)
    count_sentiment_types(twitter_data_frame)
    twitter_data_frame = drop_columns(twitter_data_frame, Constants.twitter_drop_column_names)
    twitter_data_frame = modify_label_in_feature(twitter_data_frame, Constants.twitter_target_column_name)
    count_sentiment_types(twitter_data_frame)

    logger.info("Cleaning and parsing the tweets...")
    logger.info("Removing '@' mentions and URLS in the tweet")
    logger.info("Decoding text with ‘utf-8-sig’, this BOM will be replaced with unicode unrecognisable special "
                "characters, then we can process this as “?”")
    logger.info("Removing hashtags in the text")

    tokenizer = WordPunctTokenizer()
    clean_tweet_texts = []

    for i in range(0, twitter_data_frame['text'].size - 1):
        if (i + 1) % 10000 == 0:
            logger.info("Tweets %d of %d has been processed" % (i + 1, twitter_data_frame.size))
        clean_tweet_texts.append(tweet_cleaner(twitter_data_frame['text'][i], tokenizer, Constants.remove_regex))

    cleaned_twitter_dataframe = pd.DataFrame(clean_tweet_texts, columns=['text'])
    cleaned_twitter_dataframe[Constants.twitter_target_column_name] = twitter_data_frame[Constants.twitter_target_column_name]
    cleaned_twitter_dataframe.to_csv(Constants.twitter_cleaned_data_path, encoding='utf-8')
    logger.info("Data pre processing has taken ")
    logger.info("--- %s seconds ---" % (time.time() - start_time))
