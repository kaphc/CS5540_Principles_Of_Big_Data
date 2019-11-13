import copy
import numpy as np
import matplotlib.pyplot as plt
import re
import nltk

nltk.download('stopwords')
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_selection import VarianceThreshold
from imblearn.over_sampling import SMOTE
from sklearn.dummy import DummyClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import RandomForestClassifier
# from sklearn.metrics import accuracy_score
# from sklearn.model_selection import cross_val_score, KFold
from sklearn.metrics import confusion_matrix
from sklearn.metrics import classification_report
import seaborn as sns

import Constants
import Logger
import os
import pandas as pd

from keras.layers import Dense, Embedding, LSTM, SpatialDropout1D
from keras.models import Sequential, load_model
from keras.preprocessing.sequence import pad_sequences
from keras.preprocessing.text import Tokenizer
from keras.utils.np_utils import to_categorical

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'


def read_data(twitter_cleaned_data_path, data_column_names):
    try:
        data_frame = pd.read_csv(twitter_cleaned_data_path, names=data_column_names)
        logger.info('Cleaned News Dataset has been loaded successfully')
        logger.info(data_frame.head(5))
        return data_frame
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    log_file = Logger.LoggerFile()
    logger = log_file.set_logger_file('News Content Classification', Constants.content_classification_log_file_name)

    news_cleaned_data_frame = read_data(Constants.vox_news_cleaned_data_path, Constants.vox_select_column_names)

    titles = news_cleaned_data_frame[Constants.vox_select_column_names[0]].values.tolist()
    titles = titles[1:]
    categories = news_cleaned_data_frame[Constants.vox_select_column_names[1]].values.tolist()
    categories = categories[1:]


