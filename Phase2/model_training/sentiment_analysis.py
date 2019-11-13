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


def read_data(twitter_cleaned_data_path, data_column_names, encoding_type):
    try:
        data_frame = pd.read_csv(twitter_cleaned_data_path, names=data_column_names,
                                 encoding=encoding_type)
        data_frame = data_frame.iloc[1:]
        logger.info('Cleaned Twitter Dataset has been loaded successfully')
        logger.info(data_frame.head(5))
        return data_frame
    except Exception as e:
        logger.error(e)


def tokenize(data_frame, column_names, max_fatures):
    try:
        tokenizer = Tokenizer(num_words=max_fatures, split=' ')
        tokenizer.fit_on_texts(data_frame[column_names].values)
        x = tokenizer.texts_to_sequences(data_frame[column_names].values)
        x = pad_sequences(x, maxlen=140)
        logger.info("Tokenizing the twitter texts")
        logger.info(x)
        return x
    except Exception as e:
        logger.error(e)


def label(data_frame, column_names):
    try:
        labelencoder = LabelEncoder()
        integer_encoded = labelencoder.fit_transform(data_frame[column_names])
        y = to_categorical(integer_encoded)
        logger.info("Labelling the twitter sentiments")
        return y
    except Exception as e:
        logger.error(e)


def train_model(x_train, y_train, max_features, x_shape, embed_dim, lstm_out, batch_size, number_of_epochs):
    try:
        model = Sequential()
        model.add(Embedding(max_features, embed_dim, input_length=x_shape))
        model.add(SpatialDropout1D(0.4))
        model.add(LSTM(lstm_out, dropout=0.2, recurrent_dropout=0.2))
        model.add(Dense(2, activation='softmax'))
        model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
        model.fit(x_train, y_train, epochs=number_of_epochs, batch_size=batch_size, verbose=2)
        logger.info("Training the model")
        return model
    except Exception as e:
        logger.error(e)


def save_model(model, model_path):
    model.save(model_path)
    m = load_model(model_path)
    logger.info(str(m.summary()))


if __name__ == '__main__':
    log_file = Logger.LoggerFile()
    logger = log_file.set_logger_file('Twitter Sentiment Analysis', Constants.sentiment_analysis_log_file_name)

    twitter_cleaned_data_frame = read_data(Constants.twitter_cleaned_data_path, Constants.twitter_select_column_names,
                                           Constants.encoding_type)
    feature_data_frame = tokenize(twitter_cleaned_data_frame, Constants.twitter_feature_column_name,
                                  Constants.max_features)
    target_data_frame = label(twitter_cleaned_data_frame, Constants.twitter_target_column_name)

    feature_train, feature_test, target_train, target_test = train_test_split(feature_data_frame, target_data_frame,
                                                                              test_size=Constants.test_size,
                                                                              random_state=42)

    sentiment_analysis_model = train_model(feature_train, target_train, Constants.max_features,
                                           feature_data_frame.shape[1], Constants.embed_dim,
                                           Constants.lstm_out, Constants.batch_size, Constants.number_of_epochs)

    save_model(sentiment_analysis_model, Constants.sentiment_analysis_model_path)
