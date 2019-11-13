import Constants
import Logger
import pyspark as ps
import time

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StringIndexer
from pyspark.sql import SQLContext


def create_spark_context(log):
    try:
        conf = ps.SparkConf().setAll([('spark.executor.memory', '16g'), ('spark.driver.memory', '16g')])
        sc = ps.SparkContext(conf=conf)
        sql_c = SQLContext(sc)
        log.info("Created a Spark Context")
        return sql_c, sc
    except ValueError as e:
        log.error(e)


def read_csv_to_df(log, sql_c, reading_format, read_header, infer_schema, data_path):
    log.info("Data has been read successfully.")
    df = sql_c.read.format(reading_format).options(header=read_header, infer_schema=infer_schema).load(data_path)
    log.info(str(df.count()) + " has been read.")
    return df


def remove_na(log, df):
    log.info("NA's has been removed from the dataset.")
    return df.dropna()


def split_data_frame(log, df, train_size, val_size, test_size, seed_value):
    log.info(
        "Dataframe has been split into " + str(train_size) + "," + str(val_size) + "," + str(
            test_size) + " size for train, val and test")
    return df.randomSplit([train_size, val_size, test_size], seed=seed_value)


def transform_data_set(log, tr_set, vl_set, tst_set, feature_column_name, target_column_name, number_of_features,
                       document_frequency, pipleline_model_path):
    log.info("Tokenizing the feature column and output is written into words column")
    tokenizer = Tokenizer(inputCol=feature_column_name, outputCol="words")

    log.info("Hashing the words column and output is written into tf column")
    hashtf = HashingTF(numFeatures=number_of_features, inputCol="words", outputCol="tf")

    log.info("IDF the tf column and output is written into  features column")
    idf = IDF(inputCol="tf", outputCol="features", minDocFreq=document_frequency)

    log.info("Labeling the target column and output is written into label column")
    label_string_idx = StringIndexer(inputCol=target_column_name, outputCol="label")

    log.info("Making all the above method into a pipeline")
    pipeline = Pipeline(stages=[tokenizer, hashtf, idf, label_string_idx])

    log.info("Fit train, test and validation set on the created pipeline")
    pipeline_fit = pipeline.fit(tr_set)
    pipeline_fit.save(pipleline_model_path)
    train_df = pipeline_fit.transform(tr_set)
    val_df = pipeline_fit.transform(vl_set)
    test_df = pipeline_fit.transform(tst_set)
    return train_df, val_df, test_df


def train_data_set(log, train_df, max_iter):
    log.info("Fitting the train dataset using LogisticRegression")
    lr = LogisticRegression(maxIter=max_iter)
    lr_model = lr.fit(train_df)
    return lr_model


def evaluate(log, model, test_df):
    log.info("Evaluating the trained model")
    predictions = model.transform(test_df)
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    log.info(evaluator.evaluate(predictions))


if __name__ == '__main__':
    start_time = time.time()
    log_file = Logger.LoggerFile()
    logger = log_file.set_logger_file('Spark Content Classification',
                                      Constants.spark_content_classification_log_file_name)

    sql_context, spark_context = create_spark_context(logger)

    data_frame = read_csv_to_df(logger, sql_context, Constants.reading_format, Constants.read_header,
                                Constants.infer_schema, Constants.vox_news_cleaned_data_path)

    data_frame = remove_na(logger, data_frame)

    train_set, val_set, test_set = split_data_frame(logger, data_frame, Constants.train_size,
                                                    Constants.val_size, Constants.test_size,
                                                    Constants.seed_value)

    train_data_frame, val_data_frame, test_data_frame = transform_data_set(logger, train_set, val_set, test_set,
                                                                           Constants.feature_column_name,
                                                                           Constants.target_column_name,
                                                                           Constants.number_of_features,
                                                                           Constants.document_frequency,
                                                                           Constants.content_tf_idf_model_path)

    sentiment_analysis_model = train_data_set(logger, train_data_frame, Constants.max_iter)
    evaluate(logger, sentiment_analysis_model, test_data_frame)
    sentiment_analysis_model.save(Constants.content_classification_model_path)

    logger.info("Data pre processing has taken ")
    logger.info("--- %s seconds ---" % (time.time() - start_time))
