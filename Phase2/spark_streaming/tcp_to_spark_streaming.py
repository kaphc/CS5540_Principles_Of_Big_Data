from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import Constants
import Logger


def set_up_spark_streaming(spark_streaming_time, port_number):
    try:
        logger.info("create spark configuration")
        conf = SparkConf()
        conf.setAppName("TwitterStreamApp")
        logger.info("Creating a spark context with the above configuration")
        sc = SparkContext(conf=conf)
        sc.setLogLevel("ERROR")
        logger.info("Creating the Streaming Context from the above spark context with interval size 2 seconds")
        ssc = StreamingContext(sc, spark_streaming_time)
        logger.info("Setting a checkpoint to allow RDD recovery")
        ssc.checkpoint("checkpoint_TwitterApp")
        logger.info("reading data from the port")
        data = ssc.socketTextStream("localhost", port_number)
        return ssc, data
    except ValueError as e:
        logger.error(e)


def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        # create a DF from the Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        hashtags_df.registerTempTable("hashtags")
        # get the top 10 hashtags from the table using SQL and print them
        hashtag_counts_df = sql_context.sql(
            "select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
        hashtag_counts_df.show()
        # call this method to prepare top 10 hashtags DF and send them
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    log_file = Logger.LoggerFile()
    logger = log_file.set_logger_file('TCP to Spark Streaming', Constants.spark_streaming_log_file_name)

    streaming_context, dataStream = set_up_spark_streaming(Constants.spark_streaming_time, Constants.port_number)

    logger.info("Splitting each tweet into words")
    words = dataStream.flatMap(lambda line: line.split(" "))

    logger.info("Filtering the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)")
    hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))

    logger.info("adding the count of each hashtag to its last count")
    tags_totals = hashtags.updateStateByKey(aggregate_tags_count)

    logger.info("Processing for each RDD generated in each interval")
    tags_totals.foreachRDD(process_rdd)

    logger.info("Starting the streaming computation")
    streaming_context.start()
    logger.info("Waiting for the streaming to finish")
    streaming_context.awaitTermination()
