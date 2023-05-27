import logging
from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as SparkTypes
from pyspark.sql.utils import StreamingQueryException

import settings

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s %(levelname)-8s %(name)-12s %(funcName)s line_%(lineno)d %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def get_spark_config():
    conf = SparkConf()
    conf.setMaster(settings.MASTER)
    conf.setAppName(settings.APP_NAME)
    conf.set("spark.streaming.kafka.consumer.poll.ms", "512")
    conf.set("spark.executor.heartbeatInterval", "20s")
    conf.set("spark.network.timeout", "1200s")
    # https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
    conf.set("es.nodes", settings.ES_HOST)
    conf.set("es.port", settings.ES_PORT)
    conf.set("es.net.http.auth.user", settings.ES_USERNAME)
    conf.set("es.net.http.auth.pass", settings.ES_PASSWORD)
    conf.set("es.net.ssl", "true")
    conf.set("es.nodes.resolve.hostname", "false")
    conf.set("es.net.ssl.cert.allow.self.signed", "true")
    conf.set("es.nodes.wan.only", "true")
    conf.set("es.nodes.discovery", "false")
    return conf


# A log line example:
# 199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
re_host = '(^\S+\.[\S+\.]+\S+)'
re_time = '\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
re_method_uri_protocol = '\"(\S+)\s(\S+)\s*(\S*)\"'
re_status = '\s(\d{3})\s'
re_content_size = '(\d+)$'
log_pattern = f'{re_host}\s-\s-\s{re_time}\s{re_method_uri_protocol}{re_status}{re_content_size}'


def main():
    conf = get_spark_config()
    spark: SparkSession = SparkSession.builder \
        .config(conf=conf) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    raw_logs_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("subscribe", "messages")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_logs_df = raw_logs_df.select(
        F.regexp_extract('value', re_host, 1).alias('host'),
        F.regexp_extract('value', re_time, 1).alias('timestamp'),
        F.regexp_extract('value', re_method_uri_protocol, 1).alias('method'),
        F.regexp_extract('value', re_method_uri_protocol, 2).alias('endpoint'),
        F.regexp_extract('value', re_method_uri_protocol, 3).alias('protocol'),
        F.regexp_extract('value', re_status, 1).alias('status'),
        F.regexp_extract('value', re_content_size, 1).alias('content_size'),
    )

    query1 = parsed_logs_df.writeStream.format("console").start()
    parsed_logs_df.printSchema()
    query1.awaitTermination()

    # normalized_logs_df = parsed_logs_df.filter(
    #     (F.col('host') != '') & F.col('host').isNotNull() &
    #     (F.col('timestamp') != '') & F.col('timestamp').isNotNull() &
    #     (F.col('method') != '') & F.col('timestamp').isNotNull() &
    #     (F.col('endpoint') != '') & F.col('timestamp').isNotNull() &
    #     (F.col('protocol') != '') & F.col('timestamp').isNotNull() &
    #     (F.col('status') != '') & F.col('timestamp').isNotNull() &
    #     (F.col('content_size') != '') & F.col('timestamp').isNotNull()
    # )
    #
    # normalized_logs_df_with_column = normalized_logs_df.withColumns({
    #     "timestamp": F.udf(lambda s: datetime.strptime(s, "%d/%b/%Y:%H:%M:%S %z").strftime("%Y-%m-%dT%H:%M:%S%z"),
    #                        SparkTypes.StringType())(
    #         "timestamp"),
    #     "status": F.udf(lambda s: int(s), SparkTypes.IntegerType())('status'),
    #     "content_size": F.udf(lambda s: int(s), SparkTypes.IntegerType())('content_size')
    # })
    #
    # while True:
    #     normalized_logs_df_with_column.printSchema()
    #     final_data = normalized_logs_df_with_column.writeStream \
    #         .option("checkpointLocation", settings.CHECKPOINT_LOCATION) \
    #         .option("es.resource", f'{settings.ES_INDEX}/{settings.ES_DOC_TYPE}') \
    #         .outputMode(settings.OUTPUT_MODE) \
    #         .format(settings.DATA_SOURCE) \
    #         .start(f'{settings.ES_INDEX}')
    #     try:
    #         final_data.awaitTermination()
    #     except StreamingQueryException as error:
    #         print('Query Exception caught:', error)


if __name__ == '__main__':
    main()
