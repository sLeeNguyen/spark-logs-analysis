import argparse
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


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Parse arguments for log analysis',
        epilog='End parsing arguments')
    parser.add_argument("--host", type=str, default='localhost',
                        help='Listening for a client at host')
    parser.add_argument("--port", type=int, default=9999,
                        help='Listening for a client at port')

    args = parser.parse_args()
    return args


def get_spark_config():
    conf = SparkConf()
    conf.setMaster(settings.MASTER)
    conf.setAppName(settings.APP_NAME)
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
    args = parse_arguments()
    conf = get_spark_config()
    spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    raw_logs_df = spark.readStream \
        .format("socket") \
        .option("host", args.host) \
        .option("port", args.port) \
        .load()

    parsed_logs_df = raw_logs_df.select(
        F.regexp_extract('value', re_host, 1).alias('host'),
        F.regexp_extract('value', re_time, 1).alias('timestamp'),
        F.regexp_extract('value', re_method_uri_protocol, 1).alias('method'),
        F.regexp_extract('value', re_method_uri_protocol, 2).alias('endpoint'),
        F.regexp_extract('value', re_method_uri_protocol, 3).alias('protocol'),
        F.regexp_extract('value', re_status, 1).alias('status'),
        F.regexp_extract('value', re_content_size, 1).alias('content_size'),
    )
    # parsed_logs_df.cache()
    normalized_logs_df = parsed_logs_df.filter(
        (F.col('host') != '') & F.col('host').isNotNull() &
        (F.col('timestamp') != '') & F.col('timestamp').isNotNull() &
        (F.col('method') != '') & F.col('timestamp').isNotNull() &
        (F.col('endpoint') != '') & F.col('timestamp').isNotNull() &
        (F.col('protocol') != '') & F.col('timestamp').isNotNull() &
        (F.col('status') != '') & F.col('timestamp').isNotNull() &
        (F.col('content_size') != '') & F.col('timestamp').isNotNull()
    ).withColumns({
        "timestamp": F.udf(lambda s: datetime.strptime(s, "%d/%b/%Y:%H:%M:%S %z"), SparkTypes.TimestampType())(
            "timestamp"),
        "status": F.udf(lambda s: int(s), SparkTypes.IntegerType())('status'),
        "content_size": F.udf(lambda s: int(s), SparkTypes.IntegerType())('content_size')
    })

    while True:
        query = normalized_logs_df.writeStream \
            .option("checkpointLocation", settings.CHECKPOINT_LOCATION) \
            .option("es.resource", f'{settings.ES_INDEX}/{settings.ES_DOC_TYPE}') \
            .outputMode(settings.OUTPUT_MODE) \
            .format(settings.DATA_SOURCE) \
            .start(f'{settings.ES_INDEX}')
        try:
            query.awaitTermination()
        except StreamingQueryException as error:
            print('Query Exception caught:', error)



if __name__ == '__main__':
    main()
