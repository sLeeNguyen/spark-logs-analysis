#!/bin/sh

./venv/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 --jars elasticsearch-hadoop-8.7.1/dist/elasticsearch-spark-30_2.12-8.7.1.jar main_kafka.py --host localhost --port 9999