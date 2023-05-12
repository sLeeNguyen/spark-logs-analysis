#!/bin/sh

# ./venv/bin/spark-submit --jars elasticsearch-hadoop-8.7.1/dist/elasticsearch-hadoop-8.7.1.jar main.py --host localhost --port 9999
./venv/bin/spark-submit --jars elasticsearch-hadoop-8.7.1/dist/elasticsearch-spark-30_2.12-8.7.1.jar main.py --host localhost --port 9999