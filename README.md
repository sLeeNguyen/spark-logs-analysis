# Spark Web Logs Analysis

This is an academic project which aim to create a data streaming pipeline using Spark Structured Streaming, Elasticsearch and Kibana. The project uses real-world production logs from NASA then process it using Spark Structured Streaming. The output is stored on Elasticsearch and visualized in a Dashboard using Kibana.

**Resources:**

- Data: [NASA-HTTP](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html)

**References:**

- [Elasticsearch for Apache Hadoop](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/reference.html)

This project also inspired by several similar projects:

- [Scalable Web Server Log Analytics with Apache Spark](https://github.com/dipanjanS/data_science_for_all/blob/master/tds_scalable_log_analytics/Scalable_Log_Analytics_Spark.ipynb)
- [Web-Server-Log-Analysis-with-PySpark](https://github.com/olalakul/Web-Server-Log-Analysis-PySpark)
- [Log Analysis 📈 with Spark Streaming 📺, ElasticSearch 📊 and Kibana 👀 :](https://github.com/isbainemohamed/Log-Analysis-using-Spark-Streaming-ELK)

**Required:**

- Python 3.8
- docker, docker-compose

## Set up

```
# Virtual environment
python3 -m venv ./venv
source ./venv/bin/activate

# Install libraries
pip install -r requirements.txt
```

## Run

Create `.env` file follow the format in `.env.sample`

```
cp .env.sample .env
```

Download NASA logs data:

```
chmod +x *.sh // make files executable
./prepare.sh // download
```

Elasticsearch and Kibana:

```
docker-compose -f elasticsearch-kibana-compose.yaml up
```

_Note: Elasticsearch node runs on port 9200 (`ES_PORT` in `.env`) (https://localhost:9200), Kibana 5601 (https://localhost:5601) (`KIBANA_PORT` in `.env`)._

Simulated data server: run Netcat as a data server

```
nc -lk 9999
```

Spark:

```
./run.sh
```

Push some logs on data server and see the result on Kibana
