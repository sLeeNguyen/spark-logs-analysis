import json
from kafka import KafkaConsumer

if __name__ == "__main__":
    consumer = KafkaConsumer("messages", bootstrap_servers="localhost:9092", auto_offset_reset="earliest")
    for message in consumer:
        my_bytes_value = message.value
        my_json = my_bytes_value.decode("utf8").replace("'", '"')
        print(json.loads(my_json))
