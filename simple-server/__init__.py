import json
import random
import sys
import time

from kafka import KafkaProducer


def serializer(message):
    return json.dumps(message).encode("utf-8")


if __name__ == "__main__":
    args = sys.argv
    file_path = "../data/NASA_access_log_Aug95"
    if len(args) >= 2:
        file_path = args[1]
    producer = KafkaProducer(bootstrap_servers=["localhost:29092"], value_serializer=serializer)
    try:
        with open(file_path) as f:
            for line in f:
                print('Sending line', line)
                producer.send("messages", line)
                time.sleep(random.random() * 5)
    except Exception as error:
        print('Error Occured.\n\nClient disconnected.\n')
