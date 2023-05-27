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
    # log_list = open(file_path, "r")
    # log_data = log_list.readline()
    producer = KafkaProducer(bootstrap_servers=["localhost:29092"], value_serializer=serializer)
    try:
        with open(file_path) as f:
            for line in f:
                # out = line.encode('utf-8')
                print('Sending line', line)
                producer.send("messages", line)
                time.sleep(random.random() * 5)
    except Exception as error:
        print('Error Occured.\n\nClient disconnected.\n')
    # while log_data:
    #     _len = len(log_data)
    #     if log_data[_len - 1] == "\n":
    #         log_data = log_data[0:_len - 1]
    #     print(log_data)
    #     producer.send("messages", log_data)
    #     time_to_sleep = random.randint(1, 5)
    #     time.sleep(time_to_sleep)
    #     log_data = log_list.readline()
