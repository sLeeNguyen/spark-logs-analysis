import string
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

user_ids = list(range(1, 101))
recipient_ids = list(range(1, 101))


def generate_message() -> dict:
    random_user_id = random.choice(user_ids)
    recipient_ids_copy = recipient_ids.copy()
    recipient_ids_copy.remove(random_user_id)
    random_recipient_id = random.choice(recipient_ids_copy)
    message = "".join(random.choice(string.ascii_letters) for i in range(32))
    return {"user_id": random_user_id, "recipient_id": random_recipient_id, "message": message}


def serializer(message):
    return json.dumps(message).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=["localhost:29092"], value_serializer=serializer)

if __name__ == "__main__":
    print(f"config: {producer.config}")
    print(f"connect: {producer.bootstrap_connected()}")
    while True:
        dummy_message = generate_message()
        print(f"Producing message @ {datetime.now()} | Message = {str(dummy_message)}")
        producer.send("messages", dummy_message)
        time_to_sleep = random.randint(1, 11)
        time.sleep(time_to_sleep)
