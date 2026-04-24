from google.cloud import pubsub_v1
import json
import time
import random

project_id = "project-36c16f64-83f8-4df3-b8b"
topic_id = "employee-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

names = ["Alice", "Bob", "Charlie", "David"]
departments = ["IT", "HR", "Finance", "Sales"]

while True:
    data = {
        "id": random.randint(1, 1000),
        "name": random.choice(names),
        "age": random.randint(22, 50),
        "department": random.choice(departments),
        "salary": round(random.uniform(30000, 90000), 2)
    }

    message = json.dumps(data).encode("utf-8")
    publisher.publish(topic_path, message)

    print("Published:", data)
    time.sleep(2)