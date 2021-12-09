#!/usr/bin/env python
from datetime import datetime, timezone
import json
import threading, time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        while not self.stop_event.is_set():
            curdate = datetime.now().replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            vin_time = "testvin" + curdate
            sample_data = {"H11108": "5.000000", "H22040": "0.28", "cangen": "H2G2C", "ignitiontime": curdate,
                           "vin_time": vin_time}

            # a = {"H11108":"5.000000", "H22040": "0.28", "cangen":"H2G2C", "ignitiontime":"20211206152435", "vin_time":"testvin_20211206152435"}

            message = json.dumps(sample_data).encode('utf-8')
            print(message)

            producer.send('my-topic', message)
            time.sleep(1)

        producer.close()


def main():
    # Create 'my-topic' Kafka topic
    """
    try:
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')

        topic = NewTopic(name='my-topic',
                         num_partitions=1,
                         replication_factor=1)
        admin.create_topics([topic])
    except Exception:
        pass
    """

    tasks = [
        Producer(),
    ]

    # Start threads of a publisher/producer and a subscriber/consumer to 'my-topic' Kafka topic
    for t in tasks:
        t.start()

    time.sleep(10)

    # Stop threads
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    main()
