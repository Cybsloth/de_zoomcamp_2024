import json
import time

import click
import pandas as pd
from kafka import KafkaProducer


REQUIRED_FIELDS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
]

def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def connect_to_kafka(host: str, port: int) -> KafkaProducer:
    server = f"{host}:{port}"

    producer = KafkaProducer(
        bootstrap_servers=[server], value_serializer=json_serializer
    )
    producer.bootstrap_connected()

    return producer



def send_data(producer: KafkaProducer, topic_name: str, path: str, read_chunk: int = 1000):
    start = time.time()
    df = pd.read_csv(path, chunksize=read_chunk)
    num_messages = 0
    for chunk in df:
        for row in chunk.itertuples(index=False):
            row_dict = {col: getattr(row, col) for col in row._fields if col in REQUIRED_FIELDS}

            producer.send(topic_name, value=row_dict)
        num_messages += chunk.shape[0]

    end = time.time()

    print(f'Sending took {int(end - start):.2f}')


@click.command
@click.option("-d", "data_path", help="Path to taxi-data")
@click.option("-h", "host", default="localhost", help="Kafka's host.")
@click.option("-p", "port", default=9092, help="Kafka's port.")
@click.option("-t", "topic", default='test-topic', help="Kafka's topic.")
def main(data_path, host, port, topic):
    kafka_producer = connect_to_kafka(host, port)

    send_data(kafka_producer, topic_name=topic, path=data_path)



if __name__ == "__main__":
    main()
