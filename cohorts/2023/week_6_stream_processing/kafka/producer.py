import orjson
import numpy as np
import pandas as pd

from kafka import KafkaProducer
from connection import get_kafka_config
from tqdm.auto import tqdm

from typing import Iterator


def csv_reader(path: str) -> Iterator[pd.Series]:
    for chunk in pd.read_csv(path, chunksize=10_000):
        for _, row in chunk.iterrows():
            yield row


def pd_series_serializer(s: pd.Series) -> bytes:
    return orjson.dumps(s.to_dict())


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(prog="TaxiProducer")
    parser.add_argument("-d", "--data", required=True, help="Input taxi dataset")
    parser.add_argument("-c", "--color", choices=["fhv", "green"], required=True, help="Input taxi color")
    parser.add_argument("-t", "--topic", required=True, help="Kafka topic")
    args = parser.parse_args()

    producer = KafkaProducer(
        value_serializer=pd_series_serializer,
        compression_type="gzip",
        **get_kafka_config(),
    )

    taxi_data = csv_reader(args.data)

    try:
        for row in tqdm(taxi_data):
            # NOTE: you should check key distribution
            if args.color == "fhv":
                key = row.PUlocationID
                key = int(key) if not np.isnan(key) else None
            elif args.color == "green":
                key = row.PULocationID
            else:
                raise KeyError("Unknow color for taxi", args.color)

            if key is None:
                continue

            producer.send(args.topic, row, bytes(key))
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()