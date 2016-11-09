import concurrent.futures
import logging
import os
from contextlib import ExitStack, closing

from drive4data.data.charge import preprocess_cycles
from drive4data.data.trips import preprocess_trips
from iss4e.db.influxdb import InfluxDBStreamingClient as InfluxDBClient
from iss4e.util.config import load_config

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)

TIME_EPOCH = 'n'


def main():
    config = load_config()
    cred = config["drive4data.influx"]

    os.makedirs("out", exist_ok=True)
    with ExitStack() as stack:
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=8)
        stack.enter_context(executor)

        client = InfluxDBClient(batched=False, time_epoch=TIME_EPOCH, **cred)
        stack.enter_context(closing(client))

        client.delete_series(measurement="trips")
        preprocess_trips(client, executor)
        client.delete_series(measurement="charge_cycles")
        preprocess_cycles(client, executor)


if __name__ == "__main__":
    main()
