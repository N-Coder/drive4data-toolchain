import concurrent.futures
import logging
import os
from contextlib import ExitStack, closing

from iss4e.db.influxdb import InfluxDBStreamingClient as InfluxDBClient
from iss4e.util.config import load_config

from drive4data.data.charge import preprocess_cycles
from drive4data.data.trips import preprocess_trips

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)

TIME_EPOCH = 'n'


def main():
    config = load_config()
    cred = config["drive4data.influx"]

    os.makedirs("out", exist_ok=True)
    with ExitStack() as stack:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        stack.enter_context(executor)

        client = InfluxDBClient(batched=False, async_executor=executor, time_epoch=TIME_EPOCH, **cred)
        stack.enter_context(closing(client))

        client.delete_series(measurement="trips")
        preprocess_trips(client)
        client.delete_series(measurement="charge_cycles")
        preprocess_cycles(client)


if __name__ == "__main__":
    main()
