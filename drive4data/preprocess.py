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
    dry_run = bool(config.get("dry_run", False))

    os.makedirs("out", exist_ok=True)
    with ExitStack() as stack:
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=8)
        stack.enter_context(executor)

        client = InfluxDBClient(batched=False, time_epoch=TIME_EPOCH, **cred)
        stack.enter_context(closing(client))

        if not dry_run:
            client.drop_measurement("trips")
        preprocess_trips(client, executor, dry_run=dry_run)
        if not dry_run:
            client.drop_measurement("charge_cycles")
        preprocess_cycles(client, executor, dry_run=dry_run)


if __name__ == "__main__":
    main()
