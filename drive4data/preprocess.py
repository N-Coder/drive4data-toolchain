import concurrent.futures
import logging
import os
import signal
from contextlib import ExitStack, closing
from multiprocessing.managers import SyncManager

from drive4data.data.charge import preprocess_cycles
from drive4data.data.trips import preprocess_trips
from iss4e.db.influxdb import InfluxDBStreamingClient as InfluxDBClient
from iss4e.util import BraceMessage as __
from iss4e.util.config import load_config

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)

TIME_EPOCH = 'n'


def mgr_init():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logger.debug(__("Initialized manager in process {}", os.getpid()))


def main():
    config = load_config()
    cred = config["drive4data.influx"]
    dry_run = bool(config.get("dry_run", False))

    os.makedirs("out", exist_ok=True)
    with ExitStack() as stack:
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=8)
        stack.enter_context(executor)

        manager = SyncManager()
        manager.start(mgr_init)
        stack.enter_context(manager)

        client = InfluxDBClient(batched=False, async_executor=True, time_epoch=TIME_EPOCH, **cred)
        stack.enter_context(closing(client))

        try:
            if not dry_run:
                client.drop_measurement("trips")
            preprocess_trips(client, executor, manager, dry_run=dry_run)
            if not dry_run:
                client.drop_measurement("charge_cycles")
            preprocess_cycles(client, executor, manager, dry_run=dry_run)
        except:
            executor.shutdown(wait=False)
            raise


if __name__ == "__main__":
    main()
