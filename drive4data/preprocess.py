import concurrent.futures
import logging
import os
from contextlib import ExitStack, closing

from webike.util.DB import default_credentials

from data.trips import preprocess_trips
from util.InfluxDB import InfluxDBStreamingClient as InfluxDBClient

__author__ = "Niko Fink"
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

TIME_EPOCH = 'n'
cred = default_credentials("Drive4Data-DB")


def main():
    os.makedirs("out", exist_ok=True)
    with ExitStack() as stack:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        stack.enter_context(executor)

        client = InfluxDBClient(
            cred['host'], cred['port'], cred['user'], cred['passwd'], cred['db'],
            batched=False, async_executor=executor, time_epoch=TIME_EPOCH)
        stack.enter_context(closing(client))

        client.delete_series(measurement="trips")

        preprocess_trips(client)


if __name__ == "__main__":
    main()
