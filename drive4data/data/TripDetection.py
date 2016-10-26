import concurrent.futures
import logging
from datetime import timedelta, datetime

from webike.data.ChargeCycle import extract_cycles
from webike.util.DB import default_credentials

from util.InfluxDB import InfluxDBStreamingClient as InfluxDBClient

__author__ = "Niko Fink"
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

cred = default_credentials("Drive4Data-DB")


def parse_time(sample):
    return datetime.fromtimestamp(sample['time'] / 1000000)


with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    client = InfluxDBClient(
        cred['host'], cred['port'], cred['user'], cred['passwd'], cred['db'],
        batched=False, async_executor=executor)

    res = client.stream_series("samples", fields="time, veh_speed", batch_size=1000000,
                               where="participant='5' AND veh_speed > 0")

    for nr, (series, iter) in enumerate(res):
        logger.info("#{}: {}".format(nr, series))

        cycles_curr, cycles_curr_disc = \
            extract_cycles(iter, "veh_speed", lambda x: x > 1, lambda x: x < 1, 100, timedelta(minutes=10),
                           timedelta(minutes=1), parse_time)
        # if cycles_curr:
        #     logger.info(tabulate(cycles_curr))
        #     logger.info(tabulate(cycles_curr_disc))
