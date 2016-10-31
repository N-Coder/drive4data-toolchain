import concurrent.futures
import logging
from datetime import timedelta

from webike.util import MergingActivityDetection
from webike.util.DB import default_credentials
from webike.util.Logging import BraceMessage as __
from webike.util.Utils import progress

from data.activity import InfluxActivityDetection, cycles_to_timeseries
from util.InfluxDB import InfluxDBStreamingClient as InfluxDBClient

__author__ = "Niko Fink"
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

TIME_EPOCH = 'n'
cred = default_credentials("Drive4Data-DB")


class TripDetection(InfluxActivityDetection, MergingActivityDetection):
    def __init__(self, *args, **kwargs):
        super().__init__('veh_speed', *args, **kwargs)

    def is_start(self, sample, previous):
        return sample[self.attr] > 1

    def is_end(self, sample, previous):
        return sample[self.attr] < 1 or self.get_duration(previous, sample) > timedelta(minutes=10)


with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    client = InfluxDBClient(
        cred['host'], cred['port'], cred['user'], cred['passwd'], cred['db'],
        batched=False, async_executor=executor, time_epoch=TIME_EPOCH)
    client.delete_series(measurement="trips")

    res = client.stream_series("samples", fields="time, veh_speed, participant", batch_size=500000,
                               where="veh_speed > 0")

    for nr, (series, iter) in enumerate(res):
        logger.info(__("#{}: {}", nr, series))

        detector = TripDetection(time_epoch=TIME_EPOCH)
        cycles_curr, cycles_curr_disc = detector(progress(iter))

        logger.info(__("Writing {} + {} = {} cycles", len(cycles_curr), len(cycles_curr_disc),
                       len(cycles_curr) + len(cycles_curr_disc)))
        client.write_points(cycles_to_timeseries("trips", detector.attr, cycles_curr + cycles_curr_disc),
                            time_precision=TIME_EPOCH)
