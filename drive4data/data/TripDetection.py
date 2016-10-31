import concurrent.futures
import itertools
import logging
from datetime import timedelta
from typing import List

from webike.util import Cycle, MergingActivityDetection
from webike.util.DB import default_credentials
from webike.util.Logging import BraceMessage as __
from webike.util.Utils import progress

from util.InfluxDB import InfluxDBStreamingClient as InfluxDBClient, TO_SECONDS

__author__ = "Niko Fink"
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

cred = default_credentials("Drive4Data-DB")

TIME_EPOCH = 'n'


class TripDetection(MergingActivityDetection):
    def __init__(self):
        self.attr = 'veh_speed'

    def is_start(self, sample, previous):
        return sample[self.attr] > 1

    def is_end(self, sample, previous):
        return sample[self.attr] < 1 or self.get_duration(previous, sample) > timedelta(minutes=10)

    def accumulate_samples(self, new_sample, accumulator):
        if accumulator is not None:
            avg, cnt = accumulator
            return (avg + new_sample[self.attr]) / 2, cnt + 1
        else:
            return float(new_sample[self.attr]), 1

    def merge_stats(self, stats1, stats2):
        return (stats1[0] + stats2[0]) / 2, stats1[1] + stats2[1]

    def check_reject_reason(self, cycle):
        acc_avg, acc_cnt = cycle.stats
        if acc_cnt < 100:
            return "acc_cnt<100"
        elif self.get_duration(cycle.start, cycle.end) < timedelta(minutes=5):
            return "duration<5min"
        else:
            return None

    @staticmethod
    def get_duration(first, second):
        dur = (second['time'] - first['time']) * TO_SECONDS[TIME_EPOCH]
        assert dur >= 0, "second sample {} happened before first {}".format(second, first)
        return timedelta(seconds=dur)

    def extract_cycle_time(self, cycle: Cycle):
        return cycle.start['time'], cycle.end['time']

    def can_merge_times(self, last_start, last_end, new_start, new_end):
        return (new_start - last_end) * TO_SECONDS[TIME_EPOCH] < 10 * TO_SECONDS['m']


def write_influx(measurement, detector, cycles_curr: List[Cycle], cycles_curr_disc: List[Cycle]):
    timeseries = itertools.chain.from_iterable(cycle_to_events(
        cycle=cycle,
        measurement=measurement,
        participant=cycle.start['participant'],
        discarded=cycle.reject_reason,
        detector=detector
    ) for cycle in cycles_curr + cycles_curr_disc)
    if timeseries:
        client.write_points(timeseries, time_precision=TIME_EPOCH)


def cycle_to_events(cycle: Cycle, measurement, **tags):
    if tags is None:
        tags = {}
    for time, is_start in [(cycle.start['time'], True), (cycle.end['time'], False)]:
        yield {
            'measurement': measurement,
            'tags': {
                **tags,
                'started': is_start
            },
            'time': time,
            'fields': {
                'duration': cycle.end['time'] - cycle.start['time'],
                'value': cycle.stats[0],
                'sample_count': cycle.stats[1]
            }
        }


with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    client = InfluxDBClient(
        cred['host'], cred['port'], cred['user'], cred['passwd'], cred['db'],
        batched=False, async_executor=executor, time_epoch=TIME_EPOCH)
    client.delete_series(measurement="trips")

    res = client.stream_series("samples", fields="time, veh_speed, participant", batch_size=200000,
                               where="participant='5' AND veh_speed > 0")

    for nr, (series, iter) in enumerate(res):
        logger.info(__("#{}: {}", nr, series))

        detector = TripDetection()
        cycles_curr, cycles_curr_disc = detector(progress(iter))

        logger.info(__("Writing {} + {} = {} cycles", len(cycles_curr), len(cycles_curr_disc),
                       len(cycles_curr) + len(cycles_curr_disc)))
        write_influx("trips", detector.attr, cycles_curr, cycles_curr_disc)
