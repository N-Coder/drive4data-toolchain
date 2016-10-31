import itertools
from datetime import timedelta
from typing import List

from webike.util import ActivityDetection, Cycle

from util.InfluxDB import TO_SECONDS


class InfluxActivityDetection(ActivityDetection):
    def __init__(self, attr, time_epoch='n', min_sample_count=100, min_cycle_duration=timedelta(minutes=5),
                 max_merge_gap=timedelta(minutes=10)):
        self.attr = attr
        self.epoch = time_epoch
        self.min_sample_count = min_sample_count
        self.min_cycle_duration = min_cycle_duration
        self.max_merge_gap = max_merge_gap
        super().__init__()

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
        if acc_cnt < self.min_sample_count:
            return "acc_cnt<{}".format(self.min_sample_count)
        elif self.get_duration(cycle.start, cycle.end) < self.min_cycle_duration:
            return "duration<{}".format(self.min_cycle_duration)
        else:
            return None

    def get_duration(self, first, second):
        dur = (second['time'] - first['time']) * TO_SECONDS[self.epoch]
        assert dur >= 0, "second sample {} happened before first {}".format(second, first)
        return timedelta(seconds=dur)

    def extract_cycle_time(self, cycle: Cycle):
        return cycle.start['time'], cycle.end['time']

    def can_merge_times(self, last_start, last_end, new_start, new_end):
        return timedelta(seconds=(new_start - last_end) * TO_SECONDS[self.epoch]) < self.max_merge_gap


def cycles_to_timeseries(measurement, detector, cycles: List[Cycle]):
    return itertools.chain.from_iterable(cycle_to_events(
        cycle=cycle,
        measurement=measurement,
        participant=cycle.start['participant'],
        discarded=cycle.reject_reason,
        detector=detector
    ) for cycle in cycles)


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
