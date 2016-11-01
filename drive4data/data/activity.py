import itertools
import sys
from datetime import timedelta
from typing import List

from webike.util import ActivityDetection, Cycle

from util.InfluxDB import TO_SECONDS


class ActivityMetric:
    def __init__(self, name):
        self.name = name
        self.first = self.last = None

    def update(self, sample):
        if sample and self.name in sample \
                and sample[self.name] is not None \
                and sample[self.name] < sys.float_info.max:
            if not self.first:
                self.first = sample
            self.last = sample
        return self

    def merge(self, later):
        assert later.name == self.name
        merged = ActivityMetric(self.name)
        merged.first = self.first
        merged.last = later.last
        return merged

    def first_value(self):
        return float(self.first[self.name]) if self.first else None

    def last_value(self):
        return float(self.last[self.name]) if self.last else None

    def get_time_gap(self, cycle, delta_func, missing_value=None):
        diff_first = diff_last = missing_value
        if self.first:
            diff_first = delta_func(cycle.start, self.first)
        if self.last:
            diff_last = delta_func(self.last, cycle.end)
        return diff_first, diff_last


class InfluxActivityDetection(ActivityDetection):
    def __init__(self, attr, time_epoch='n', min_sample_count=100, min_cycle_duration=timedelta(minutes=5),
                 max_merge_gap=timedelta(minutes=10)):
        self.attr = attr
        self.epoch = time_epoch
        self.min_sample_count = min_sample_count
        self.min_cycle_duration_s = min_cycle_duration / timedelta(seconds=1)
        self.max_merge_gap = max_merge_gap
        super().__init__()

    def accumulate_samples(self, new_sample, accumulator):
        if 'avg' in accumulator:
            accumulator['avg'] = (accumulator['avg'] + new_sample[self.attr]) / 2
        else:
            accumulator['avg'] = float(new_sample[self.attr])

        if 'cnt' not in accumulator:
            accumulator['cnt'] = 0
        accumulator['cnt'] += 1
        return accumulator

    def merge_stats(self, stats1, stats2):
        return {
            'avg': (stats1['avg'] + stats2['avg']) / 2,
            'cnt': stats1['cnt'] + stats2['cnt'],
        }

    def check_reject_reason(self, cycle):
        if cycle.stats['cnt'] < self.min_sample_count:
            return "acc_cnt<{}".format(self.min_sample_count)
        elif self.get_duration(cycle.start, cycle.end) < self.min_cycle_duration_s:
            return "duration<{}s".format(self.min_cycle_duration_s)
        else:
            return None

    def get_duration(self, first, second):
        dur = (second['time'] - first['time']) * TO_SECONDS[self.epoch]
        assert dur >= 0, "second sample {} happened before first {}".format(second, first)
        return dur

    def extract_cycle_time(self, cycle: Cycle):
        return cycle.start['time'], cycle.end['time']

    def can_merge_times(self, last_start, last_end, new_start, new_end):
        return timedelta(seconds=(new_start - last_end) * TO_SECONDS[self.epoch]) < self.max_merge_gap

    def cycles_to_timeseries(self, cycles: List[Cycle], measurement):
        return itertools.chain.from_iterable(self.cycle_to_events(cycle, measurement) for cycle in cycles)

    def cycle_to_events(self, cycle: Cycle, measurement):
        for time, is_start in [(cycle.start['time'], True), (cycle.end['time'], False)]:
            yield {
                'measurement': measurement,
                'time': time,
                'tags': {
                    'participant': cycle.start['participant'],
                    'discarded': cycle.reject_reason,
                    'started': is_start
                },
                'fields': {
                    'duration': cycle.end['time'] - cycle.start['time'],
                    'value': cycle.stats['avg'],
                    'sample_count': cycle.stats['cnt'],

                }
            }
