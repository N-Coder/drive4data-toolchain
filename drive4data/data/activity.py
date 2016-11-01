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

        # TODO make more generic
        if 'metrics' not in accumulator:
            accumulator['metrics'] = dict((n, ActivityMetric(n))
                                          for n in ["veh_odometer", "hvbatt_soc", "outside_air_temp"])
        for metric in accumulator['metrics'].values():
            metric.update(new_sample)

        if 'distance' not in accumulator:
            accumulator['distance'] = 0
        if '__prev' in accumulator:
            interval = self.get_duration(accumulator['__prev'], new_sample)
            accumulator['distance'] += (interval / TO_SECONDS['h']) * new_sample['veh_speed']
        accumulator['__prev'] = new_sample

        return accumulator

    def merge_stats(self, stats1, stats2):
        metrics = {}
        metrics1, metrics2 = stats1['metrics'], stats2['metrics']
        for key in metrics1.keys() & metrics2.keys():
            metrics[key] = metrics1[key].merge(metrics2[key])
        return {
            'avg': (stats1['avg'] + stats2['avg']) / 2,
            'cnt': stats1['cnt'] + stats2['cnt'],
            'metrics': metrics,
            'distance': stats1['distance'] + stats2['distance']
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
        # TODO check availability, time gap from start/end
        metrics_odo = cycle.stats['metrics']["veh_odometer"]
        metrics_soc = cycle.stats['metrics']["hvbatt_soc"]
        metrics_temp = cycle.stats['metrics']["outside_air_temp"]
        yield {
            'measurement': measurement,
            'tags': {
                **tags,
                'started': is_start
            },
            'time': time,
            'fields': {
                'duration': cycle.end['time'] - cycle.start['time'],
                'value': cycle.stats['avg'],
                'sample_count': cycle.stats['cnt'],
                'est_distance': cycle.stats['distance'],
                'odo_start': metrics_odo.first_value(),
                'odo_end': metrics_odo.last_value(),
                'soc_start': metrics_soc.first_value(),
                'soc_end': metrics_soc.last_value(),
                'outside_air_temp': metrics_temp.first_value()
            }
        }
