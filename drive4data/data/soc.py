from datetime import datetime

from drive4data.data.activity import ValueMemory
from iss4e.db.influxdb import TO_SECONDS
from webike.util.activity import Cycle


def _d(date):
    return datetime.strptime(date, '%Y-%m-%d %H:%M:%S')


SCALING_FACTORS = {
    '1': [(None, 15, 85)],
    '2': [(None, 15, 85)],
    '3': [(None, 0, 100)],
    '4': [(_d('2014-01-24 15:57:49'), 15, 85),
          (None, 0, 100)],
    '5': [(None, 15, 85)],
    '6': [(_d('2016-02-25 01:55:28'), 0, 100),
          (None, 0, 100)],
    '7': [(None, 0, 100)],
    '8': [(None, 0, 100)],
    '9': [(None, 0, 100)],
    '10': [(None, 0, 100)]
}


def rescale_soc(time, participant=None, soc_value=None):
    min = max = None
    for (end, min, max) in SCALING_FACTORS[participant]:
        if not end or end >= time:
            break
    return (soc_value - min) / (max - min)


class SoCMixin(object):
    def accumulate_samples(self, new_sample, accumulator):
        accumulator = super().accumulate_samples(new_sample, accumulator)

        if 'soc' not in accumulator:
            accumulator['soc'] = ValueMemory("hvbatt_soc")
        accumulator['soc'].update(new_sample)

        return accumulator

    def merge_stats(self, stats1, stats2):
        stats = super().merge_stats(stats1, stats2)
        stats['soc'] = stats1['soc'].merge(stats2['soc'])
        return stats

    def cycle_to_events(self, cycle: Cycle, measurement):
        soc = cycle.stats["soc"]
        data = {
            'soc_start': self.rescale_soc(soc.first) if soc.first else None,
            'soc_end': self.rescale_soc(soc.last) if soc.last else None
        }
        for event in super().cycle_to_events(cycle, measurement):
            event['fields'].update(data)
            yield event

    def rescale_soc(self, sample):
        dt = datetime.fromtimestamp(sample['time'] * TO_SECONDS[self.epoch])
        return rescale_soc(dt, sample['participant'], sample['hvbatt_soc'])
