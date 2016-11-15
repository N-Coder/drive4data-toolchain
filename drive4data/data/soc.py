import warnings
from datetime import datetime

import numpy as np
from drive4data.data.activity import ValueMemory
from webike.util.activity import Cycle


def _d(date):
    return datetime.strptime(date, '%Y-%m-%d %H:%M:%S')


# participants 4 and 6 changed cars at a certain date and their new cars have different SoC ranges
LINEAR_FACTORS = {
    '1': [(None, np.poly1d((1.525, -34)))],
    '2': [(None, np.poly1d((1.525, -34)))],
    '3': [(_d('2014-11-18 18:35:00'), np.poly1d((1.525, -34))),
          (None, np.poly1d((1, 0)))],
    '4': [(_d('2014-01-24 15:57:49'), np.poly1d((1.525, -34))),
          (None, np.poly1d((1, 0)))],
    '5': [(None, np.poly1d((1.525, -34)))],
    '6': [(_d('2016-02-25 01:55:28'), np.poly1d((1, 0))),
          (None, np.poly1d((1, 0)))],
    '7': [(None, np.poly1d((1.28, -15)))],
    '8': [(None, np.poly1d((1, 0)))],
    '9': [(None, np.poly1d((1, 0)))],
    '10': [(None, np.poly1d((1, 0)))]
}


def rescale_soc(time, participant, soc_value):
    for end, poly in LINEAR_FACTORS[participant]:
        if not end or end >= time:
            soc_value = poly(soc_value)
            break
    else:
        warnings.warn("could not find a soc transformation for participant {} and time {}".format(participant, time))
    return float(np.clip(soc_value, 0, 100))


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

    def cycle_to_events(self, cycle: Cycle, measurement=""):
        soc = cycle.stats["soc"]
        data = {
            'soc_start': self.rescale_soc(soc.first) if soc.first else None,
            'soc_end': self.rescale_soc(soc.last) if soc.last else None
        }
        for event in super().cycle_to_events(cycle, measurement):
            event['fields'].update(data)
            yield event

    def rescale_soc(self, sample):
        return float(sample['hvbatt_soc'])
        # dt = datetime.fromtimestamp(sample['time'] * TO_SECONDS[self.epoch])
        # return rescale_soc(dt, sample['participant'], sample['hvbatt_soc'])
