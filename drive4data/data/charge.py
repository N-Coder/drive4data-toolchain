import csv
import logging
from datetime import timedelta

from drive4data.data.activity import InfluxActivityDetection, MergeDebugMixin
from drive4data.data.soc import SoCMixin
from iss4e.db.influxdb import TO_SECONDS
from iss4e.util import BraceMessage as __
from iss4e.util import progress
from iss4e.util.math import differentiate, smooth
from webike.util.activity import Cycle

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)


class ChargeCycleDerivDetection(SoCMixin, MergeDebugMixin, InfluxActivityDetection):
    MAX_DELAY = timedelta(minutes=10) / timedelta(seconds=1)

    def __init__(self, **kwargs):
        super().__init__(attr='soc_diff', max_merge_gap=timedelta(hours=2), **kwargs)

    def __call__(self, cycle_samples):
        cycle_samples = (sample for sample in cycle_samples if sample['hvbatt_soc'] < 200)
        cycle_samples = smooth(cycle_samples, 'hvbatt_soc', 'soc', alpha=0.999)
        cycle_samples = differentiate(cycle_samples, 'soc', label_diff='soc_diff_raw',
                                      attr_time='time', delta_time=TO_SECONDS['h'] / TO_SECONDS['n'])
        cycle_samples = smooth(cycle_samples, 'soc_diff_raw', 'soc_diff', alpha=0.999)
        return super().__call__(cycle_samples)

    def is_start(self, sample, previous):
        return sample['soc_diff'] > 5

    def is_end(self, sample, previous):
        return sample['soc_diff'] < -0.1 or \
               sample['soc_diff'] > 97 or \
               self.get_duration(previous, sample) > self.MAX_DELAY


class ChargeCycleCurrentDetection(SoCMixin, MergeDebugMixin, InfluxActivityDetection):
    MAX_DELAY = timedelta(hours=1) / timedelta(seconds=1)

    def __init__(self, **kwargs):
        super().__init__(attr='charger_accurrent', max_merge_gap=timedelta(minutes=30),
                         min_cycle_duration=timedelta(minutes=10), **kwargs)

    def is_start(self, sample, previous):
        return sample[self.attr] > 4

    def is_end(self, sample, previous):
        return sample[self.attr] < 4 or self.get_duration(previous, sample) > self.MAX_DELAY

    def check_reject_reason(self, cycle: Cycle):
        if (cycle.end['hvbatt_soc'] - cycle.start['hvbatt_soc']) < 10:
            return "delta_soc<10%"
        return super().check_reject_reason(cycle)

    def can_merge(self, last_cycle, new_cycle: Cycle):
        if super().can_merge(last_cycle, new_cycle):
            soc_change = new_cycle.start['hvbatt_soc'] - last_cycle.end['hvbatt_soc']
            if soc_change < -2:
                return False
        else:
            return False


def preprocess_cycles(client):
    logger.info("Preprocessing charge cycles")
    detector = ChargeCycleCurrentDetection(time_epoch=client.time_epoch)
    res = client.stream_series(
        "samples",
        fields="time, hvbatt_soc, charger_accurrent, participant",
        batch_size=500000,
        where="hvbatt_soc < 200 OR charger_accurrent > 0")
    for nr, (series, iter) in enumerate(res):
        logger.info(__("#{}: {}", nr, series))
        cycles_curr, cycles_curr_disc = detector(progress(iter))

        logger.info(__("Writing {} + {} = {} cycles", len(cycles_curr), len(cycles_curr_disc),
                       len(cycles_curr) + len(cycles_curr_disc)))
        client.write_points(
            detector.cycles_to_timeseries(cycles_curr + cycles_curr_disc, "charge_cycles"),
            tags={'detector': detector.attr},
            time_precision=client.time_epoch)

        for name, hist in [('merges', detector.merges)]:
            with open('out/hist_charge_{}_{}.csv'.format(name, nr), 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(hist)

        detector.merges = []
