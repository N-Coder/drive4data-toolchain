import logging
from datetime import timedelta

from webike.util import Cycle
from webike.util import MergingActivityDetection
from webike.util.Logging import BraceMessage as __
from webike.util.Utils import progress, differentiate, smooth

from data.activity import InfluxActivityDetection
from util.InfluxDB import TO_SECONDS

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)


class ChargeCycleDerivDetection(InfluxActivityDetection, MergingActivityDetection):
    MAX_DELAY = timedelta(minutes=10) / timedelta(seconds=1)

    def __init__(self, *args, **kwargs):
        super().__init__('soc_diff', max_merge_gap=timedelta(hours=2), *args, **kwargs)

    def __call__(self, cycle_samples):
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


class ChargeCycleCurrentDetection(InfluxActivityDetection, MergingActivityDetection):
    MAX_DELAY = timedelta(hours=1) / timedelta(seconds=1)

    def __init__(self, *args, **kwargs):
        super().__init__('charger_accurrent', max_merge_gap=timedelta(minutes=30),
                         min_cycle_duration=timedelta(hours=1),
                         *args, **kwargs)

    def is_start(self, sample, previous):
        return sample[self.attr] > 4

    def is_end(self, sample, previous):
        return sample[self.attr] < 4 or self.get_duration(previous, sample) > self.MAX_DELAY

    def check_reject_reason(self, cycle: Cycle):
        reason = super().check_reject_reason(cycle)
        if reason:
            return reason
        elif (cycle.end['hvbatt_soc'] - cycle.start['hvbatt_soc']) < 10:
            return "delta_soc<10%"
        else:
            return None


def preprocess_cycles(client):
    logger.info("Preprocessing charge cycles")
    detector = ChargeCycleCurrentDetection(time_epoch=client.time_epoch)
    res = client.stream_series(
        "samples",
        fields="time, hvbatt_soc, charger_accurrent, participant",
        batch_size=500000,
        where="hvbatt_soc < 200 AND charger_accurrent > 0")
    for nr, (series, iter) in enumerate(res):
        logger.info(__("#{}: {}", nr, series))
        cycles_curr, cycles_curr_disc = detector(progress(iter))

        logger.info(__("Writing {} + {} = {} cycles", len(cycles_curr), len(cycles_curr_disc),
                       len(cycles_curr) + len(cycles_curr_disc)))
        client.write_points(
            detector.cycles_to_timeseries(cycles_curr + cycles_curr_disc, "charge_cycles"),
            tags={'detector': detector.attr},
            time_precision=client.time_epoch)
