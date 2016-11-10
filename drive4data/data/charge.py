import csv
import logging
import multiprocessing
from concurrent.futures import Executor
from datetime import timedelta

from drive4data.data.activity import InfluxActivityDetection, MergeDebugMixin
from drive4data.data.soc import SoCMixin
from drive4data.preprocess import DRY_RUN
from iss4e.db.influxdb import InfluxDBStreamingClient as InfluxDBClient
from iss4e.db.influxdb import TO_SECONDS
from iss4e.util import BraceMessage as __, async_progress
from iss4e.util import progress
from iss4e.util.math import differentiate, smooth
from tabulate import tabulate
from webike.util.activity import Cycle

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)


class ChargeCycleDerivDetection(SoCMixin, MergeDebugMixin, InfluxActivityDetection):
    MAX_DELAY = timedelta(hours=1) / timedelta(seconds=1)

    def __init__(self, **kwargs):
        super().__init__(attr='soc_diff', max_merge_gap=timedelta(hours=2),
                         min_cycle_duration=timedelta(minutes=10), **kwargs)

    def __call__(self, cycle_samples):
        cycle_samples = smooth(cycle_samples, 'hvbatt_soc', 'soc', alpha=0.95)
        cycle_samples = differentiate(cycle_samples, 'soc', attr_time='time',
                                      delta_time=TO_SECONDS['h'] / TO_SECONDS['n'])
        return super().__call__(cycle_samples)

    def is_start(self, sample, previous):
        return sample[self.attr] > 5

    def is_end(self, sample, previous):
        return sample[self.attr] < 5 or self.get_duration(previous, sample) > self.MAX_DELAY

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


class ChargeCycleAttrDetection(SoCMixin, MergeDebugMixin, InfluxActivityDetection):
    MAX_DELAY = timedelta(hours=1) / timedelta(seconds=1)

    def __init__(self, **kwargs):
        super().__init__(max_merge_gap=timedelta(minutes=30),
                         min_cycle_duration=timedelta(minutes=10), **kwargs)

    def is_end(self, sample, previous):
        return self.get_duration(previous, sample) > self.MAX_DELAY

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


class ChargeCycleACVoltageDetection(ChargeCycleAttrDetection):
    def __init__(self, **kwargs):
        super().__init__(attr='charger_acvoltage', **kwargs)

    def is_start(self, sample, previous):
        return sample[self.attr] > 4

    def is_end(self, sample, previous):
        return sample[self.attr] < 4 or super().is_end(sample, previous)


class ChargeCycleIsChargingDetection(ChargeCycleAttrDetection):
    def __init__(self, **kwargs):
        super().__init__(attr='ischarging', **kwargs)

    def is_start(self, sample, previous):
        return sample[self.attr] > 3

    def is_end(self, sample, previous):
        return sample[self.attr] < 3 or super().is_end(sample, previous)


class ChargeCycleACHVPowerDetection(ChargeCycleAttrDetection):
    def __init__(self, **kwargs):
        super().__init__(attr='ac_hvpower', **kwargs)

    def is_start(self, sample, previous):
        return sample[self.attr] > 0

    def is_end(self, sample, previous):
        return sample[self.attr] <= 0 or super().is_end(sample, previous)


def preprocess_cycles(client: InfluxDBClient, executor: Executor):
    logger.info("Preprocessing charge cycles")

    manager = multiprocessing.Manager()
    queue = manager.Queue()
    futures = []
    for attr, where, detector in [
        ('charger_acvoltage', 'charger_acvoltage>0', ChargeCycleACVoltageDetection(time_epoch=client.time_epoch)),
        ('ischarging', 'ischarging>0', ChargeCycleIsChargingDetection(time_epoch=client.time_epoch)),
        ('ac_hvpower', 'ac_hvpower>0', ChargeCycleACHVPowerDetection(time_epoch=client.time_epoch)),
        ('hvbatt_soc', 'hvbatt_soc<200', ChargeCycleDerivDetection(time_epoch=client.time_epoch))
    ]:
        fields = ["time", "participant", "hvbatt_soc"]
        if attr not in fields:
            fields.append(attr)
        res = client.stream_series("samples", fields=fields, where=where, batch_size=500000)
        futures += [executor.submit(preprocess_cycle, client, nr, series, detector, iter, queue)
                    for nr, (series, iter) in enumerate(res)]

    logger.debug("Tasks started, waiting for results...")
    async_progress(futures, queue)
    futures = list(futures)
    logger.debug("Tasks done")
    futures.sort(key=lambda a: a[0:1])
    logger.info(__("Detected charge cycles:\n{}", tabulate(futures, headers=["attr", "#", "cycles", "cycles_disc"])))


def preprocess_cycle(client, nr, series, detector, iter, queue):
    logger.info(__("Processing #{}: {} {}", nr, detector.attr, series))
    cycles, cycles_disc = detector(progress(iter, delay=4, remote=queue.put))

    if not DRY_RUN:
        logger.info(__("Writing {} + {} = {} cycles", len(cycles), len(cycles_disc),
                       len(cycles) + len(cycles_disc)))
        client.write_points(
            detector.cycles_to_timeseries(cycles + cycles_disc, "charge_cycles"),
            tags={'detector': detector.attr},
            time_precision=client.time_epoch)

        for name, hist in [('merges', detector.merges)]:
            with open('out/hist_charge_{}_{}_{}.csv'.format(name, detector.attr, nr), 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(hist)

    logger.info(__("Task #{}: {} {} completed", nr, detector.attr, series))
    return detector.attr, nr, len(cycles), len(cycles_disc)
