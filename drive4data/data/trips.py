import csv
import logging
from datetime import timedelta

from webike.util import Cycle
from webike.util import MergingActivityDetection
from webike.util.Logging import BraceMessage as __
from webike.util.Utils import progress

from data.activity import InfluxActivityDetection, ActivityMetric
from util.InfluxDB import TO_SECONDS

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)


class TripDetection(InfluxActivityDetection, MergingActivityDetection):
    MIN_DURATION = timedelta(minutes=10) / timedelta(seconds=1)

    def __init__(self, *args, **kwargs):
        self.hist_metrics_odo = []
        self.hist_metrics_soc = []
        self.hist_metrics_temp = []
        self.hist_distance = []
        super().__init__('veh_speed', *args, **kwargs)

    def is_start(self, sample, previous):
        return sample[self.attr] > 1

    def is_end(self, sample, previous):
        return sample[self.attr] < 1 or self.get_duration(previous, sample) > self.MIN_DURATION

    def accumulate_samples(self, new_sample, accumulator):
        accumulator = super().accumulate_samples(new_sample, accumulator)

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
        stats = super().merge_stats(stats1, stats2)

        stats['metrics'] = {}
        metrics1, metrics2 = stats1['metrics'], stats2['metrics']
        for key in metrics1.keys() & metrics2.keys():
            stats['metrics'][key] = metrics1[key].merge(metrics2[key])

        stats['distance'] = stats1['distance'] + stats2['distance']

        return stats

    def cycle_to_events(self, cycle: Cycle, measurement):
        metrics_odo = cycle.stats['metrics']["veh_odometer"]
        self.hist_metrics_odo.append(metrics_odo.get_time_gap(cycle, self.get_duration, missing_value="X"))
        metrics_soc = cycle.stats['metrics']["hvbatt_soc"]
        self.hist_metrics_soc.append(metrics_soc.get_time_gap(cycle, self.get_duration, missing_value="X"))
        metrics_temp = cycle.stats['metrics']["outside_air_temp"]
        self.hist_metrics_temp.append(metrics_temp.get_time_gap(cycle, self.get_duration, missing_value="X"))

        if metrics_odo.first_value() and metrics_odo.last_value():
            self.hist_distance.append((metrics_odo.first_value(), metrics_odo.last_value(), cycle.stats['distance']))

        for event in super().cycle_to_events(cycle, measurement):
            event['fields'].update({
                'est_distance': cycle.stats['distance'],
                'odo_start': metrics_odo.first_value(),
                'odo_end': metrics_odo.last_value(),
                'soc_start': metrics_soc.first_value(),
                'soc_end': metrics_soc.last_value(),
                'outside_air_temp': metrics_temp.first_value()
            })
            yield event


def preprocess_trips(client):
    detector = TripDetection(time_epoch=client.time_epoch)
    res = client.stream_series(
        "samples",
        fields="time, veh_speed, participant, veh_odometer, hvbatt_soc, outside_air_temp",
        batch_size=500000,
        where="veh_speed > 0")
    for nr, (series, iter) in enumerate(res):
        logger.info(__("#{}: {}", nr, series))
        cycles_curr, cycles_curr_disc = detector(progress(iter))

        logger.info(__("Writing {} + {} = {} cycles", len(cycles_curr), len(cycles_curr_disc),
                       len(cycles_curr) + len(cycles_curr_disc)))
        client.write_points(
            detector.cycles_to_timeseries(cycles_curr + cycles_curr_disc, "trips"),
            tags={'detector': detector.attr},
            time_precision=client.time_epoch)

        for name, hist in [('odo', detector.hist_metrics_odo), ('soc', detector.hist_metrics_soc),
                           ('temp', detector.hist_metrics_temp), ('dist', detector.hist_distance)]:
            with open('out/hist_{}_{}.csv'.format(name, nr), 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(hist)

        detector.hist_metrics_odo = []
        detector.hist_metrics_soc = []
        detector.hist_metrics_temp = []
        detector.hist_distance = []
