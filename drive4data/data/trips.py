import copy
import csv
import logging
from datetime import datetime
from datetime import timedelta

from iss4e.db.influxdb import InfluxDBStreamingClient as InfluxDBClient
from iss4e.db.influxdb import TO_SECONDS
from iss4e.util import BraceMessage as __
from iss4e.util import progress

import webike
from data.activity import ActivityMetric
from data.activity import InfluxActivityDetection
from webike.data import Trips
from webike.util.activity import MergingActivityDetection, Cycle
from webike.util.plot import to_hour_bin

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
        return sample[self.attr] > 0.1

    def is_end(self, sample, previous):
        return sample[self.attr] < 0.1 or self.get_duration(previous, sample) > self.MIN_DURATION

    def accumulate_samples(self, new_sample, accumulator):
        accumulator = super().accumulate_samples(new_sample, accumulator)

        if 'metrics' not in accumulator:
            accumulator['metrics'] = dict((n, ActivityMetric(n))
                                          for n in ["veh_odometer", "hvbatt_soc", "outside_air_temp"])
        for metric in accumulator['metrics'].values():
            metric.update(new_sample)

        if 'distance' not in accumulator:
            accumulator['distance'] = 0.0
        if '__prev' in accumulator:
            interval = self.get_duration(accumulator['__prev'], new_sample)
            distance = (interval / TO_SECONDS['h']) * new_sample['veh_speed']
            accumulator['distance'] += distance
            # TODO these values are not right:
            if new_sample.get('fuel_rate') is not None:
                if 'cons_gasoline' not in accumulator:
                    accumulator['cons_gasoline'] = 0.0
                accumulator['cons_gasoline'] += distance * new_sample['fuel_rate']
            if new_sample.get('hvbatt_current') is not None and new_sample.get('hvbatt_voltage') is not None:
                if 'cons_energy' not in accumulator:
                    accumulator['cons_energy'] = 0.0
                accumulator['cons_energy'] += \
                    interval * new_sample['hvbatt_current'] * new_sample['hvbatt_voltage'] / TO_SECONDS['h']

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
            for key in ['cons_energy', 'cons_gasoline']:
                if key in cycle.stats:
                    event['fields'][key] = float(cycle.stats[key])
            event['fields'].update({
                'est_distance': float(cycle.stats['distance']),
                'odo_start': metrics_odo.first_value(),
                'odo_end': metrics_odo.last_value(),
                'soc_start': metrics_soc.first_value(),
                'soc_end': metrics_soc.last_value(),
                'outside_air_temp': metrics_temp.last_value()
            })
            yield event


def preprocess_trips(client):
    logger.info("Preprocessing trips")
    detector = TripDetection(time_epoch=client.time_epoch)
    res = client.stream_series(
        "samples",
        fields="time, veh_speed, participant, veh_odometer, hvbatt_soc, outside_air_temp, fuel_rate, hvbatt_current",
        batch_size=500000,
        where="veh_speed > 0")
    for nr, (series, iter) in enumerate(res):
        logger.info(__("#{}: {}", nr, series))
        cycles_curr, cycles_curr_disc = detector(progress(iter))

        logger.info(__("Writing {} + {} = {} trips", len(cycles_curr), len(cycles_curr_disc),
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


def extract_hist(client: InfluxDBClient):
    logger.info("Generating trip histogram data")
    hist_data = copy.deepcopy(webike.data.Trips.HIST_DATA)
    res = client.stream_series("trips", where="discarded !~ /./ AND started = 'True'")
    for nr, (series, iter) in enumerate(res):
        logger.info(__("#{}: {}", nr, series))

        for trip in progress(iter):
            trip_time = datetime.fromtimestamp(trip['time'] * TO_SECONDS[client.time_epoch])
            hist_data['start_times'].append(to_hour_bin(trip_time))
            hist_data['start_weekday'].append(trip_time.weekday())
            hist_data['start_month'].append(trip_time.month)
            hist_data['durations'].append(round(trip['duration'] * TO_SECONDS['n'] / 60))

            hist_trip_mappings = \
                [('distances', 'est_distance'), ('trip_temp', 'outside_air_temp'), ('initial_soc', 'soc_start'),
                 ('final_soc', 'soc_end')]
            for hist_key, trip_key in hist_trip_mappings:
                if trip_key in trip and trip[trip_key] is not None:
                    hist_data[hist_key].append(trip[trip_key])

    return hist_data
