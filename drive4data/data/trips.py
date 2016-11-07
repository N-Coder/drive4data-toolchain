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
from data.activity import InfluxActivityDetection, ValueMemoryMixin, ValueMemory
from data.soc import SoCMixin
from webike.data import Trips
from webike.util.activity import MergingActivityDetection, Cycle
from webike.util.plot import to_hour_bin

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)


def get_current(sample):
    current = None
    if sample.get('hvbatt_current') is not None:
        current = sample['hvbatt_current']
    elif sample.get('hvbs_fn_crnt') is not None and -23 < sample['hvbs_fn_crnt'] < 22:
        current = sample['hvbs_fn_crnt']
    elif sample.get('hvbs_cors_crnt') is not None:
        current = sample['hvbs_cors_crnt']
    return current


class TripDetection(InfluxActivityDetection, MergingActivityDetection, ValueMemoryMixin, SoCMixin):
    MIN_DURATION = timedelta(minutes=10) / timedelta(seconds=1)

    def __init__(self, *args, **kwargs):
        # save these values and store the respective first and last value with each cycle
        memorized_values = [
            ValueMemory('veh_odometer', save_first='odo_start', save_last='odo_end'),
            ValueMemory('outside_air_temp', save_last='temp_last')]
        super().__init__('veh_speed',
                         min_sample_count=60, min_cycle_duration=timedelta(minutes=1),
                         max_merge_gap=timedelta(minutes=10),
                         memorized_values=memorized_values, *args, **kwargs)

    def is_start(self, sample, previous):
        return sample[self.attr] > 0.1

    def is_end(self, sample, previous):
        return sample[self.attr] < 0.1 or self.get_duration(previous, sample) > self.MIN_DURATION

    def accumulate_samples(self, new_sample, accumulator):
        accumulator = super().accumulate_samples(new_sample, accumulator)

        # accumulated values depending on previous sample
        if 'distance' not in accumulator:
            accumulator['distance'] = 0.0
        if '__prev' in accumulator:
            interval = self.get_duration(accumulator['__prev'], new_sample)

            # distance
            distance = (interval / TO_SECONDS['h']) * new_sample['veh_speed']
            accumulator['distance'] += distance

            # cons_energy
            current = get_current(new_sample)
            if current is not None and new_sample.get('hvbatt_voltage') is not None:
                if 'cons_energy' not in accumulator:
                    accumulator['cons_energy'] = 0.0
                accumulator['cons_energy'] += interval * current * new_sample['hvbatt_voltage'] / TO_SECONDS['h']

            # cons_gasoline
            if new_sample.get('fuel_rate') is not None:
                if 'cons_gasoline' not in accumulator:
                    accumulator['cons_gasoline'] = 0.0
                accumulator['cons_gasoline'] += interval * new_sample['fuel_rate']

        # temp_avg
        if 'temp_avg' in accumulator:
            accumulator['temp_avg'] = (accumulator['temp_avg'] + new_sample['outside_air_temp']) / 2
        else:
            accumulator['temp_avg'] = float(new_sample['outside_air_temp'])

        accumulator['__prev'] = new_sample
        return accumulator

    def merge_stats(self, stats1, stats2):
        stats = super().merge_stats(stats1, stats2)

        stats['distance'] = stats1['distance'] + stats2['distance']
        stats['cons_energy'] = stats1['cons_energy'] + stats2['cons_energy']
        stats['cons_gasoline'] = stats1['cons_gasoline'] + stats2['cons_gasoline']
        stats['temp_avg'] = (stats1['temp_avg'] + stats2['temp_avg']) / 2

        return stats

    def cycle_to_events(self, cycle: Cycle, measurement):
        data = {
            'est_distance': float(cycle.stats['distance']),
            'cons_energy': float(cycle.stats['cons_energy']),
            'cons_gasoline': float(cycle.stats['cons_gasoline']),
            'temp_avg': float(cycle.stats['temp_avg'])
        }
        for event in super().cycle_to_events(cycle, measurement):
            event['fields'].update(data)
            yield event


class TripDetectionHistogram(TripDetection):
    def __init__(self, *args, **kwargs):
        self.hist_metrics_odo = []
        self.hist_metrics_soc = []
        self.hist_metrics_temp = []
        self.hist_distance = []
        super().__init__(*args, **kwargs)

    def cycle_to_events(self, cycle: Cycle, measurement):
        metrics_odo = cycle.stats['metrics']["veh_odometer"]
        self.hist_metrics_odo.append(metrics_odo.get_time_gap(cycle, self.get_duration, missing_value="X"))
        metrics_soc = cycle.stats["soc"]
        self.hist_metrics_soc.append(metrics_soc.get_time_gap(cycle, self.get_duration, missing_value="X"))
        metrics_temp = cycle.stats['metrics']["outside_air_temp"]
        self.hist_metrics_temp.append(metrics_temp.get_time_gap(cycle, self.get_duration, missing_value="X"))

        if metrics_odo.first_value() and metrics_odo.last_value():
            self.hist_distance.append((metrics_odo.first_value(), metrics_odo.last_value(), cycle.stats['distance']))

        return super().cycle_to_events(cycle, measurement)


def preprocess_trips(client):
    logger.info("Preprocessing trips")
    detector = TripDetectionHistogram(time_epoch=client.time_epoch)
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
