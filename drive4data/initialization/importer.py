import abc
import contextlib
import csv
import itertools
import logging
import math
import os
import pickle
import re
from datetime import datetime, timedelta
from multiprocessing.pool import Pool
from os.path import join

import geohash
import pytz
from more_itertools import peekable
from webike.util.Logging import BraceMessage as __
from webike.util.Utils import progress

from initialization.pre_import import FW3I_VALUES, FW3I_FOLDER
from util.InfluxDB import InfluxDBStreamingClient as InfluxDBClient
from util.SafeFileWalker import SafeFileWalker

__author__ = "Niko Fink"


def chunkify(lst, n):
    return [lst[i::n] for i in range(n)]


# noinspection PyMethodMayBeStatic
class Importer:
    __metaclass__ = abc.ABCMeta

    def __init__(self, cred, measurement, logger=None, processes=4,
                 checkpoint_file=None,
                 checkpoint_copy_file=None):
        if not logger:
            self.logger = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self.cred = cred
        self.measurement = measurement
        self.processes = processes
        if not checkpoint_file:
            checkpoint_file = "tmp/{}-checkpoint{{}}.pickle".format(self.__class__.__name__)
        self.checkpoint_file = checkpoint_file
        if not checkpoint_copy_file:
            checkpoint_copy_file = "tmp/{}-checkpoint{{}}.pickle.tmp".format(self.__class__.__name__)
        self.checkpoint_copy_file = checkpoint_copy_file

    def new_client(self):
        return contextlib.closing(InfluxDBClient(
            self.cred['host'], self.cred['port'],
            self.cred['user'], self.cred['passwd'],
            self.cred['db']))

    def do_import(self, root):
        if all([os.path.isfile(self.checkpoint_file.format(i)) for i in range(0, self.processes)]):
            self.logger.info("Loading checkpoint")
            iters = [pickle.load(open(self.checkpoint_file.format(i), "rb")) for i in range(0, self.processes)]

        else:
            self.logger.info("Performing cold start")

            with self.new_client() as client:
                client.delete_series(measurement=self.measurement)

            files = chunkify([join(root, sub) for sub in os.listdir(root)], self.processes)
            iters = list([SafeFileWalker(f) for f in files])

            for nr, it in enumerate(iters):
                pickle.dump(it, open(self.checkpoint_file.format(nr), "wb"))

        assert len(iters) == self.processes

        self.logger.info("Iterators loaded, starting pool")
        with Pool(processes=self.processes) as pool:
            row_count = pool.map(self.walk_files, [(nr, it) for (nr, it) in enumerate(iters)], chunksize=1)
        self.logger.info(__("Imported {} = {} rows", row_count, sum(row_count)))

    def walk_files(self, args):
        nr, files = args
        old_logger = self.logger
        self.logger = self.logger.getChild(str(nr))
        self.logger.info(__("{} starting", nr))

        with self.new_client() as client:
            row_count = 0
            last_save = -1
            has_tmp_file = False
            for file in progress(files, logger=self.logger):
                try:
                    if last_save != row_count:
                        pickle.dump(files, open(self.checkpoint_copy_file.format(nr), "wb"))
                        last_save = row_count
                        has_tmp_file = True

                    row_count += self.parse_file(client, file)

                    if has_tmp_file:
                        os.replace(self.checkpoint_copy_file.format(nr), self.checkpoint_file.format(nr))
                        has_tmp_file = False
                except:
                    self.logger.error(__("In file  {}", file))
                    raise

        self.logger.info(__("finished reading {} rows", row_count))
        self.logger = old_logger
        return row_count

    def parse_file(self, client, file):
        # extract the participant
        participant = self.extract_participant(file)
        stat = os.stat(file)
        with open(file, 'rt', newline='') as f:
            reader = csv.reader(f)

            # extract header data
            header = self.extract_header(file, reader)
            if not header:
                return 0

            rows = self.parse_rows(file, stat, participant, header, reader)
            counter = itertools.count()  # zipping with a counter is the most efficient way to count an iterable
            rows = [r for c, r in zip(counter, rows)]  # so, increase counter with each consumed item
            rows = peekable(rows)  # many files contain no data, so peek into the iter and skip if it's empty

            # save the data
            if rows.peek(None):
                client.write_points(rows)

            return next(counter) - 1  # number of consumed items was the previous value of the counter

    def extract_participant(self, file):
        m = re.search('Participant ([0-9]{1,2}b?)', file)
        participant = m.group(1)
        if participant == "10b":
            participant = 11
        else:
            participant = int(participant)
        return participant

    @abc.abstractmethod
    def extract_header(self, file, reader):
        pass

    def parse_rows(self, file, stat, participant, header, reader):
        pass

    def __getstate__(self):
        state = self.__dict__.copy()
        state['logger'] = self.logger.name
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = logging.getLogger(self.logger)


# noinspection PyMethodMayBeStatic
class SamplesImporter(Importer):
    COLS = ['ac_hvpower', 'boardtemperature', 'charger_accurrent', 'charger_acvoltage', 'chargerplugstatus',
            'chargetimeremaining', 'engine_afr', 'engine_rpm', 'ev_range_remaining', 'fuel_rate', 'gps_alt_metres',
            'gps_lat_deg', 'gps_lon_deg', 'gps_speed_kph', 'gps_time', 'hvbatt_current', 'hvbatt_soc', 'hvbatt_temp',
            'hvbatt_voltage', 'hvbs_cors_crnt', 'hvbs_fn_crnt', 'inputvoltage', 'ischarging', 'maf', 'motorvoltages',
            'outside_air_temp', 'veh_odometer', 'veh_speed', 'vin_1', 'vin_2', 'vin_3', 'vin_digit', 'vin_frame1',
            'vin_frame2', 'vin_index', 'reltime']

    def extract_header(self, file, reader):
        header = next(reader)
        if "Trip" in header or "Trip Id" in header:
            self.logger.warning(__("Skipping trip file {}", file))
            return None
        assert header[0] == "Timestamp", "Illegal header row {} in file {}".format(header, file)
        header[0] = "reltime"
        regex = re.compile(r"\[.*\]$")
        header = [regex.sub("", h.lower()) for h in header]
        return header

    def parse_rows(self, file, stat, participant, header, reader):
        # extract the info row
        base_time, car_id = self.extract_infos(file, reader)
        # transform all the following rows for the InfluxDB client
        rows = [{
                    'measurement': 'samples',
                    'time': base_time + timedelta(milliseconds=int(row[0])),
                    'tags': {
                        'participant': participant
                    },
                    'fields': self.extract_row(row, header, {
                        'source': stat.st_ino,
                        'car_id': car_id
                    })
                } for row in reader]
        return rows

    def extract_infos(self, file, reader):
        infos = next(reader)
        assert len(infos) == 3, "Illegal info row {}".format(infos)
        assert len(infos[2]) == 0 or (infos[2] in FW3I_VALUES and FW3I_FOLDER in file), \
            "Illegal info row {}".format(infos)
        # base_time is in UTC
        base_time = datetime.strptime(infos[0], "%m/%d/%Y %I:%M:%S %p")
        car_id = infos[1]
        return base_time, car_id

    def extract_row(self, row, header, constants):
        values = dict([(k, float(v)) for k, v in zip(header, row)
                       if k in self.COLS and math.isfinite(float(v))])
        if 'gps_lat_deg' in values and 'gps_lon_deg' in values and \
                (values['gps_lat_deg'] != 0 or values['gps_lon_deg'] != 0):
            values['gps_geohash'] = geohash.encode(values['gps_lat_deg'], values['gps_lon_deg'])
        values.update(constants)
        return values


# noinspection PyMethodMayBeStatic
class SummaryImporter(Importer):
    COLS = ['Vehicle', 'Make', 'Model', 'Year', 'Trip Id', 'Date', 'Duration', 'Trip Distance (km)', 'Starting SOC (%)',
            'Ending SOC (%)', 'Electrical Energy Consumed (kWh)', 'Gasoline Consumed']
    COLS_MAPPING = ['vehicle', 'make', 'model', 'year', 'trip_id', 'date', 'duration', 'distance', 'soc_start',
                    'soc_end',
                    'cons_energy', 'cons_gasoline']
    COLS_FLOAT = ['year', 'distance', 'soc_start', 'soc_end', 'cons_energy', 'cons_gasoline']

    def extract_header(self, file, reader):
        header = next(reader)
        if header[0] == '\ufeff"Vehicle"':
            header[0] = 'Vehicle'
        if header == self.COLS:
            return self.COLS_MAPPING
        elif header == self.COLS[:-1]:
            return self.COLS_MAPPING[:-1]
        else:
            raise ValueError("Illegal header row {} in file {}".format(header, file))

    def parse_rows(self, file, stat, participant, header, reader):
        # transform all the following rows for the InfluxDB client
        return [{
                    'measurement': 'trips_import',
                    'time': self.get_time(row),
                    'tags': {
                        'participant': participant
                    },
                    'fields': self.extract_row(row, header)
                } for row in reader]

    def extract_participant(self, file):
        m = re.search('Participant ([0-9]{1,2}b?)', file)
        participant = m.group(1)
        if participant == "10b":
            participant = 11
        else:
            participant = int(participant)
        return participant

    def extract_row(self, row, header):
        values = dict([(k, v) for k, v in zip(header, row)])
        del values['date']
        t = datetime.strptime(values['duration'], "%H:%M:%S")
        values['duration'] = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second).total_seconds()
        for h in self.COLS_FLOAT:
            if h in values:
                values[h] = float(values[h])
        return values

    def get_time(self, row):
        naive = datetime.strptime(row[5], "%B %d, %Y %I:%M:%S %p")
        # Canada/Eastern without DST is just a guess
        local = pytz.timezone("Canada/Eastern")
        local_dt = local.localize(naive, is_dst=None)
        utc_dt = local_dt.astimezone(pytz.utc)
        return utc_dt
