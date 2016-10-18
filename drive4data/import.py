import csv
import logging
import os
import pickle
import re
import sys
from datetime import datetime, timedelta

from influxdb import InfluxDBClient
from more_itertools import peekable
from webike.util.DB import default_credentials
from webike.util.Logging import BraceMessage as __
from webike.util.Utils import progress

from util.SafeFileWalker import SafeFileWalker

CHECKPOINT_FILE = "checkpoint.pickle"
CHECKPOINT_COPY_FILE = "checkpoint.pickle.tmp"
COLS = ['ac_hvpower', 'boardtemperature', 'charger_accurrent', 'charger_acvoltage', 'chargerplugstatus',
        'chargetimeremaining', 'engine_afr', 'engine_rpm', 'ev_range_remaining', 'fuel_rate', 'gps_alt_metres',
        'gps_lat_deg', 'gps_lon_deg', 'gps_speed_kph', 'gps_time', 'hvbatt_current', 'hvbatt_soc', 'hvbatt_temp',
        'hvbatt_voltage', 'hvbs_cors_crnt', 'hvbs_fn_crnt', 'inputvoltage', 'ischarging', 'maf', 'motorvoltages',
        'outside_air_temp', 'veh_odometer', 'veh_speed', 'vin_1', 'vin_2', 'vin_3', 'vin_digit', 'vin_frame1',
        'vin_frame2', 'vin_index', 'reltime']

__author__ = "Niko Fink"
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")
logger = logging.getLogger(__name__)


def num(s):
    try:
        return int(s)
    except ValueError:
        try:
            return float(s)
        except ValueError:
            return s


def main():
    root = sys.argv[1]

    if os.path.isfile(CHECKPOINT_FILE):
        iterator = pickle.load(open(CHECKPOINT_FILE, "rb"))
    else:
        iterator = SafeFileWalker(root)

    for file in progress(iterator):
        m = re.search('Participant ([0-9]{2}b?)', file)
        participant = m.group(1)
        if participant == "10b":
            participant = 11
        else:
            participant = int(participant)

        with open(file, 'rt', newline='') as f:
            reader = csv.reader(f)

            header = next(reader)
            if "Trip" in header:
                logger.warning(__("Skipping trip file {}", file))
                return
            assert header[0] == "Timestamp", "Illegal header row {}".format(header)
            header[0] = "reltime"
            regex = re.compile(r"\[.*\]$")
            header = [regex.sub("", h.lower()) for h in header]

            infos = next(reader)
            assert len(infos) == 3, "Illegal info row {}".format(infos)
            assert len(infos[2]) == 0, "Illegal info row {}".format(infos)
            base_time = datetime.strptime(infos[0], "%m/%d/%Y %I:%M:%S %p")
            car_id = infos[1]

            rows = [{
                        'measurement': 'samples',
                        'time': base_time + timedelta(milliseconds=int(row[0])),
                        'tags': {
                            'participant': participant,
                            'base_time': base_time,
                            'car_id': car_id,
                            'source': file
                        },
                        'fields': dict([(k, float(v)) for k, v in zip(header, row) if k in COLS])
                    } for row in reader]
            rows = peekable(rows)

            if rows.peek(None):
                # pickle.dump(iterator, open(CHECKPOINT_COPY_FILE, "wb"))
                client.write_points(rows)
                # os.replace(CHECKPOINT_COPY_FILE, CHECKPOINT_FILE)


if __name__ == "__main__":
    cred = default_credentials("Drive4Data-DB")
    client = InfluxDBClient(cred['host'], cred['port'], cred['user'], cred['passwd'], cred['db'])
    main()


# SQL STUFF
# with DelayedKeyboardInterrupt():
# pickle.dump(iterator, open(CHECKPOINT_COPY_FILE, "wb"))
# os.replace(CHECKPOINT_COPY_FILE, CHECKPOINT_FILE)

# rows = [[participant, base_time, car_id, base_time + timedelta(seconds=int(row[0]))]
#         + [v for k, v in zip(header, row) if k in COLS]
#         for row in reader]
# sql_header = ['"{}"'.format(h) for h in header if h in COLS]
# sql_header = ['"participant"', '"basetime"', '"csv_id"', '"abstime"'] + sql_header
# sql_values = ["%s"] * len(sql_header)
# sql = "INSERT INTO drive4data_sfink.samples ({}) VALUES ({})" \
#     .format(", ".join(sql_header), ", ".join(sql_values))
#
# inserted = cursor.executemany(sql, rows)
# logger.info(__("Inserted {} rows from file {}", inserted, file))
# return inserted
