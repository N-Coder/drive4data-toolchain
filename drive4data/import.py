import csv
import itertools
import logging
import math
import os
import pickle
import re
import sys
from datetime import datetime, timedelta
from multiprocessing.pool import Pool
from os.path import join

from influxdb import InfluxDBClient
from more_itertools import peekable
from webike.util.DB import default_credentials
from webike.util.Logging import BraceMessage as __
from webike.util.Utils import progress

from analyse import FW3I_VALUES, FW3I_FOLDER
from util.SafeFileWalker import SafeFileWalker

CHECKPOINT_FILE = "checkpoint{}.pickle"
CHECKPOINT_COPY_FILE = "checkpoint{}.pickle.tmp"
COLS = ['ac_hvpower', 'boardtemperature', 'charger_accurrent', 'charger_acvoltage', 'chargerplugstatus',
        'chargetimeremaining', 'engine_afr', 'engine_rpm', 'ev_range_remaining', 'fuel_rate', 'gps_alt_metres',
        'gps_lat_deg', 'gps_lon_deg', 'gps_speed_kph', 'gps_time', 'hvbatt_current', 'hvbatt_soc', 'hvbatt_temp',
        'hvbatt_voltage', 'hvbs_cors_crnt', 'hvbs_fn_crnt', 'inputvoltage', 'ischarging', 'maf', 'motorvoltages',
        'outside_air_temp', 'veh_odometer', 'veh_speed', 'vin_1', 'vin_2', 'vin_3', 'vin_digit', 'vin_frame1',
        'vin_frame2', 'vin_index', 'reltime']

__author__ = "Niko Fink"
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")
logging.getLogger("urllib3").setLevel(logging.INFO)
logger = logging.getLogger(__name__)

PROCESSES = 4


def chunkify(lst, n):
    return [lst[i::n] for i in range(n)]


def main(cred, root):
    if all([os.path.isfile(CHECKPOINT_FILE.format(i)) for i in range(0, PROCESSES)]):
        logger.info("Loading checkpoint")
        iters = [pickle.load(open(CHECKPOINT_FILE.format(i), "rb")) for i in range(0, PROCESSES)]

    else:
        logger.info("Performing cold start")

        client = InfluxDBClient(cred['host'], cred['port'], cred['user'], cred['passwd'], cred['db'])
        client.delete_series(measurement='samples')

        files = chunkify([join(root, sub) for sub in os.listdir(root)], PROCESSES)
        iters = list([SafeFileWalker(f) for f in files])

        for nr, it in enumerate(iters):
            pickle.dump(it, open(CHECKPOINT_FILE.format(nr), "wb"))

    assert len(iters) == PROCESSES

    logger.info("Iterators loaded, starting pool")
    with Pool(processes=PROCESSES) as pool:
        row_count = pool.map(walk_files, [(nr, it, cred) for (nr, it) in enumerate(iters)], chunksize=1)
    logger.info(__("Imported {} = {} rows", row_count, sum(row_count)))


def walk_files(args):
    global logger
    nr, files, cred = args
    logger = logger.getChild(str(nr))
    logger.info(__("{} starting", nr))
    client = InfluxDBClient(cred['host'], cred['port'], cred['user'], cred['passwd'], cred['db'])

    row_count = 0
    last_save = -1
    has_tmp_file = False
    for file in progress(files, logger=logger):
        try:
            if last_save != row_count:
                pickle.dump(files, open(CHECKPOINT_COPY_FILE.format(nr), "wb"))
                last_save = row_count
                has_tmp_file = True

            row_count += parse_file(client, file)

            if has_tmp_file:
                os.replace(CHECKPOINT_COPY_FILE.format(nr), CHECKPOINT_FILE.format(nr))
                has_tmp_file = False
        except:
            logger.error(__("In file  {}", file))
            raise

    logger.info(__("finished reading {} rows", nr, row_count))
    return row_count


def parse_file(client, file):
    # extract the participant
    participant = extract_participant(file)
    stat = os.stat(file)
    with open(file, 'rt', newline='') as f:
        reader = csv.reader(f)

        # extract header data
        header = extract_header(file, reader)
        if not header:
            return 0

        # extract the info row
        base_time, car_id = extract_infos(file, reader)

        # transform all the following rows for the InfluxDB client
        rows = [{
                    'measurement': 'samples',
                    'time': base_time + timedelta(milliseconds=int(row[0])),
                    'tags': {
                        'participant': participant,
                        # 'base_time': base_time,
                        'car_id': car_id,
                        # 'source': file
                    },
                    'fields': dict(
                        [(k, float(v)) for k, v in zip(header, row)
                         if k in COLS and math.isfinite(float(v))] +
                        [('source', stat.st_ino)]
                    )
                } for row in reader]
        counter = itertools.count()  # zipping with a counter is the most efficient way to count an iterable
        rows = [r for c, r in zip(counter, rows)]  # so, increase counter with each consumed item
        rows = peekable(rows)  # many files contain no data, so peek into the iter and skip if it's empty

        # save the data
        if rows.peek(None):
            client.write_points(rows)

        return next(counter) - 1  # number of consumed items was the previous value of the counter


def extract_participant(file):
    m = re.search('Participant ([0-9]{2}b?)', file)
    participant = m.group(1)
    if participant == "10b":
        participant = 11
    else:
        participant = int(participant)
    return participant


def extract_header(file, reader):
    header = next(reader)
    if "Trip" in header or "Trip Id" in header:
        logger.warning(__("Skipping trip file {}", file))
        return None
    assert header[0] == "Timestamp", "Illegal header row {}".format(header)
    header[0] = "reltime"
    regex = re.compile(r"\[.*\]$")
    header = [regex.sub("", h.lower()) for h in header]
    return header


def extract_infos(file, reader):
    infos = next(reader)
    assert len(infos) == 3, "Illegal info row {}".format(infos)
    assert len(infos[2]) == 0 or (infos[2] in FW3I_VALUES and FW3I_FOLDER in file), \
        "Illegal info row {}".format(infos)
    base_time = datetime.strptime(infos[0], "%m/%d/%Y %I:%M:%S %p")
    car_id = infos[1]
    return base_time, car_id


if __name__ == "__main__":
    main(default_credentials("Drive4Data-DB"), sys.argv[1])


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
