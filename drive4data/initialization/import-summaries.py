import csv
import itertools
import logging
import os
import pickle
import re
import sys
from datetime import datetime, timedelta
from multiprocessing.pool import Pool
from os.path import join

import pytz
from influxdb import InfluxDBClient
from more_itertools import peekable
from webike.util.DB import default_credentials
from webike.util.Logging import BraceMessage as __
from webike.util.Utils import progress

from util.SafeFileWalker import SafeFileWalker

CHECKPOINT_FILE = "checkpoint{}.pickle"
CHECKPOINT_COPY_FILE = "checkpoint{}.pickle.tmp"
COLS = ['Vehicle', 'Make', 'Model', 'Year', 'Trip Id', 'Date', 'Duration', 'Trip Distance (km)', 'Starting SOC (%)',
        'Ending SOC (%)', 'Electrical Energy Consumed (kWh)', 'Gasoline Consumed']
COLS_MAPPING = ['vehicle', 'make', 'model', 'year', 'trip_id', 'date', 'duration', 'distance', 'soc_start', 'soc_end',
                'cons_energy', 'cons_gasoline']
COLS_FLOAT = ['year', 'distance', 'soc_start', 'soc_end', 'cons_energy', 'cons_gasoline']

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
        client.delete_series(measurement='trips_import')

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

    logger.info(__("finished reading {} rows", row_count))
    return row_count


def get_time(row):
    naive = datetime.strptime(row[5], "%B %d, %Y %I:%M:%S %p")
    # Canada/Eastern without DST is just a guess
    local = pytz.timezone("Canada/Eastern")
    local_dt = local.localize(naive, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt


def extract_row(row, header):
    values = dict([(k, v) for k, v in zip(header, row)])
    del values['date']
    t = datetime.strptime(values['duration'], "%H:%M:%S")
    values['duration'] = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second).total_seconds()
    for h in COLS_FLOAT:
        if h in values:
            values[h] = float(values[h])
    return values


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

        # transform all the following rows for the InfluxDB client
        rows = [{
                    'measurement': 'trips_import',
                    'time': get_time(row),
                    'tags': {
                        'participant': participant
                    },
                    'fields': extract_row(row, header)
                } for row in reader]
        counter = itertools.count()  # zipping with a counter is the most efficient way to count an iterable
        rows = [r for c, r in zip(counter, rows)]  # so, increase counter with each consumed item
        rows = peekable(rows)  # many files contain no data, so peek into the iter and skip if it's empty

        # save the data
        if rows.peek(None):
            client.write_points(rows)

        return next(counter) - 1  # number of consumed items was the previous value of the counter


def extract_participant(file):
    m = re.search('Participant ([0-9]{1,2}b?)', file)
    participant = m.group(1)
    if participant == "10b":
        participant = 11
    else:
        participant = int(participant)
    return participant


def extract_header(file, reader):
    header = next(reader)
    if header[0] == '\ufeff"Vehicle"':
        header[0] = 'Vehicle'
    if header == COLS:
        return COLS_MAPPING
    elif header == COLS[:-1]:
        return COLS_MAPPING[:-1]
    else:
        raise ValueError("Illegal header row {}".format(header))


if __name__ == "__main__":
    main(default_credentials("Drive4Data-DB"), sys.argv[1])
