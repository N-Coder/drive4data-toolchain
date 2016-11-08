import copy
import logging
from datetime import datetime

import webike
from iss4e.db.influxdb import InfluxDBStreamingClient as InfluxDBClient
from iss4e.db.influxdb import TO_SECONDS
from iss4e.util import BraceMessage as __
from iss4e.util import progress
from webike.data import Trips
from webike.util.plot import to_hour_bin

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)


def extract_hist(client: InfluxDBClient):
    logger.info("Generating trip histogram data")
    hist_data = copy.deepcopy(webike.data.Trips.HIST_DATA)
    res = client.stream_series("trips", where="discarded = 'False' AND started = 'True'")
    for nr, (series, iter) in enumerate(res):
        logger.info(__("#{}: {}", nr, series))

        for trip in progress(iter):
            trip_time = datetime.fromtimestamp(trip['time'] * TO_SECONDS[client.time_epoch])
            hist_data['start_times'].append(to_hour_bin(trip_time))
            hist_data['start_weekday'].append(trip_time.weekday())
            hist_data['start_month'].append(trip_time.month)
            hist_data['durations'].append(round(trip['duration'] * TO_SECONDS['n'] / 60))

            hist_trip_mappings = \
                [('distances', 'est_distance'), ('trip_temp', 'temp_avg'), ('initial_soc', 'soc_start'),
                 ('final_soc', 'soc_end')]
            for hist_key, trip_key in hist_trip_mappings:
                if trip_key in trip and trip[trip_key] is not None:
                    hist_data[hist_key].append(trip[trip_key])

    return hist_data
