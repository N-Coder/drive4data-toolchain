import copy
import logging
from datetime import datetime

import webike
from drive4data.data.soc import rescale_soc
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
    res = client.stream_measurement("trips", where="discarded = 'False' AND started = True")
    for nr, (tags, series, iter) in enumerate(res):
        logger.info(__("#{}: {}", nr, series))

        for trip in progress(iter):
            trip_time = datetime.fromtimestamp(trip['time'] * TO_SECONDS[client.time_epoch])
            hist_data['start_times'].append(to_hour_bin(trip_time))
            hist_data['start_weekday'].append(trip_time.weekday())
            hist_data['start_month'].append(trip_time.month)
            hist_data['durations'].append(round(trip['duration'] * TO_SECONDS['n'] / TO_SECONDS['m']))

            if trip.get('est_distance'):
                hist_data['distances'].append(trip['est_distance'])
            if trip.get('temp_avg'):
                hist_data['trip_temp'].append(trip['temp_avg'])
            if trip.get('soc_start'):
                hist_data['initial_soc'].append(rescale_soc(trip_time, trip['participant'], trip['soc_start']))
            if trip.get('soc_end'):
                hist_data['final_soc'].append(rescale_soc(trip_time, trip['participant'], trip['soc_end']))

    return hist_data
