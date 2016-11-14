import copy
import logging
from datetime import datetime
from datetime import timedelta

import webike
from drive4data.data.soc import rescale_soc
from iss4e.db.influxdb import InfluxDBStreamingClient as InfluxDBClient
from iss4e.db.influxdb import TO_SECONDS
from iss4e.util import BraceMessage as __
from iss4e.util import progress
from webike.data import ChargeCycle
from webike.util.plot import to_hour_bin

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)


def extract_hist(client: InfluxDBClient):
    logger.info("Generating charge cycle histogram data")
    hist_data = copy.deepcopy(webike.data.ChargeCycle.HIST_DATA)
    res = client.stream_measurement("charge_cycles", where="discarded = 'False' AND started = True")
    for nr, (tags, series, iter) in enumerate(res):
        logger.info(__("#{}: {}", nr, series))

        for cycle in progress(iter):
            trip_time = datetime.fromtimestamp(cycle['time'] * TO_SECONDS[client.time_epoch])
            duration = cycle['duration'] * TO_SECONDS['n']
            duration_td = timedelta(seconds=duration)
            hist_data['start_times'].append(to_hour_bin(trip_time))
            hist_data['start_weekday'].append(trip_time.weekday())
            hist_data['start_month'].append(trip_time.month)
            hist_data['durations'].append(duration_td)
            hist_data['end_times'].append(to_hour_bin(trip_time + duration_td))

            hist_data['initial_soc'].append(rescale_soc(trip_time, cycle['participant'], cycle['soc_start']))
            hist_data['final_soc'].append(rescale_soc(trip_time, cycle['participant'], cycle['soc_end']))

    return hist_data
