import copy
import itertools
import logging
from datetime import datetime
from datetime import timedelta

import webike
from drive4data.data.soc import rescale_soc
from iss4e.db.influxdb import InfluxDBStreamingClient as InfluxDBClient, series_tag_to_id
from iss4e.db.influxdb import TO_SECONDS
from iss4e.util import BraceMessage as __
from iss4e.util import progress
from webike.data import ChargeCycle
from webike.util.plot import to_hour_bin

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)


def extract_hist(client: InfluxDBClient):
    logger.info("Generating charge cycle histogram data")
    hist_data_particip = {}
    res = client.stream_measurement("charge_cycles", where="discarded = 'False' AND started = True")
    for nr, (tags, series, iter) in enumerate(res):
        logger.info(__("#{}: {}", nr, series))
        hist_data = None
        for cycle in progress(iter):
            if not hist_data:
                hist_data = copy.deepcopy(webike.data.ChargeCycle.HIST_DATA)
                hist_data['tags'] = tags
                hist_data_particip["_".join(series_tag_to_id(t) for t in tags)] = hist_data
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

    hist_data_summary = {k: list(itertools.chain.from_iterable(p[k] for p in hist_data_particip.values()))
                         for k in webike.data.ChargeCycle.HIST_DATA.keys()}
    return hist_data_particip, hist_data_summary
