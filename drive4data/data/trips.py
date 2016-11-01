import logging
from datetime import timedelta

from webike.util import MergingActivityDetection
from webike.util.Logging import BraceMessage as __
from webike.util.Utils import progress

from data.activity import InfluxActivityDetection, cycles_to_timeseries

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)


class TripDetection(InfluxActivityDetection, MergingActivityDetection):
    MIN_DURATION = timedelta(minutes=10) / timedelta(seconds=1)

    def __init__(self, *args, **kwargs):
        super().__init__('veh_speed', *args, **kwargs)

    def is_start(self, sample, previous):
        return sample[self.attr] > 1

    def is_end(self, sample, previous):
        return sample[self.attr] < 1 or self.get_duration(previous, sample) > self.MIN_DURATION


def preprocess_trips(client):
    detector = TripDetection(time_epoch=client.time_epoch)
    res = client.stream_series("samples", fields="time, veh_speed, participant", batch_size=500000,
                               where="veh_speed > 0")
    for nr, (series, iter) in enumerate(res):
        logger.info(__("#{}: {}", nr, series))
        cycles_curr, cycles_curr_disc = detector(progress(iter))

        logger.info(__("Writing {} + {} = {} cycles", len(cycles_curr), len(cycles_curr_disc),
                       len(cycles_curr) + len(cycles_curr_disc)))
        client.write_points(
            cycles_to_timeseries("trips", detector.attr, cycles_curr + cycles_curr_disc),
            time_precision=client.time_epoch)
