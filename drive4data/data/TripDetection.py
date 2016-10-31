import concurrent.futures
import logging
from datetime import timedelta, datetime

from webike.util import ActivityDetection
from webike.util.DB import default_credentials

from util.InfluxDB import InfluxDBStreamingClient as InfluxDBClient

__author__ = "Niko Fink"
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

cred = default_credentials("Drive4Data-DB")


def parse_time(value, epoch):
    assert epoch == 'u'
    return datetime.fromtimestamp(value / 1000000)


class TripDetection(ActivityDetection):
    def is_start(self, sample, previous):
        return sample['veh_speed'] > 1

    def is_end(self, sample, previous):
        return sample['veh_speed'] < 1 or self.get_duration(previous, sample) > timedelta(minutes=10)

    def accumulate_samples(self, new_sample, accumulator):
        if accumulator is not None:
            avg, cnt = accumulator
            return (avg + new_sample['veh_speed']) / 2, cnt + 1
        else:
            return new_sample['veh_speed'], 1

    def check_reject_reason(self, cycle):
        cycle_start, cycle_end, cycle_acc = cycle
        acc_avg, acc_cnt = cycle_acc
        if acc_cnt < 100:
            return "acc_cnt<100"
        elif self.get_duration(cycle_end, cycle_start) < timedelta(minutes=5):
            return "duration<5min"
        else:
            return None

    @staticmethod
    def get_duration(cycle_end, cycle_start):
        return cycle_start['time'] - cycle_end['time']


with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    client = InfluxDBClient(
        cred['host'], cred['port'], cred['user'], cred['passwd'], cred['db'],
        batched=False, async_executor=executor, time_epoch='u', time_format=parse_time)

    res = client.stream_series("samples", fields="time, veh_speed", batch_size=1000000,
                               where="participant='5' AND veh_speed > 0")

    for nr, (series, iter) in enumerate(res):
        logger.info("#{}: {}".format(nr, series))

        cycles_curr, cycles_curr_disc = TripDetection()(iter)
        # TODO store results
        # if cycles_curr:
        #     logger.info(tabulate(cycles_curr))
        #     logger.info(tabulate(cycles_curr_disc))
