import itertools
import logging
from time import perf_counter

from influxdb import InfluxDBClient

from util.AsyncLookaheadIterator import AsyncLookaheadIterator

DEFAULT_BATCH_SIZE = 50000

logger = logging.getLogger(__name__)
async_logger = logger.getChild("async")
_marker = object()


def escape_series_tag(p):
    k, v = p.split("=")
    return "{}='{}'".format(k, v)


class InfluxDBStreamingClient(InfluxDBClient):
    def __init__(self, *args, **kwargs):
        self.batched = kwargs.pop('batched', False)
        self.async_executor = kwargs.pop('async_executor', None)
        super().__init__(*args, **kwargs)

    def stream_series(self, measurement, fields=None, where="", group_order_by="", batch_size=DEFAULT_BATCH_SIZE):
        # fetch all series for this measurement and parse the result
        series_res = self.query("SHOW SERIES FROM \"{}\"".format(measurement))
        series = [v['key'].split(",")[1:] for v in series_res.get_points()]
        # for each series, create a WHERE clause selecting only entries from that exact series
        series_selectors = [" AND ".join([escape_series_tag(v) for v in a]) for a in series]

        # iterate all series independently
        for sselector in series_selectors:
            # join series WHERE clause and WHERE clause from params
            selector = " AND ".join(["({})".format(w) for w in [where, sselector] if w])

            # paginate entries in this series
            yield (sselector, self.stream_params(measurement, fields, selector, group_order_by, batch_size))

    def stream_params(self, measurement, fields=None, selector="", group_order_by="", batch_size=DEFAULT_BATCH_SIZE):
        if fields is None:
            fields = "*"
        elif not isinstance(fields, str):
            fields = ", ".join(fields)

        base_query = "SELECT {fields} FROM {measurement} WHERE {where} {group_order_by} " \
                     "LIMIT {{limit}} OFFSET {{offset}}".format(
            fields=fields, measurement=measurement,
            where=selector, group_order_by=group_order_by)

        yield from self.stream_query(base_query, batch_size)

    def __async_decorate(func):
        def func_wrapper(self, *args, **kwargs):
            iter = func(self, *args, **kwargs)
            if self.async_executor:
                iter = AsyncLookaheadIterator(self.async_executor, iter, logger=async_logger, warm_start=True)
            if not self.batched:
                iter = itertools.chain.from_iterable(iter)
            return iter

        return func_wrapper

    @__async_decorate
    def stream_query(self, query_format, batch_size):
        for offset in itertools.count(0, batch_size):
            query = query_format.format(offset=offset, limit=batch_size)
            before = perf_counter()
            async_logger.debug(" < block before")
            result = self.query(query, params={'epoch': 'u'})  # TODO central time parsing
            async_logger.debug(" > block after, blocked for {}s".format(perf_counter() - before))
            points = result.get_points()

            counter = itertools.count()  # zipping with a counter is the most efficient way to count an iterable
            yield [r for c, r in zip(counter, points)]  # so, increase counter with each consumed item
            count = next(counter) - 1  # number of consumed items was the previous value of the counter

            # if the got less results than LIMIT, we read all values from this series
            if count < batch_size:
                break

    def _batches(self, iterable, size):
        args = [iter(iterable)] * size
        iters = itertools.zip_longest(*args, fillvalue=_marker)
        return [filter(_marker.__ne__, it) for it in iters]
