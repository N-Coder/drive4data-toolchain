import logging
from time import perf_counter

my_logger = logging.getLogger(__name__)


class AsyncLookaheadIterator(object):
    def __init__(self, executor, iterable, logger=my_logger, warm_start=False):
        self._log = logger
        self._exec = executor
        self._it = iter(iterable)
        self._pending = None
        if warm_start:
            self.__queue_next()

    def __iter__(self):
        return self

    def __queue_next(self):
        self._pending = self._exec.submit(self._it.__next__)

    def __next__(self):
        # on first call, schedule the first execution
        if not self._pending:
            self.__queue_next()

        # return the last result and schedule the next execution in the background
        before = perf_counter()
        self._log.debug("[  wait before")
        try:
            result = self._pending.result()
            self._log.debug("]  wait after, waited for {}s".format(perf_counter() - before))
            self.__queue_next()
            return result
        except StopIteration:
            self._log.debug("]  wait stopped at end, waited for {}s".format(perf_counter() - before))
            raise
