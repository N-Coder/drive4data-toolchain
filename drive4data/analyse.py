import logging
import sys
from collections import Counter

from webike.util.Logging import BraceMessage as __
from webike.util.Utils import progress

from util.SafeFileWalker import SafeFileWalker

__author__ = "Niko Fink"
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-3.3s %(name)-12.12s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    headers = Counter()
    dates = set()
    ids = set()

    root = sys.argv[1]
    for path in progress(SafeFileWalker(root)):
        with open(path, 'r') as f:
            header = f.readline().strip()
            if "Trip" in header:
                logger.warning(__("Skipping trip file {}", path))
                continue
            headers.update(header.split(","))

            infos = f.readline().strip().split(",")
            dates.add(infos[0])
            ids.add(infos[1])
            assert len(infos) == 3

    print(ids)
    print(headers)


if __name__ == "__main__":
    main()
