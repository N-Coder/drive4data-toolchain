import logging
import os
import re
from collections import Counter
from datetime import datetime, timedelta

from iss4e.util import BraceMessage as __
from iss4e.util import SafeFileWalker
from iss4e.util import progress

__author__ = "Niko Fink"
logger = logging.getLogger(__name__)

FW3I_VALUES = ["JTDKN3DP1D3034553", "5NPEB4AC6BH004902", "?????????????????"]
FW3I_FOLDER = "Participant 04-2013-05-08T14-43-54-2015-01-30T16-51-00"


def analyze(root):
    headers = Counter()
    ids = {}
    files_with_3_infos = []

    for path in progress(SafeFileWalker(root)):
        try:
            m = re.search('Participant ([0-9]{2}b?)', path)
            participant = m.group(1)
            if participant == "10b":
                participant = 11
            else:
                participant = int(participant)
            if participant not in range(1, 12):
                logger.warning(__("Illegal participant {} from file {}", participant, path))

            size = os.stat(path).st_size
            with open(path, 'rb') as f:
                first = f.readline().decode()
                second = f.readline().decode()
                last = None
                if f.tell() < size:
                    offs = -100
                    while True:
                        f.seek(max(offs, -size), 2)
                        lines = f.readlines()
                        if len(lines) > 1:
                            last = lines[-1].decode()
                            break
                        if -offs > size:
                            break
                        offs *= 2

                if "Trip" in first:
                    logger.warning(__("Skipping trip file {}", path))
                    continue
                header = first.strip().split(",")
                if header[0] != "Timestamp":
                    logger.warning(__("Illegal header row in {}:1 '{}'", path, first.strip()))
                headers.update(header)

                infos = second.strip().split(",")
                if len(infos) != 3 or len(infos[2]) != 0:
                    if infos[2] in FW3I_VALUES and FW3I_FOLDER in path:
                        files_with_3_infos.append(path)
                    else:
                        logger.warning(__("Invalid info in {}:2 '{}'", path, second.strip()))
                if infos[1] not in ids:
                    ids[infos[1]] = {"min": datetime(year=2100, month=1, day=1),
                                     "max": datetime(year=1900, month=1, day=1),
                                     "participants": Counter(), "count": 0}
                ids[infos[1]]["count"] += 1
                ids[infos[1]]["participants"].update([participant])

                if last:
                    values = last.split(",")
                    min_time = datetime.strptime(infos[0], "%m/%d/%Y %I:%M:%S %p")
                    max_time = min_time + timedelta(milliseconds=int(values[0]))
                    if ids[infos[1]]["min"] > min_time:
                        ids[infos[1]]["min"] = min_time
                        ids[infos[1]]["min_file"] = path
                    if ids[infos[1]]["max"] < max_time:
                        ids[infos[1]]["max"] = max_time
                        ids[infos[1]]["max_file"] = path
        except:
            logging.error(__("In file {}", path))
            raise

    for k, v in ids.items():
        v["min"] = str(v["min"])
        v["max"] = str(v["max"])
    return headers, ids, files_with_3_infos
