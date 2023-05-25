from decimal import Decimal
from typing import Dict, Union, Optional

from time import time
from datetime import datetime

from copy import copy

from tinyflux import TinyFlux, Point, TimeQuery

from utils.logging import info, error

ZERO_DATETIME = datetime.fromtimestamp(0)

FeesDict = Dict[str, Union[str, int, Decimal]]

def _fees_dict_to_datapoint(fees: FeesDict) -> Point:
    data = copy(fees)
    dt = datetime.fromtimestamp(data['dt'])
    id = data['id']
    del data['dt']
    del data['id']

    for k in data:
        data[k] = float(data[k])

    return Point(
                 time = dt,
                 tags = {'id': id},
                 fields = data
           )

def _datapoint_to_fees_dict(fees_point: Point) -> FeesDict:
    retval = fees_point.fields
    retval.update({
        'dt': int(fees_point.time.timestamp()),
        'id': fees_point.tags['id']
    })
    return retval

class DBGenericAdapter:
    _fees_stats_filename: str
    _log_prefix: str
    _pool_id: str
    _lastest_db_time: datetime

    def __init__(self):
        self._lastest_db_time = ZERO_DATETIME

    def store(self, fees: FeesDict):
        info(f'{self._log_prefix}: storing data to timeseries db')
        fees_points = []
        if len(fees) > 2:
            point = _fees_dict_to_datapoint(fees)
            fees_points.append(point)

        if len(fees_points) > 0:
            with TinyFlux(self._fees_stats_filename) as fees_db:
                fees_db.insert_multiple(fees_points)
            info(f'{self._log_prefix}: timeseries db updated successfully')
        else:
            error(f'{self._log_prefix}: no data to update timeseries')

    def _is_empty(self) -> bool:
        retval = True
        with TinyFlux(self._fees_stats_filename) as fees_db:
            qtime = TimeQuery()
            if fees_db.contains(qtime > ZERO_DATETIME):
                retval = False
        return retval
    
    def discover_time_of_latest_point(self, step_back: int = 3600) -> datetime:
        retval = ZERO_DATETIME
        if self._is_empty():
            error(f'{self._log_prefix}: database does not contain points')
        else:
            qtime = TimeQuery()
            with TinyFlux(self._fees_stats_filename) as fees_db:
                for point in fees_db:
                    # assuming that points are inserted chronologically
                    earliest_point = point.time
                    break

            required_ts = int(time()) - step_back
            
            while True:
                threshold_dt = datetime.fromtimestamp(required_ts)
                info(f"{self._log_prefix}: looking for data points after {threshold_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                threshold_query = qtime >= threshold_dt
                
                # Since the db is being updated by another process, read access must be
                # as short as possible to avoid collisions
                with TinyFlux(self._fees_stats_filename) as fees_db:
                    points = fees_db.search(threshold_query)

                if len(points) > 0:
                    retval = points[-1].time
                    break
                else:
                    if earliest_point >= threshold_dt.astimezone(earliest_point.tzinfo):
                        retval = earliest_point
                        break
                    required_ts -= step_back

            info(f"{self._log_prefix}: time of latest point: {retval.strftime('%Y-%m-%d %H:%M:%S')}")
        self._lastest_db_time = retval
        
    def discover_latest_point(self) -> Optional[FeesDict]:
        if not self._lastest_db_time:
            error(f'{self._log_prefix}: time of the latest point was not set')
            return None
        
        info(f"{self._log_prefix}: looking for data points after {self._lastest_db_time.strftime('%Y-%m-%d %H:%M:%S')}")
        qtime = TimeQuery()
        threshold_query = qtime >= self._lastest_db_time

        # Since the db is being updated by another process, read access must be
        # as short as possible to avoid collisions
        with TinyFlux(self._fees_stats_filename) as fees_db:
            points = fees_db.search(threshold_query)

        retval = None
        if len(points) > 0:
            point = points[-1]
            self._lastest_db_time = point.time
            retval = _datapoint_to_fees_dict(point)
        return retval