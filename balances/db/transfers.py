from typing import Dict, List
from decimal import Decimal

from copy import copy

from time import gmtime, strftime
from datetime import datetime

from tinyflux import TinyFlux, Point

from utils.logging import info

from ..settings  import Settings

ONE_BLN = Decimal(10 ** 9)

def _to_1bln_base(_value):
    _val = Decimal(_value)
    a0 = _val - ((_val // ONE_BLN) * ONE_BLN)
    v_tmp = (_val - a0) // ONE_BLN 
    a1 = v_tmp - ((v_tmp // ONE_BLN) * ONE_BLN)
    v_tmp = (v_tmp - a1) // ONE_BLN 
    a2 = v_tmp - ((v_tmp // ONE_BLN) * ONE_BLN)
    v_tmp = (v_tmp - a2) // ONE_BLN
    a3 = v_tmp - ((v_tmp // ONE_BLN) * ONE_BLN)
    return int(a3), int(a2), int(a1), int(a0)

class TransfersDB:
    _chain: str
    _tsdb_dir: str
    _tsdb_file_sufix: str
    _points: Dict[str, List[Point]]

    def __init__(self, chainid: str, settings: Settings):
        self._chain = chainid
        self._tsdb_dir = settings.tsdb_dir
        self._tsdb_file_sufix = settings.tsdb_file_suffix

    def prepare_transaction(self):
        self._points = {}

    def register_log(self, log: dict):
        record = copy(log)
        record_ts = record['timestamp']
        (a3, a2, a1, a0) = _to_1bln_base(record['fields']['value'])
        record['fields'] = {
            'a0': a0,
            'a1': a1,
            'a2': a2,
            'a3': a3
        }

        group = strftime('%Y%m', gmtime(record_ts))
        if not group in self._points:
            self._points[group] = []
        self._points[group].append(Point(
            time = datetime.fromtimestamp(record_ts),
            tags = record['tags'],
            fields = record['fields']
        ))

    def finish_transaction(self):
        info(f'{self._chain}: storing {sum([len(self._points[g]) for g in self._points])} to timeseries db')
        for grp in self._points:
            with TinyFlux(f'{self._tsdb_dir}/{self._chain}-{grp}-{self._tsdb_file_sufix}') as tsdb:
                tsdb.insert_multiple(self._points[grp])
