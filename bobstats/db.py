from typing import Dict, Tuple
from decimal import Decimal

from time import gmtime, strftime
from datetime import datetime

from tinyflux import TinyFlux, Point, TimeQuery

from utils.logging import info, error
from utils.constants import ONE_DAY

from .settings import Settings
from .common import StatsByChains, ChainStats

def _chainstats_to_datapoint(chaindata: ChainStats) -> Tuple[Point, Point]:
    ch_d = chaindata.dict()
    dt = datetime.fromtimestamp(ch_d['dt'])
    chain_tag = ch_d['chain']
    fees = ch_d['fees']
    del ch_d['dt']
    del ch_d['chain']
    del ch_d['fees']

    return (Point(
                time = dt,
                tags = {'chain': chain_tag},
                fields = ch_d
            ),
            Point(
                time = dt,
                tags = {'chain': chain_tag},
                fields = fees
            ))

def _datapoint_to_chainstats(gen_point: Point, fees_point: Point = None) -> ChainStats:
    def parse_fee_point(fees_point: Point) -> Dict[str, Decimal]:
        # TODO: add ability to parse fees
        return {}
    
    source = gen_point.fields
    source.update(
        {
            'dt': int(datetime.timestamp(gen_point.time)),
            'chain': gen_point.tags['chain'],
            'fees': parse_fee_point(fees_point)
        }
    )
    return ChainStats.parse_obj(source)

class DBAdapter:
    _composed_stats_filename: str
    _composed_fee_stats_filename: str
    _measurements_range: int

    def __init__(self, settings: Settings):
        self._measurements_range = settings.measurements_interval
        self._composed_stats_filename = settings.tsdb_dir + '/' + settings.bob_composed_stat_db
        self._composed_fee_stats_filename = settings.tsdb_dir + '/' + settings.bob_composed_fees_stat_db

    def store(self, stats: StatsByChains):
        info('db: storing data to timeseries db')
        composed_points = []
        comp_fees_points = []
        for orig_ch_d in stats:
            gen_point, fees_point = _chainstats_to_datapoint(orig_ch_d)
            composed_points.append(gen_point)
            comp_fees_points.append(fees_point)

        if len(composed_points) > 0:
            with TinyFlux(self._composed_stats_filename) as composed_db:
                composed_db.insert_multiple(composed_points)
        if len(comp_fees_points) > 0:
            with TinyFlux(self._composed_fee_stats_filename) as comp_fees_db:
                comp_fees_db.insert_multiple(comp_fees_points)

        info('db: timeseries db updated successfully')

    def get_nearest_to_timespot(self, required_ts: int) -> StatsByChains:
        info(f"db: looking for data points near {strftime('%Y-%m-%d %H:%M:%S', gmtime(required_ts))}")
        exploration_step = self._measurements_range // 2
        exploration_half_range = exploration_step

        dps = []
        with TinyFlux(self._composed_stats_filename) as composed_db:
            qtime = TimeQuery()
            if not composed_db.contains(qtime > datetime.fromtimestamp(0)):
                error(f'db: database does not contain points')
                return []
            
            for point in composed_db:
                # assuming that points are inserted chronologically
                earliest_point = point.time
                break

            while True:
                left_dt = required_ts - exploration_half_range
                right_dt = required_ts + exploration_half_range
                info(f"db: data points extending exploration interval is {strftime('%Y-%m-%d %H:%M:%S', gmtime(left_dt))} - {strftime('%Y-%m-%d %H:%M:%S', gmtime(right_dt))}")
                left_dt = datetime.fromtimestamp(left_dt)
                right_dt = datetime.fromtimestamp(right_dt)

                query_left = qtime >= left_dt
                query_right = qtime <= right_dt
                if composed_db.contains(query_left & query_right):
                    break

                if right_dt.astimezone(earliest_point.tzinfo) < earliest_point:
                    error(f'db: all points are after required timespot')
                    return []

                exploration_half_range = exploration_half_range + exploration_step

            points = composed_db.search(query_left & query_right)
            suitable_time = 0
            # if there are two sets of points in the time interval
            # it is necessary to choose the set closest to the required timespot
            for p in points:
                p_ts = datetime.timestamp(p.time)
                if abs(required_ts - p_ts) < abs(required_ts - suitable_time) and \
                    abs(required_ts - p_ts) < ONE_DAY // 2:
                    suitable_time = p_ts

            if suitable_time == 0:
                error(f'db: found data points are out 12 hrs threshold')
                return []
                
            dps = composed_db.search(qtime == datetime.fromtimestamp(suitable_time))
            info(f"db: found {len(dps)} records at {strftime('%Y-%m-%d %H:%M:%S', gmtime(suitable_time))}")
        
        ret = []
        for dp in dps:
            ret.append(_datapoint_to_chainstats(dp))
        return ret
