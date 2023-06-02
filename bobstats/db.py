from typing import List, Union
from decimal import Decimal
from dataclasses import dataclass

from time import gmtime, strftime
from datetime import datetime

from tinyflux import TinyFlux, Point, TimeQuery

from utils.logging import info, error
from utils.constants import ONE_DAY, ZERO_DATETIME

from .settings import Settings
from .common import StatsByChains, ChainStats, GainStats, YieldSet, OneTokenAcc

INVENTORY_FEES_TABLE = 'fees'
COMPOUNDING_INTEREST_TABLE = 'interest'

@dataclass(frozen=True)
class StatsDataPoints:
    main: Point
    fees: Point
    interest: Point

def _yieldset_to_point(ys: YieldSet, table: str, dt: datetime, ct: str) -> Union[Point, None]:
    retval = None
    if ys and len(ys) > 0:
        retval = Point(
            measurement = table,
            time = dt,
            tags = {'chain': ct},
            fields = dict([(f['symbol'], f['amount']) for f in ys])
        )
    return retval 

def _chainstats_to_datapoints(chaindata: ChainStats) -> StatsDataPoints:
    ch_d = chaindata.dict()
    dt = datetime.fromtimestamp(ch_d['dt'])
    chain_tag = ch_d['chain']
    if ch_d['gain']:
        inventory_fees = ch_d['gain']['fees']
        interest = ch_d['gain']['interest'] if 'interest' in ch_d['gain'] else None
    else: 
        inventory_fees = []
        interest = None
    del ch_d['dt']
    del ch_d['chain']
    del ch_d['gain']

    inventory_fees_point = _yieldset_to_point(inventory_fees, INVENTORY_FEES_TABLE, dt, chain_tag)
    interest_point = _yieldset_to_point(interest, COMPOUNDING_INTEREST_TABLE, dt, chain_tag)

    return StatsDataPoints(
        main = Point(
            time = dt,
            tags = {'chain': chain_tag},
            fields = ch_d
        ),
        fees = inventory_fees_point,
        interest = interest_point
    )

def _datapoint_to_chainstats(dpts: StatsDataPoints) -> ChainStats:
    def parse_fee_point(yield_point: Point) -> YieldSet:
        if not yield_point:
            return []
        else:
            return [{
                'symbol': t,
                'amount': yield_point.fields[t]
            } for t in yield_point.fields]
    
    source = dpts.main.fields
    source.update(
        {
            'dt': int(datetime.timestamp(dpts.main.time)),
            'chain': dpts.main.tags['chain'],
            'gain': {
                'fees': parse_fee_point(dpts.fees)
            }
        }
    )

    interest = parse_fee_point(dpts.interest)
    if len(interest) > 0:
        source['gain'].update({'interest': interest})

    return ChainStats.parse_obj(source)

def _find_datapoint_by_chain(dpts: List[Point], chain: str) -> Union[Point, None]:
    retval = None
    for dp in dpts:
        if dp.tags['chain'] == chain:
            retval = dp
            break
    return retval

def _is_empty(tsdb_fn: str) -> bool:
    retval = True
    with TinyFlux(tsdb_fn) as dbase:
        qtime = TimeQuery()
        if dbase.contains(qtime > ZERO_DATETIME):
            retval = False
    return retval

def _get_nearest_to_timespot(
        tsdb_fn: str,
        required_ts: int,
        exploration_step: int,
        table: str = "_default"
) -> List[Point]:
    info(f"db:{tsdb_fn}: looking for data points near {strftime('%Y-%m-%d %H:%M:%S', gmtime(required_ts))}")
    exploration_half_range = exploration_step

    dpts = []

    if _is_empty(tsdb_fn):
        error(f'db:{tsdb_fn}: database does not contain points')
        return []
    
    with TinyFlux(tsdb_fn) as dbase:
        qtime = TimeQuery()
        
        toi = dbase.measurement(table)
        earliest_point = None
        for point in toi:
            # assuming that points are inserted chronologically
            earliest_point = point.time
            break
        if not earliest_point:
            error(f'db:{tsdb_fn}: database does not contain points for {table}')
            return []

        while True:
            left_dt = required_ts - exploration_half_range
            right_dt = required_ts + exploration_half_range
            info(f"db:{tsdb_fn}: data points extending exploration interval is {strftime('%Y-%m-%d %H:%M:%S', gmtime(left_dt))} - {strftime('%Y-%m-%d %H:%M:%S', gmtime(right_dt))}")
            left_dt = datetime.fromtimestamp(left_dt)
            right_dt = datetime.fromtimestamp(right_dt)

            query_left = qtime >= left_dt
            query_right = qtime <= right_dt
            if toi.contains(query_left & query_right):
                break

            if right_dt.astimezone(earliest_point.tzinfo) < earliest_point:
                error(f'db:{tsdb_fn}: all points are after required timespot')
                return []

            exploration_half_range = exploration_half_range + exploration_step

        points = toi.search(query_left & query_right)
        suitable_time = 0
        # if there are two sets of points in the time interval
        # it is necessary to choose the set closest to the required timespot
        for p in points:
            p_ts = datetime.timestamp(p.time)
            if abs(required_ts - p_ts) < abs(required_ts - suitable_time) and \
                abs(required_ts - p_ts) < ONE_DAY // 2:
                suitable_time = p_ts

        if suitable_time == 0:
            error(f'db:{tsdb_fn}: found data points are out 12 hrs threshold')
            return []
            
        dpts = toi.search(qtime == datetime.fromtimestamp(suitable_time))
        info(f"db:{tsdb_fn}: found {len(dpts)} records at {strftime('%Y-%m-%d %H:%M:%S', gmtime(suitable_time))}")
    return dpts

def _find_exact_or_nearest(
        tsdb_fn: str,
        required_ts: int,
        exploration_step: int,
        table: str = "_default"
) -> List[Point]:
    suitable_time = datetime.fromtimestamp(required_ts)

    dpts = []
    if _is_empty(tsdb_fn):
        error(f'db:{tsdb_fn}: database does not contain points')
    else:
        with TinyFlux(tsdb_fn) as dbase:
            qtime = TimeQuery()
            
            toi = dbase.measurement(table)
            dpts = toi.search(qtime == suitable_time)

            if len(dpts) == 0:
                dpts = _get_nearest_to_timespot(
                    tsdb_fn,
                    required_ts,
                    exploration_step,
                    table = table,
                )
            else:
                info(f"db:{tsdb_fn}: found {len(dpts)} records at {suitable_time.strftime('%Y-%m-%d %H:%M:%S')}")
    return dpts

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
        comp_yield_points = []
        for orig_ch_d in stats:
            stats_dps = _chainstats_to_datapoints(orig_ch_d)
            composed_points.append(stats_dps.main)
            if stats_dps.fees:
                comp_yield_points.append(stats_dps.fees)
            if stats_dps.interest:
                comp_yield_points.append(stats_dps.interest)

        if len(composed_points) > 0:
            with TinyFlux(self._composed_stats_filename) as composed_db:
                composed_db.insert_multiple(composed_points)
        if len(comp_yield_points) > 0:
            with TinyFlux(self._composed_fee_stats_filename) as comp_fees_db:
                comp_fees_db.insert_multiple(comp_yield_points)

        info('db: timeseries db updated successfully')

    def get_nearest_to_timespot(self, required_ts: int) -> StatsByChains:
        dpts = _get_nearest_to_timespot(
            self._composed_stats_filename,
            required_ts,
            self._measurements_range // 2
        )

        fees_dpts = []
        interest_dpts = []
        if len(dpts) > 0:
            fees_dpts = _find_exact_or_nearest(
                self._composed_fee_stats_filename,
                datetime.timestamp(dpts[0].time),
                self._measurements_range // 2,
                table = INVENTORY_FEES_TABLE
            )
            interest_dpts = _find_exact_or_nearest(
                self._composed_fee_stats_filename,
                datetime.timestamp(dpts[0].time),
                self._measurements_range // 2,
                table = COMPOUNDING_INTEREST_TABLE
            )

        ret = []
        for dp in dpts:
            stats_dpts = StatsDataPoints(
                main=dp,
                fees=_find_datapoint_by_chain(fees_dpts, dp.tags['chain']),
                interest=_find_datapoint_by_chain(interest_dpts, dp.tags['chain'])
            )
            ret.append(_datapoint_to_chainstats(stats_dpts))
        return ret
