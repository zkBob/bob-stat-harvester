from typing import List, Tuple
from decimal import Decimal

from time import gmtime, strftime
from datetime import datetime

from tinyflux import TinyFlux, Point, TimeQuery

from utils.logging import info, error
from utils.constants import ONE_DAY, ZERO_DATETIME

from .settings import Settings
from .common import StatsByChains, ChainStats, GainStats, GainSet, OneTokenAcc

INVENTORY_FEES_TABLE = 'fees'
COMPOUNDING_INTEREST_TABLE = 'interest'

def _chainstats_to_datapoint(chaindata: ChainStats) -> Tuple[Point, Point]:
    ch_d = chaindata.dict()
    dt = datetime.fromtimestamp(ch_d['dt'])
    chain_tag = ch_d['chain']
    inventory_fees = ch_d['gain']['fees'] if ch_d['gain'] else []
    del ch_d['dt']
    del ch_d['chain']
    del ch_d['gain']

    inventory_fees_point = None
    if len(inventory_fees) > 0:
        inventory_fees_point = Point(
            measurement = INVENTORY_FEES_TABLE,
            time = dt,
            tags = {'chain': chain_tag},
            fields = dict([(f['symbol'], f['amount']) for f in inventory_fees])
        )        

    return (Point(
                time = dt,
                tags = {'chain': chain_tag},
                fields = ch_d
            ),
            inventory_fees_point
            )

def _datapoint_to_chainstats(gen_point: Point, fees_point: Point = None) -> ChainStats:
    def parse_fee_point(fees_point: Point) -> GainSet:
        if not fees_point:
            return []
        else:
            return [OneTokenAcc(
                symbol=t,
                amount=fees_point.fields[t]
            ) for t in fees_point.fields]
    
    source = gen_point.fields
    source.update(
        {
            'dt': int(datetime.timestamp(gen_point.time)),
            'chain': gen_point.tags['chain'],
            'gain': GainStats(
                fees = parse_fee_point(fees_point)
            )
        }
    )
    return ChainStats.parse_obj(source)

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
        for point in toi:
            # assuming that points are inserted chronologically
            earliest_point = point.time
            break

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
        comp_fees_points = []
        for orig_ch_d in stats:
            gen_point, fees_point = _chainstats_to_datapoint(orig_ch_d)
            composed_points.append(gen_point)
            if fees_point:
                comp_fees_points.append(fees_point)

        if len(composed_points) > 0:
            with TinyFlux(self._composed_stats_filename) as composed_db:
                composed_db.insert_multiple(composed_points)
        if len(comp_fees_points) > 0:
            with TinyFlux(self._composed_fee_stats_filename) as comp_fees_db:
                comp_fees_db.insert_multiple(comp_fees_points)

        info('db: timeseries db updated successfully')

    def get_nearest_to_timespot(self, required_ts: int) -> StatsByChains:
        dpts = _get_nearest_to_timespot(
            self._composed_stats_filename,
            required_ts,
            self._measurements_range // 2
        )

        fees_dpts = []
        if len(dpts) > 0:
            fees_dpts = _find_exact_or_nearest(
                self._composed_fee_stats_filename,
                datetime.timestamp(dpts[0].time),
                self._measurements_range // 2,
                table = INVENTORY_FEES_TABLE
            )

        ret = []
        for dp in dpts:
            same_chain_fees_dpt = None
            for fees in fees_dpts:
                if dp.tags['chain'] == fees.tags['chain']:
                    same_chain_fees_dpt = fees
                    break
            ret.append(_datapoint_to_chainstats(dp, same_chain_fees_dpt))
        return ret
