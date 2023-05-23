from decimal import Decimal

from pydantic import Extra

from time import time

from json import dumps

from feeding.connector import UploadingConnector

from utils.logging import info
from utils.models import TimestampedBaseModel
from utils.constants import ONE_DAY
from utils.misc import CustomJSONEncoder, DACheckResults

from .db import DBAdapter
from .common import StatsByChains

class BobStatsPeriodData(TimestampedBaseModel, extra=Extra.forbid):
    totalSupply: Decimal
    collaterisedCirculatedSupply: Decimal
    volumeUSD: Decimal
    holders: int

class BobStatsDataForTwoPeriods(TimestampedBaseModel, extra=Extra.forbid):
    current: BobStatsPeriodData
    previous: BobStatsPeriodData

def _chainsdata_to_bobstats(stats: StatsByChains) -> BobStatsPeriodData:
    bobstats = BobStatsPeriodData(
        timestamp = 0,
        totalSupply = 0,
        collaterisedCirculatedSupply = 0,
        volumeUSD = 0,
        holders = 0
    )
    ts = 0
    for ch_d in stats:
        ts = ch_d.dt
        bobstats.totalSupply += ch_d.totalSupply
        bobstats.collaterisedCirculatedSupply += ch_d.colCirculatingSupply
        bobstats.volumeUSD += ch_d.volumeUSD
        bobstats.holders += ch_d.holders
    bobstats.timestamp = ts
    return bobstats

def prepare_data_for_feeding(stats: StatsByChains, db: DBAdapter) -> BobStatsDataForTwoPeriods:
    cur = _chainsdata_to_bobstats(stats)
    info(f'Current stat: {cur}')

    ts_24h_ago = cur.timestamp - ONE_DAY
    previous_data = db.get_nearest_to_timespot(ts_24h_ago)
    if len(previous_data) == 0:
        return None
    prev = _chainsdata_to_bobstats(previous_data)
    info(f'Previous stat: {prev}')

    return BobStatsDataForTwoPeriods(
        timestamp = int(time()),
        current = cur,
        previous = prev
    )

class BobStatsConnector(UploadingConnector):

    def upload_bobstats(self, data: BobStatsDataForTwoPeriods) -> bool:
        data_as_str = dumps(data.dict(), cls=CustomJSONEncoder)
        info(f'connector: uploading stats')

        return self._upload(data_as_str)

    def check_data_availability(self) -> DACheckResults:
        ret = DACheckResults(accessible=False, available=False)
        (status, stuctured) = self._get_health_data()
        if status:
            ret.accessible = True
            if stuctured:
                health = stuctured.modules['BobStats']
                ret.available = self._check_availability(health)
        return ret