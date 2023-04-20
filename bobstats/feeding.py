from decimal import Decimal

from pydantic import Extra, BaseModel

from time import time

import requests
from json import dumps

from feeding.connector import UploadingConnector

from utils.logging import info, error, warning
from utils.models import TimestampedBaseModel
from utils.constants import ONE_DAY
from utils.misc import CustomJSONEncoder
from utils.health import HealthOut

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

class DACheckResults(BaseModel):
    accessible: bool
    available: bool

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

        return self._upload(data_as_str)

    def check_data_availability(self) -> DACheckResults:
        ret = DACheckResults(accessible=False, available=False)
        try:
            r = requests.get(f'{self._service_url}{self._health_path}', timeout=(3.05, 27))
        except IOError as e :
            error(f'connector: cannot get feeding service health status: {e}')
        except ValueError as e :
            error(f'connector: cannot get feeding service health status: {e}')
        else:
            if r.status_code != 200:
                error(f'connector: cannot get health data (status code: {r.status_code}, error: {r.text})')
            else:
                ret.accessible = True
                resp = r.json()
                try:
                    stuctured = HealthOut.parse_obj(resp)
                    health = stuctured.modules['BobStats']
                except Exception as e: 
                    error(f'connector: cannot find health data: {e}')
                else:
                    if health.status == 'error' and \
                       health.lastSuccessTimestamp == 0 and \
                       health.lastErrorTimestamp == 0:
                        warning(f'connector: no data on the feeding service') 
                    else:
                        ret.available = True
        return ret