from decimal import Decimal
from typing import Dict, Union

from datetime import datetime

from copy import copy

from tinyflux import TinyFlux, Point

from utils.logging import info, error
from utils.misc import InitException
from utils.settings.models import BobVaultInventory

from bobvault.settings import Settings, discover_inventory

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

class DBAdapter:
    _fees_stats_filename: str
    _log_prefix: str
    _pool_id: str

    def __init__(self, chainid: str, settings: Settings):
        def inventory_setup(inv: BobVaultInventory):
            self._pool_id = inv.coingecko_poolid
        
        self._log_prefix = f'db:{chainid}'
        if not discover_inventory(settings.chains[chainid].inventories, inventory_setup):
            error(f'{self._log_prefix }: inventory is not found')
            raise InitException
        self._fees_stats_filename = f'{settings.tsdb_dir}/{self._pool_id}-{settings.fees_stat_db}'

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
