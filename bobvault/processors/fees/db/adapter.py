from utils.logging import error
from utils.misc import InitException
from utils.settings.models import BobVaultInventory

from bobvault.settings import Settings, discover_inventory

from .base import DBGenericAdapter

class DBAdapter(DBGenericAdapter):

    def __init__(self, chainid: str, settings: Settings):
        def inventory_setup(inv: BobVaultInventory):
            self._pool_id = inv.coingecko_poolid
        
        super().__init__()
        self._log_prefix = f'db:{chainid}'
        if not discover_inventory(settings.chains[chainid].inventories, inventory_setup):
            error(f'{self._log_prefix }: inventory is not found')
            raise InitException
        self._fees_stats_filename = f'{settings.tsdb_dir}/{self._pool_id}-{settings.fees_stat_db_suffix}'
