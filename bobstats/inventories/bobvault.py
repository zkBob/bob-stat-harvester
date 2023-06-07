from typing import Dict, Optional
from decimal import Decimal

from tinyflux import TinyFlux, TimeQuery, Point

from time import time, gmtime, strftime
from datetime import datetime

from web3 import Web3

from bobstats.settings import Settings
from bobvault.processors.fees.db.base import DBGenericAdapter

from utils.web3 import Web3Provider, CachedERC20Token as ERC20Token
from utils.settings.models import BobVaultInventory
from utils.constants import BOB_TOKEN_ADDRESS
from utils.logging import info, error

from .common import BaseInventoryStats, InventoryHandler, BobVaultInventoryStats

class DBAdapter(DBGenericAdapter):
    def __init__(self, pool_id: str, full_fn: str):
        super().__init__()
        self._pool_id = pool_id
        self._log_prefix = f'db:{pool_id}'
        self._fees_stats_filename = full_fn

class BobVaultInventoryHandler(InventoryHandler):
    w3prov: Web3
    vault_addr: str
    pool_id: str
    db: DBAdapter

    def __init__(
        self,
        w3_provider: Web3Provider,
        vault_addr: str,
        poolid: str,
        settings: Settings
    ):
        self.w3prov = w3_provider
        self.vault_addr = vault_addr
        self.pool_id = poolid
        self._db = DBAdapter(poolid, f'{settings.tsdb_dir}/{poolid}-{settings.bobvault_fees_db_suffix}')
        self._db.discover_time_of_latest_point(settings.measurements_interval)

    @classmethod
    def generate_handler(cls, w3: Web3Provider, params: BobVaultInventory, settings: Settings):
        vault_addr = Web3.toChecksumAddress(params.address)
        return cls(w3, vault_addr, params.coingecko_poolid, settings)

    def get_stats(self) -> Dict[str, BobVaultInventoryStats]:
        info(f'{self.pool_id}: getting BobVault info')
        inventory_stats = {}

        fees = self._db.discover_latest_point()
        if fees:
            del fees['dt']
            del fees['id']
            info(f'{self.pool_id}: discovered fees {fees}')
        else:
            error(f'{self.pool_id}: no fees discovered')
            fees = {}

        bobtoken = ERC20Token(self.w3prov, BOB_TOKEN_ADDRESS)
        try:
            tvl = bobtoken.balanceOf(self.vault_addr)
        except:
            error(f'{self.pool_id}: not able to get data')
        else:
            bob_symbol = bobtoken.symbol()
            fees_value = Decimal('0.0')
            if bob_symbol in fees:
                fees_value = Decimal(str(fees[bob_symbol]))
                del fees[bob_symbol]
            info(f'{self.pool_id}: {bob_symbol} tvl: {tvl}, fees: {fees_value}')
            inventory_stats[f'{bob_symbol}_on_{self.pool_id}'] = BobVaultInventoryStats(
                token0 = BaseInventoryStats(
                    symbol = bob_symbol,
                    tvl = tvl,
                    fees = fees_value
                )
            )

            for symbol in fees:
                fees_value = Decimal(str(fees[symbol]))
                info(f'{self.pool_id}: {symbol} fees: {fees_value}')
                inventory_stats[f'{symbol}_on_{self.pool_id}'] = BobVaultInventoryStats(
                    token0 = BaseInventoryStats(
                        symbol = symbol,
                        tvl = Decimal('0.0'),
                        fees = fees_value
                    )
                )

        return inventory_stats
