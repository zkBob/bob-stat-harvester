from decimal import Decimal
from typing import Dict

from time import time

from utils.web3 import Web3Provider, ERC20Token
from utils.logging import info, error
from utils.misc import InitException
from utils.settings.models import BobVaultInventory

from bobvault.base_processor import BobVaultLogsProcessor
from bobvault.settings import Settings, discover_inventory
from bobvault.models import BobVaultTrade

from .db import DBAdapter

class FeesAdapter(BobVaultLogsProcessor):
    _w3prov: Web3Provider
    _pool_id: str
    _db: DBAdapter
    _collected_fees: Dict[str, Decimal]

    def __init__(self, chainid: str, settings: Settings):
        def inventory_setup(inv: BobVaultInventory):
            self._pool_id = inv.coingecko_poolid

        super().__init__(chainid)
        self._w3prov = settings.w3_providers[chainid]
        self._db = DBAdapter(self, settings)
        if not discover_inventory(settings.chains[chainid].inventories, inventory_setup):
            error(f'coingecko:{self._chainid}: inventory is not found')
            raise InitException
        
    def pre(self, snapshot: dict) -> bool:
        self._collected_fees = {}
        info(f'fees:{self._chainid}: preparation to analyze collected fees')
        return True

    def process(self, trade: BobVaultTrade) -> bool:
        token = trade.args.inToken
        fees = trade.args.amountIn - trade.args.amountOut
        
        token_sym = ERC20Token(self._w3prov, token).symbol()
        if not token_sym in self._collected_fees:
            self._collected_fees[token_sym] = fees
        else: 
            self._collected_fees[token_sym] += fees

    def post(self) -> bool:
        info(f'fees:{self._chainid}: collected {self._collected_fees}')

        self._collected_fees['dt'] = int(time())
        self._collected_fees['id'] = self._pool_id

        self._db.store(self._collected_fees)

        return True