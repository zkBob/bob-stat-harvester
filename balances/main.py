
from typing import Tuple

from utils.logging import info, error

from .settings import Settings
from .web3 import Web3ProviderExt
from .token import BobTokenContract
from .db.adapter import DBAdapter

class Indexer:
    _chain: str
    _w3prov: Web3ProviderExt
    _token: BobTokenContract
    _db: DBAdapter

    def __init__(self, chainid: str, settings: Settings):
        self._chain = chainid
        self._w3prov = settings.w3_providers[chainid]
        self._token = BobTokenContract(self._w3prov)
        self._db = DBAdapter(chainid, settings)

    def discover_balance_updates(self) -> Tuple[bool, bool]:
        info(f'{self._chain}: identifying dump range to identify balances updates')
        start_block = self._db.get_last_block() + 1
        last_block = self._w3prov.get_finalized_block()
        info(f'{self._chain}: dump range: {start_block} - {last_block}')

        storages_updated = False
        head_achieved = False

        if last_block < start_block:
            error(f'{self._chain}: something wrong with RPC endpoit')
            return storages_updated, head_achieved
        
        achieved_block, logs = self._token.get_transfer_logs(start_block, last_block)

        head_achieved = last_block == achieved_block

        storages_updated = self._db.update(achieved_block, logs)
            
        return storages_updated, head_achieved