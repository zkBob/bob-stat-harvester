from .balances import BalancesDB
from .transfers import TransfersDB
from ..settings import Settings

class DBAdapter:
    _balances: BalancesDB

    def __init__(self, chainid: str, settings: Settings):
        self._balances = BalancesDB(chainid, settings)
        self._transfers = TransfersDB(chainid, settings)

    def get_last_block(self) -> int:
        return self._balances.get_last_block()
    
    def update(self, new_last_block: int, logs: list) -> bool:
        self._transfers.prepare_transaction()
        storages_updated = False

        if len(logs) > 0:
            for log in logs:
                self._transfers.register_log(log)
                self._balances.register_log(log)

            self._transfers.finish_transaction()
            
            storages_updated = True

        self._balances.sync(new_last_block)
        return storages_updated
