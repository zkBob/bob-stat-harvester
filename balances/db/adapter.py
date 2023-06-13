from .balances import BalancesDB
from .transfers import TransfersDB
from .models import DBAConfig
from .exceptions import NotInitialized

class DBAdapter:
    _balances: BalancesDB
    _transfers: TransfersDB

    def __init__(self, config: DBAConfig):
        self._balances = BalancesDB(config)
        if config.tsdb_dir and config.tsdb_file_suffix:
            self._transfers = TransfersDB(config)

    def get_last_block(self) -> int:
        return self._balances.get_last_block()
    
    def get_holders_count(self) -> int:
        return self._balances.get_holders_count()
    
    def update(self, new_last_block: int, logs: list) -> bool:
        if not self._transfers:
            raise NotInitialized()
        
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
