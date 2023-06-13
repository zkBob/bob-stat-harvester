from typing import Dict

from .settings import Settings

from json import load

from utils.logging import info, error
from utils.constants import BOB_TOKEN_ADDRESS

from balances.db.adapter import DBAdapter
from balances.db.models import DBAConfig

class Holders:
    _chains: Dict[str, int]
    _snapshot_dir: str
    _file_suffix: str

    def __init__(self, settings: Settings):
        self._snapshot_dir = settings.snapshot_dir
        self._file_suffix = settings.balances_snapshot_file_suffix
        self._chains = {}
        for ch in settings.chains:
            self._chains[ch] = settings.chains[ch].token.start_block

    def get_bob_holders_amount(self) -> Dict[str, int]:
        # Due to disk IO operations to read big (potentially) blob of data it does not
        # make sense to read files in separate threads to preserve consequent reads of
        # data blocks
        ret = {}
        info(f'Getting amounf of token holders for {BOB_TOKEN_ADDRESS}')
        for chainid in self._chains:
            db = DBAdapter(DBAConfig(
                chainid=chainid,
                snapshot_dir=self._snapshot_dir,
                snapshot_file_suffix=self._file_suffix,
                init_block=self._chains[chainid]
            ))
            holders_num = db.get_holders_count()
            info(f'{chainid}: number of token holders {holders_num}')
            ret[chainid] = holders_num
        return ret