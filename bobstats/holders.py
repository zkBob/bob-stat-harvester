from typing import Dict

from .settings import Settings

from json import load

from utils.logging import info, error
from utils.constants import BOB_TOKEN_ADDRESS

class Holders:

    def __init__(self, settings: Settings):
        self._chainids = list(settings.chains.keys())
        self._snapshot_dir = settings.snapshot_dir
        self._file_suffix = settings.balances_snapshot_file_suffix

    def get_bob_holders_amount(self) -> Dict[str, int]:
        # Due to disk IO operations to read big (potentially) blob of data it does not
        # make sense to read files in separate threads to preserve consequent reads of
        # data blocks
        ret = {}
        info(f'Getting amounf of token holders for {BOB_TOKEN_ADDRESS}')
        for chainid in self._chainids:
            fname = f'{self._snapshot_dir}/{chainid}-{self._file_suffix}'
            try:
                with open(fname, 'r') as json_file:
                    snapshot = load(json_file)
            except Exception as e:
                error(f'{chainid}: cannot read {fname}: {e}')
                snapshot = {}
            holders_num = len(snapshot['balances'])
            info(f'{chainid}: number of token holders {holders_num}')
            ret[chainid] = holders_num
        return ret