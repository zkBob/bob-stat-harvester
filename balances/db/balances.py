from decimal import Decimal

from json import load, dump

from utils.logging import info
from utils.constants import ZERO_ADDRESS

from .models import DBAConfig
from .exceptions import NotInitialized

class BalancesDB:
    _chain: str
    _snapshot_fn: str
    _token_start_block: int
    _snapshot: int

    def __init__(self, config: DBAConfig):
        self._chain = config.chainid
        self._snapshot_fn = f'{config.snapshot_dir}/{config.chainid}-{config.snapshot_file_suffix}'
        self._token_start_block = config.init_block
        self._snapshot = None

    def _empty_snapshot(self) -> dict:
        return {
            "start_block": self._token_start_block,
            "last_block": self._token_start_block - 1,
            "balances": {}
        }

    def load(self):
        info(f'{self._chain}: reading balances snapshot')
        try:
            with open(self._snapshot_fn, 'r') as json_file:
                self._snapshot = load(json_file)
        except IOError:
            info(f'{self._chain}: empty snapshot will be used')
            self._snapshot = self._empty_snapshot()

    def get_last_block(self) -> int:
        if not self._snapshot:
            self.load()
        return self._snapshot['last_block']

    def get_holders_count(self) -> int:
        if not self._snapshot:
            self.load()
        return len(self._snapshot['balances'])

    def _change_balance(self, account: str, value: Decimal):
        prev_balance = Decimal(0)
        if account in self._snapshot['balances']:
            prev_balance = self._snapshot['balances'][account]
            if not type(prev_balance) == str:
                prev_balance = str(prev_balance)
            prev_balance = Decimal(prev_balance)
        new_balance = prev_balance + value
        if new_balance == 0:
            del self._snapshot['balances'][account]
        else:
            self._snapshot['balances'][account] = str(new_balance)
    
    def register_log(self, log: dict):
        sender = log['tags']['from']
        reciever = log['tags']['to']
        value = log['methods']['denominate'](log['fields']['value'])
        if  value != 0:
            if sender != ZERO_ADDRESS:
                self._change_balance(sender, -value)
            if reciever != ZERO_ADDRESS:
                self._change_balance(reciever, value)

    def sync(self, new_last_block: int, clean: bool = True):
        if not self._snapshot:
            raise NotInitialized()

        self._snapshot['last_block'] = new_last_block
        info(f'{self._chain}: Updating snapshot with new last block {new_last_block}')

        with open(self._snapshot_fn, 'w') as json_file:
            dump(self._snapshot, json_file)
        
        if clean:
            self.clean()

    def clean(self):
        self._snapshot = None
