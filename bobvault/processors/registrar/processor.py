from typing import Dict

from json import dump

from utils.web3 import Web3Provider, ERC20Token
from utils.logging import info, error
from utils.misc import InitException
from utils.settings.models import BobVaultInventory

from bobvault.base_processor import BobVaultLogsProcessor
from bobvault.settings import Settings, discover_inventory
from bobvault.models import BobVaultTrade

class Registrar(BobVaultLogsProcessor):
    _w3prov: Web3Provider
    _registrar_filename: str
    _registrar: Dict[str, str]

    def __init__(self, chainid: str, settings: Settings):
        def inventory_setup(inv: BobVaultInventory):
            self._registrar_filename = f'{settings.snapshot_dir}/{inv.coingecko_poolid}-{settings.registrar_file_suffix}'

        super().__init__(chainid)
        self._w3prov = settings.w3_providers[chainid]
        if not discover_inventory(settings.chains[chainid].inventories, inventory_setup):
            error(f'registrar:{self._chainid}: inventory is not found')
            raise InitException
        
    def pre(self, snapshot: dict) -> bool:
        self._registrar = {}
        info(f'registrar:{self._chainid}: preparation to register tokens')
        return True

    def process(self, trade: BobVaultTrade) -> bool:
        token = trade.args.inToken
        
        token_sym = ERC20Token(self._w3prov, token).symbol()
        if not token in self._registrar:
            self._registrar[token] = token_sym

    def post(self) -> bool:
        info(f'registrar:{self._chainid}: registered {len(self._registrar)} tokens')

        retval = False
        try:
            with open(self._registrar_filename, 'w') as json_file:
                dump(self._registrar, json_file)
                retval = True
        except Exception as e:
            error(f'registrar:{self._chainid}: cannot save tokens with the reason {e}')

        return retval
