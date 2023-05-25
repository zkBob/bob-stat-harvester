from typing import Dict, Union, List

from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION

from .settings import Settings

from utils.logging import info, error
from utils.web3 import Web3Provider
from utils.settings.models import DepoloymentDescriptor

from .inventories.common import InventoryHandler, UniswapLikeInventoryStats, BobVaultInventoryStats
from .inventories.uniswap import UniswapInventoryHandler
from .inventories.kyberswap import KyberswapElasticInventoryHandler
from .inventories.bobvault import BobVaultInventoryHandler
from .inventories.holder import InventoryHolderHandler

class Inventory:
    _max_workers: int
    _handlers: Dict[str, List[InventoryHandler]]

    _inventory_protocols = {'UniswapV3': UniswapInventoryHandler,
                            'KyberSwap Elastic': KyberswapElasticInventoryHandler,
                            'BobVault': BobVaultInventoryHandler,
                            'Holder': InventoryHolderHandler
                           }

    def __init__(self, settings: Settings):
        self._handlers = {}
        for chainid in settings.chains:
            self._handlers[chainid] = []
            for inventory in settings.chains[chainid].inventories:
                info(f'{chainid}: getting handler for {inventory.protocol}')
                handler_class = self._get_inventory_handler(inventory.protocol)
                if handler_class:
                    self._handlers[chainid].append(handler_class.generate_handler(
                        settings.w3_providers[chainid],
                        inventory,
                        settings
                    ))
                else:
                    error(f'{chainid}: not able to discover inventory')
        self._max_workers = min(len(self._handlers), settings.max_workers)

    def _get_inventory_handler(self, proto: str) -> InventoryHandler:
        if proto in self._inventory_protocols:
            return self._inventory_protocols[proto]
        else:
            error(f'Handler for {proto} not found')
            return None

    def _stats_for_chain(self, chainid: str, result: dict):
        info(f'{chainid}: invoking handlers')
        result[chainid] = {}
        for handler in self._handlers[chainid]:
            poi = handler.get_stats()
            result[chainid].update(poi)
        
    def get_inventory(self) -> Dict[str, Dict[str, Union[UniswapLikeInventoryStats, BobVaultInventoryStats]]]:
        def task(chainid: str, result: dict):
            self._stats_for_chain(chainid, result)

        info(f'Getting inventory')
        ret = {}
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            inventory_futures = {executor.submit(task, chainid, ret): chainid for chainid in self._handlers}
            done = wait(inventory_futures, return_when = FIRST_EXCEPTION)[0]
            for f in done:
                ex = f.exception()
                if ex:
                    error(f'Not able to get inventory in {inventory_futures[f]}: {ex}')
                    return {}
        return ret
