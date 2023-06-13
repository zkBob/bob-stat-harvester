from typing import Dict
from decimal import Decimal

from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION

from .settings import Settings

from utils.logging import info, error
from utils.web3 import CachedERC20Token as ERC20Token, Web3Provider
from utils.constants import BOB_TOKEN_ADDRESS

class Supply:

    def __init__(self, settings: Settings):
        self._w3_providers = settings.w3_providers
        self._chainids = list(settings.chains.keys())
        self._max_workers = settings.max_workers

    def get_total_supply(self) -> Dict[str, Decimal]:
        def task(w3_providers: Web3Provider, result: dict):
            token = ERC20Token(w3_providers, BOB_TOKEN_ADDRESS)
            result[w3_providers.chainid] = token.totalSupply()

        info(f'Getting total supply for {BOB_TOKEN_ADDRESS}')
        ret = {}
        with ThreadPoolExecutor(max_workers=min(len(self._chainids), self._max_workers)) as executor:
            supply_futures = {executor.submit(task, self._w3_providers[chainid], ret): chainid for chainid in self._chainids}
            done = wait(supply_futures, return_when = FIRST_EXCEPTION)[0]
            for f in done:
                if f.exception():
                    error(f'Not able to get total supply in {supply_futures[f]}')
                    return {}
        return ret