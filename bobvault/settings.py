from typing import Dict, List, Union, Callable

from utils.settings.feeding import FeedingServiceSettings
from utils.settings.models import InventoriesList
from utils.logging import info
from utils.web3 import Web3Provider

def discover_inventory(inventories: InventoriesList, func: Callable):
    bobvault_found = False
    for inv in inventories:
        if inv.protocol == "BobVault":
            func(inv)
            bobvault_found = True
            break
    return bobvault_found

class Settings(FeedingServiceSettings):
    chain_selector: str = 'pol'
    snapshot_dir: str = '.'
    snapshot_file_suffix: str = 'bobvault-snaphsot.json'
    coingecko_file_suffix: str = 'bobvault-coingecko-data.json'
    tsdb_dir: str = '.'
    fees_stat_db_suffix: str = 'bobvault-fees.csv'
    w3_providers: dict = {}
    max_workers: int = 5

    def __init__(self):
        def init_w3_providers():
            # this source cannot be used through Config::customise_sources approach
            # since chains is not filled at the moment applying the sources
            info(f'Init web3 providers')
            for chainid in self.chains:
                self.w3_providers[chainid] = Web3Provider(
                    chainid,
                    self.chains[chainid].rpc.url,
                    self.web3_retry_attemtps,
                    self.web3_retry_delay
                )

        def web3_providers_formatter(w3_providers: Dict[str, Web3Provider]) -> str:
            providers_to_str = {}
            for c in w3_providers:
                providers_to_str[c] = w3_providers[c].w3.provider.endpoint_uri

            return str(providers_to_str)
        
        super().__init__()

        self.extend_extra_sources(init_w3_providers)
        self.extend_formatters({'w3_providers': web3_providers_formatter})