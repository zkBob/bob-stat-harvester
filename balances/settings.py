from typing import Dict

from utils.settings.common import CommonSettings
from utils.logging import info
from .web3 import Web3ProviderExt

class Settings(CommonSettings):
    snapshot_dir: str = '.'
    snapshot_file_suffix: str = 'bob-holders-snaphsot.json'
    tsdb_dir: str = '.'
    tsdb_file_suffix: str = 'bob-transfers.csv'
    default_measurements_interval: int = 5
    threads_liveness_interval: int = 60
    w3_providers: dict = {}

    def __init__(self):
        def init_w3_providers():
            # this source cannot be used through Config::customise_sources approach
            # since chains is not filled at the moment applying the sources
            info(f'Init web3 providers')
            for chainid in self.chains:
                self.w3_providers[chainid] = Web3ProviderExt(
                    chainid,
                    self.chains[chainid].rpc.url,
                    self.web3_retry_attemtps,
                    self.web3_retry_delay,
                    self.chains[chainid].finalization,
                    self.chains[chainid].rpc.history_block_range
                )

        def web3_providers_formatter(w3_providers: Dict[str, Web3ProviderExt]) -> str:
            providers_to_str = {}
            for c in w3_providers:
                providers_to_str[c] = w3_providers[c].w3.provider.endpoint_uri

            return str(providers_to_str)
        
        super().__init__()

        self.extend_extra_sources(init_w3_providers)
        self.extend_formatters({'w3_providers': web3_providers_formatter})