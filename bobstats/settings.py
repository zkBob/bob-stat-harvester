from typing import Dict

from utils.settings.feeding import FeedingServiceSettings
from utils.logging import info, error
from utils.web3 import Web3Provider

class Settings(FeedingServiceSettings):
    update_bigquery: bool = True
    bigquery_auth_json_key: str = 'bigquery-key.json'
    bigquery_project: str = 'some-project'
    bigquery_dataset: str = 'some-dashboard'
    bigquery_table: str = 'some-table'
    feeding_service_path: str = '/'
    snapshot_dir: str = '.'
    bobvault_snapshot_file_suffix: str = 'bobvault-snaphsot.json'
    balances_snapshot_file_suffix: str = 'bob-holders-snaphsot.json'
    coingecko_retry_attempts: int = 2
    coingecko_retry_delay: int = 5
    coingecko_include_anomalies: bool = True
    max_workers: int = 5
    tsdb_dir: str = '.'
    bob_composed_stat_db: str = 'bobstat_composed.csv'
    bob_composed_fees_stat_db: str = 'bobstat_comp_yield.csv'
    bobvault_fees_db_suffix: str = 'bobvault-fees.csv'
    w3_providers: dict = {}

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