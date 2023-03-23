from functools import cache
from typing import Dict

from pydantic import BaseSettings
from pydantic.utils import GetterDict

from typing import Any

from json import load

from ..logging import info, error

from .models import DepoloymentDescriptor
from ..constants import BOB_TOKEN_ADDRESS

class GenericSettings(BaseSettings):
    _comma_separated_params = []
    _extra_sources = []
    _formatters = {}

    @classmethod
    def get_comma_separated_params(cls) -> list:
        return cls._comma_separated_params

    def extend_comma_separated_params(self, params: list):
        self._comma_separated_params.extend(params)

    def extend_extra_sources(self, source):
        self._extra_sources.append(source)

    def extend_formatters(self, formatter: dict):
        self._formatters.update(formatter)

    @classmethod
    @cache
    def get(cls):
        settings = cls()

        # init of fields that cannot be filled by Config::customise_sources approach
        for fn in cls._extra_sources:
            fn()

        for (key, value) in settings:
            to_out = value
            if key in cls._formatters:
                to_out = cls._formatters[key](value)
            info(f'{key.upper()} = {to_out}')

        return settings

    class Config:
        env_file = ".env"
        getter_dict = GetterDict

        @classmethod
        def parse_env_var(cls, field_name: str, raw_val: str) -> Any:
            if field_name in GenericSettings.get_comma_separated_params():
                return [x for x in raw_val.split(',')]
            return cls.json_loads(raw_val)

class CommonSettings(GenericSettings):
    token_address: str = BOB_TOKEN_ADDRESS
    token_depoloyments_info: str = 'token-deployments-info.json'
    measurements_interval: int = 60 * 60 * 2 - 30
    snapshot_dir: str = '.'
    bobvault_snapshot_file_suffix: str = 'bobvault-snaphsot.json'
    balances_snapshot_file_suffix: str = 'bob-holders-snaphsot.json'
    tsdb_dir: str = '.'
    web3_retry_attemtps: int = 2
    web3_retry_delay: int = 5
    chains: dict = {}

    def __init__(self):
        def deployments_spec_init():
            # this source cannot be used through Config::customise_sources approach
            # since token_depoloyments_info can be default at the moment of the source applying
            fname = self.token_depoloyments_info
            info(f'Load deployments info from {fname}')

            try:
                with open(fname) as f:
                    chains = load(f)['chains']
            except IOError as e:
                error(f'Cannot read deployments info')
                raise e

            for cid in chains:
                self.chains[cid] = DepoloymentDescriptor.parse_obj(chains[cid])

        def chains_formatter(chains: Dict[str, DepoloymentDescriptor]) -> str:
            chains_to_str = []
            for c in chains:
                chains_to_str.append(chains[c].name)

            return str(chains_to_str)

        super().__init__()

        self.extend_extra_sources(deployments_spec_init)
        self.extend_formatters({'chains': chains_formatter})
