from functools import cache 
from json import load

from enum import Enum

from .logging import info, error
from .constants import ABI_DIR

class ABI(Enum):
    ERC20 = "erc20.json"
    UNIV3_PM = "uniswapv3_pm.json"
    KYBERSWAP_PM = "kyberswap_elastic_pm.json"
    KYBERSWAP_FACTORY = "kyberswap_elastic_factory.json"
    KYBERSWAP_POOL = "kyberswap_elastic_pool.json"
    BOBVAULT = "bobvault.json"

@cache
def get_abi(fname: ABI) -> dict:
    full_fname = f'{ABI_DIR}/{fname.value}'
    try:
        with open(full_fname) as f:
            abi = load(f)
    except IOError as e:
        error(f'Cannot read {full_fname}')
        raise e
    info(f'{full_fname} loaded')
    return abi

for i in ABI:
    get_abi(i)