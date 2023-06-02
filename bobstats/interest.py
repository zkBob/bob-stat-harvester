from functools import lru_cache

from decimal import Decimal
from typing import Dict, Union
from dataclasses import dataclass

from time import time

from json import load

from bobvault.contract import BobVaultContract

from .settings import Settings
from .inventories.bobvault import DBAdapter
from .common import OneTokenAcc, YieldSet

from utils.settings.utils import discover_bobvault_inventory
from utils.settings.models import BobVaultInventory
from utils.logging import info, error
from utils.constants import BOB_TOKEN_ADDRESS, ZERO_ADDRESS, ONE_DAY
from utils.web3 import ERC20Token, Web3Provider

InterestsGenerators = Dict[str, YieldSet]

@dataclass(frozen=True)
class VaultForCollateralStats:
    registrar_fn: str
    poolid: str
    bv_contract: BobVaultContract
    w3prov: Web3Provider
    db: DBAdapter

def _get_bobvault_tokens(fn: str, log_prefix: str) -> Dict[str, str]:
    try:
        with open(fn, 'r') as json_file:
            return load(json_file)
    except IOError as e:
        error(f'{log_prefix}: tokens registry {fn} not found: {e}')
        return {}

@lru_cache(maxsize=1)
def _get_fees(
    dba: DBAdapter,
    required_ts: int,
    discovery_step: int,
    log_prefix: str
) -> Dict[str, float]:
    deadline = required_ts - ONE_DAY
    dba.discover_time_of_latest_point(discovery_step)
    fees = dba.discover_latest_point()
    if fees:
        dt = fees['dt']
        del fees['dt']
        del fees['id']
        if dt >= deadline:
            info(f'{log_prefix}: discovered fees {fees}')
        else:
            error(f'{log_prefix}: discovered fees are too old')
            fees = {}
    else:
        error(f'{log_prefix}: no fees discovered')
        fees = {}
    return fees

def _get_fees_for_token(fees: Dict[str, float], symbol: str) -> Union[Decimal, None]:
    if symbol in fees:
        return Decimal(str(fees[symbol]))
    else:
        return None
    
def aggregate_interest_stats(genrs: InterestsGenerators) -> Union[YieldSet, None]:
    retval = {}
    for el in genrs:
        for t in genrs[el]:
            if t.symbol in retval:
                retval[t.symbol] += t.amount
            else:
                retval[t.symbol] = t.amount
    if len(retval) > 0:
        return [OneTokenAcc(symbol=t, amount=retval[t]) for t in retval]
    else:
        return None

class InterestStats:
    _vaults: Dict[str, Dict[str, Union[BobVaultContract, str]]]
    _discovery_step: int

    def __init__(self, settings: Settings):
        self._vaults = {}
        info(f'{settings.chains.keys()}')
        for chainid in settings.chains:

            def inventory_setup(inv: BobVaultInventory):
                poolid = inv.coingecko_poolid
                self._vaults[chainid] = VaultForCollateralStats(
                    registrar_fn=f'{settings.snapshot_dir}/{poolid}-{settings.bobvault_registrar_file_suffix}',
                    poolid=poolid,
                    bv_contract=BobVaultContract(
                        settings.w3_providers[chainid],
                        inv.address,
                        inv.start_block,
                        settings.chains[chainid].rpc.history_block_range
                    ),
                    w3prov=settings.w3_providers[chainid],
                    db=DBAdapter(poolid, f'{settings.tsdb_dir}/{poolid}-{settings.bobvault_fees_db_suffix}')
                )

            self._discovery_step = settings.measurements_interval
            discover_bobvault_inventory(settings.chains[chainid].inventories, inventory_setup)

    def get_interest(self) -> Dict[str, InterestsGenerators]:
        retval = {}
        curtime = int(time())

        for ch in self._vaults:
            poolid = self._vaults[ch].poolid
            retval[ch] = {poolid: []}
            bv_contract = self._vaults[ch].bv_contract
            dba = self._vaults[ch].db
            w3prov = self._vaults[ch].w3prov
            log_prefix = f'interest:{ch}'

            tokens = _get_bobvault_tokens(self._vaults[ch].registrar_fn, log_prefix)
            
            for t in tokens:
                symbol = tokens[t]
                if t != BOB_TOKEN_ADDRESS:
                    if bv_contract.get_collateral(t).yield_addr != ZERO_ADDRESS:
                        farmed = bv_contract.get_stat(t).farmed
                        if farmed > 0:
                            fees = _get_fees(dba, curtime, self._discovery_step, log_prefix)
                            # it is possible to get interest only if fees amount is known
                            if len(fees) > 0:
                                token_fees = _get_fees_for_token(fees, symbol)
                                if token_fees:
                                    interest = ERC20Token(w3prov, t).normalize(farmed) - token_fees
                                    info(f'{log_prefix}: {symbol}: {interest}')
                                    retval[ch][poolid].append(OneTokenAcc(
                                        symbol=symbol,
                                        amount=interest
                                    ))
                                else:
                                    error(f'{log_prefix}: no fees found for {symbol}')
        return retval