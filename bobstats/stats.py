from typing import Dict, Union
from pydantic import BaseModel
from decimal import Decimal

from time import time, gmtime, strftime

from utils.logging import info, error

from .settings import Settings
from .supply import Supply
from .holders import Holders
from .inventory import Inventory
from .volume import Volume
from .interest import InterestStats, InterestsGenerators, aggregate_interest_stats
from .common import StatsByChains, ChainStats, GainStats, OneTokenAcc, YieldSet

from .inventories.common import UniswapLikeInventoryStats, BobVaultInventoryStats

BOB_TOKEN_SYMBOL = 'BOB'

class RawStatsData(BaseModel):
    supply: Dict[str, Decimal]
    holders: Dict[str, int]
    inventory: Dict[str, Dict[str, Union[UniswapLikeInventoryStats, BobVaultInventoryStats]]]
    volume: Dict[str, Decimal]
    interest: Dict[str, InterestsGenerators]

class Stats:
    _supply: Supply
    _holders: Holders
    _inventory: Inventory
    _volume: Volume
    _interest: InterestStats
    _chain_names: Dict[str, str]

    def __init__(self, settings: Settings):
        self._supply = Supply(settings)
        self._holders = Holders(settings)
        self._inventory = Inventory(settings)
        self._volume = Volume(settings)
        self._interest = InterestStats(settings)

        self._chain_names = {}
        for chainid in settings.chains:
            self._chain_names[chainid] = settings.chains[chainid].name

    def _collect(self) -> RawStatsData:
        ts = self._supply.get_total_supply()
        inv = self._inventory.get_inventory() if len(ts) != 0 else {}
        hldrs = self._holders.get_bob_holders_amount() if len(inv) != 0 else {}
        vol = self._volume.get_volume() if len(hldrs) != 0 else {}
        intrs = self._interest.get_interest() if len(vol) != 0 else {}

        if (len(ts) == 0) or \
           (len(inv) == 0) or \
           (len(hldrs) == 0) or \
           (len(vol) == 0):
            return None

        return RawStatsData(supply=ts, holders=hldrs, inventory=inv, volume=vol, interest=intrs)

    def generate(self, timestamp = None) -> StatsByChains:
        raw_data = self._collect()
        if not raw_data:
            return []

        if not timestamp:
            timestamp = int(time())
        info(f"Data timesmap: {strftime('%Y-%m-%d %H:%M:%S', gmtime(timestamp))}")
        dat = []
        for c in self._chain_names:
            if (c in raw_data.inventory) and (c in raw_data.supply):

                unused_supply = 0
                all_inv_fees = {}
                inv_on_chain = raw_data.inventory[c]
                for inv_holder in inv_on_chain:
                    for (_, token) in iter(inv_on_chain[inv_holder]):
                        if token.symbol == BOB_TOKEN_SYMBOL:
                            unused_supply += token.tvl
                        if token.symbol in all_inv_fees:
                            all_inv_fees[token.symbol] += token.fees
                        else:
                            all_inv_fees[token.symbol] = token.fees
                d = ChainStats(
                    dt = timestamp,
                    chain = self._chain_names[c],
                    totalSupply = raw_data.supply[c],
                    colCirculatingSupply = raw_data.supply[c] - unused_supply,
                    volumeUSD = raw_data.volume[c] if c in raw_data.volume else 0,
                    holders = raw_data.holders[c] if c in raw_data.holders else 0,
                    gain = GainStats(
                        fees=[OneTokenAcc(
                            symbol=t,
                            amount=all_inv_fees[t]
                        ) for t in all_inv_fees],
                        interest=aggregate_interest_stats(raw_data.interest[c]) if raw_data.interest else None
                    )
                )
                info(f'Stats for chain {d.dict()}')
                dat.append(d)
            else:
                error(f'No data for "{c}"')
        return dat
