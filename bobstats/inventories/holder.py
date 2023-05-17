from typing import Dict
from decimal import Decimal

from web3 import Web3

from utils.web3 import Web3Provider, ERC20Token
from utils.settings.models import InventoryHolder
from utils.constants import BOB_TOKEN_ADDRESS
from utils.logging import info, error

from .common import BaseInventoryStats, InventoryHandler, InventoryHolderStats

class InventoryHolderHandler(InventoryHandler):
    w3prov: Web3
    holder_addr: str

    def __init__(
        self,
        w3_provider: Web3Provider,
        holder_addr: str
    ):
        self.w3prov = w3_provider
        self.holder_addr = holder_addr

    @classmethod
    def generate_handler(cls, w3: Web3Provider, params: InventoryHolder):
        holder_addr = Web3.toChecksumAddress(params.address)
        return cls(w3, holder_addr)

    def get_stats(self) -> Dict[str, InventoryHolderStats]:
        info(f'{self.w3prov.chainid}: getting info for Holder')
        inventory_stats = {}

        bobtoken = ERC20Token(self.w3prov, BOB_TOKEN_ADDRESS)
        try:
            tvl = bobtoken.balanceOf(self.holder_addr)
        except:
            error(f'{self.w3prov.chainid}: not able to get data')
        else:
            info(f'{self.w3prov.chainid}: tvl: {tvl}')
            inventory_stats[f'BOB_on_{self.holder_addr[:6]}'] = InventoryHolderStats(
                token0 = BaseInventoryStats(
                    symbol = bobtoken.symbol(),
                    tvl = tvl,
                    fees = Decimal(0)
                )
            )

        return inventory_stats
