from typing import Dict
from decimal import Decimal

from web3 import Web3

from utils.web3 import Web3Provider, ERC20Token
from utils.settings.models import BobVaultInventory
from utils.constants import BOB_TOKEN_ADDRESS
from utils.logging import info, error

from .common import BaseInventoryStats, InventoryHandler, BobVaultInventoryStats

class BobVaultInventoryHandler(InventoryHandler):
    w3prov: Web3
    vault_addr: str

    def __init__(
        self,
        w3_provider: Web3Provider,
        vault_addr: str
    ):
        self.w3prov = w3_provider
        self.vault_addr = vault_addr

    @classmethod
    def generate_handler(cls, w3: Web3Provider, params: BobVaultInventory):
        vault_addr = Web3.toChecksumAddress(params.address)
        return cls(w3, vault_addr)

    def get_stats(self) -> Dict[str, BobVaultInventoryStats]:
        info(f'{self.w3prov.chainid}: getting BobVault info')
        inventory_stats = {}

        bobtoken = ERC20Token(self.w3prov, BOB_TOKEN_ADDRESS)
        try:
            tvl = bobtoken.balanceOf(self.vault_addr)
        except:
            error(f'{self.w3prov.chainid}: not able to get data')
        else:
            info(f'{self.w3prov.chainid}: tvl: {tvl}')
            inventory_stats['BOB_on_BobVault'] = BobVaultInventoryStats(
                token0 = BaseInventoryStats(
                    symbol = bobtoken.symbol(),
                    tvl = tvl,
                    fees = Decimal(0)
                )
            )

        return inventory_stats
