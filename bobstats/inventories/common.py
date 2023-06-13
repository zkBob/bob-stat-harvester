from pydantic import BaseModel
from typing import List, Dict
from decimal import Decimal

from web3 import Web3

from bobstats.settings import Settings

from utils.web3 import Web3Provider, CachedERC20Token as ERC20Token
from utils.settings.models import UniswapLikeInventory
from utils.logging import error

def fields_as_list(model: BaseModel) -> List[str]:
    return list(model.schema()['properties'].keys())

class Position:
    pos_id: int
    token0_addr: str
    token1_addr: str
    fee: int
    token0_tvl: int = 0
    token1_tvl: int = 0
    token0_fees: int = 0 
    token1_fees: int = 0

class BaseInventoryStats(BaseModel):
    symbol: str
    tvl: Decimal
    fees: Decimal

class UniswapLikeInventoryStats(BaseModel):
    token0: BaseInventoryStats
    token1: BaseInventoryStats

class BobVaultInventoryStats(BaseModel):
    token0: BaseInventoryStats

class InventoryHolderStats(BaseModel):
    token0: BaseInventoryStats

class UniswapLikePositionsManager:
    postions: List[Position] = []
    fee_denominator: int = 0

    def inventory_stats(self) -> dict:
        pairs = {}
        for pos in self.postions:

            token0 = ERC20Token(self.w3prov, pos.token0_addr)
            token1 = ERC20Token(self.w3prov, pos.token1_addr)

            inventory_stats = UniswapLikeInventoryStats(
                token0 = BaseInventoryStats(
                    symbol = token0.symbol(),
                    tvl = token0.normalize(pos.token0_tvl),
                    fees = token0.normalize(pos.token0_fees)
                ),
                token1 = BaseInventoryStats(
                    symbol = token1.symbol(),
                    tvl = token1.normalize(pos.token1_tvl),
                    fees = token1.normalize(pos.token1_fees)
                )
            )

            key = f'{inventory_stats.token0.symbol}/{inventory_stats.token1.symbol}({pos.fee/self.fee_denominator})'
            pairs[key] = inventory_stats
        
        return pairs

class InventoryHandler:
    def get_stats(self) -> dict:
        return {}

    @classmethod
    def generate_handler(cls, w3: Web3Provider, params: UniswapLikeInventory, setting: Settings):
        return cls()

class UniswapLikeInventoryHandler(InventoryHandler):
    w3prov: Web3
    pm_addr: str
    owner: str

    def __init__(
        self,
        w3_provider: Web3Provider,
        position_manager: str,
        position_owner: str
    ):
        self.w3prov = w3_provider
        self.pm_addr = position_manager
        self.owner = position_owner

    @classmethod
    def generate_handler(cls, w3: Web3Provider, params: UniswapLikeInventory, _: Settings):
        pm = Web3.toChecksumAddress(params.pos_manager)
        ow = Web3.toChecksumAddress(params.owner)
        return cls(w3, pm, ow)

    def _get_stats(self, manager: UniswapLikePositionsManager) -> Dict[str, UniswapLikeInventoryStats]:
        inventory_stats = {}
        try:
            manager.get_postions()
        except Exception as e: 
            error(f'{self.w3prov.chainid}: not able to get positions: {e}')
        else:
            try:
                inventory_stats = manager.inventory_stats()
            except Exception as e: 
                error(f'{self.w3prov.chainid}: not able to prepare inventory stats: {e}')
        return inventory_stats