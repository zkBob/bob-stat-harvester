from typing import Dict, List, Union, Optional

from pydantic import BaseModel

class RPCSpec(BaseModel):
    url: str
    history_block_range: int

class TokenSpec(BaseModel):
    start_block: int

class UniswapLikeInventory(BaseModel):
    protocol: str
    pos_manager: str
    owner: str

class BobVaultInventory(BaseModel):
    protocol: str
    address: str
    start_block: int
    coingecko_poolid: str
    feeding_service_path: str
    feeding_service_health_container: str

class CoinGeckoMarkets(BaseModel):
    known: Optional[List[str]]
    exclude: Optional[List[str]]

class DepoloymentDescriptor(BaseModel):
    name: str
    finalization: int
    events_pull_interval: int
    rpc: RPCSpec
    token: TokenSpec
    inventories: Optional[List[Union[UniswapLikeInventory, BobVaultInventory]]]
    coingecko: Optional[CoinGeckoMarkets]

