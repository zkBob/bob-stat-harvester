from decimal import Decimal
from typing import List

from pydantic import BaseModel

class TradeArgs(BaseModel):
    inToken: str
    outToken: str
    amountIn: Decimal
    amountOut: Decimal

class BobVaultTrade(BaseModel):
    name: str
    args: TradeArgs
    logIndex: int
    transactionIndex: int
    transactionHash: str
    blockHash: str
    blockNumber: int
    timestamp: int

class BobVaultTradesSnapshot(BaseModel):
    start_block: int = 0
    last_block: int = -1
    logs: List[BobVaultTrade] = []

class BobVaultCollateral(BaseModel):
    balance: int
    buffer: int
    dust: int
    yield_addr: str
    price: int
    inFee: int
    outFee: int
