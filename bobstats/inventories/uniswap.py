from functools import cache
from typing import Dict
from pydantic import BaseModel

from time import time

from web3.eth import Contract

from utils.abi import get_abi, ABI
from utils.web3 import Web3Provider
from utils.logging import info, debug
from utils.constants import ONE_DAY, MAX_INT

from .common import fields_as_list, Position, UniswapLikePositionsManager, \
                    UniswapLikeInventoryHandler, UniswapLikeInventoryStats

class UniswapV3PositionRaw(BaseModel):
    nonce: int
    operator: str
    token0: str
    token1: str
    fee: int
    tickLower: int
    tickUpper: int
    liquidity: int
    feeGrowthInside0LastX128: int
    feeGrowthInside1LastX128: int
    tokensOwed0: int
    tokensOwed1: int

class UniswapV3Position(Position):

    def __init__(self, w3prov: Web3Provider, pos_owner: str, pos_manager: Contract, idx: int):
        def get_postion_raw_details():
            position_details = w3prov.make_call(pos_manager.functions.positions(self.pos_id).call)
            raw_details = UniswapV3PositionRaw.parse_obj(dict(zip(fields_as_list(UniswapV3PositionRaw),
                                                                  position_details
                                                                 )
                                                             )
                                                        )
            self.token0_addr = raw_details.token0
            self.token1_addr = raw_details.token1
            self.liquidity = raw_details.liquidity
            self.fee = raw_details.fee
            info(f'{w3prov.chainid}/{self.pos_id}: pair: {self.token0_addr}/{self.token1_addr}, liquidity {self.liquidity}')
            debug(f'{w3prov.chainid}/{self.pos_id}: pair: {raw_details}')

        def get_tvl_for_postion():
            params = {
                "tokenId": self.pos_id,
                "liquidity": self.liquidity,
                "amount0Min": 0,
                "amount1Min": 0,
                "deadline": int(time())+ ONE_DAY
            }
            tvl_for_pair = w3prov.make_call(pos_manager.functions.decreaseLiquidity(params).call)
            self.token0_tvl = tvl_for_pair[0]
            self.token1_tvl = tvl_for_pair[1]
            info(f'{w3prov.chainid}/{self.pos_id}: pair: tvl: {self.token0_tvl, self.token1_tvl}')

        def get_fees_for_postion():
            params = {"tokenId": self.pos_id,
                    "recipient": pos_owner,
                    "amount0Max": MAX_INT,
                    "amount1Max": MAX_INT
                    }
            fees_for_pair = w3prov.make_call(pos_manager.functions.collect(params).call)
            self.token0_fees = fees_for_pair[0]
            self.token1_fees = fees_for_pair[1]
            info(f'{w3prov.chainid}/{self.pos_id}: pair: fees: {self.token0_fees, self.token1_fees}')

        self.pos_id = w3prov.make_call(pos_manager.functions.tokenOfOwnerByIndex(pos_owner, idx).call)
        info(f'{w3prov.chainid}: intialising position {self.pos_id}')
        get_postion_raw_details()
        if self.liquidity != 0:
            get_tvl_for_postion()
            get_fees_for_postion()
        else:
            info(f'{w3prov.chainid}: position {self.pos_id} does not contain liquidity')

@cache
class UniswapV3PositionsManager(UniswapLikePositionsManager):

    def __init__(self,
        w3_provider: Web3Provider,
        position_manager: str,
        position_owner: str
    ):
        self.w3prov = w3_provider
        self.owner = position_owner
        self.pm = w3_provider.w3.eth.contract(abi = get_abi(ABI.UNIV3_PM), address = position_manager)
        self.fee_denominator = 10000

    def get_postions(self):
        info(f'{self.w3prov.chainid}: getting UniSwapV3 positions for owner {self.owner}')

        pos_num = self.w3prov.make_call(self.pm.functions.balanceOf(self.owner).call)

        info(f'{self.w3prov.chainid}: found {pos_num} positions')

        self.postions = []
        for i in range(pos_num):
            pos = UniswapV3Position(self.w3prov, self.owner, self.pm, i)
            if pos.liquidity == 0:
                continue
            self.postions.append(pos)

class UniswapInventoryHandler(UniswapLikeInventoryHandler):
    def get_stats(self) -> Dict[str, UniswapLikeInventoryStats]:
        manager = UniswapV3PositionsManager(self.w3prov, self.pm_addr, self.owner)
        return self._get_stats(manager)
