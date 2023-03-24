from functools import cache
from typing import Dict
from pydantic import BaseModel

from time import time

from web3.eth import Contract

from utils.abi import get_abi, ABI
from utils.web3 import Web3Provider
from utils.logging import info, debug, error
from utils.constants import ONE_DAY, TWO_POW_96

from .common import fields_as_list, Position, UniswapLikePositionsManager, \
                    UniswapLikeInventoryHandler, UniswapLikeInventoryStats

class KyberswapElastisPoolPair(BaseModel):
    token0: str
    fee: int
    token1: str

class KyberswapElasticPool(BaseModel):
    nonce: int
    operator: str
    poolId: int
    tickLower: int
    tickUpper: int
    liquidity: int
    rTokenOwed: int
    feeGrowthInsideLast: int

class KyberswapElasticPosition(Position):

    def __init__(self, w3prov: Web3Provider, pos_owner: str, pos_manager: Contract, idx: int):
        
        @cache
        def get_pool_contract():
            factory_addr = w3prov.make_call(pos_manager.functions.factory().call)
            factory = w3prov.w3.eth.contract(abi = get_abi(ABI.KYBERSWAP_FACTORY), address = factory_addr)
            pool_addr = w3prov.make_call(factory.functions.getPool(
                self.token0_addr, 
                self.token1_addr, 
                self.fee
            ).call)
            return w3prov.w3.eth.contract(abi = get_abi(ABI.KYBERSWAP_POOL), address = pool_addr)

        def get_postion_raw_details():
            position_details = w3prov.make_call(pos_manager.functions.positions(self.pos_id).call)
            raw_pair = KyberswapElastisPoolPair.parse_obj(dict(zip(fields_as_list(KyberswapElastisPoolPair),
                                                                   position_details[1]
                                                                  )
                                                              )
                                                         )
            raw_details = KyberswapElasticPool.parse_obj(dict(zip(fields_as_list(KyberswapElasticPool),
                                                                  position_details[0]
                                                                 )
                                                             )
                                                        )
            self.token0_addr = raw_pair.token0
            self.token1_addr = raw_pair.token1
            self.liquidity = raw_details.liquidity
            self.rTokenOwed = raw_details.rTokenOwed
            self.fee = raw_pair.fee
            info(f'{w3prov.chainid}/{self.pos_id}: pair: {self.token0_addr}/{self.token1_addr}, liquidity {self.liquidity}')
            debug(f'{w3prov.chainid}/{self.pos_id}: pair: {raw_details}, {raw_pair}')

        def get_tvl_and_fees_for_postion():
            params = {
                "tokenId": self.pos_id,
                "liquidity": self.liquidity,
                "amount0Min": 0,
                "amount1Min": 0,
                "deadline": int(time())+ ONE_DAY
            }
            removeLiquidity_encoded = pos_manager.encodeABI(fn_name="removeLiquidity", args=[params])
            del params["liquidity"]
            burnRTokens_encoded = pos_manager.encodeABI(fn_name="burnRTokens", args=[params])

            mc_retvall = w3prov.make_call(pos_manager.functions.multicall([
                removeLiquidity_encoded,
                burnRTokens_encoded
            ]).call)
            if len(mc_retvall) != 2:
                error(f"{w3prov.chainid}/{self.pos_id}: KyberSwap's multicall returned unexpected value")
                BaseException(f"KyberSwap's multicall returned unexpected value")

            tvl_for_pair = w3prov.w3.codec.decode(["(uint256,uint256,uint256)"], mc_retvall[0])[0]
            fees_for_pair = w3prov.w3.codec.decode(["(uint256,uint256,uint256)"], mc_retvall[1])[0]

            self.token0_tvl = tvl_for_pair[0]
            self.token1_tvl = tvl_for_pair[1]
            info(f'{w3prov.chainid}/{self.pos_id}: pair: tvl: {self.token0_tvl, self.token1_tvl}')
            self.token0_fees = fees_for_pair[1]
            self.token1_fees = fees_for_pair[2]
            info(f'{w3prov.chainid}/{self.pos_id}: pair: fees: {self.token0_fees, self.token1_fees}')

        self.pos_id = w3prov.make_call(pos_manager.functions.tokenOfOwnerByIndex(pos_owner, idx).call)
        info(f'{w3prov.chainid}: intialising position {self.pos_id}')
        get_postion_raw_details()
        if self.liquidity != 0:
            get_tvl_and_fees_for_postion()
        else:
            info(f'{w3prov.chainid}: position {self.pos_id} does not contain liquidity')

@cache
class KyberswapElasticPositionsManager(UniswapLikePositionsManager):

    def __init__(self,
        w3_provider: Web3Provider,
        position_manager: str,
        position_owner: str
    ):
        self.w3prov = w3_provider
        self.owner = position_owner
        self.pm = w3_provider.w3.eth.contract(abi = get_abi(ABI.KYBERSWAP_PM), address = position_manager)
        self.fee_denominator = 1000

    def get_postions(self):
        info(f'{self.w3prov.chainid}: getting KyberSwap Elastic positions for owner {self.owner}')

        pos_num = self.w3prov.make_call(self.pm.functions.balanceOf(self.owner).call)

        info(f'{self.w3prov.chainid}: found {pos_num} positions')

        self.postions = []
        for i in range(pos_num):
            pos = KyberswapElasticPosition(self.w3prov, self.owner, self.pm, i)
            if pos.liquidity == 0:
                continue
            self.postions.append(pos)

class KyberswapElasticInventoryHandler(UniswapLikeInventoryHandler):
    def get_stats(self) -> Dict[str, UniswapLikeInventoryStats]:
        manager = KyberswapElasticPositionsManager(self.w3prov, self.pm_addr, self.owner)
        return self._get_stats(manager)
