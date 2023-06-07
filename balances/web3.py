from functools import cache
from typing import Tuple

from utils.web3 import Web3Provider
from utils.logging import info, debug

class Web3ProviderExt(Web3Provider):
    _finalization_delay: int
    _block_range: int

    def __init__(
        self,
        chainid: str,
        url: str,
        retry_attemtps: int,
        retry_delay: int,
        finalization_delay: int,
        block_range_limit: int
    ):
        super().__init__(chainid, url, retry_attemtps, retry_delay)
        self._finalization_delay = finalization_delay
        self._block_range = block_range_limit

    def get_logs(self, block_from: int, block_to: int, emitter: str, topics: list) -> Tuple[list, int]:
        info(f'{self.chainid}: Assuming to look for logs within [{block_from}, {block_to}]')
        start_block = block_from
        finish_block = min(start_block + self._block_range, block_to)
        if finish_block != block_to:
            info(f'{self.chainid}: Looking for logs within a smaler range [{start_block}, {finish_block}]')
        logs = self.make_call(
            self.w3.eth.getLogs,
            {
                'fromBlock': start_block, 
                'toBlock': finish_block, 
                'address': emitter, 
                'topics': topics
            }
        )
        return logs, finish_block

    @cache
    def get_timestamp_by_blockhash(self, blockhash: str) -> int:
        debug(f'Getting timestamp for {blockhash}')
        resp = self.make_call(self.w3.eth.get_block, blockhash).timestamp
        debug(f'Timestamp {resp}')
        return resp
    
    def get_finalized_block(self) -> int:
        latest_block = self.make_call(self.w3.eth.get_block, 'latest').number
        return latest_block - self._finalization_delay
