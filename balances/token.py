from functools import cache

from web3 import Web3

from utils.web3 import ERC20Token
from utils.logging import info
from utils.constants import BOB_TOKEN_ADDRESS

from .web3 import Web3ProviderExt

@cache
class BobTokenContract(ERC20Token):
    _transfer_filter: any

    def __init__(self, w3_provider: Web3ProviderExt):
        super().__init__(w3_provider, BOB_TOKEN_ADDRESS)
        self._transfer_filter = self.contract.events.Transfer.build_filter()

    def process_transfer_log(self, _event):
        pl = self.contract.events.Transfer().processLog(_event)
        blockhash = Web3.toHex(pl.blockHash)
        l = { 
            'tags': {
                "logIndex": str(pl.logIndex),
                "transactionIndex": str(pl.transactionIndex),
                "transactionHash": Web3.toHex(pl.transactionHash),
                "blockHash": blockhash,
                "blockNumber": str(pl.blockNumber),
                "from": pl.args['from'],
                "to": pl.args['to']
            },
            'fields': {
                'value': pl.args['value']
            },
            'methods': {
                'denominate': self.normalize
            }
        }
        l['timestamp'] = self.w3_provider.get_timestamp_by_blockhash(blockhash)
        return l

    def get_transfer_logs(self, _from_block, _to_block):
        raw_logs, finish_block = self.w3_provider.get_logs(
            _from_block,
            _to_block,
            self._transfer_filter.address, 
            self._transfer_filter.topics
        )
        len_raw_logs = len(raw_logs)
        info(f"{self.w3_provider.chainid}: Found {len_raw_logs} of {self._transfer_filter.event_abi['name']} events")
        if len_raw_logs > 0:
            logs = [self.process_transfer_log(e) for e in raw_logs]
        else:
            logs = []
            
        return finish_block, logs