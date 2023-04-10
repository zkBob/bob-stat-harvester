from functools import cache

from web3 import Web3
from web3.eth import Contract

from utils.logging import info
from utils.web3 import Web3Provider, ERC20Token
from utils.abi import get_abi, ABI
from utils.constants import BOB_TOKEN_ADDRESS

class BobVaultContract:
    start_block: int
    _w3prov: Web3Provider
    _contract: Contract
    _block_range: int

    def __init__(self, w3_provider: Web3Provider, address: str, start_block: int, block_range: int):
        self._w3prov = w3_provider
        self._contract = w3_provider.w3.eth.contract(
            abi = get_abi(ABI.BOBVAULT),
            address = Web3.toChecksumAddress(address)
        )
        self.start_block = start_block
        self._block_range = block_range
    
    @cache
    def _get_filters(self) -> list:
        return [
            self._contract.events.Buy.build_filter(),
            self._contract.events.Sell.build_filter(),
            self._contract.events.Swap.build_filter()
        ]

    @cache
    def _get_event_decoders(self) -> dict:
        def buy_decoder(l):
            pl = self._contract.events.Buy().processLog(l)
            return pl, {
                        "inToken": pl.args.token,
                        "outToken": BOB_TOKEN_ADDRESS,
                        "amountIn": ERC20Token(self._w3prov, pl.args.token).normalize(pl.args.amountIn),
                        "amountOut": ERC20Token(self._w3prov, BOB_TOKEN_ADDRESS).normalize(pl.args.amountOut)
                    }
        def sell_decoder(l):
            pl = self._contract.events.Sell().processLog(l)
            return pl, {
                        "inToken": BOB_TOKEN_ADDRESS,
                        "outToken": pl.args.token,
                        "amountIn": ERC20Token(self._w3prov, BOB_TOKEN_ADDRESS).normalize(pl.args.amountIn),
                        "amountOut": ERC20Token(self._w3prov, pl.args.token).normalize(pl.args.amountOut)
                    }
        def swap_decoder(l):
            pl = self._contract.events.Swap().processLog(l)
            return pl, {
                        "inToken": pl.args.inToken,
                        "outToken": pl.args.outToken,
                        "amountIn": ERC20Token(self._w3prov, pl.args.inToken).normalize(pl.args.amountIn),
                        "amountOut": ERC20Token(self._w3prov, pl.args.outToken).normalize(pl.args.amountOut)
                    }

        efilters = self._get_filters()
        buyTopic = efilters[0].event_topic
        sellTopic = efilters[1].event_topic
        swapTopic = efilters[2].event_topic
        return {
            buyTopic: (buy_decoder, 'Buy'),
            sellTopic: (sell_decoder, 'Sell'),
            swapTopic: (swap_decoder, 'Swap'),
        }

    def process_log(self, log_rec) -> dict:
        with_events = self._get_event_decoders()
        event_topic = log_rec.topics[0]
        event_handler = with_events[event_topic][0]
        event_name = with_events[event_topic][1]
        pl, pl_args = event_handler(log_rec)
        blockhash = Web3.toHex(pl.blockHash)
        l = {
            "name": event_name,
            "args": pl_args,
            "logIndex": pl.logIndex,
            "transactionIndex": pl.transactionIndex,
            "transactionHash": Web3.toHex(pl.transactionHash),
            "blockHash": blockhash,
            "blockNumber": pl.blockNumber
        }
        l["timestamp"] = self._w3prov.make_call(self._w3prov.w3.eth.get_block, blockhash).timestamp
        if not "timestamp" in l:
            raise BaseException(f'Timestamp cannot be set for block {blockhash}')
        return l

    def get_logs_for_range(self, from_block, to_block) -> dict:
        info(f'Looking for events within [{from_block}, {to_block}]')
        logs = []
        for b in range(from_block, to_block, self._block_range + 1):
            start_block = b
            finish_block = min(b + self._block_range, to_block)
            if (from_block != start_block) or (to_block != finish_block):
                info(f'Looking for events within a smaler range [{start_block}, {finish_block}]')
            vault_logs = []
            for efilter in self._get_filters():
                bss_logs = self._w3prov.make_call(
                    self._w3prov.w3.eth.getLogs,
                    {
                        'fromBlock': start_block, 
                        'toBlock': finish_block, 
                        'address': efilter.address, 
                        'topics': efilter.topics
                    }
                )
                len_bss_logs = len(bss_logs)
                info(f"Found {len_bss_logs} of {efilter.event_abi['name']} events")
                if len_bss_logs > 0:
                    vault_logs.extend(bss_logs)
            logs.extend([self.process_log(l) for l in vault_logs])
        info(f'Collected {len(logs)} events')
        return logs