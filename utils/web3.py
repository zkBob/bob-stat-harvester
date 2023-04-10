from functools import cache
from decimal import Decimal

from typing import Callable, Any

from time import sleep

from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware
from web3.eth import Contract

from .logging import info, error
from .abi import get_abi, ABI

class Web3Provider:

    def __init__(
        self,
        chainid: str,
        url: str,
        retry_attemtps: int,
        retry_delay: int
    ):
        self.chainid = chainid
        self.w3 = Web3(HTTPProvider(url))
        if chainid != 'eth':
            self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self._retry_attemtps = retry_attemtps
        self._retry_delay = retry_delay

    def make_call(self, func: Callable, *args, **kwargs) -> Any:
        exc = None
        attempts = 0
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                exc = e
                error(f'{self.chainid}: not able to get data: {e}')
            attempts += 1
            if attempts < self._retry_attemtps:
                info(f'{self.chainid}: repeat attempt in {self._retry_delay} seconds')
                sleep(self._retry_delay)
            else:
                break
        raise exc

@cache
class ERC20Token:
    contract: Contract
    w3_provider: Web3Provider

    def __init__(self, w3_provider: Web3Provider, address: str):
        self.contract = w3_provider.w3.eth.contract(abi = get_abi(ABI.ERC20), address = address)
        self.w3_provider = w3_provider

    @cache
    def decimals(self) -> int:
        info(f'{self.w3_provider.chainid}: getting decimals for {self.contract.address}')
        retval = self.w3_provider.make_call(self.contract.functions.decimals().call)
        info(f'{self.w3_provider.chainid}: decimals {retval}')
        return retval

    @cache
    def symbol(self) -> int:
        info(f'{self.w3_provider.chainid}: getting symbol for {self.contract.address}')
        retval = self.w3_provider.make_call(self.contract.functions.symbol().call)
        info(f'{self.w3_provider.chainid}: symbol {retval}')
        return retval

    def normalize(self, value: int) -> Decimal:
        return Decimal(value) / Decimal(10 ** self.decimals())

    def totalSupply(self, normalize: bool = True) -> Decimal:
        info(f'{self.w3_provider.chainid}: getting total supply')
        retval = self.w3_provider.make_call(self.contract.functions.totalSupply().call)
        if normalize:
            denominator_power = self.decimals()
            retval = Decimal(retval / 10 ** denominator_power)
        else:
            retval = Decimal(retval)
        info(f'{self.w3_provider.chainid}: totalSupply is {retval} (normalized = {normalize})')
        return retval

    def balanceOf(self, owner: str, normalize: bool = True) -> Decimal:
        info(f'{self.w3_provider.chainid}: getting balance of {owner}')
        retval = self.w3_provider.make_call(self.contract.functions.balanceOf(owner).call)
        if normalize:
            denominator_power = self.decimals()
            retval = Decimal(retval / 10 ** denominator_power)
        else:
            retval = Decimal(retval)
        info(f'{self.w3_provider.chainid}: balanceOf is {retval} (normalized = {normalize})')
        return retval
