from pydantic import BaseModel
from typing import Optional, List, Dict, Set
from decimal import Decimal

from time import sleep, strptime, time, mktime

import requests

from utils.logging import info, error, warning
from utils.constants import ONE_DAY

from ..settings import Settings

from .common import GenericVolumeAdapter

COINGECKO_TICKERS_URL = "https://api.coingecko.com/api/v3/coins/bob/tickers"

class TheeCoinsVolume(BaseModel):
    usd: Decimal
    btc: Optional[Decimal]
    eth: Optional[Decimal]

class TickerId(BaseModel):
    identifier: str
    name: Optional[str]
    has_trading_incentive: Optional[bool]

class CoinGeckoTickerInfo(BaseModel):
    market: TickerId
    converted_volume: TheeCoinsVolume
    last_traded_at: str
    is_anomaly: bool
    is_stale: bool
    coin_id: str
    target_coin_id: str
    base: Optional[str]
    target: Optional[str]
    last: Optional[Decimal]
    volume: Optional[Decimal]
    converted_last: Optional[TheeCoinsVolume]
    trust_score: Optional[str]
    bid_ask_spread_percentage: Optional[Decimal]
    timestamp: Optional[str]
    last_fetch_at: Optional[str]
    trade_url: Optional[str]
    token_info_url: Optional[str]

class CoinGeckoTickers(BaseModel):
    name: str
    tickers: List[CoinGeckoTickerInfo]

class VolumeOnCoinGecko(GenericVolumeAdapter):
    _retry_attempts: int
    _retry_delay: int
    _include_anomalies: bool
    _markets_on_chains: Dict[str, str]
    _markets_to_skip: Set[str]

    def __init__(self, settings: Settings):
        self._retry_attempts = settings.coingecko_retry_attempts
        self._retry_delay = settings.coingecko_retry_delay
        self._include_anomalies = settings.coingecko_include_anomalies

        self._markets_on_chains = {}
        self._markets_to_skip = set()

        for chainid in settings.chains:
            if settings.chains[chainid].coingecko:
                cg_settings = settings.chains[chainid].coingecko
                if cg_settings.known:
                    for m in cg_settings.known:
                        self._markets_on_chains[m] = chainid
                if cg_settings.exclude:
                    for m in cg_settings.exclude:
                        self._markets_to_skip.add(m)

    def _get_data(self) -> List[CoinGeckoTickerInfo]:
        info(f'coingecko: getting tickers data')
        attempts = 0
        exc = None
        while True:
            try:
                resp = requests.get(COINGECKO_TICKERS_URL)
                resp.raise_for_status()
            except Exception as e:
                error(f'coingecko: request failed with response {e})')
                exc = e
            else:
                return CoinGeckoTickers.parse_obj(resp.json()).tickers

            attempts += 1
            if attempts < self._retry_attemtps:
                info(f'coingecko: repeat attempt in {self._retry_delay} seconds')
                sleep(self._retry_delay)
            else:
                break

        raise exc

    def get_volume(self) -> Dict[str, Decimal]:
        info(f'coingecko: getting markets volume')
        chains = {}

        try:
            tickers = self._get_data()
        except:
            pass
        else:
            info(f'coingecko: parsing markets')
            for ticker in tickers:
                exchange_id = ticker.market.identifier
                if exchange_id in self._markets_to_skip:
                    info(f"coingecko: market {exchange_id} configured to be skipped")
                    continue
                if not exchange_id in self._markets_on_chains:
                    error(f"coingecko: market {exchange_id} skipped since its home chain is unknown")
                    continue
                if (ticker.is_anomaly == True) and (self._include_anomalies == False):
                    warning(f"coingecko: market {exchange_id} -- {ticker.coin_id}/{ticker.target_coin_id} skipped due to anomaly")
                    continue
                if ticker.is_stale == True:
                    last_traded_time = ticker.last_traded_at
                    last_traded_time = strptime(last_traded_time[:-3]+last_traded_time[-2:], '%Y-%m-%dT%H:%M:%S%z')
                    if ((time() - mktime(last_traded_time)) // ONE_DAY) >= 1:
                        warning(f"coingecko: market {exchange_id} -- {ticker.coin_id}/{ticker.target_coin_id} skipped since staled, last traded time {ticker.last_traded_at}")
                        continue
                chain = self._markets_on_chains[exchange_id]
                volumeUSD = ticker.converted_volume.usd
                info(f'coingecko: {exchange_id} -- {ticker.coin_id}/{ticker.target_coin_id}: {volumeUSD}')
                if chain in chains:
                    chains[chain] += volumeUSD
                else:
                    chains[chain] = volumeUSD
                    
        return chains