#!/usr/bin/env python

from os import getenv
from logging import basicConfig, getLogger, info, error, debug, warning, WARNING, INFO, DEBUG

from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware

import requests

from time import time, sleep, strptime, mktime, gmtime, strftime

from json import load, dump

import pandas as pd

from google.oauth2 import service_account
import pandas_gbq

basicConfig(level=INFO)
getLogger('pandas_gbq').setLevel(WARNING)

MAX_INT = (2 ** 128) - 1
ONE_DAY = 24 * 60 * 60
TWO_POW_96 = 2 ** 96

BOB_TOKEN_ADDRESS = Web3.toChecksumAddress("0xB0B195aEFA3650A6908f15CdaC7D92F8a5791B0B")
BOB_TOKEN_SYMBOL = 'BOB'

COINGECKO_TICKERS_URL = "https://api.coingecko.com/api/v3/coins/bob/tickers"

univ3_positions_fields = ["nonce", "operator", "token0", "token1", "fee", 
                          "tickLower", "tickUpper", "liquidity", "feeGrowthInside0LastX128", 
                          "feeGrowthInside1LastX128", "tokensOwed0", "tokensOwed1"]
kyberswap_positions_fields_pos = ["nonce", "operator", "poolId", "tickLower", "tickUpper", 
                                "liquidity", "rTokenOwed", "feeGrowthInsideLast"]
kyberswap_positions_fields_info = ["token0", "fee", "token1"]

exchanges_to_chains = {'uniswap_v3_polygon_pos': 'pol',
                       'kyberswap_elastic_polygon': 'pol',
                       'balancer_polygon': 'pol',
                       'uniswap_v3': 'eth',
                       'quickswap_v3': 'eth',
                       'kyberswap_elastic_bsc': 'bsc',
                       'pancakeswap_new': 'bsc',
                       'uniswap_v3_optimism': 'opt',
                       'kyberswap_elastic_optimism': 'opt',
                       'velodrome': 'opt'
                      }

ABI_UNIV3_PM = "abi/uniswapv3_pm.json"
ABI_KYBERSWAP_PM = "abi/kyberswap_elastic_pm.json"
ABI_KYBERSWAP_FACTORY = "abi/kyberswap_elastic_factory.json"
ABI_KYBERSWAP_POOL = "abi/kyberswap_elastic_pool.json"
ABI_ERC20 = "abi/erc20.json"

TOKEN_DEPLOYMENTS_INFO = getenv('TOKEN_DEPLOYMENTS_INFO', 'token-deployments-info.json')
BIGQUERY_AUTH_JSON_KEY = getenv('BIGQUERY_AUTH_JSON_KEY', 'bigquery-key.json')
BIGQUERY_PROJECT = getenv('BIGQUERY_PROJECT', 'some-project')
BIGQUERY_DATASET = getenv('BIGQUERY_DATASET', 'some-dashboard')
BIGQUERY_TABLE = getenv('BIGQUERY_TABLE', 'some-table')
MEASUREMENTS_INTERVAL = int(getenv('MEASUREMENTS_INTERVAL', 60 * 60 * 2 - 30))
COINGECKO_RETRY_ATTEMPTS = int(getenv('COINGECKO_RETRY_ATTEMPTS', 2))
COINGECKO_RETRY_DELAY = int(getenv('COINGECKO_RETRY_DELAY', 5))
WEB3_RETRY_ATTEMPTS = int(getenv('WEB3_RETRY_ATTEMPTS', 2))
WEB3_RETRY_DELAY = int(getenv('WEB3_RETRY_DELAY', 5))

info(f'TOKEN_DEPLOYMENTS_INFO = {TOKEN_DEPLOYMENTS_INFO}')
info(f'BIGQUERY_AUTH_JSON_KEY = {BIGQUERY_AUTH_JSON_KEY}')
info(f'BIGQUERY_PROJECT = {BIGQUERY_PROJECT}')
info(f'BIGQUERY_DATASET = {BIGQUERY_DATASET}')
info(f'BIGQUERY_TABLE = {BIGQUERY_TABLE}')
info(f'MEASUREMENTS_INTERVAL = {MEASUREMENTS_INTERVAL}')
info(f'COINGECKO_RETRY_ATTEMPTS = {COINGECKO_RETRY_ATTEMPTS}')
info(f'COINGECKO_RETRY_DELAY = {COINGECKO_RETRY_DELAY}')
info(f'WEB3_RETRY_ATTEMPTS = {WEB3_RETRY_ATTEMPTS}')
info(f'WEB3_RETRY_DELAY = {WEB3_RETRY_DELAY}')

try:
    with open(f'{TOKEN_DEPLOYMENTS_INFO}') as f:
        chains = load(f)['chains']
except IOError:
    raise BaseException(f'Cannot get {BOB_TOKEN_SYMBOL} deployment info')
info(f'Stats will be gathered for chains: {list(chains.keys())}')

credentials = service_account.Credentials.from_service_account_file(BIGQUERY_AUTH_JSON_KEY)
info('BigQuery auth key applied')

pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = BIGQUERY_PROJECT

def load_abi(_file):
    try:
        with open(_file) as f:
            abi = load(f)
    except IOError:
        raise BaseException(f'Cannot read {_file}')
    info(f'{_file} loaded')
    return abi

uniV3_abi = load_abi(ABI_UNIV3_PM)
kyberswap_abi = load_abi(ABI_KYBERSWAP_PM)
kyberswap_factory_abi = load_abi(ABI_KYBERSWAP_FACTORY)
kyberswap_pool_abi = load_abi(ABI_KYBERSWAP_POOL)
erc20_abi = load_abi(ABI_ERC20)

chain_names = {}
w3_providers = {}
for chainid in chains:
    chain_names[chainid] = chains[chainid]['name']
    
    url = chains[chainid]['rpc']['url']
    w3_providers[chainid] = Web3(HTTPProvider(url))
    if chainid != 'eth':
        w3_providers[chainid].middleware_onion.inject(geth_poa_middleware, layer=0)
    info(f'Web3 provider for "{chainid}" attached to "{url}"')

rpc_response_cache = {}

def get_token_decimals(_token):
    global rpc_response_cache
    if not 'decimals' in rpc_response_cache: 
        rpc_response_cache['decimals'] = {}
    endpoint = _token.web3.provider.endpoint_uri
    if not endpoint in rpc_response_cache['decimals']:
        rpc_response_cache['decimals'][endpoint] = {}
    if not _token.address in rpc_response_cache['decimals'][endpoint]:
        rpc_response_cache['decimals'][endpoint][_token.address] = _token.functions.decimals().call()
    return rpc_response_cache['decimals'][endpoint][_token.address]

def get_token_symbol(_token):
    global rpc_response_cache
    if not 'symbols' in rpc_response_cache: 
        rpc_response_cache['symbols'] = {}
    endpoint = _token.web3.provider.endpoint_uri
    if not endpoint in rpc_response_cache['symbols']:
        rpc_response_cache['symbols'][endpoint] = {}
    if not _token.address in rpc_response_cache['symbols'][endpoint]:
        rpc_response_cache['symbols'][endpoint][_token.address] = _token.functions.symbol().call()
    return rpc_response_cache['symbols'][endpoint][_token.address]

def getForUniswapPairs(_w3, _pm_addr, _io_addr):
    pairs = {}
    
    info(f'Getting UniSwapV3 positions for {_io_addr} on {_w3.provider.endpoint_uri}')
    position_manager = _w3.eth.contract(abi = uniV3_abi, address = _pm_addr)
    
    attempts = 0
    while attempts < WEB3_RETRY_ATTEMPTS:
        try:    
            pos_num = position_manager.functions.balanceOf(_io_addr).call()

            info(f'Found {pos_num} positions')

            for i in range(pos_num):
                pos_id = position_manager.functions.tokenOfOwnerByIndex(_io_addr, i).call()

                info(f'Handling position {pos_id}')

                position_details = position_manager.functions.positions(pos_id).call()
                position_details = dict(zip(univ3_positions_fields, position_details))

                token0_addr = position_details['token0']
                token1_addr = position_details['token1']
                pos_liquidity = position_details['liquidity']
                info(f'pair: {token0_addr}/{token1_addr}, liquidity {pos_liquidity}')
                debug(f'{position_details}')

                if pos_liquidity == 0:
                    continue

                params = {"tokenId": pos_id,
                      "liquidity": pos_liquidity,
                      "amount0Min": 0,
                      "amount1Min": 0,
                      "deadline": int(time())+ ONE_DAY
                     }
                tvl_pair = position_manager.functions.decreaseLiquidity(params).call()

                info(f'tvl: {tvl_pair}')

                params = {"tokenId": pos_id,
                  "recipient": _io_addr,
                  "amount0Max": MAX_INT,
                  "amount1Max": MAX_INT
                 }
                fees_pair = position_manager.functions.collect(params).call()

                info(f'fees: {fees_pair}')

                pair = {}
                if token0_addr == BOB_TOKEN_ADDRESS:
                    token0 = _w3.eth.contract(abi = erc20_abi, address = token0_addr)
                    token0_decimals = get_token_decimals(token0)
                    pair['token0'] = {'symbol': BOB_TOKEN_SYMBOL,
                                      'tvl': tvl_pair[0] / (10 ** token0_decimals),
                                      'fees': fees_pair[0] / (10 ** token0_decimals)
                                     }

                    token1 = _w3.eth.contract(abi = erc20_abi, address = token1_addr)
                    token1_decimals = get_token_decimals(token1)
                    pair['token1'] = {'symbol': get_token_symbol(token1),
                                      'tvl': tvl_pair[1] / (10 ** token1_decimals),
                                      'fees': fees_pair[1] / (10 ** token1_decimals)
                                     }
                else:
                    token0 = _w3.eth.contract(abi = erc20_abi, address = token0_addr)
                    token0_decimals = get_token_decimals(token0)
                    pair['token0'] = {'symbol': get_token_symbol(token0),
                                      'tvl': tvl_pair[0] / (10 ** token0_decimals),
                                      'fees': fees_pair[0] / (10 ** token0_decimals)
                                     }

                    token1 = _w3.eth.contract(abi = erc20_abi, address = token1_addr)
                    token1_decimals = get_token_decimals(token1)
                    pair['token1'] = {'symbol': BOB_TOKEN_SYMBOL,
                                      'tvl': tvl_pair[1] / (10 ** token1_decimals),
                                      'fees': fees_pair[1] / (10 ** token1_decimals)
                                     }

                pairs[f"{pair['token0']['symbol']}/{pair['token1']['symbol']}({position_details['fee']/10000})"] = pair
            break
        except:
            error(f'Not able to get data')

        attempts += 1
        info(f'Repeat attempt in {WEB3_RETRY_DELAY} seconds')
        sleep(WEB3_RETRY_DELAY)

    return pairs

def getForKyberSwapPairs(_w3, _pm_addr, _io_addr):
    pairs = {}

    info(f'Getting KyberSwap Elastic positions for {_io_addr} on {_w3.provider.endpoint_uri}')
    position_manager = _w3.eth.contract(abi = kyberswap_abi, address = _pm_addr)

    attempts = 0
    while attempts < WEB3_RETRY_ATTEMPTS:
        try:    
            pos_num = position_manager.functions.balanceOf(_io_addr).call()

            info(f'Found {pos_num} positions')

            for i in range(pos_num):
                pos_id = position_manager.functions.tokenOfOwnerByIndex(_io_addr, i).call()

                info(f'Handling position {pos_id}')

                position_details = position_manager.functions.positions(pos_id).call()
                tmp_position_details = dict(zip(kyberswap_positions_fields_pos, position_details[0]))
                tmp_position_details.update(dict(zip(kyberswap_positions_fields_info, position_details[1])))
                position_details = tmp_position_details

                token0_addr = position_details['token0']
                token1_addr = position_details['token1']
                pos_liquidity = position_details['liquidity']
                info(f'pair: {token0_addr}/{token1_addr}, liquidity {pos_liquidity}')
                debug(f'{position_details}')

                if pos_liquidity == 0:
                    continue

                params = {"tokenId": pos_id,
                      "liquidity": pos_liquidity,
                      "amount0Min": 0,
                      "amount1Min": 0,
                      "deadline": int(time())+ ONE_DAY
                     }
                tvl_pair = position_manager.functions.removeLiquidity(params).call()

                info(f'tvl: {tvl_pair}')

                # This approach cannot be used since removeLiquidity does not change state actually
                # to update the position's rTokenOwed, so burnRTokens fails with 'no tokens to burn' 
                # params = {"tokenId": pos_id,
                #   "amount0Min": MAX_INT,
                #   "amount1Min": MAX_INT,
                #   "deadline": int(time())+ ONE_DAY
                #  }
                # fees_pair = position_manager.functions.burnRTokens(params).call()

                pool_fee = position_details['fee']

                factory_addr = position_manager.functions.factory().call()
                factory = _w3.eth.contract(abi = kyberswap_factory_abi, address = factory_addr)
                pool_addr = factory.functions.getPool(token0_addr, token1_addr, pool_fee).call()
                pool = _w3.eth.contract(abi = kyberswap_pool_abi, address = pool_addr)

                sqrtPrice = pool.functions.getPoolState().call()[0]
                reinvestTokens = position_details['rTokenOwed'] + tvl_pair[2]
                # Naive approach to calculate fees. It must be verified and tuned later when delta
                # between actual fees and values below become obvious
                fees_pair = [ reinvestTokens * (TWO_POW_96 / sqrtPrice) * (TWO_POW_96 / sqrtPrice),
                              reinvestTokens * (sqrtPrice / TWO_POW_96) * (sqrtPrice / TWO_POW_96)
                            ]    
                info(f'fees: {fees_pair}')

                pair = {}
                if token0_addr == BOB_TOKEN_ADDRESS:
                    token0 = _w3.eth.contract(abi = erc20_abi, address = token0_addr)
                    token0_decimals = get_token_decimals(token0)
                    pair['token0'] = {'symbol': BOB_TOKEN_SYMBOL,
                                      'tvl': tvl_pair[0] / (10 ** token0_decimals),
                                      'fees': fees_pair[0] / (10 ** token0_decimals)
                                     }

                    token1 = _w3.eth.contract(abi = erc20_abi, address = token1_addr)
                    token1_decimals = get_token_decimals(token1)
                    pair['token1'] = {'symbol': get_token_symbol(token1),
                                      'tvl': tvl_pair[1] / (10 ** token1_decimals),
                                      'fees': fees_pair[1] / (10 ** token1_decimals)
                                     }
                else:
                    token0 = _w3.eth.contract(abi = erc20_abi, address = token0_addr)
                    token0_decimals = get_token_decimals(token0)
                    pair['token0'] = {'symbol': get_token_symbol(token0),
                                      'tvl': tvl_pair[0] / (10 ** token0_decimals),
                                      'fees': fees_pair[0] / (10 ** token0_decimals)
                                     }

                    token1 = _w3.eth.contract(abi = erc20_abi, address = token1_addr)
                    token1_decimals = get_token_decimals(token1)
                    pair['token1'] = {'symbol': BOB_TOKEN_SYMBOL,
                                      'tvl': tvl_pair[1] / (10 ** token1_decimals),
                                      'fees': fees_pair[1] / (10 ** token1_decimals)
                                     }

                pairs[f"{pair['token0']['symbol']}/{pair['token1']['symbol']}({pool_fee/1000})"] = pair
            break
        except:
            error(f'Not able to get data')

        attempts += 1
        info(f'Repeat attempt in {WEB3_RETRY_DELAY} seconds')
        sleep(WEB3_RETRY_DELAY)
                
    return pairs

inventory_protocols = {'UniswapV3': getForUniswapPairs,
                       'KyberSwap Elastic': getForKyberSwapPairs
                      }

def getTotalSupply(_w3):
    token_TS = -1
    info(f'Getting total supply on {_w3.provider.endpoint_uri}')

    token = _w3.eth.contract(abi = erc20_abi, address = BOB_TOKEN_ADDRESS)
    token_decimals = get_token_decimals(token)

    attempts = 0
    while attempts < WEB3_RETRY_ATTEMPTS:
        try:
            token_TS = token.functions.totalSupply().call() / (10 ** token_decimals)
            info(f'total supply: {token_TS}')
            break
        except:
            error(f'Not able to call totalSupply()')

        attempts += 1
        info(f'Repeat attempt in {WEB3_RETRY_DELAY} seconds')
        sleep(WEB3_RETRY_DELAY)

    return token_TS

def getVolumeFromCoinGecko():
    info(f'Getting markets volume from CoinGecko')
    chains = {}

    attempts = 0
    while attempts < COINGECKO_RETRY_ATTEMPTS:
        try: 
            resp = requests.get(COINGECKO_TICKERS_URL)
            if resp.status_code != requests.codes.ok:
                error(f'Request failed with response {resp.status_code}({resp.reason})')
            else:
                tickers = resp.json()['tickers']
                for ticker in tickers:
                    exchange_id = ticker['market']['identifier']
                    if not exchange_id in exchanges_to_chains:
                        error(f"Market {exchange_id} skipped")
                        continue
                    if ticker['is_anomaly'] == True:
                        warning(f"Market {exchange_id} -- {ticker['coin_id']}/{ticker['target_coin_id']} skipped due to anomaly")
                        continue
                    if ticker['is_stale'] == True:
                        last_traded_time = ticker['last_traded_at']
                        last_traded_time = strptime(last_traded_time[:-3]+last_traded_time[-2:], '%Y-%m-%dT%H:%M:%S%z')
                        if (time() - mktime(last_traded_time) // ONE_DAY) >= 1:
                            warning(f"Market {exchange_id} -- {ticker['coin_id']}/{ticker['target_coin_id']} skipped since staled, last traded time {ticker['last_traded_at']}")
                            continue
                    chain = exchanges_to_chains[exchange_id]
                    volumeUSD = ticker['converted_volume']['usd']
                    info(f'{exchange_id} -- {ticker["coin_id"]}/{ticker["target_coin_id"]}: {volumeUSD}')
                    if chain in chains:
                        chains[chain] += volumeUSD
                    else:
                        chains[chain] = volumeUSD
                break
        except:
            error(f'Not able to execute request to {COINGECKO_TICKERS_URL}')

        attempts += 1
        info(f'Repeat attempt in {COINGECKO_RETRY_DELAY} seconds')
        sleep(COINGECKO_RETRY_DELAY)
                
    return chains

def getStatsForPairs(_pairs, _ts, _vol):
    unused_supply = 0
    total_supply = 0
    fees = {}
    volume = 0
    for chain in _pairs:
        if chain in _ts:
            for pair in _pairs[chain]:
                for token in _pairs[chain][pair]:
                    t_symbol = _pairs[chain][pair][token]['symbol']
                    t_fees = _pairs[chain][pair][token]['fees']
                    if t_symbol == BOB_TOKEN_SYMBOL:
                        unused_supply += _pairs[chain][pair][token]['tvl']
                    if t_symbol in fees:
                        fees[t_symbol] += t_fees
                    else:
                        fees[t_symbol] = t_fees
            total_supply += _ts[chain]
            if chain in _vol:
                volume += _vol[chain]
        else:
            error(f"Chain '{chain}' no found total supply map")

    return {'totalSupply': total_supply,
            'colCirculatingSupply': total_supply - unused_supply,
            'volumeUSD': volume,
            'fees': fees
           }

def generateStatsForChains(_pairs, _ts, _vol, _time = None):
    if not _time:
        _time = int(time())
    info(f"Data timesmap: {strftime('%Y-%m-%d %H:%M:%S', gmtime(_time))}")
    dat = []
    for c in chain_names:
        if (c in _pairs) and (c in _ts):
            if not c in _vol:
                d = getStatsForPairs({c: _pairs[c]}, {c: _ts[c]}, {c: 0})
            else:
                d = getStatsForPairs({c: _pairs[c]}, {c: _ts[c]}, {c: _vol[c]})
            d['chain'] = chain_names[c]
            d['dt'] = _time
            info(f'Stats for chain {d}')
            dat.append(d)
        else:
            error(f'No data for "{c}"')
    return dat

while True:
    totalSupply = {}
    for chain in chains:
        totalSupply[chain] = getTotalSupply(w3_providers[chain])
        if totalSupply[chain] == 0:
            error(f'Error happens during total supply collecting. Interrupt measurements for the next time')
            continue
    
    pairs = {}
    for chain in chains:
        w3 = w3_providers[chain]
        for inventory in chains[chain]['inventories']:
            pm = Web3.toChecksumAddress(inventory['pos_manager'])
            owner = Web3.toChecksumAddress(inventory['owner'])
            if inventory['protocol'] in inventory_protocols:
                pairs[chain] = inventory_protocols[inventory['protocol']](w3, pm, owner)
                if len(pairs[chain]) == 0:
                    error(f'Error happens during inventory discover. Interrupt measurements for the next time')
                    continue
            else:
                error(f'Handler for {inventory["protocol"]} not found')

    volume = getVolumeFromCoinGecko()
    if len(volume) == 0:
        error(f'Error happens during volume data collecting. Interrupt measurements for the next time')
        continue
    
    stats = generateStatsForChains(pairs, totalSupply, volume)
    if len(stats) == len(chains):
        df = pd.json_normalize(stats, sep='_')
        df['dt'] = pd.to_datetime(df['dt'], unit='s', utc=False)

        info('sending data to BigQuery')
        try:
            pandas_gbq.to_gbq(df, f'{BIGQUERY_DATASET}.{BIGQUERY_TABLE}', if_exists='append', progress_bar=False)
            info('data sent to BigQuery successfully')
        except:
            error(f'Something wrong with sending data to BigQuery. Interrupt measurements for the next time')
    else:
        error(f'Something wrong with amount of collected data. Interrupt measurements for the next time')
    
    sleep(MEASUREMENTS_INTERVAL)
