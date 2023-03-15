#!/usr/bin/env python

from os import getenv
from logging import basicConfig, getLogger, info, error, debug, warning, WARNING, INFO, DEBUG

from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware

import requests
from requests.auth import AuthBase

from time import time, sleep, strptime, mktime, gmtime, strftime
from datetime import datetime

from json import load, dump

from tinyflux import TinyFlux, Point, TimeQuery

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
                       'wombat': 'bsc',
                       'uniswap_v3_optimism': 'opt',
                       'kyberswap_elastic_optimism': 'opt',
                       'velodrome': 'opt',
                       'uniswap_v3_arbitrum': 'arb1',
                       'kyberswap_elastic_arbitrum': 'arb1'
                      }

ABI_UNIV3_PM = "abi/uniswapv3_pm.json"
ABI_KYBERSWAP_PM = "abi/kyberswap_elastic_pm.json"
ABI_KYBERSWAP_FACTORY = "abi/kyberswap_elastic_factory.json"
ABI_KYBERSWAP_POOL = "abi/kyberswap_elastic_pool.json"
ABI_ERC20 = "abi/erc20.json"

TOKEN_DEPLOYMENTS_INFO = getenv('TOKEN_DEPLOYMENTS_INFO', 'token-deployments-info.json')
SNAPSHOT_DIR = getenv('SNAPSHOT_DIR', '.')
BOBVAULT_SNAPSHOT_FILE_SUFIX = getenv('BOBVAULT_SNAPSHOT_FILE_SUFIX', 'bobvault-snaphsot.json')
BALANCES_SNAPSHOT_FILE_SUFFIX = getenv('BALANCES_SNAPSHOT_FILE_SUFFIX', 'bob-holders-snaphsot.json')
UPDATE_BIGQUERY = getenv('UPDATE_BIGQUERY', 'true')
BIGQUERY_AUTH_JSON_KEY = getenv('BIGQUERY_AUTH_JSON_KEY', 'bigquery-key.json')
BIGQUERY_PROJECT = getenv('BIGQUERY_PROJECT', 'some-project')
BIGQUERY_DATASET = getenv('BIGQUERY_DATASET', 'some-dashboard')
BIGQUERY_TABLE = getenv('BIGQUERY_TABLE', 'some-table')
MEASUREMENTS_INTERVAL = int(getenv('MEASUREMENTS_INTERVAL', 60 * 60 * 2 - 30))
COINGECKO_RETRY_ATTEMPTS = int(getenv('COINGECKO_RETRY_ATTEMPTS', 2))
COINGECKO_RETRY_DELAY = int(getenv('COINGECKO_RETRY_DELAY', 5))
WEB3_RETRY_ATTEMPTS = int(getenv('WEB3_RETRY_ATTEMPTS', 2))
WEB3_RETRY_DELAY = int(getenv('WEB3_RETRY_DELAY', 5))
TSDB_DIR = getenv('TSDB_DIR', '.')
BOB_COMPOSED_STAT_DB = getenv('BOB_COMPOSED_STAT_DB', 'bobstat_composed.csv')
BOB_COMPOSED_FEES_STAT_DB = getenv('BOB_COMPOSED_FEES_STAT_DB', 'bobstat_comp_fees.csv')
FEEDING_SERVICE_URL = getenv('FEEDING_SERVICE_URL', 'http://127.0.0.1:8080')
FEEDING_SERVICE_PATH = getenv('FEEDING_SERVICE_PATH', '/')
FEEDING_SERVICE_HEALTH_PATH = getenv('FEEDING_SERVICE_HEALTH_PATH', '/health')
FEEDING_SERVICE_UPLOAD_TOKEN = getenv('FEEDING_SERVICE_UPLOAD_TOKEN', 'default')
FEEDING_SERVICE_MONITOR_INTERVAL = int(getenv('FEEDING_SERVICE_MONITOR_INTERVAL', 60))
FEEDING_SERVICE_MONITOR_ATTEMPTS_FOR_INFO = int(getenv('FEEDING_SERVICE_MONITOR_ATTEMPTS_FOR_INFO', 60))

if UPDATE_BIGQUERY == 'true' or UPDATE_BIGQUERY == 'True':
    UPDATE_BIGQUERY = True
else:
    UPDATE_BIGQUERY = False

info(f'TOKEN_DEPLOYMENTS_INFO = {TOKEN_DEPLOYMENTS_INFO}')
info(f'SNAPSHOT_DIR = {SNAPSHOT_DIR}')
info(f'BOBVAULT_SNAPSHOT_FILE_SUFIX = {BOBVAULT_SNAPSHOT_FILE_SUFIX}')
info(f'BALANCES_SNAPSHOT_FILE_SUFFIX = {BALANCES_SNAPSHOT_FILE_SUFFIX}')
info(f'UPDATE_BIGQUERY = {UPDATE_BIGQUERY}')
info(f'BIGQUERY_AUTH_JSON_KEY = {BIGQUERY_AUTH_JSON_KEY}')
info(f'BIGQUERY_PROJECT = {BIGQUERY_PROJECT}')
info(f'BIGQUERY_DATASET = {BIGQUERY_DATASET}')
info(f'BIGQUERY_TABLE = {BIGQUERY_TABLE}')
info(f'MEASUREMENTS_INTERVAL = {MEASUREMENTS_INTERVAL}')
info(f'COINGECKO_RETRY_ATTEMPTS = {COINGECKO_RETRY_ATTEMPTS}')
info(f'COINGECKO_RETRY_DELAY = {COINGECKO_RETRY_DELAY}')
info(f'WEB3_RETRY_ATTEMPTS = {WEB3_RETRY_ATTEMPTS}')
info(f'WEB3_RETRY_DELAY = {WEB3_RETRY_DELAY}')
info(f'TSDB_DIR = {TSDB_DIR}')
info(f'BOB_COMPOSED_STAT_DB = {BOB_COMPOSED_STAT_DB}')
info(f'BOB_COMPOSED_FEES_STAT_DB = {BOB_COMPOSED_FEES_STAT_DB}')
info(f'FEEDING_SERVICE_URL = {FEEDING_SERVICE_URL}')
info(f'FEEDING_SERVICE_PATH = {FEEDING_SERVICE_PATH}')
info(f'FEEDING_SERVICE_HEALTH_PATH = {FEEDING_SERVICE_HEALTH_PATH}')
if FEEDING_SERVICE_UPLOAD_TOKEN != 'default':
    info(f'FEEDING_SERVICE_UPLOAD_TOKEN is set')
else:
    info(f'FEEDING_SERVICE_UPLOAD_TOKEN = {FEEDING_SERVICE_UPLOAD_TOKEN}')
info(f'FEEDING_SERVICE_MONITOR_INTERVAL = {FEEDING_SERVICE_MONITOR_INTERVAL}')
info(f'FEEDING_SERVICE_MONITOR_ATTEMPTS_FOR_INFO = {FEEDING_SERVICE_MONITOR_ATTEMPTS_FOR_INFO}')

try:
    with open(f'{TOKEN_DEPLOYMENTS_INFO}') as f:
        chains = load(f)['chains']
except IOError as e:
    error(f'Cannot get {BOB_TOKEN_SYMBOL} deployment info')
    raise e
info(f'Stats will be gathered for chains: {list(chains.keys())}')

if UPDATE_BIGQUERY:
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

def make_web3_call(func, *args, **kwargs):
    attempts = 0
    while attempts < WEB3_RETRY_ATTEMPTS:
        try:
            return func(*args, **kwargs)
        except:
            error(f'Not able to get data')
        attempts += 1
        info(f'Repeat attempt in {WEB3_RETRY_DELAY} seconds')
        sleep(WEB3_RETRY_DELAY)
    raise BaseException(f'Cannot make web3 call')

rpc_response_cache = {}

def get_token_decimals(_token):
    global rpc_response_cache
    if not 'decimals' in rpc_response_cache: 
        rpc_response_cache['decimals'] = {}
    endpoint = _token.web3.provider.endpoint_uri
    if not endpoint in rpc_response_cache['decimals']:
        rpc_response_cache['decimals'][endpoint] = {}
    if not _token.address in rpc_response_cache['decimals'][endpoint]:
        info(f'Getting decimals for {_token.address} to cache')
        resp = make_web3_call(_token.functions.decimals().call)
        info(f'Decimals {resp}')
        rpc_response_cache['decimals'][endpoint][_token.address] = resp
    return rpc_response_cache['decimals'][endpoint][_token.address]

def get_token_symbol(_token):
    global rpc_response_cache
    if not 'symbols' in rpc_response_cache: 
        rpc_response_cache['symbols'] = {}
    endpoint = _token.web3.provider.endpoint_uri
    if not endpoint in rpc_response_cache['symbols']:
        rpc_response_cache['symbols'][endpoint] = {}
    if not _token.address in rpc_response_cache['symbols'][endpoint]:
        info(f'Getting symbol for {_token.address} to cache')
        resp = make_web3_call(_token.functions.symbol().call)
        info(f'Symbol {resp}')
        rpc_response_cache['symbols'][endpoint][_token.address] = resp
    return rpc_response_cache['symbols'][endpoint][_token.address]

def getForUniswapPairs(_w3, params):
    pm_addr = Web3.toChecksumAddress(params['pos_manager'])
    io_addr = Web3.toChecksumAddress(params['owner'])
    pairs = {}
    
    info(f'Getting UniSwapV3 positions for {io_addr} on {_w3.provider.endpoint_uri}')
    position_manager = _w3.eth.contract(abi = uniV3_abi, address = pm_addr)
    
    try:    
        pos_num = make_web3_call(position_manager.functions.balanceOf(io_addr).call)

        info(f'Found {pos_num} positions')

        for i in range(pos_num):
            pos_id = make_web3_call(position_manager.functions.tokenOfOwnerByIndex(io_addr, i).call)

            info(f'Handling position {pos_id}')

            position_details = make_web3_call(position_manager.functions.positions(pos_id).call)
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
            tvl_pair = make_web3_call(position_manager.functions.decreaseLiquidity(params).call)

            info(f'tvl: {tvl_pair}')

            params = {"tokenId": pos_id,
                "recipient": io_addr,
                "amount0Max": MAX_INT,
                "amount1Max": MAX_INT
                }
            fees_pair = make_web3_call(position_manager.functions.collect(params).call)

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
    except:
        error(f'Not able to get data')

    return pairs

def getForKyberSwapPairs(_w3, params):
    pm_addr = Web3.toChecksumAddress(params['pos_manager'])
    io_addr = Web3.toChecksumAddress(params['owner'])
    pairs = {}

    info(f'Getting KyberSwap Elastic positions for {io_addr} on {_w3.provider.endpoint_uri}')
    position_manager = _w3.eth.contract(abi = kyberswap_abi, address = pm_addr)

    try:    
        pos_num = make_web3_call(position_manager.functions.balanceOf(io_addr).call)

        info(f'Found {pos_num} positions')

        for i in range(pos_num):
            pos_id = make_web3_call(position_manager.functions.tokenOfOwnerByIndex(io_addr, i).call)

            info(f'Handling position {pos_id}')

            position_details = make_web3_call(position_manager.functions.positions(pos_id).call)
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
            tvl_pair = make_web3_call(position_manager.functions.removeLiquidity(params).call)

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

            factory_addr = make_web3_call(position_manager.functions.factory().call)
            factory = _w3.eth.contract(abi = kyberswap_factory_abi, address = factory_addr)
            pool_addr = make_web3_call(factory.functions.getPool(token0_addr, token1_addr, pool_fee).call)
            pool = _w3.eth.contract(abi = kyberswap_pool_abi, address = pool_addr)

            sqrtPrice = make_web3_call(pool.functions.getPoolState().call)[0]
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
    except:
        error(f'Not able to get data')

    return pairs

def getInventoryForBobVault(_w3, params):
    bv_addr = Web3.toChecksumAddress(params['address'])
    bobstat = {}

    info(f'Getting BobVault info on {_w3.provider.endpoint_uri}')

    bobtoken = _w3.eth.contract(abi = erc20_abi, address = BOB_TOKEN_ADDRESS)
    try:
        bob_decimals = get_token_decimals(bobtoken)
        tvl = make_web3_call(bobtoken.functions.balanceOf(bv_addr).call)
    except: 
        error(f'Not able to get data')
    else:
        info(f'tvl: {tvl}')
        bobstat['BOB_on_BobVault'] = {'bob':
            {
             'symbol': BOB_TOKEN_SYMBOL,
             'tvl': tvl / (10 ** bob_decimals),
             'fees': 0
            }
        }

    return bobstat

inventory_protocols = {'UniswapV3': getForUniswapPairs,
                       'KyberSwap Elastic': getForKyberSwapPairs,
                       'BobVault': getInventoryForBobVault,
                      }

def getTotalSupply(_w3):
    token_TS = -1
    info(f'Getting total supply on {_w3.provider.endpoint_uri}')

    token = _w3.eth.contract(abi = erc20_abi, address = BOB_TOKEN_ADDRESS)
    token_decimals = get_token_decimals(token)

    attempts = 0
    try:
        token_TS = make_web3_call(token.functions.totalSupply().call) / (10 ** token_decimals)
        info(f'total supply: {token_TS}')
    except:
        error(f'Not able to call totalSupply()')

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

def get_bobvault_volume_for_timeframe(logs, ts_start, ts_end):
    info(f'Getting volume between {ts_start} and {ts_end}')
    logs_len = len(logs)
    prev_indices = [-1, logs_len]
    first_index = sum(prev_indices) // 2
    no_error = True
    while no_error:
        debug(f'binary search: {prev_indices} - {first_index}')
        if (logs[first_index]['timestamp'] >= ts_start):
            if (first_index == 0) or (logs[first_index-1]['timestamp'] < ts_start):
                break
            else:
                prev_indices[1] = first_index
                first_index = sum(prev_indices) // 2
        else:
            prev_indices[0] = first_index
            first_index = sum(prev_indices) // 2
        if prev_indices[0] == logs_len - 1:
            no_error = False
    volume_tf = 0.0
    if not no_error:
        info("No events for last required time frame")
    else:
        for trade in logs[first_index:]:
            if trade['timestamp'] < ts_end:
                if trade['args']['inToken'] == BOB_TOKEN_ADDRESS:
                    trade_volume = trade['args']['amountIn']
                elif trade['args']['outToken'] == BOB_TOKEN_ADDRESS:
                    trade_volume = trade['args']['amountOut']
                else:
                    info(f'Swap operations skipped')
                    trade_volume = 0
                volume_tf += trade_volume
            else:
                break
    return volume_tf

def get_bobvault_volume_24h():
    ret = {}
    for chainid in chains:
        vol = 0.0
        try:
            with open(f'{SNAPSHOT_DIR}/{chainid}-{BOBVAULT_SNAPSHOT_FILE_SUFIX}', 'r') as json_file:
                snapshot = load(json_file)
        except IOError:
            error(f'No snapshot {chainid}-{BOBVAULT_SNAPSHOT_FILE_SUFIX} found')
        else:
            info(f'Collecting 24h volume from bobvault snapshot on {chain_names[chainid]}')
            now = int(time())
            now_minus_24h = now - ONE_DAY
            vol = get_bobvault_volume_for_timeframe(snapshot['logs'], now_minus_24h, now)
            info(f'Discovered volume on {chain_names[chainid]}: {vol}')
        ret.update({chainid: vol})
    return ret

def get_bob_holders_amount(_chain):
    info(f'Getting token holders for {_chain}')
    with open(f'{SNAPSHOT_DIR}/{_chain}-{BALANCES_SNAPSHOT_FILE_SUFFIX}', 'r') as json_file:
        snapshot = load(json_file)
    holders_num = len(snapshot['balances'])
    info(f'Number of token holders: for {holders_num}')
    return holders_num

def generateStatsForChains(_pairs, _ts, _vol, _holders, _time = None):
    if not _time:
        _time = int(time())
    info(f"Data timesmap: {strftime('%Y-%m-%d %H:%M:%S', gmtime(_time))}")
    dat = []
    for c in chain_names:
        if (c in _pairs) and (c in _ts):

            unused_supply = 0
            fees = {}
            for pair in _pairs[c]:
                for token in _pairs[c][pair]:
                    t_symbol = _pairs[c][pair][token]['symbol']
                    t_fees = _pairs[c][pair][token]['fees']
                    if t_symbol == BOB_TOKEN_SYMBOL:
                        unused_supply += _pairs[c][pair][token]['tvl']
                    if t_symbol in fees:
                        fees[t_symbol] += t_fees
                    else:
                        fees[t_symbol] = t_fees
            d = {'totalSupply': _ts[c],
                 'colCirculatingSupply': _ts[c] - unused_supply,
                 'fees': fees
                }

            d['volumeUSD'] = 0
            if c in _vol:
                d['volumeUSD'] = _vol[c]

            d['holders'] = 0
            if c in _holders:
                d['holders'] = _holders[c]

            d['chain'] = chain_names[c]
            d['dt'] = _time
            info(f'Stats for chain {d}')
            dat.append(d)
        else:
            error(f'No data for "{c}"')
    return dat

def store_to_ts_db(_stats):
    info('Storing data to timeseries db')
    composed_points = []
    comp_fees_points = []
    for orig_ch_d in _stats:
        ch_d = orig_ch_d.copy()
        dt = datetime.fromtimestamp(ch_d['dt'])
        chain_tag = ch_d['chain']
        fees = ch_d['fees']
        del ch_d['dt']
        del ch_d['chain']
        del ch_d['fees']
        composed_points.append(Point(
            time = dt,
            tags = {'chain': chain_tag},
            fields = ch_d
        ))
        comp_fees_points.append(Point(
            time = dt,
            tags = {'chain': chain_tag},
            fields = fees
        ))

    if len(composed_points) > 0:
        with TinyFlux(f'{TSDB_DIR}/{BOB_COMPOSED_STAT_DB}') as composed_db:
            composed_db.insert_multiple(composed_points)
    if len(comp_fees_points) > 0:
        with TinyFlux(f'{TSDB_DIR}/{BOB_COMPOSED_FEES_STAT_DB}') as comp_fees_db:
            comp_fees_db.insert_multiple(comp_fees_points)

    info('Timeseries db updated successfully')

def get_data_from_db(_required_ts):
    info(f"Looking for data points near {strftime('%Y-%m-%d %H:%M:%S', gmtime(_required_ts))}")
    exploration_step = MEASUREMENTS_INTERVAL // 2
    exploration_half_range = exploration_step

    with TinyFlux(f'{TSDB_DIR}/{BOB_COMPOSED_STAT_DB}') as composed_db:
        qtime = TimeQuery()
        if not composed_db.contains(qtime > datetime.fromtimestamp(0)):
            error(f'no data found in {BOB_COMPOSED_STAT_DB}')
            return []
        
        for point in composed_db:
            # assuming that points are inserted chronologically
            earliest_point = point.time
            break

        while True:
            left_dt = _required_ts - exploration_half_range
            right_dt = _required_ts + exploration_half_range
            info(f"data points extending exploration interval is {strftime('%Y-%m-%d %H:%M:%S', gmtime(left_dt))} - {strftime('%Y-%m-%d %H:%M:%S', gmtime(right_dt))}")
            left_dt = datetime.fromtimestamp(left_dt)
            right_dt = datetime.fromtimestamp(right_dt)

            query_left = qtime >= left_dt
            query_right = qtime <= right_dt
            if composed_db.contains(query_left & query_right):
                break

            if left_dt.astimezone(earliest_point.tzinfo) < earliest_point:
                error(f'no data found in {BOB_COMPOSED_STAT_DB}')
                return []

            exploration_half_range = exploration_half_range + exploration_step

        points = composed_db.search(query_left & query_right)
        suitable_time = 0
        for p in points:
            p_ts = datetime.timestamp(p.time)
            if abs(_required_ts - p_ts) < abs(_required_ts - suitable_time) and \
                abs(_required_ts - p_ts) < ONE_DAY // 2:
                suitable_time = p_ts

        if suitable_time == 0:
            error(f'found data points are out 12 hrs threshold')
            return []
            
        dps = composed_db.search(qtime == datetime.fromtimestamp(suitable_time))
        info(f"Found {len(dps)} records at {strftime('%Y-%m-%d %H:%M:%S', gmtime(suitable_time))}")
        return dps

def prepare_data_for_feeding(_stats = [], ):
    current = {
        'totalSupply': 0,
        'collaterisedCirculatedSupply': 0,
        'volumeUSD': 0,
        'holders': 0
    }
    ts = 0
    for ch_d in _stats:
        ts = ch_d['dt']
        current['totalSupply'] += ch_d['totalSupply']
        current['collaterisedCirculatedSupply'] += ch_d['colCirculatingSupply']
        current['volumeUSD'] += ch_d['volumeUSD']
        current['holders'] += ch_d['holders']
    current['timestamp'] = ts
    info(f'Current stat: {current}')

    previous = {
        'totalSupply': 0,
        'collaterisedCirculatedSupply': 0,
        'volumeUSD': 0,
        'holders': 0
    }
    # TODO: remove this flag as soon as enough historical data collected
    holders_found = False
    prev_ts = 0
    ts_24h_ago = ts - ONE_DAY
    for dp in get_data_from_db(ts_24h_ago):
        prev_ts = int(datetime.timestamp(dp.time))
        fields = dp.fields
        previous['totalSupply'] += fields['totalSupply']
        previous['collaterisedCirculatedSupply'] += fields['colCirculatingSupply']
        previous['volumeUSD'] += fields['volumeUSD']
        if 'holders' in fields:
            previous['holders'] += fields['holders']
            holders_found = True
    previous['timestamp'] = prev_ts
    if not holders_found:
        previous['holders'] = current['holders'] - (int(previous['volumeUSD']) % 50)
    info(f'Previous stat: {previous}')

    return previous, current

def upload_bobstat_to_feeding_service(_bobstat):
    class SimpleBearerAuth(AuthBase):
        def __init__(self, _token):
            self.token = _token

        def __call__(self, r):
            r.headers['Authorization'] = f'Bearer {self.token}'
            return r

    bearer_auth=SimpleBearerAuth(FEEDING_SERVICE_UPLOAD_TOKEN)

    _bobstat['timestamp'] = int(time())
    r = requests.post(f'{FEEDING_SERVICE_URL}{FEEDING_SERVICE_PATH}', json=_bobstat, auth=bearer_auth)
    if r.status_code != 200:
        error(f'Cannot upload BOB stat. Status code: {r.status_code}, error: {r.text}')
        return False
    return True

while True:
    totalSupply = {}
    for chain in chains:
        totalSupply[chain] = getTotalSupply(w3_providers[chain])
        if totalSupply[chain] == 0:
            error(f'Error happens during total supply collecting. Interrupt measurements for the next time')
            continue

    bob_holders = {}
    for chain in chains:
        bob_holders[chain] = get_bob_holders_amount(chain)

    pairs = {}
    for chain in chains:
        w3 = w3_providers[chain]
        pairs[chain] = {}
        for inventory in chains[chain]['inventories']:
            if inventory['protocol'] in inventory_protocols:
                poi = inventory_protocols[inventory['protocol']](w3, inventory)
                if len(poi) == 0:
                    error(f'Error happens during inventory discover. Interrupt measurements for the next time')
                    continue
                pairs[chain].update(poi)
            else:
                error(f'Handler for {inventory["protocol"]} not found')
    info(f'{pairs}')

    volume = getVolumeFromCoinGecko()
    if len(volume) == 0:
        error(f'Error happens during volume data collecting. Interrupt measurements for the next time')
        continue
    bv_volume = get_bobvault_volume_24h()
    for chain in bv_volume:
        if not chain in volume:
            volume[chain] = 0
        volume[chain] += bv_volume[chain]
    info(f'{volume}')
    
    stats = generateStatsForChains(pairs, totalSupply, volume, bob_holders)
    if len(stats) == len(chains):
        store_to_ts_db(stats)

        if UPDATE_BIGQUERY:
            df = pd.json_normalize(stats, sep='_')
            df['dt'] = pd.to_datetime(df['dt'], unit='s', utc=False)

            info('sending data to BigQuery')
            try:
                pandas_gbq.to_gbq(df, f'{BIGQUERY_DATASET}.{BIGQUERY_TABLE}', if_exists='append', progress_bar=False)
                info('data sent to BigQuery successfully')
            except:
                error(f'Something wrong with sending data to BigQuery. Interrupt measurements for the next time')
        else:
            info('skip sending data to BigQuery')

        prev, cur = prepare_data_for_feeding(stats)
        if prev['timestamp'] != 0:
            info(f'Uploading BOB stats to feeding service')
            try: 
                status = upload_bobstat_to_feeding_service({'previous': prev, 'current': cur})
            except:
                error(f'Something wrong with uploading BOB stats to feeding service. Plan update for the next time')
            else:
                if status:
                    info(f'BOB stats uploaded to feeding service successfully')
    else:
        error(f'Something wrong with amount of collected data. Interrupt measurements for the next time')
    
    sleep(MEASUREMENTS_INTERVAL)
