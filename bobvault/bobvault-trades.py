#!/usr/bin/env python

from os import getenv

from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware

from time import sleep, time, gmtime

from json import load, dump

from logging import basicConfig, getLogger, info, error, debug, warning, WARNING, INFO, DEBUG

import requests
from requests.auth import AuthBase

import threading

basicConfig(level=INFO)

ABI_ERC20 = "abi/erc20.json"
ABI_BOBVAULT = "abi/bobvault.json"

ONE_ETHER = 10 ** 18
DEFAULT_SMALL_VALUE = float(0)
DEFAULT_BIG_VALUE = float(10 ** 9)

BOBVAULT_ADDRESS = Web3.toChecksumAddress("0x25e6505297b44f4817538fb2d91b88e1cf841b54")
BOB_TOKEN_ADDRESS = Web3.toChecksumAddress("0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b")

TOKEN_DEPLOYMENTS_INFO = getenv('TOKEN_DEPLOYMENTS_INFO', 'token-deployments-info.json')
SNAPSHOT_DIR = getenv('SNAPSHOT_DIR', '.')
SNAPSHOT_FILE = getenv('SNAPSHOT_FILE', 'bobvault-snaphsot.json')
COINGECKO_SNAPSHOT_FILE = getenv('COINGECKO_SNAPSHOT_FILE', 'bobvault-coingecko-data.json')
FINALIZATION_DELAY = int(getenv('FINALIZATION_DELAY', 100))
HISTORY_BLOCK_RANGE = int(getenv('HISTORY_BLOCK_RANGE', 3000))
MEASUREMENTS_INTERVAL = int(getenv('MEASUREMENTS_INTERVAL', 60 * 60 * 2 - 30))
WEB3_RETRY_ATTEMPTS = int(getenv('WEB3_RETRY_ATTEMPTS', 2))
WEB3_RETRY_DELAY = int(getenv('WEB3_RETRY_DELAY', 5))
FEEDING_SERVICE_URL = getenv('FEEDING_SERVICE_URL', 'http://127.0.0.1:8080')
FEEDING_SERVICE_PATH = getenv('FEEDING_SERVICE_PATH', '/')
FEEDING_SERVICE_HEALTH_PATH = getenv('FEEDING_SERVICE_HEALTH_PATH', '/health')
FEEDING_SERVICE_UPLOAD_TOKEN = getenv('FEEDING_SERVICE_UPLOAD_TOKEN', 'default')
FEEDING_SERVICE_MONITOR_INTERVAL = int(getenv('FEEDING_SERVICE_MONITOR_INTERVAL', 60))
FEEDING_SERVICE_MONITOR_ATTEMPTS_FOR_INFO = int(getenv('FEEDING_SERVICE_MONITOR_ATTEMPTS_FOR_INFO', 60))

if FEEDING_SERVICE_PATH[0] != '/':
    error(f'FEEDING_SERVICE_PATH must start with /')
    raise BaseException(f'Incorrect configuration')
if FEEDING_SERVICE_HEALTH_PATH[0] != '/':
    error(f'FEEDING_SERVICE_HEALTH_PATH must start with /')
    raise BaseException(f'Incorrect configuration')

info(f'TOKEN_DEPLOYMENTS_INFO = {TOKEN_DEPLOYMENTS_INFO}')
info(f'SNAPSHOT_DIR = {SNAPSHOT_DIR}')
info(f'SNAPSHOT_FILE = {SNAPSHOT_FILE}')
info(f'COINGECKO_SNAPSHOT_FILE = {COINGECKO_SNAPSHOT_FILE}')
info(f'FINALIZATION_DELAY = {FINALIZATION_DELAY}')
info(f'HISTORY_BLOCK_RANGE = {HISTORY_BLOCK_RANGE}')
info(f'MEASUREMENTS_INTERVAL = {MEASUREMENTS_INTERVAL}')
info(f'WEB3_RETRY_ATTEMPTS = {WEB3_RETRY_ATTEMPTS}')
info(f'WEB3_RETRY_DELAY = {WEB3_RETRY_DELAY}')
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
except IOError:
    raise BaseException(f'Cannot get BOB token deployment info')
    
if 'vault' in chains['pol']:
    start_block = chains['pol']['vault']['start_block'] # 36750276
else:
    raise BaseException(f'Cannot find vault related info in the deployment specs')
    
def load_abi(_file):
    try:
        with open(_file) as f:
            abi = load(f)
    except IOError:
        raise BaseException(f'Cannot read {_file}')
    info(f'{_file} loaded')
    return abi

erc20_abi = load_abi(ABI_ERC20)
bobVault_abi = load_abi(ABI_BOBVAULT)

plg_w3 = Web3(HTTPProvider(chains['pol']['rpc']['url']))
plg_w3.middleware_onion.inject(geth_poa_middleware, layer=0)

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

def get_token_decimals_by_token_str(_token, _w3 = plg_w3):
    global rpc_response_cache
    endpoint = _w3.provider.endpoint_uri
    if ('decimals' in rpc_response_cache) and \
       (endpoint in rpc_response_cache['decimals']) and \
       (_token in rpc_response_cache['decimals'][endpoint]):
        return rpc_response_cache['decimals'][endpoint][_token]
    else:
        token_contract = _w3.eth.contract(abi = erc20_abi, address = _token)
        return get_token_decimals(token_contract)
    
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

def get_token_symbol_by_token_str(_token, _w3 = plg_w3):
    global rpc_response_cache
    endpoint = _w3.provider.endpoint_uri
    if ('symbols' in rpc_response_cache) and \
       (endpoint in rpc_response_cache['symbols']) and \
       (_token in rpc_response_cache['symbols'][endpoint]):
        return rpc_response_cache['symbols'][endpoint][_token]
    else:
        token_contract = _w3.eth.contract(abi = erc20_abi, address = _token)
        return get_token_symbol(token_contract)

def get_collateral_info_by_token_str(_token, _bn = -1):
    global rpc_response_cache
    if not 'col_info' in rpc_response_cache: 
        rpc_response_cache['col_info'] = {}
    endpoint = vault_c.web3.provider.endpoint_uri
    if not endpoint in rpc_response_cache['col_info']:
        rpc_response_cache['col_info'][endpoint] = {}
    if not _token in rpc_response_cache['col_info'][endpoint]:
        info(f'Getting collateral info for {_token} to cache')
        if _bn == -1:
            resp = make_web3_call(vault_c.functions.collateral(_token).call)
        else:
            resp = make_web3_call(vault_c.functions.collateral(_token).call, block_identifier=_bn)
        info(f'Collateral info is: balance {resp[0]}, buffer {resp[1]}, dust {resp[2]}, yield {resp[3]}, price {resp[4]}, inFee {resp[5]}, outFee {resp[6]}')
        rpc_response_cache['col_info'][endpoint][_token] = resp
    return rpc_response_cache['col_info'][endpoint][_token]

def reset_cache_for_collateral_info():
    if 'col_info' in rpc_response_cache:
        info(f'reset cache for collateral info')
        del rpc_response_cache['col_info']

def get_bob_balance(_owner, _bn = -1, _w3 = plg_w3):
    global rpc_response_cache
    if not 'bob_balance' in rpc_response_cache: 
        rpc_response_cache['bob_balance'] = {}
    endpoint = vault_c.web3.provider.endpoint_uri
    if not endpoint in rpc_response_cache['bob_balance']:
        rpc_response_cache['bob_balance'][endpoint] = {}
    if not _owner in rpc_response_cache['bob_balance'][endpoint]:
        info(f'Getting bob balance of {_owner} to cache')
        token_contract = _w3.eth.contract(abi = erc20_abi, address = BOB_TOKEN_ADDRESS)
        if _bn == -1:
            resp = make_web3_call(token_contract.functions.balanceOf(_owner).call)
        else:
            resp = make_web3_call(token_contract.functions.balanceOf(_owner).call, block_identifier=_bn)
        info(f'Balance is {resp}')
        rpc_response_cache['bob_balance'][endpoint][_owner] = resp
    return rpc_response_cache['bob_balance'][endpoint][_owner]

def reset_cache_for_bob_balance():
    if 'bob_balance' in rpc_response_cache:
        info(f'reset cache for bob balance')
        del rpc_response_cache['bob_balance']
    
vault_c = plg_w3.eth.contract(abi = bobVault_abi, address = BOBVAULT_ADDRESS)

buy_event_filter = vault_c.events.Buy.build_filter()
buyTopic = buy_event_filter.event_topic
sell_event_filter = vault_c.events.Sell.build_filter()
sellTopic = sell_event_filter.event_topic
swap_event_filter = vault_c.events.Swap.build_filter()
swapTopic = swap_event_filter.event_topic

def normalise_amount(amount, token):
    decimals = get_token_decimals_by_token_str(token, plg_w3)
    return amount / (10 ** decimals)

def buy_decoder(l):
    pl = vault_c.events.Buy().processLog(l)
    return pl, {
                "inToken": pl.args.token,
                "outToken": BOB_TOKEN_ADDRESS,
                "amountIn": normalise_amount(pl.args.amountIn, pl.args.token),
                "amountOut": normalise_amount(pl.args.amountOut, BOB_TOKEN_ADDRESS)
               }

def sell_decoder(l):
    pl = vault_c.events.Sell().processLog(l)
    return pl, {
                "inToken": BOB_TOKEN_ADDRESS,
                "outToken": pl.args.token,
                "amountIn": normalise_amount(pl.args.amountIn, BOB_TOKEN_ADDRESS),
                "amountOut": normalise_amount(pl.args.amountOut, pl.args.token)
               }

def swap_decoder(l):
    pl = vault_c.events.Swap().processLog(l)
    return pl, {
                "inToken": pl.args.inToken,
                "outToken": pl.args.outToken,
                "amountIn": normalise_amount(pl.args.amountIn, pl.args.inToken),
                "amountOut": normalise_amount(pl.args.amountOut, pl.args.outToken)
               }

with_events = {
    buyTopic: (buy_decoder, 'Buy'),
    sellTopic: (sell_decoder, 'Sell'),
    swapTopic: (swap_decoder, 'Swap'),
}

def process_log(l):
    event_topic = l.topics[0]
    event_handler = with_events[event_topic][0]
    event_name = with_events[event_topic][1]
    pl, pl_args = event_handler(l)
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
    l["timestamp"] = make_web3_call(plg_w3.eth.get_block, blockhash).timestamp
    if not "timestamp" in l:
        raise BaseException(f'Timestamp cannot be set for block {blockhash}')
    return l

def get_logs(efilters, from_block, to_block):
    info(f'Looking for events within [{from_block}, {to_block}]')
    logs = []
    for b in range(from_block, to_block, HISTORY_BLOCK_RANGE+1):
        start_block = b
        finish_block = min(b + HISTORY_BLOCK_RANGE, to_block)
        if (from_block != start_block) or (to_block != finish_block):
            info(f'Looking for events within a smaler range [{start_block}, {finish_block}]')
        vault_logs = []
        for efilter in efilters:
            bss_logs = make_web3_call(plg_w3.eth.getLogs, {'fromBlock': start_block, 
                                                           'toBlock': finish_block, 
                                                           'address': efilter.address, 
                                                           'topics': efilter.topics})
            len_bss_logs = len(bss_logs)
            info(f"Found {len_bss_logs} of {efilter.event_abi['name']} events")
            if len_bss_logs > 0:
                 vault_logs.extend(bss_logs)
        logs.extend([process_log(l) for l in vault_logs])
    info(f'Collected {len(logs)} events')
    return logs

def gc_encode(obj):
    if type(obj) == int:
        return str(obj)
    if type(obj) == float:
        return str(obj)
    return obj

def bobvault_data_for_coingecko(snapshot, ts_start, ts_end):
    logs = snapshot['logs']
    max_log_index = 0
    cg_data = {}
    for trade in logs:
        if trade['name'] == 'Swap':
            token1 = trade['args']['inToken']
            token2 = trade['args']['outToken']
            if token1 < token2:
                action_type = 'buy'
                base = token1
                target = token2
                base_volume = trade['args']['amountIn']
                target_volume = trade['args']['amountOut']
            else:
                action_type = 'sell'
                base = token2
                target = token1
                base_volume = trade['args']['amountOut']
                target_volume = trade['args']['amountIn']
        elif trade['name'] == 'Buy':
            # BOB is base, another stable is target: user sells target for base
            action_type = 'sell'
            base = trade['args']['outToken']
            target = trade['args']['inToken']
            base_volume = trade['args']['amountOut']
            target_volume = trade['args']['amountIn']
        elif trade['name'] == 'Sell':
            # BOB is base, another stable is target: user buys target for base
            action_type = 'buy'
            base = trade['args']['inToken']
            target = trade['args']['outToken']
            base_volume = trade['args']['amountIn']
            target_volume = trade['args']['amountOut']
        base_sym = get_token_symbol_by_token_str(base)
        target_sym = get_token_symbol_by_token_str(target)
        ticker_id = f'{base_sym}_{target_sym}'

        if trade['logIndex'] > max_log_index:
            max_log_index = trade['logIndex']
        
        if not ticker_id in cg_data:
            cg_data[ticker_id] = {
                'pool_id': f'bobvault_polygon',
                'base_address': base,
                'target_address': target,                
                'base_currency': base_sym,
                'target_currency': target_sym,
                'timestamp': 0, # receive from timestamp of the last_block
                'last_price': 0, # receive from the last trade
                'base_volume': 0.0,
                'target_volume': 0.0,
                'bid': 0, # receive from bobvault based on fees
                'ask': 0, # receive from bobvault based on fees
                'high': 0,
                'low': 0,
                'high_buy': DEFAULT_SMALL_VALUE,
                'high_sell': DEFAULT_SMALL_VALUE,
                'low_buy': DEFAULT_BIG_VALUE,
                'low_sell': DEFAULT_BIG_VALUE,
                'orderbook': {'bids': [[gc_encode(0.0), gc_encode(0.0)]],
                              'asks': [[gc_encode(0.0), gc_encode(0.0)]]}, # for BOB bids receive from bobvault
                'trades': {'buy': [], 'sell': []}
            }
            info(ticker_id)
        price = target_volume / base_volume
            
        if (trade['timestamp'] >= ts_start) and (trade['timestamp'] < ts_end):
            cg_data[ticker_id]['base_volume'] += base_volume
            cg_data[ticker_id]['target_volume'] += target_volume
            if action_type == 'buy':
                if price > cg_data[ticker_id]['high_buy']:
                    cg_data[ticker_id]['high_buy'] = price
                if price < cg_data[ticker_id]['low_buy']:
                    cg_data[ticker_id]['low_buy'] = price
            if action_type == 'sell':
                if price > cg_data[ticker_id]['high_sell']:
                    cg_data[ticker_id]['high_sell'] = price
                if price < cg_data[ticker_id]['low_sell']:
                    cg_data[ticker_id]['low_sell'] = price
        xtrade = {
            'trade_id': (trade['blockNumber'] - snapshot['start_block']) * (max_log_index + 1) + trade['logIndex'],
            'price' : gc_encode(price),
            'base_volume': gc_encode(base_volume),
            'target_volume': gc_encode(target_volume),
            'trade_timestamp': gc_encode(trade['timestamp']),
            'type': action_type
        }

        cg_data[ticker_id]['trades'][action_type].append(xtrade)

    if len(cg_data) > 0:
        one_timestamp = gc_encode(make_web3_call(plg_w3.eth.get_block, snapshot['last_block']).timestamp)
#         one_timestamp = int(time())

    for ticker_id in cg_data:
        cg_data[ticker_id]['timestamp'] = one_timestamp
        cg_data[ticker_id]['base_volume'] = gc_encode(cg_data[ticker_id]['base_volume'])
        cg_data[ticker_id]['target_volume'] = gc_encode(cg_data[ticker_id]['target_volume'])

        # BOB is base, another stable is target: sell target for base
        # BOB is base, another stable is target: buy target for base
        # 'high_buy': 0.9999000016536099,
        # 'high_sell': 1.0000800064005122,
        # 'low_buy': 0.9998999989355484,
        # 'low_sell': 1.000079993223124
        if cg_data[ticker_id]['high_sell'] != DEFAULT_SMALL_VALUE:
            cg_data[ticker_id]['high'] = cg_data[ticker_id]['high_sell']
        elif cg_data[ticker_id]['high_buy'] != DEFAULT_SMALL_VALUE:
            cg_data[ticker_id]['high'] = cg_data[ticker_id]['high_buy']
        cg_data[ticker_id]['high'] = gc_encode(cg_data[ticker_id]['high'])
        if cg_data[ticker_id]['low_buy'] != DEFAULT_BIG_VALUE:
            cg_data[ticker_id]['low'] = cg_data[ticker_id]['low_buy']
        elif cg_data[ticker_id]['low_sell'] != DEFAULT_BIG_VALUE:
            cg_data[ticker_id]['low'] = cg_data[ticker_id]['low_sell']
        cg_data[ticker_id]['low'] = gc_encode(cg_data[ticker_id]['low'])
        del cg_data[ticker_id]['high_sell']
        del cg_data[ticker_id]['high_buy']
        del cg_data[ticker_id]['low_sell']
        del cg_data[ticker_id]['low_buy']

        col_info = get_collateral_info_by_token_str(cg_data[ticker_id]['target_address'], snapshot['last_block'])
        token2_one = 10 ** get_token_decimals_by_token_str(cg_data[ticker_id]['target_address'], plg_w3)
        token2_balance = normalise_amount(col_info[0], cg_data[ticker_id]['target_address'])
        token2_price = col_info[4]
        token2_inFee = col_info[5]
        token2_outFee = col_info[6]        
        if cg_data[ticker_id]['base_address'] == BOB_TOKEN_ADDRESS:
            bal = get_bob_balance(BOBVAULT_ADDRESS, snapshot['last_block'])
            token1_balance = normalise_amount(bal, BOB_TOKEN_ADDRESS)
            token1_one = 10 ** get_token_decimals_by_token_str(BOB_TOKEN_ADDRESS, plg_w3)
            cg_data[ticker_id]['orderbook']['bids'][0][1] = 1 - (token1_one / ONE_ETHER * token2_price / token2_one * token2_outFee / ONE_ETHER)
            cg_data[ticker_id]['orderbook']['asks'][0][1] = token1_one / token2_one * token2_price / (ONE_ETHER - token2_inFee)
        else:
            col_info = get_collateral_info_by_token_str(cg_data[ticker_id]['base_address'], snapshot['last_block'])
            token1_one = 10 ** get_token_decimals_by_token_str(cg_data[ticker_id]['base_address'], plg_w3)
            token1_balance = normalise_amount(col_info[0], cg_data[ticker_id]['base_address'])
            token1_price = col_info[4]
            token1_inFee = col_info[5]
            token1_outFee = col_info[6]
            
            cg_data[ticker_id]['orderbook']['bids'][0][1] = token2_one / (token1_one * (1 - token2_inFee / ONE_ETHER) * token1_price * (1 - token1_outFee / ONE_ETHER) / token2_price)
            cg_data[ticker_id]['orderbook']['asks'][0][1] = token1_one / (token2_one * (1 - token1_inFee / ONE_ETHER) * token2_price * (1 - token2_outFee / ONE_ETHER) / token1_price)

        del cg_data[ticker_id]['base_address']
        del cg_data[ticker_id]['target_address']
            
        cg_data[ticker_id]['orderbook']['bids'][0][0] = gc_encode(token2_balance)
        cg_data[ticker_id]['orderbook']['bids'][0][1] = gc_encode(cg_data[ticker_id]['orderbook']['bids'][0][1])
        cg_data[ticker_id]['orderbook']['asks'][0][0] = gc_encode(token1_balance)
        cg_data[ticker_id]['orderbook']['asks'][0][1] = gc_encode(cg_data[ticker_id]['orderbook']['asks'][0][1])

        cg_data[ticker_id]['bid'] = cg_data[ticker_id]['orderbook']['bids'][0][1]
        cg_data[ticker_id]['ask'] = cg_data[ticker_id]['orderbook']['asks'][0][1]

        if len(cg_data[ticker_id]['trades']['buy']) > 0:
            if len(cg_data[ticker_id]['trades']['sell']) > 0:
                if cg_data[ticker_id]['trades']['buy'][-1]['trade_id'] > cg_data[ticker_id]['trades']['sell'][-1]['trade_id']:
                    cg_data[ticker_id]['last_price'] = cg_data[ticker_id]['trades']['buy'][-1]['price']
                else:
                    cg_data[ticker_id]['last_price'] = cg_data[ticker_id]['trades']['sell'][-1]['price']
            else:
                cg_data[ticker_id]['last_price'] = cg_data[ticker_id]['trades']['buy'][-1]['price']
        elif len(cg_data[ticker_id]['trades']['sell']) > 0:
            cg_data[ticker_id]['last_price'] = cg_data[ticker_id]['trades']['sell'][-1]['price']

    cg_data['timestamp'] = int(time())
    return cg_data

def upload_coingecko_data_to_feeding_service(cg):
    class SimpleBearerAuth(AuthBase):
        def __init__(self, _token):
            self.token = _token

        def __call__(self, r):
            r.headers['Authorization'] = f'Bearer {self.token}'
            return r

    bearer_auth=SimpleBearerAuth(FEEDING_SERVICE_UPLOAD_TOKEN)

    r = requests.post(f'{FEEDING_SERVICE_URL}{FEEDING_SERVICE_PATH}', json=cg, auth=bearer_auth)
    if r.status_code != 200:
        error(f'Cannot upload CG data. Status code: {r.status_code}, error: {r.text}')

monitor_feedback_counter = 0

def monitor_feeding_service():
    global monitor_feedback_counter
    if monitor_feedback_counter > FEEDING_SERVICE_MONITOR_ATTEMPTS_FOR_INFO:
        info(f'Checking feeding service for data availability')
        monitor_feedback_counter = 0
    else:
        monitor_feedback_counter += 1

    try:
        r = requests.get(f'{FEEDING_SERVICE_URL}{FEEDING_SERVICE_HEALTH_PATH}')
    except IOError as e :
        error(f'Cannot upload get feeding service health status: {e}')
    except ValueError as e :
        error(f'Cannot upload get feeding service health status: {e}')
    else:
        if r.status_code != 200:
            error(f'Cannot upload CG data. Status code: {r.status_code}, error: {r.text}')
        resp = r.json()
        if resp['BobVault']['polygon']['status'] == 'error' and \
            resp['BobVault']['polygon']['lastSuccessTimestamp'] == 0 and \
            resp['BobVault']['polygon']['lastErrorTimestamp'] == 0:
                warning(f'No data on the feeding service')
                try:
                    with open(f'{SNAPSHOT_DIR}/{COINGECKO_SNAPSHOT_FILE}', 'r') as json_file:
                        cg = load(json_file)
                except IOError:
                    error(f'No snapshot {COINGECKO_SNAPSHOT_FILE} found')
                else:
                    info(f'Uploading data to feeding service')
                    try: 
                        upload_coingecko_data_to_feeding_service(cg)
                    except:
                        error(f'Something wrong with uploading data to feeding service. Plan update for the next time')
                    else:
                        info(f'Data uploaded to feeding service successfully')
            
# Taken from https://stackoverflow.com/questions/474528/what-is-the-best-way-to-repeatedly-execute-a-function-every-x-seconds/49801719#49801719
def every(delay, task):
    first_time = True
    next_time = time() + delay
    while True:
        if not first_time:
            sleep(max(0, next_time - time()))
        else:
            first_time = False
        task()
        next_time += (time() - next_time) // delay * delay + delay

bg_task = threading.Thread(target=lambda: every(FEEDING_SERVICE_MONITOR_INTERVAL, monitor_feeding_service))
bg_task.daemon = True
bg_task.start()

while True:
    try:
        with open(f'{SNAPSHOT_DIR}/{SNAPSHOT_FILE}', 'r') as json_file:
            snapshot = load(json_file)
    except IOError:
        info(f'No snapshot {SNAPSHOT_FILE} found')
        last_block = make_web3_call(plg_w3.eth.getBlock, 'latest').number - FINALIZATION_DELAY
        info(f'Initialize empty structure for snapshot with the block range {start_block} - {last_block}')
        snapshot = {
            "start_block": start_block,
            "last_block": last_block,
            "logs": []
        }

    if len(snapshot['logs']) != 0:
        info(f'Identifying dump range to extend existing snapshot')
        dump_range = (snapshot['last_block'] + 1, make_web3_call(plg_w3.eth.getBlock, 'latest').number - FINALIZATION_DELAY)
        info(f'Dump range: {dump_range[0]} - {dump_range[1]}')
    else:
        dump_range = (start_block, last_block)

    try:
        logs = get_logs([buy_event_filter, sell_event_filter, swap_event_filter], dump_range[0], dump_range[1])
    except:
        error(f'Cannot collect new logs. Interrupt stats collecting for the next time')
    else:
        snapshot['logs'].extend(logs)
        snapshot['last_block'] = dump_range[1]

    info(f'Saving snapshot')
    with open(f'{SNAPSHOT_DIR}/{SNAPSHOT_FILE}', 'w') as json_file:
        dump(snapshot, json_file)

    ## start of part related to uploading CG data
    now = int(time())
    now_minus_24h = int(now - (24 * 60 * 60))
    info(f'Transform snapshot for usage by CG (24h interval: {now_minus_24h} - {now})')
    cg = bobvault_data_for_coingecko(snapshot, now_minus_24h, now)
    info(f'Saving CG data snapshot')
    with open(f'{SNAPSHOT_DIR}/{COINGECKO_SNAPSHOT_FILE}', 'w') as json_file:
        dump(cg, json_file)
    info(f'Uploading data to feeding service')
    try: 
        upload_coingecko_data_to_feeding_service(cg)
    except:
        error(f'Something wrong with uploading data to feeding service. Plan update for the next time')
    else:
        info(f'Data uploaded to feeding service successfully')
    
    reset_cache_for_collateral_info()
    reset_cache_for_bob_balance()

    del cg
    ## end of part related to uploading CG data

    del snapshot
    
    sleep(MEASUREMENTS_INTERVAL)
