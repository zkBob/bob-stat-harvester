#!/usr/bin/env python

from os import getenv

from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware

from time import sleep, time, gmtime

from json import load, dump

from logging import basicConfig, getLogger, info, error, debug, warning, WARNING, INFO, DEBUG

basicConfig(level=INFO)

ABI_ERC20 = "abi/erc20.json"
ABI_BOBVAULT = "abi/bobvault.json"

bobVault_addr = Web3.toChecksumAddress("0x25e6505297b44f4817538fb2d91b88e1cf841b54")
BOB_TOKEN_ADDRESS = Web3.toChecksumAddress("0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b")

TOKEN_DEPLOYMENTS_INFO = getenv('TOKEN_DEPLOYMENTS_INFO', 'token-deployments-info.json')
SNAPSHOT_DIR = getenv('SNAPSHOT_DIR', '.')
SNAPSHOT_FILE = getenv('SNAPSHOT_FILE', 'bobvault-snaphsot.json')
FINALIZATION_DELAY = getenv('FINALIZATION_DELAY', 100)
HISTORY_BLOCK_RANGE = getenv('HISTORY_BLOCK_RANGE', 3000)
MEASUREMENTS_INTERVAL = int(getenv('MEASUREMENTS_INTERVAL', 60 * 60 * 2 - 30))
WEB3_RETRY_ATTEMPTS = int(getenv('WEB3_RETRY_ATTEMPTS', 2))
WEB3_RETRY_DELAY = int(getenv('WEB3_RETRY_DELAY', 5))

info(f'TOKEN_DEPLOYMENTS_INFO = {TOKEN_DEPLOYMENTS_INFO}')
info(f'SNAPSHOT_DIR = {SNAPSHOT_DIR}')
info(f'SNAPSHOT_FILE = {SNAPSHOT_FILE}')
info(f'FINALIZATION_DELAY = {FINALIZATION_DELAY}')
info(f'HISTORY_BLOCK_RANGE = {HISTORY_BLOCK_RANGE}')
info(f'MEASUREMENTS_INTERVAL = {MEASUREMENTS_INTERVAL}')
info(f'WEB3_RETRY_ATTEMPTS = {WEB3_RETRY_ATTEMPTS}')
info(f'WEB3_RETRY_DELAY = {WEB3_RETRY_DELAY}')

try:
    with open(f'{TOKEN_DEPLOYMENTS_INFO}') as f:
        chains = load(f)['chains']
except IOError:
    raise BaseException(f'Cannot get {BOB_TOKEN_SYMBOL} deployment info')
    
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
        rpc_response_cache['decimals'][endpoint][_token.address] = make_web3_call(_token.functions.decimals().call)
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
    
vault_c = plg_w3.eth.contract(abi = bobVault_abi, address = bobVault_addr)

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
        
    del snapshot
    
    sleep(MEASUREMENTS_INTERVAL)
