from os import getenv

from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware

from time import sleep, time, gmtime
from datetime import datetime

from json import load, dump

from tinyflux import TinyFlux, Point

from logging import basicConfig, getLogger, info, error, debug, warning, WARNING, INFO, DEBUG

fromtimestamp = datetime.fromtimestamp

import threading

basicConfig(level=INFO)

ABI_ERC20 = "abi/erc20.json"

BOB_TOKEN_ADDRESS = Web3.toChecksumAddress("0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b")
ZERO_ACCOUNT = Web3.toChecksumAddress("0x0000000000000000000000000000000000000000")

TOKEN_DEPLOYMENTS_INFO = getenv('TOKEN_DEPLOYMENTS_INFO', 'token-deployments-info.json')
SNAPSHOT_DIR = getenv('SNAPSHOT_DIR', '.')
SNAPSHOT_FILE_SUFFIX = getenv('SNAPSHOT_FILE_SUFFIX', 'bob-holders-snaphsot.json')
HISTORY_BLOCK_RANGE = getenv('HISTORY_BLOCK_RANGE', 3000)
DEFAULT_MEASUREMENTS_INTERVAL = int(getenv('DEFAULT_MEASUREMENTS_INTERVAL', 1))
THREADS_LIVENESS_INTERVAL = int(getenv('THREADS_LIVENESS_INTERVAL', 60))
WEB3_RETRY_ATTEMPTS = int(getenv('WEB3_RETRY_ATTEMPTS', 2))
WEB3_RETRY_DELAY = int(getenv('WEB3_RETRY_DELAY', 5))
TSDB_DIR = getenv('TSDB_DIR', '.')
TSDB_FILE_SUFFIX = getenv('TSDB_FILE_SUFFIX', 'bob-transfers.csv')

info(f'TOKEN_DEPLOYMENTS_INFO = {TOKEN_DEPLOYMENTS_INFO}')
info(f'SNAPSHOT_DIR = {SNAPSHOT_DIR}')
info(f'SNAPSHOT_FILE_SUFFIX = {SNAPSHOT_FILE_SUFFIX}')
info(f'HISTORY_BLOCK_RANGE = {HISTORY_BLOCK_RANGE}')
info(f'DEFAULT_MEASUREMENTS_INTERVAL = {DEFAULT_MEASUREMENTS_INTERVAL}')
info(f'THREADS_LIVENESS_INTERVAL = {THREADS_LIVENESS_INTERVAL}')
info(f'WEB3_RETRY_ATTEMPTS = {WEB3_RETRY_ATTEMPTS}')
info(f'WEB3_RETRY_DELAY = {WEB3_RETRY_DELAY}')
info(f'TSDB_DIR = {TSDB_DIR}')
info(f'TSDB_FILE_SUFFIX = {TSDB_FILE_SUFFIX}')

try:
    with open(f'{TOKEN_DEPLOYMENTS_INFO}') as f:
        chains = load(f)['chains']
except IOError:
    raise BaseException(f'Cannot get {BOB_TOKEN_ADDRESS} deployment info')

def load_abi(_file):
    try:
        with open(_file) as f:
            abi = load(f)
    except IOError:
        raise BaseException(f'Cannot read {_file}')
    info(f'{_file} loaded')
    return abi

erc20_abi = load_abi(ABI_ERC20)

chain_names = {}
w3_providers = {}
for chainid in chains:
    chain_names[chainid] = chains[chainid]['name']
    
    url = chains[chainid]['rpc']['url']
    w3_providers[chainid] = {
        'w3': None,
        'finalization': 0,
        'token': {
            'cnt': None,
            'efilter': None,
            'handler': None
        }
    }
    w3_providers[chainid]['w3'] = Web3(HTTPProvider(url))
    w3_providers[chainid]['finalization'] = chains[chainid]['finalization']
    if chainid != 'eth':
        w3_providers[chainid]['w3'].middleware_onion.inject(geth_poa_middleware, layer=0)
    w3_providers[chainid]['token']['cnt'] = w3_providers[chainid]['w3'].eth.contract(abi = erc20_abi, address = BOB_TOKEN_ADDRESS)
    w3_providers[chainid]['token']['efilter'] = w3_providers[chainid]['token']['cnt'].events.Transfer.build_filter()
    w3_providers[chainid]['token']['handler'] = w3_providers[chainid]['token']['cnt'].events.Transfer().processLog
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
        debug(f'Getting decimals for {_token.address} to cache')
        resp = make_web3_call(_token.functions.decimals().call)
        debug(f'Decimals {resp}')
        rpc_response_cache['decimals'][endpoint][_token.address] = resp
    return rpc_response_cache['decimals'][endpoint][_token.address]

def normalise_amount(_amount, _token):
    decimals = get_token_decimals(_token)
    return _amount / (10 ** decimals)

blocks_cache = {}

def get_ts_by_blockhash(_w3, _bh):
    global blocks_cache
    endpoint = _w3.provider.endpoint_uri
    if not endpoint in blocks_cache:
        blocks_cache[endpoint] = {}
    if not _bh in blocks_cache[endpoint]:
        debug(f'Getting timestamp for {_bh} to cache')
        resp = make_web3_call(_w3.eth.get_block, _bh).timestamp
        debug(f'Timestamp {resp}')
        blocks_cache[endpoint][_bh] = resp
    return blocks_cache[endpoint][_bh]

def read_balances_snapshot_for_chain(_chain):
    try:
        with open(f'{SNAPSHOT_DIR}/{_chain}-{SNAPSHOT_FILE_SUFFIX}', 'r') as json_file:
            snapshot = load(json_file)
    except IOError:
        info(f'No snapshot {_chain}-{SNAPSHOT_FILE_SUFFIX} found')
        snapshot = {}
        start_block = int(chains[_chain]['token']['start_block'])
        last_block = start_block - 1
        info(f'Initialize empty structure for snapshot for {_chain}')
        snapshot = {
            "start_block": start_block,
            "last_block": last_block,
            "balances": {}
        }
    return snapshot

def process_log(_token, _event):
    pl = _token['handler'](_event)
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
            'amount': normalise_amount(pl.args['value'], _token['cnt'])
        }
    }
    l['timestamp'] = get_ts_by_blockhash(_token['cnt'].web3, blockhash)
    return l

def get_logs(_token, _from_block, _to_block):
    info(f'Assumiing to looking for events within [{_from_block}, {_to_block}]')
    start_block = _from_block
    finish_block = min(start_block + HISTORY_BLOCK_RANGE, _to_block)
    if finish_block != _to_block:
        info(f'Looking for events within a smaler range [{start_block}, {finish_block}]')
    events = make_web3_call(_token['cnt'].web3.eth.getLogs, {'fromBlock': start_block, 
                                                             'toBlock': finish_block, 
                                                             'address': _token['efilter'].address, 
                                                             'topics': _token['efilter'].topics})
    len_events = len(events)
    info(f"Found {len_events} of {_token['efilter'].event_abi['name']} events")
    if len_events > 0:
        logs = [process_log(_token, e) for e in events]
    else:
        logs = []
        
    return finish_block, logs

def change_balance(_balances, _transfer):
    if _transfer['from'] != ZERO_ACCOUNT:
        prev_balance = 0
        if _transfer['from'] in _balances:
            prev_balance = _balances[_transfer['from']]
        _balances[_transfer['from']] = prev_balance - _transfer['value']
        if _balances[_transfer['from']] == 0:
            del _balances[_transfer['from']]

    if _transfer['to'] != ZERO_ACCOUNT:
        prev_balance = 0
        if _transfer['to'] in _balances:
            prev_balance = _balances[_transfer['to']]
        _balances[_transfer['to']] = prev_balance + _transfer['value']
        if _balances[_transfer['to']] == 0:
            del _balances[_transfer['to']]

def write_balances_snapshot_for_chain(_chain, _snapshot):
    with open(f'{SNAPSHOT_DIR}/{_chain}-{SNAPSHOT_FILE_SUFFIX}', 'w') as json_file:
        dump(_snapshot, json_file)

def discover_balance_updates(_chain):
    info(f'Reading snapshot for "{_chain}"')
    snapshot = read_balances_snapshot_for_chain(_chain)

    info(f'Identifying dump range to extend existing snapshot')
    dump_range = (snapshot['last_block'] + 1, 
                  make_web3_call(w3_providers[_chain]['w3'].eth.getBlock, 'latest').number - w3_providers[_chain]['finalization'])
    info(f'Dump range: {dump_range[0]} - {dump_range[1]}')

    try:
        new_last_block, logs = get_logs(w3_providers[_chain]['token'], dump_range[0], dump_range[1])
    except:
        raise BaseException(f'Cannot collect new logs')
    head_achieved = dump_range[1] == new_last_block

    storages_updated = False
    if len(logs) > 0:
        points = []
        for log in logs:
            points.append(Point(
                time = datetime.fromtimestamp(log['timestamp']),
                tags = log['tags'],
                fields = log['fields']
            ))

            change_balance(
                snapshot['balances'],
                {'from': log['tags']['from'],
                 'to': log['tags']['to'],
                 'value': log['fields']['amount']
                }
            )

        info(f'Storing {len(logs)} from "{_chain}" to timeseries db')
        with TinyFlux(f'{TSDB_DIR}/{_chain}-{TSDB_FILE_SUFFIX}') as tsdb:
            tsdb.insert_multiple(points)
        
        storages_updated = True

    snapshot['last_block'] = new_last_block
    info(f'Updating snapshot for "{_chain}" with new last block {new_last_block}')
    write_balances_snapshot_for_chain(_chain, snapshot)
        
    return storages_updated, head_achieved

def pull_data_from_chain(_chain):
    head_achieved = False
    while True:
        first_time = True
        while not head_achieved:
            if not first_time:
                sleep(DEFAULT_MEASUREMENTS_INTERVAL)
            else:
                first_time = False
            head_achieved = discover_balance_updates(_chain)[1]
        info('historical events received, reducing pulling frequency')
        delay = chains[_chain]['events_pull_interval']
        next_time = time() + delay
        while head_achieved:
            sleep(max(0, next_time - time()))
            head_achieved = discover_balance_updates(_chain)[1]
            next_time += (time() - next_time) // delay * delay + delay
        info('more historical events discovered, increasing pulling frequency')

scheduled_tasks = {}
while True:
    stopped = {}
    if len(scheduled_tasks) > 0:
        sleep(THREADS_LIVENESS_INTERVAL)
        for chainid in scheduled_tasks:
            if not scheduled_tasks[chainid].is_alive():
                warning(f'THREADS MONITORING: Polling thread for {chainid} is not alive')
                stopped[chainid] = True
        if len(stopped) == len(scheduled_tasks):
            error('THREADS MONITORING: All threads stopped. Exiting')
            break
    else:
        for chainid in chains:
            stopped[chainid] = True
    for chainid in stopped:
        info(f'THREADS MONITORING: Restarting thread for {chainid}')
        scheduled_tasks[chainid] = threading.Thread(target=lambda: pull_data_from_chain(chainid))
        scheduled_tasks[chainid].daemon = True
        scheduled_tasks[chainid].start()