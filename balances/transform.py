from os import getenv

from web3 import Web3

import os.path
import requests
from json import load

from tinyflux import TinyFlux, TagQuery, Point

from time import sleep, time

from decimal import Decimal

from logging import basicConfig, info, error, debug, warning, WARNING, INFO, DEBUG

import threading

basicConfig(level=INFO)

TSDB_DIR = getenv('TSDB_DIR', 'tsdb')
TSDB_FILE_SUFFIX = getenv('TSDB_FILE_SUFFIX', 'bob-transfers.csv')
TOKEN_DEPLOYMENTS_INFO = getenv('TOKEN_DEPLOYMENTS_INFO', 'token-deployments-info.json')
REQUESTS_IN_BATCH = int(getenv('REQUESTS_IN_BATCH', 50))
LIST_OF_CHAINS = getenv('LIST_OF_CHAINS', 'bsc')
THREADS_LIVENESS_INTERVAL = int(getenv('THREADS_LIVENESS_INTERVAL', 60))

info(f'TOKEN_DEPLOYMENTS_INFO = {TOKEN_DEPLOYMENTS_INFO}')
info(f'TSDB_DIR = {TSDB_DIR}')
info(f'TSDB_FILE_SUFFIX = {TSDB_FILE_SUFFIX}')
info(f'REQUESTS_IN_BATCH = {REQUESTS_IN_BATCH}')
info(f'LIST_OF_CHAINS = {LIST_OF_CHAINS}')
info(f'THREADS_LIVENESS_INTERVAL = {THREADS_LIVENESS_INTERVAL}')

NOSENSE_AMOUNT = 0
BOB_TOKEN_ADDRESS = Web3.toChecksumAddress("0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b")
#months = ['202209', '202210', '202211', '202212', '202301', '202302']
months = ['202209', '202210', '202211', '202212', '202301']
#months = ['202302']

Tags = TagQuery()

req_chains = LIST_OF_CHAINS.split()

try:
    with open(f'{TOKEN_DEPLOYMENTS_INFO}') as f:
        chains = load(f)['chains']
        for _c in req_chains:
            if not _c in chains:
                raise BaseException(f'{_c} is not in the chains list')
except IOError:
    raise BaseException(f'Cannot get {BOB_TOKEN_ADDRESS} deployment info')

def to_1bln_base(_value):
    one_bln = Decimal(10 ** 9)
    _val = Decimal(_value)
    a0 = _val - ((_val // one_bln) * one_bln)
    v_tmp = (_val - a0) // one_bln 
    a1 = v_tmp - ((v_tmp // one_bln) * one_bln)
    v_tmp = (v_tmp - a1) // one_bln 
    a2 = v_tmp - ((v_tmp // one_bln) * one_bln)
    v_tmp = (v_tmp - a2) // one_bln
    a3 = v_tmp - ((v_tmp // one_bln) * one_bln)
    return int(a3), int(a2), int(a1), int(a0)

def get_receipts_for_batch(_provider_url: str, _batch: list) -> list:
    json_data = []
    for i in range(len(_batch)):
        json_data.append({"jsonrpc": "2.0",
                          "id": 1337 + i,
                          "method": "eth_getTransactionReceipt",
                          "params": [_batch[i]]
                         })
    r = requests.post(_provider_url, json=json_data)
    r.raise_for_status()
    receipts = []
    # info(f"{len(r.json())}"+", "+f"{r.json()}"[:50])
    for resp in r.json():
        # info(f"{resp['id']}"+", " +f"{resp['result']}"[:50])
        if resp['result'] != 'None':
            receipts.append(resp['result'])
        else:
            error(f'Response {len(receipts)}: {resp}')
    if len(receipts) != len(_batch): 
        raise BaseException(f'Number of receipts ({len(receipts)}) does not equal number of requests ({len(_batch)})')
    return receipts

def match_receipts_with_txs(_receipts: list, _txs: dict):
    for receipt in _receipts:
        txhash = receipt['transactionHash']
        if _txs[txhash]:
            raise BaseException(f'Ambiguity detected: {txhash} already filled')
        _txs[txhash] = receipt['logs']

def update_points_with_new_values(_pts: list, _txs: dict) -> list:
    pts_to_update = []
    for ele in _pts:
        pt = ele[1]
        if ele[0]:
            txhash = pt.tags['transactionHash']
            logindex = int(pt.tags['logIndex'])
            log_found = False
            # Iterate through all logs in the transaction to discover a log with required index
            for l in _txs[txhash]:
                if Web3.toInt(hexstr=l['logIndex']) == logindex:
                    value = Web3.toInt(hexstr=l['data'])
                    (a3, a2, a1, a0) = to_1bln_base(value)
                    pts_to_update.append(Point(
                        time = pt.time,
                        tags = pt.tags,
                        fields = {'a0': a0, 'a1': a1, 'a2': a2, 'a3': a3}
                    ))
                    log_found = True
                    break
            if not log_found:
                raise BaseException(f'Log with index {logindex} not found in {txhash}')
        else:
            pts_to_update.append(pt)
    return pts_to_update

def inform_duration_time(_st: int, _chain: str):
    job_duration = int(time() - _st)
    info(f'{_chain}: {job_duration // 3600} hours {(job_duration - (job_duration // 3600) * 3600) // 60} mins {job_duration % 60} secs from month handling start')


def transform_transaction_values(_chain: str, _month: str):
    info(f'Starting transactions values transformation for chain {_chain} and {_month}')
    # Cache of received transactions to reduce amount of requests
    # There is no share txs cache among different months since there could be
    # no possibility for logs of the transaction in two consequent months
    txs = {}
    db_file = f'{TSDB_DIR}/{_chain}-{_month}-{TSDB_FILE_SUFFIX}'
    if not os.path.isfile(db_file):
        warning(f'Cannot open "{_chain}" data for {_month}')
    else:
        start_time = time()
        past_tr_pt = None
        already_tranformed_found = True
        tr_db_file = f'{TSDB_DIR}/tr-{_chain}-{_month}-{TSDB_FILE_SUFFIX}'
        if os.path.isfile(tr_db_file):
            info(f'{_chain}: found {tr_db_file}')
            with TinyFlux(tr_db_file) as trtsdb:
                counter = 0
                for past_tr_pt in trtsdb:
                    counter += 1
                already_tranformed_found = False
                info(f'{_chain}: the last element in {tr_db_file} discovered at position {counter - 1}')
                inform_duration_time(start_time, _chain)
        else:
            info(f'{_chain}: {tr_db_file} not found')
        trtsdb=TinyFlux(tr_db_file)
        tsdb=TinyFlux(db_file)
        # Set of points needs to be handled in batch
        # It contains both types of points - with and without transformation
        # More than one points could refer to a transaction in the request
        points = []
        # Set of transactions corresponding to the points in batch
        to_request = []
        counter = 0
        for point in tsdb:
            if already_tranformed_found:
                if not 'a0' in point.fields:
                    new_pt = (True, Point(
                        time = point.time,
                        tags = point.tags,
                        fields = {}
                    ))
                    tx = point.tags['transactionHash']
                    if not tx in txs:
                        txs[tx] = None
                        to_request.append(tx)
                else:
                    new_pt = (False, Point(
                        time = point.time,
                        tags = point.tags,
                        fields = {
                            'a0': point.fields['a0'],
                            'a1': point.fields['a1'],
                            'a2': point.fields['a2'],
                            'a3': point.fields['a3']
                        }
                    ))
                points.append(new_pt)

                if len(to_request) == REQUESTS_IN_BATCH:
                    info(f'{_chain}: handle {len(to_request)} discovered transactions for {_month}')
                    receipts = get_receipts_for_batch(chains[_chain]['rpc']['url'], to_request)
                    match_receipts_with_txs(receipts, txs)
                    new_pts = update_points_with_new_values(points, txs)
                    trtsdb.insert_multiple(new_pts)
                    info(f'{_chain}: {len(new_pts)} points transformed')
                    
                    inform_duration_time(start_time, _chain)
                    del new_pts
                    del points
                    del to_request
                    points = []
                    to_request = []
            else:
                if past_tr_pt.tags['logIndex'] == point.tags['logIndex'] and \
                   past_tr_pt.tags['transactionHash'] == point.tags['transactionHash']:
                    already_tranformed_found = True
                    info(f'{_chain}: positioned on required element (pos #{counter})')
                    inform_duration_time(start_time, _chain)
            counter += 1

        if len(to_request) != 0:
            info(f'{_chain}: handle {len(to_request)} discovered transactions for {_month}')
            receipts = get_receipts_for_batch(chains[_chain]['rpc']['url'], to_request)
            match_receipts_with_txs(receipts, txs)
            del to_request
        if len(points) != 0:
            new_pts = update_points_with_new_values(points, txs)
            trtsdb.insert_multiple(new_pts)
            info(f'{_chain}: {len(new_pts)} points transformed')

            inform_duration_time(start_time, _chain)
            del new_pts
            del points
        tsdb.close()
        trtsdb.close()
        info(f'{_chain}: all points in month {_month} handled')
    del txs

# for chainid in req_chains:
#     for month in months:
#         transform_transaction_values(chainid, month)

scheduled_tasks = {}
for chainid in req_chains:
    for month in months:
        k = f'{chainid}-{month}'
        scheduled_tasks[k] = threading.Thread(target=lambda: transform_transaction_values(chainid, month))
        scheduled_tasks[k].daemon = True
        scheduled_tasks[k].name = f'{k}-transformer'
        scheduled_tasks[k].start()

while True:
    stopped = {}
    if len(scheduled_tasks) > 0:
        sleep(THREADS_LIVENESS_INTERVAL)
        for chainid in scheduled_tasks:
            if not scheduled_tasks[chainid].is_alive():
                stopped[chainid] = True
        if len(stopped) == len(scheduled_tasks):
            error('THREADS MONITORING: All threads stopped. Exiting')
            break
