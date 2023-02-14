from os import getenv
import os.path

from web3 import Web3

from time import sleep, time

from json import load, dump
from tinyflux import TinyFlux

from decimal import Decimal

from logging import basicConfig, getLogger, info, error, debug, warning, WARNING, INFO, DEBUG

basicConfig(level=INFO)

ZERO_ACCOUNT = Web3.toChecksumAddress("0x0000000000000000000000000000000000000000")
BOB_TOKEN_DECIMALS = 18
BOB_TOKEN_DENOMINATOR = Decimal(10) ** BOB_TOKEN_DECIMALS
months = ['202209', '202210', '202211', '202212', '202301', '202302']

SNAPSHOT_DIR = getenv('SNAPSHOT_DIR', 'snapshots')
SNAPSHOT_FILE_SUFFIX = getenv('SNAPSHOT_FILE_SUFFIX', 'bob-holders-snaphsot.json')
TSDB_DIR = getenv('TSDB_DIR', 'tsdb')
TSDB_FILE_SUFFIX = getenv('TSDB_FILE_SUFFIX', 'bob-transfers.csv')
LIST_OF_CHAINS = getenv('LIST_OF_CHAINS', 'bsc eth opt pol')

info(f'SNAPSHOT_DIR = {SNAPSHOT_DIR}')
info(f'SNAPSHOT_FILE_SUFFIX = {SNAPSHOT_FILE_SUFFIX}')
info(f'TSDB_DIR = {TSDB_DIR}')
info(f'TSDB_FILE_SUFFIX = {TSDB_FILE_SUFFIX}')
info(f'LIST_OF_CHAINS = {LIST_OF_CHAINS}')

req_chains = LIST_OF_CHAINS.split()

def inform_duration_time(_st: int, _chain: str):
    job_duration = int(time() - _st)
    info(f'{_chain}: {job_duration // 3600} hours {(job_duration - (job_duration // 3600) * 3600) // 60} mins {job_duration % 60} secs from month handling start')

def from_1bln_base(_a3, _a2, _a1, _a0):
    retval = Decimal(_a3)
    for a in (_a2, _a1, _a0):
        retval = retval * Decimal(10 ** 9) + Decimal(a)
    return retval

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

def normalise_amount(_amount: Decimal) -> Decimal:
    return _amount / BOB_TOKEN_DENOMINATOR
#     return _amount

def change_balance(_balances, _transfer):
    if  _transfer['value'] != 0:
        if _transfer['from'] != ZERO_ACCOUNT:
            prev_balance = Decimal(0)
            if _transfer['from'] in _balances:
                prev_balance = Decimal(_balances[_transfer['from']])
            new_balance = prev_balance - _transfer['value']
            if new_balance == 0:
                del _balances[_transfer['from']]
            else:
                _balances[_transfer['from']] = str(new_balance)

        if _transfer['to'] != ZERO_ACCOUNT:
            prev_balance = Decimal(0)
            if _transfer['to'] in _balances:
                prev_balance = Decimal(_balances[_transfer['to']])
            new_balance = prev_balance + _transfer['value']
            if new_balance == 0:
                del _balances[_transfer['to']]
            else:
                _balances[_transfer['to']] = str(new_balance)

def write_balances_snapshot_for_chain(_chain, _snapshot):
    with open(f'{SNAPSHOT_DIR}/{_chain}-{SNAPSHOT_FILE_SUFFIX}', 'w') as json_file:
        dump(_snapshot, json_file)

def get_amount_from_fields(_fields):
    return from_1bln_base(_fields['a3'], _fields['a2'], _fields['a1'], _fields['a0'])

for _chain in req_chains:
    first_month_discovered = False
    for _month in months:
        db_file = f'{TSDB_DIR}/{_chain}-{_month}-{TSDB_FILE_SUFFIX}'
        if not os.path.isfile(db_file):
            warning(f'Cannot open "{_chain}" data for {_month}')
        else:
            info(f'{_chain}: Reading snapshot')
            snapshot = read_balances_snapshot_for_chain(_chain)
            start_time = time()
            if not first_month_discovered:
                snapshot['balances'] = {}
                first_month_discovered = True
            info(f'{_chain}: Open {db_file}')
            tsdb=TinyFlux(db_file)
            counter = 0
            for point in tsdb:
                value = normalise_amount(get_amount_from_fields(point.fields))
                
                change_balance(
                    snapshot['balances'],
                    {'from': point.tags['from'],
                     'to': point.tags['to'],
                     'value': value
                    }
                )
                counter += 1
                if counter % 10000 == 0:
                    info(f'{_chain}: handled {counter} transactions')
            info(f'{_chain}: handled {counter} transactions')

            info(f"{_chain}: Updating snapshot with {len(snapshot['balances'])} accounts balances")
            write_balances_snapshot_for_chain(_chain, snapshot)
            inform_duration_time(start_time, _chain)
            del snapshot
            tsdb.close()