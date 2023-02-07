from os import getenv

from datetime import datetime

from tinyflux import TinyFlux, Point

TSDB_DIR = getenv('TSDB_DIR', 'tsdb')
TSDB_FILE_SUFFIX = getenv('TSDB_FILE_SUFFIX', 'bob-transfers.csv')

def store_logs_in_tsdb(_chain: str, _pts: dict):
    for grp in _pts:
        with TinyFlux(f'{TSDB_DIR}/{_chain}-{grp}-{TSDB_FILE_SUFFIX}') as tsdb:
            tsdb.insert_multiple(_pts[grp])

for _chain in ['pol', 'eth', 'opt']:
    print(f'Chain {_chain}')
    with TinyFlux(f'{TSDB_DIR}/{_chain}-{TSDB_FILE_SUFFIX}') as tsdb:
        iterator = iter(tsdb)
        counter = 0
        points = {}
        for point in iterator:
            group = datetime.strftime(point.time,'%Y%m',)
            if not group in points:
                points[group] = []
            points[group].append(point)
            counter += 1
            if counter == 5000:
                print(f'{points.keys()}')
                store_logs_in_tsdb(_chain, points)
                counter = 0
                points = {}
        if len(points) > 0:
            store_logs_in_tsdb(_chain, points)