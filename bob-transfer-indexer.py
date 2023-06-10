from typing import Dict

from time import sleep, time

import threading

from utils.logging import info, error, warning
from utils.misc import every

from balances.settings import Settings
from balances.main import Indexer

class IndexerWorker:
    _chain: str
    _indexer: Indexer
    _pull_interval: int
    _last_pull: int
    _head_achieved: bool

    def __init__(self, chainid: str, settings: Settings):
        self._chain = chainid
        self._indexer = Indexer(chainid, settings)
        self._pull_interval = settings.chains[chainid].events_pull_interval

    def prepare(self):
        self._head_achieved = False

    def loop(self):
        curtime = int(time())
        if self._head_achieved:
            if self._last_pull + self._pull_interval <= curtime:
                self._head_achieved = self._indexer.discover_balance_updates()[1]
                if not self._head_achieved:
                    info(f'{self._chain}: more historical events discovered, increasing pulling frequency')
                self._last_pull = curtime
        else:
            self._head_achieved = self._indexer.discover_balance_updates()[1]
            if self._head_achieved:
                info(f'{self._chain}: historical events received, reducing pulling frequency')
                self._last_pull = curtime

class BalancesIndexer():
    _workers: Dict[str, IndexerWorker]
    _liveness_interval: int
    _measurements_interval: int

    def __init__(self, settings: Settings):
       self._liveness_interval = settings.threads_liveness_interval
       self._measurements_interval = settings.default_measurements_interval
       self._workers = {}
       for chainid in settings.chains:
            self._workers[chainid] = IndexerWorker(chainid, settings)

    def job_for(self, _chainid):
        self._workers[_chainid].prepare()
        every(
            self._workers[_chainid].loop,
            self._measurements_interval
        )

    def run(self):
        scheduled_tasks = {}
        while True:
            stopped = {}
            if len(scheduled_tasks) > 0:
                sleep(self._liveness_interval)
                for chainid in scheduled_tasks:
                    if not scheduled_tasks[chainid].is_alive():
                        warning(f'THREADS MONITORING: Polling thread for {chainid} is not alive')
                        stopped[chainid] = True
                if len(stopped) == len(scheduled_tasks):
                    error('THREADS MONITORING: All threads stopped. Exiting')
                    break
            else:
                for chainid in self._workers:
                    stopped[chainid] = True
            for chainid in stopped:
                info(f'THREADS MONITORING: Restarting thread for {chainid}')
                scheduled_tasks[chainid] = threading.Thread(target=lambda: self.job_for(chainid))
                scheduled_tasks[chainid].daemon = True
                scheduled_tasks[chainid].name = f'{chainid}-indexer'
                scheduled_tasks[chainid].start()

if __name__ == '__main__':
    settings = Settings.get()
    worker = BalancesIndexer(settings)
    worker.run()
