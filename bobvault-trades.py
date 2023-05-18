from typing import List

from time import time

from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION

from bobvault.settings import Settings
from bobvault.vault import BobVault
from bobvault.processors.coingecko.coingecko import CoinGeckoAdapter

from utils.logging import error, info
from utils.misc import InitException
from utils.misc import every

class BobVaultTrades:
    _last_main: int = 0
    _last_monitor: int = 0
    _monitor_feedback_counter: int = 0
    _measurements_interval: int
    _monitor_interval: int
    _monitor_attempts_for_info: int
    _vaults: List[BobVault]

    def __init__(self, settings: Settings):
        self._measurements_interval = settings.measurements_interval
        self._monitor_interval = settings.feeding_service_monitor_interval
        self._monitor_attempts_for_info = settings.feeding_service_monitor_attempts_for_info
        self._max_workers = settings.max_workers
        chains = settings.chain_selector.split(",")
        if len(chains) > 0:
            self._vaults = []
            for ch in chains:
                v = BobVault(ch, settings)
                v.register_processor(CoinGeckoAdapter(ch, settings))
                self._vaults.append(v)
        else:
            error(f'Chain list is emtpy')
            raise InitException
        
    def collect_and_publish(self):
        def task(vault: BobVault):
            vault.collect_and_update(keep_snapshot=True)
            vault.process()

        with ThreadPoolExecutor(max_workers=min(len(self._vaults), self._max_workers)) as executor:
            vault_futures = {executor.submit(task, vault): vault.getChainId() for vault in self._vaults}
            done = wait(vault_futures, return_when = FIRST_EXCEPTION)[0]
            for f in done:
                if f.exception():
                    error(f'Not able to handle BobVault successfully in {vault_futures[f]}')

    def monitor_feeding_service(self):
        if self._monitor_feedback_counter == (self._monitor_attempts_for_info - 1):
            info(f'Checking feeding service for data availability')
            self._monitor_feedback_counter = 0
        else:
            self._monitor_feedback_counter += 1

    def loop(self):
        curtime = time()
        if curtime - self._last_main > self._measurements_interval:
            self.collect_and_publish()
            self._last_main = curtime
        elif curtime - self._last_monitor > self._monitor_interval:
            self.monitor_feeding_service()
            self._last_monitor = curtime

if __name__ == '__main__':
    settings = Settings.get()
    worker = BobVaultTrades(settings)
    every(
        worker.loop,
        min(settings.measurements_interval, settings.feeding_service_monitor_interval)
    )