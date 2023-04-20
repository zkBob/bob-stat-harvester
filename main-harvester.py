from time import time

from bobstats.settings import Settings
from bobstats.stats import Stats
from bobstats.db import DBAdapter
from bobstats.feeding import prepare_data_for_feeding, BobStatsConnector

from feeding.connector import ConnectorConfig

from utils.logging import error, info
from utils.misc import every

class BobStats:
    _last_main: int = 0
    _last_monitor: int = 0
    _monitor_feedback_counter: int = 0
    _measurements_interval: int
    _monitor_interval: int
    _monitor_attempts_for_info: int
    _stats: Stats
    _db: DBAdapter
    _connector: BobStatsConnector

    def __init__(self, settings: Settings):
        self._stats = Stats(settings)
        self._db = DBAdapter(settings)
        self._connector = BobStatsConnector(
            ConnectorConfig(
                base_url=settings.feeding_service_url,
                upload_path=settings.feeding_service_path,
                upload_token=settings.feeding_service_upload_token,
                health_path=settings.feeding_service_health_path
            )
        )
        self._measurements_interval = settings.measurements_interval
        self._monitor_interval = settings.feeding_service_monitor_interval
        self._monitor_attempts_for_info = settings.feeding_service_monitor_attempts_for_info

    def measure_and_publish(self):
        stats = self._stats.generate()
        if len(stats) == len(settings.chains):
            self._db.store(stats)
            data = prepare_data_for_feeding(stats, self._db)
            if data:
                if not self._connector.upload_bobstats(data):
                    error(f'Plan to upload data next time')
            else:
                error(f'Something wrong with preparing data for the feeding service. Plan to upload data next time')
        else:
            error(f'Something wrong with amount of collected data. Interrupt measurements for the next time')

    def monitor_feeding_service(self):
        if self._monitor_feedback_counter == (self._monitor_attempts_for_info - 1):
            info(f'Checking feeding service for data availability')
            self._monitor_feedback_counter = 0
        else:
            self._monitor_feedback_counter += 1
        status = self._connector.check_data_availability()
        if status.accessible and not status.available:
            latest_stats = self._db.get_nearest_to_timespot(int(time())-1)
            if latest_stats:
                data = prepare_data_for_feeding(latest_stats, self._db)
                if data:
                    if not self._connector.upload_bobstats(data):
                        error(f'Plan to upload data next time')
                else:
                    error(f'Something wrong with preparing data for the feeding service. Plan to upload data next time')

    def loop(self):
        curtime = time()
        if curtime - self._last_main > self._measurements_interval:
            self.measure_and_publish()
            self._last_main = curtime
        elif curtime - self._last_monitor > self._monitor_interval:
            self.monitor_feeding_service()
            self._last_monitor = curtime

if __name__ == '__main__':
    settings = Settings.get()
    worker = BobStats(settings)
    every(
        worker.loop,
        min(settings.measurements_interval, settings.feeding_service_monitor_interval)
    )