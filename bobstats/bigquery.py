from utils.logging import getLogger, WARNING, info, error

import pandas as pd

from google.oauth2 import service_account
import pandas_gbq

from .settings import Settings
from .common import StatsByChains

def init_pandas_logger():
    getLogger('pandas_gbq').setLevel(WARNING)

class BigQueryAdapter:
    _dataset: str
    _table: str

    def __init__(self, settings: Settings):
        credentials = service_account.Credentials.from_service_account_file(settings.bigquery_auth_json_key)
        info('BigQuery auth key applied')

        pandas_gbq.context.credentials = credentials
        pandas_gbq.context.project = settings.bigquery_project

        self._dataset = settings.bigquery_dataset
        self._table = settings.bigquery_table

    def send(self, stats: StatsByChains) -> bool:
        status = False
        df = pd.json_normalize(stats, sep='_')
        df['dt'] = pd.to_datetime(df['dt'], unit='s', utc=False)

        info('bigquery: sending data')
        try:
            pandas_gbq.to_gbq(df, f'{self._dataset}.{self._table}', if_exists='append', progress_bar=False)
            info('bigquery: data sent successfully')
            status = True
        except Exception as e:
            error(f'bigquery: something wrong with sending data: {e}')
        return status
