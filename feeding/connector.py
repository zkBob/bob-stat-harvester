from functools import lru_cache
from typing import Tuple, Union

from time import time

from requests import post, get
from requests.auth import AuthBase

from utils.logging import info, error
from utils.health import HealthOut

class SimpleBearerAuth(AuthBase):
    def __init__(self, _token):
        self.token = _token

    def __call__(self, r):
        r.headers['Authorization'] = f'Bearer {self.token}'
        return r

class BaseConnector:
    _service_url: str
    _upload_path: str
    _health_path: str
    _bearer_auth: SimpleBearerAuth

    def __init__(self, base_url: str, upload_path: str, upload_token: str, health_path: str):
        self._service_url = base_url
        self._upload_path = upload_path
        self._health_path = health_path
        self._bearer_auth = SimpleBearerAuth(upload_token)

@lru_cache(maxsize=1)
def _low_level_get_health_data(url: str, ttl_hash=None) -> Tuple[bool, Union[HealthOut, None]]:
    info(f'connector: requesting {url}')
    status = False
    try:
        r = get(url, timeout=(3.05, 27))
    except IOError as e :
        error(f'connector: cannot get feeding service health status: {e}')
    except ValueError as e :
        error(f'connector: cannot get feeding service health status: {e}')
    else:
        if r.status_code != 200:
            error(f'connector: cannot get health data (status code: {r.status_code}, error: {r.text})')
        else:
            status = True
            try:
                retval = r.json()
                stuctured = HealthOut.parse_obj(retval)
                return (status, stuctured)
            except Exception as e: 
                error(f'connector: cannot parse health data: {e}')
    return (status, None)

class UploadingConnector(BaseConnector):
    _ttl_hash: int = 0
    _ttl_timestamp: int = 0

    def _upload(self, data: str) -> bool:
        upload_url = f'{self._service_url}{self._upload_path}'
        info(f'connector: uploading data to feeding service by {upload_url}')
        try:
            r = post(
                upload_url,
                data=data,
                headers={'Content-Type': 'application/json'},
                auth=self._bearer_auth,
                timeout=(3.05, 27)
            )
        except Exception as e:
            error(f'connector: something wrong with uploading data to feeding service: {e}')
            return False
        else:
            if r.status_code != 200:
                error(f'connector: cannot upload data (status code: {r.status_code}, error: {r.text})')
                return False
        info(f'connector: data uploaded to feeding service successfully')
        return True
    
    def _get_health_data(self, cache_ttl: int = 0) -> Tuple[bool, Union[HealthOut, None]]:
        info(f'connector: requesting health data')
        curtime = int(time())
        if self._ttl_timestamp < curtime:
            self._ttl_timestamp = curtime + cache_ttl
            self._ttl_hash = self._ttl_hash + 1
        retval = _low_level_get_health_data(f'{self._service_url}{self._health_path}', self._ttl_hash)
        if not retval[0]:
            self._ttl_hash = self._ttl_hash + 1
        return retval
