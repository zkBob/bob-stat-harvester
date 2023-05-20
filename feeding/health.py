from functools import cache
from typing import Tuple, Union

from time import time

from requests import get

from utils.logging import info, error
from utils.health import HealthOut

HTTPHealthDataWithStatus = Tuple[bool, Union[HealthOut, None]]

class HTTPHealthDataCache:
    _url: str
    _health_data_cache_ttl: int
    _ttl_timestamp: int
    _cached_health_data: HealthOut

    def __init__(self, url: str, cache_ttl: int = 0):
        self._url = url
        self._health_data_cache_ttl = cache_ttl

        self._ttl_timestamp = 0
        self._cached_health_data = None

    @classmethod
    @cache
    def get(cls, url: str, cache_ttl: int):
        c = cls(url, cache_ttl)
        return c

    def _get_and_parse(self) -> HTTPHealthDataWithStatus:
        status = False
        try:
            r = get(self._url, timeout=(3.05, 27))
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
    
    def get_health_data(self, use_cached = False) -> HTTPHealthDataWithStatus:
        curtime = int(time())

        if (not use_cached) or \
           (self._cached_health_data == None) or (self._ttl_timestamp < curtime):
            retval = self._get_and_parse()
            if retval[0]:
                self._cached_health_data = retval
                self._ttl_timestamp = curtime + self._health_data_cache_ttl
            else:
                self._cached_health_data = None
                self._ttl_timestamp = 0
        else:
            retval = self._cached_health_data
        
        return retval