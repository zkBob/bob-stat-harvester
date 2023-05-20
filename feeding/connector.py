from requests import post
from requests.auth import AuthBase

from utils.logging import info, error, warning
from utils.health import WorkerHealthModelBase

from .health import HTTPHealthDataCache, HTTPHealthDataWithStatus

class SimpleBearerAuth(AuthBase):
    def __init__(self, _token):
        self.token = _token

    def __call__(self, r):
        r.headers['Authorization'] = f'Bearer {self.token}'
        return r

class BaseConnector:
    _service_url: str
    _upload_path: str
    _bearer_auth: SimpleBearerAuth

    def __init__(self, base_url: str, upload_path: str, upload_token: str):
        self._service_url = base_url
        self._upload_path = upload_path
        self._bearer_auth = SimpleBearerAuth(upload_token)

class UploadingConnector(BaseConnector):
    _http_cache: HTTPHealthDataCache

    def __init__(
        self, 
        base_url: str, 
        upload_path: str, 
        upload_token: str, 
        health_path: str, 
        cache_ttl: int = 0
    ):
        super().__init__(base_url, upload_path, upload_token)
        self._http_cache = HTTPHealthDataCache.get(f'{self._service_url}{health_path}', cache_ttl)

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
    
    def _get_health_data(self, use_cached: bool = False) -> HTTPHealthDataWithStatus:
        retval = self._http_cache.get_health_data(use_cached)
        return retval

    def _check_availability(self, health: WorkerHealthModelBase) -> bool:
        retval = False
        if health.status == 'error' and \
            health.lastSuccessTimestamp == 0 and \
            health.lastErrorTimestamp == 0:
            warning(f'connector: no data on the feeding service') 
        else:
            retval = True
        return retval
