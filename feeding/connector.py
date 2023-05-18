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

class UploadingConnector(BaseConnector):

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
    
    def _get_health_data(self) -> bool:
        retval = None
        try:
            r = get(f'{self._service_url}{self._health_path}', timeout=(3.05, 27))
        except IOError as e :
            error(f'connector: cannot get feeding service health status: {e}')
        except ValueError as e :
            error(f'connector: cannot get feeding service health status: {e}')
        else:
            if r.status_code != 200:
                error(f'connector: cannot get health data (status code: {r.status_code}, error: {r.text})')
            else:
                retval = r.json()
        return retval
    
    def _parse_health_data(self, raw_data: dict) -> HealthOut:
        try:
            stuctured = HealthOut.parse_obj(raw_data)
            return stuctured
        except Exception as e: 
            error(f'connector: cannot parse health data: {e}')
