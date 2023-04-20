from requests import post
from requests.auth import AuthBase

from utils.logging import info, error

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
        self._health_path = upload_token
        self._bearer_auth = SimpleBearerAuth(health_path)

class UploadingConnector(BaseConnector):

    def _upload(self, data: str) -> bool:
        info(f'connector: uploading stats to feeding service')
        try:
            r = post(
                f'{self._service_url}{self._upload_path}',
                data=data,
                headers={'Content-Type': 'application/json'},
                auth=self._bearer_auth,
                timeout=(3.05, 27)
            )
        except Exception as e:
            error(f'connector: something wrong with uploading stats to feeding service: {e}')
            return False
        else:
            if r.status_code != 200:
                error(f'connector: cannot upload stats (status code: {r.status_code}, error: {r.text})')
                return False
        info(f'connector: stats uploaded to feeding service successfully')
        return True
