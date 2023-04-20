from requests.auth import AuthBase

from pydantic import BaseModel

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