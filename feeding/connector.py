from requests.auth import AuthBase

from utils.settings.feeding import FeedingServiceSettings

from pydantic import BaseModel

class SimpleBearerAuth(AuthBase):
    def __init__(self, _token):
        self.token = _token

    def __call__(self, r):
        r.headers['Authorization'] = f'Bearer {self.token}'
        return r

class ConnectorConfig(BaseModel):
    base_url: str
    upload_path: str
    upload_token: str
    health_path: str

class BaseConnector:
    _service_url: str
    _upload_path: str
    _health_path: str
    _bearer_auth: SimpleBearerAuth

    def __init__(self, config: ConnectorConfig):
        self._service_url = config.base_url
        self._upload_path = config.upload_path
        self._health_path = config.upload_token
        self._bearer_auth = SimpleBearerAuth(config.health_path)