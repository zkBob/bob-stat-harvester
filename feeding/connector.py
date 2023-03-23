from requests.auth import AuthBase

from utils.settings.feeding import FeedingServiceSettings

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

    def __init__(self, settings: FeedingServiceSettings):
        self._service_url = settings.feeding_service_url
        self._upload_path = settings.feeding_service_path
        self._health_path = settings.feeding_service_health_path
        self._bearer_auth = SimpleBearerAuth(settings.feeding_service_upload_token)