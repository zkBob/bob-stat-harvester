from .common import CommonSettings

class FeedingServiceSettings(CommonSettings):
    feeding_service_url: str = 'http://127.0.0.1:8080'
    feeding_service_path: str = '/'
    feeding_service_health_path: str = '/health'
    feeding_service_upload_token: str = 'default'
    feeding_service_monitor_interval: int = 60
    feeding_service_monitor_attempts_for_info: int = 60

    def __init__(self):
        def token_formatter(token: str) -> str:
            out = token
            if token != 'default':
                out = '********'
            return out

        super().__init__()

        self.extend_formatters({'feeding_service_upload_token': token_formatter})
