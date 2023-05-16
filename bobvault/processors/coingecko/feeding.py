from json import dumps

from feeding.connector import UploadingConnector

from utils.logging import info
from utils.misc import CustomJSONEncoder

class CoinGeckoFeedingServiceConnector(UploadingConnector):
    _feeding_service_health_container: str

    def __init__(
        self, 
        base_url: str, 
        upload_path: str, 
        upload_token: str, 
        health_path: str, 
        health_container: str
    ):
        super().__init__(base_url, upload_path, upload_token, health_path)
        self._feeding_service_health_container = health_container

    def upload_cg_data(self, data: dict) -> bool:
        data_as_str = dumps(data.dict(), cls=CustomJSONEncoder)
        info(f'connector: uploading stats in coingecko compatible format')

        return self._upload(data_as_str)
