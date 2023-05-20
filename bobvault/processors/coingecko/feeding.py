from json import dumps

from feeding.connector import UploadingConnector

from utils.logging import info, warning
from utils.misc import CustomJSONEncoder, DACheckResults

class CoinGeckoFeedingServiceConnector(UploadingConnector):
    _feeding_service_health_container: str

    def __init__(
        self, 
        base_url: str, 
        upload_path: str, 
        upload_token: str, 
        health_path: str, 
        health_container: str,
        cache_ttl: int
    ):
        super().__init__(base_url, upload_path, upload_token, health_path, cache_ttl = cache_ttl)
        self._feeding_service_health_container = health_container

    def upload_cg_data(self, data: dict) -> bool:
        data_as_str = dumps(data, cls=CustomJSONEncoder)
        info(f'connector: uploading stats in coingecko compatible format')

        return self._upload(data_as_str)

    def check_data_availability(self) -> DACheckResults:
        ret = DACheckResults(accessible=False, available=False)
        # assuming that the method calls after TTL expiration,
        # it is OK to specify use cache as True even if data was not
        # available last time -- the cache will be invalidated as per TTL reset
        (status, stuctured) = self._get_health_data(use_cached = True)
        if status:
            ret.accessible = True
            if stuctured:
                health = stuctured.modules['BobVaults'][self._feeding_service_health_container]
                ret.available = self._check_availability(health)
        return ret