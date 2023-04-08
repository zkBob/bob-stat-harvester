from typing import List, Dict
from decimal import Decimal

from utils.logging import info, error

from .settings import Settings

from .volume_adapters.common import GenericVolumeAdapter
from .volume_adapters.coingecko import VolumeOnCoinGecko
from .volume_adapters.bobvault import VolumeOnBobVaults

class Volume:
    _adapters: List[GenericVolumeAdapter]

    def __init__(self, settings: Settings):
        self._adapters = [VolumeOnCoinGecko(settings), VolumeOnBobVaults(settings)]

    def get_volume(self) -> Dict[str, Decimal]:
        info(f'Getting 24h volume')
        one_day_volume = {}
        for adpt in self._adapters:
            one_source = adpt.get_volume()
            if len(one_source) != 0:
                for k in one_source:
                    if k in one_day_volume:
                        one_day_volume[k] += one_source[k]
                    else:
                        one_day_volume[k] = one_source[k]
            else:
                error(f'Not able to get volume')
                return {}
        return one_day_volume