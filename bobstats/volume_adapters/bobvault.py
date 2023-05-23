from typing import Dict
from decimal import Decimal

from bobvault.base_vault import BaseBobVault

from utils.logging import info

from ..settings import Settings

from .common import GenericVolumeAdapter

class VolumeOnBobVaults(GenericVolumeAdapter):
    _vaults: Dict[str, BaseBobVault]

    def __init__(self, settings: Settings):
        self._vaults = {}
        for chainid in settings.chains:
            for inv in settings.chains[chainid].inventories:
                if inv.protocol == "BobVault":
                    self._vaults[chainid] = BaseBobVault(chainid, settings.snapshot_dir, settings.bobvault_snapshot_file_suffix)
                    break

    def get_volume(self) -> Dict[str, Decimal]:
        info(f'bobvault: getting volume through {"/".join(self._vaults)} chains')
        ret = {}
        for chainid in self._vaults:
            ret.update({chainid: self._vaults[chainid].get_volume_24h()})
        return ret