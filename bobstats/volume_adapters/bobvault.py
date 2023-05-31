from typing import Dict
from decimal import Decimal

from bobvault.base_vault import BaseBobVault

from utils.logging import info
from utils.settings.models import BobVaultInventory
from utils.settings.utils import discover_bobvault_inventory

from ..settings import Settings

from .common import GenericVolumeAdapter

class VolumeOnBobVaults(GenericVolumeAdapter):
    _vaults: Dict[str, BaseBobVault]

    def __init__(self, settings: Settings):
        self._vaults = {}
        for chainid in settings.chains:
            def inventory_setup(inv: BobVaultInventory):
                self._vaults[chainid] = BaseBobVault(chainid, settings.snapshot_dir, settings.bobvault_snapshot_file_suffix)

            discover_bobvault_inventory(settings.chains[chainid].inventories, inventory_setup)

    def get_volume(self) -> Dict[str, Decimal]:
        info(f'bobvault: getting volume through {"/".join(self._vaults)} chains')
        ret = {}
        for chainid in self._vaults:
            ret.update({chainid: self._vaults[chainid].get_volume_24h()})
        return ret