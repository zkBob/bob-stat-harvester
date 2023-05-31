from typing import Callable

from utils.settings.models import InventoriesList

def discover_bobvault_inventory(inventories: InventoriesList, func: Callable):
    bobvault_found = False
    for inv in inventories:
        if inv.protocol == "BobVault":
            func(inv)
            bobvault_found = True
            break
    return bobvault_found