from typing import Tuple

from json import dump

from utils.logging import info, error
from utils.web3 import Web3Provider
from utils.misc import CustomJSONEncoder

from .base_vault import BaseBobVault
from .contract import BobVaultContract
from .settings import Settings

class BobVault(BaseBobVault):
    _w3prov: Web3Provider
    _cg_file_suffix: str
    _finalization_delay: int
    _contract: BobVaultContract
    _cg_poolid: str
    _feeding_service_path: str
    _feeding_service_health_container: str

    def __init__(self, chainid: str, settings: Settings):
        super().__init__(chainid, settings.snapshot_dir, settings.snapshot_file_suffix)
        self._cg_file_suffix = settings.coingecko_file_suffix
        self._w3prov = settings.w3_providers[chainid]
        self._finalization_delay = settings.chains[chainid].finalization
        for inv in settings.chains[chainid].inventories:
            if inv.protocol == "BobVault":
                self._contract = BobVaultContract(
                    self._w3prov,
                    inv.address,
                    inv.start_block,
                    settings.chains[chainid].rpc.history_block_range
                )
                self._cg_poolid = inv.coingecko_poolid
                self._feeding_service_path = inv.feeding_service_path
                self._feeding_service_health_container = inv.feeding_service_health_container
                break

    def _load_or_init(self) -> dict:
        snapshot = self._load()
        if not 'last_block' in snapshot:
            start_block = self._contract.start_block
            last_block = self._w3prov.make_call(self._w3prov.w3.eth.getBlock, 'latest').number
            last_block -= self._finalization_delay
            info(f'bobvault:{self._chainid}: initialize empty structure for snapshot with the block range {start_block} - {last_block}')
            snapshot.update({
                'start_block': start_block,
                'last_block': last_block,
            })
        return snapshot

    def _save(self, snapshot):
        info(f'bobvault:{self._chainid}: saving snapshot')
        with open(self._full_filename, 'w') as json_file:
            dump(snapshot, json_file, cls=CustomJSONEncoder)

    def _get_dump_range(self, prev_start_block: int, prev_last_block, first_time: bool) -> Tuple[int, int]:
        if not first_time:
            info(f'bobvault:{self._chainid}: identifying dump range to extend existing snapshot')
            dump_range = (
                prev_last_block + 1,
                self._w3prov.make_call(self._w3prov.w3.eth.getBlock, 'latest').number - self._finalization_delay
            )
            info(f'bobvault:{self._chainid}: dump range: {dump_range[0]} - {dump_range[1]}')
        else:
            dump_range = (prev_start_block, prev_last_block)
        return dump_range

    def collect_and_update(self) -> bool:
        snapshot = self._load_or_init()

        dump_range = self._get_dump_range(
            snapshot['start_block'],
            snapshot['last_block'],
            len(snapshot['logs']) == 0
        )

        try:
            logs = self._contract.get_logs_for_range(dump_range[0], dump_range[1])
        except Exception as e:
            error(f'bobvault:{self._chainid}: cannot collect new logs ({e})')
            return False
        else:
            snapshot['logs'].extend(logs)
            snapshot['last_block'] = dump_range[1]
            self._save(snapshot)
        return True
