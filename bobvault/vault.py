from typing import Tuple, List

from json import dump

from time import time

from utils.logging import info, error, warning
from utils.web3 import Web3Provider
from utils.misc import CustomJSONEncoder, InitException
from utils.settings.models import BobVaultInventory

from .base_vault import BaseBobVault
from .contract import BobVaultContract
from .settings import Settings, discover_inventory
from .base_processor import BobVaultLogsProcessor

class BobVault(BaseBobVault):
    _w3prov: Web3Provider
    _finalization_delay: int
    _contract: BobVaultContract
    _processors: List[BobVaultLogsProcessor]
    _snapshot: Tuple[int, dict]

    def __init__(self, chainid: str, settings: Settings):
        def inventory_setup(inv: BobVaultInventory):
            self._contract = BobVaultContract(
                self._w3prov,
                inv.address,
                inv.start_block,
                settings.chains[chainid].rpc.history_block_range
            )

        super().__init__(chainid, settings.snapshot_dir, settings.snapshot_file_suffix)
        self._w3prov = settings.w3_providers[chainid]
        self._finalization_delay = settings.chains[chainid].finalization
        if not discover_inventory(settings.chains[chainid].inventories, inventory_setup):
            error(f'bobvault:{self._chainid}: inventory is not found')
            raise InitException
        
        self._processors = []
        self._snapshot = ()

    def register_processor(self, proc: BobVaultLogsProcessor):
        self._processors.append(proc)

    def _load_or_init(self) -> dict:
        snapshot = self._load()
        if snapshot.last_block == -1:
            start_block = self._contract.start_block
            last_block = self._w3prov.make_call(self._w3prov.w3.eth.getBlock, 'latest').number
            last_block -= self._finalization_delay
            info(f'bobvault:{self._chainid}: initialize empty structure for snapshot with the block range {start_block} - {last_block}')
            snapshot.start_block = start_block
            snapshot.last_block = last_block
        return snapshot

    def _save(self, snapshot):
        info(f'bobvault:{self._chainid}: saving snapshot')
        with open(self._full_filename, 'w') as json_file:
            dump(snapshot.dict(), json_file, cls=CustomJSONEncoder)

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

    def collect_and_update(self, keep_snapshot = False) -> bool:
        snapshot = self._load_or_init()

        dump_range = self._get_dump_range(
            snapshot.start_block,
            snapshot.last_block,
            len(snapshot.logs) == 0
        )

        try:
            logs = self._contract.get_logs_for_range(dump_range[0], dump_range[1])
        except Exception as e:
            error(f'bobvault:{self._chainid}: cannot collect new logs ({e})')
            self._snapshot = ()
            return False
        else:
            snapshot.logs.extend(logs)
            snapshot.last_block = dump_range[1]
            self._save(snapshot)
        
        if keep_snapshot:
            self._snapshot = (int(time()), snapshot)
        else:
            self._snapshot = ()

        return True

    def process(self, keep_snapshot = False):
        info(f'bobvault:{self._chainid}: start processing snapshot with processors {self._processors}')
        if len(self._snapshot) == 0:
            warning(f'bobvault:{self._chainid}: no snapshot to process')
            return False

        for p in self._processors:
            info(f'bobvault:{self._chainid}: pre-processing by {p}')
            p.pre(self._snapshot[1])

        for trade in self._snapshot[1].logs:
            for p in self._processors:
                p.process(trade)

        for p in self._processors:
            info(f'bobvault:{self._chainid}: post-processing by {p}')
            p.post()

        if not keep_snapshot:
            self._snapshot = ()

    def monitor(self, log = False):
        if log:
            info(f'bobvault:{self._chainid}: start monitor tasks with processors {self._processors}')
        for p in self._processors:
            if log:
                info(f'bobvault:{self._chainid}: monitor actions by {p}')
            p.monitor()
