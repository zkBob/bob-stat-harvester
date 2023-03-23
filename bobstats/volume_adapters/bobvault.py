from typing import Dict, List
from decimal import Decimal

from time import time
from json import load

from utils.logging import info, error, debug
from utils.constants import BOB_TOKEN_ADDRESS, ONE_DAY

from ..settings import Settings

from .common import GenericVolumeAdapter

def _get_bobvault_volume_for_timeframe(chainid, logs, ts_start, ts_end):
    info(f'bobvault:{chainid}: getting volume between {ts_start} and {ts_end}')
    logs_len = len(logs)
    prev_indices = [-1, logs_len]
    first_index = sum(prev_indices) // 2
    no_error = True
    while no_error:
        debug(f'bobvault:{chainid}: binary search: {prev_indices} - {first_index}')
        if (logs[first_index]['timestamp'] >= ts_start):
            if (first_index == 0) or (logs[first_index-1]['timestamp'] < ts_start):
                break
            else:
                prev_indices[1] = first_index
                first_index = sum(prev_indices) // 2
        else:
            prev_indices[0] = first_index
            first_index = sum(prev_indices) // 2
        if prev_indices[0] == logs_len - 1:
            no_error = False
    volume_tf = Decimal(0)
    if not no_error:
        info(f'bobvault:{chainid}: no events for last required time frame')
    else:
        for trade in logs[first_index:]:
            if trade['timestamp'] < ts_end:
                if trade['args']['inToken'] == BOB_TOKEN_ADDRESS:
                    trade_volume = trade['args']['amountIn']
                elif trade['args']['outToken'] == BOB_TOKEN_ADDRESS:
                    trade_volume = trade['args']['amountOut']
                else:
                    info(f'bobvault:{chainid}: swap operations skipped')
                    trade_volume = 0
                volume_tf += Decimal(trade_volume)
            else:
                break
    return volume_tf

class BobVault(GenericVolumeAdapter):
    _snapshot_dir: str
    _file_suffix: str
    _chainids: List[str]

    def __init__(self, settings: Settings):
        self._snapshot_dir = settings.snapshot_dir
        self._file_suffix = settings.bobvault_snapshot_file_suffix

        self._chainids = []
        for chainid in settings.chains:
            for inv in settings.chains[chainid].inventories:
                if inv.protocol == "BobVault":
                    self._chainids.append(chainid)
                    break

    def _get_snapshot(self, chainid: str) -> dict:
        full_filename = f'{self._snapshot_dir}/{chainid}-{self._file_suffix}'
        info(f'bobvault:{chainid}: looking for snapshot {full_filename}')
        try:
            with open(full_filename, 'r') as json_file:
                snapshot = load(json_file)
        except IOError:
            error(f'bobvault:{chainid}: snapshot not found')
            return {}
        return snapshot['logs'] 

    def _get_volume_24h(self) -> Dict[str, Decimal]:
        ret = {}
        for chainid in self._chainids:
            vol = Decimal(0)
            logs = self._get_snapshot(chainid)
            if len(logs) != 0:
                info(f'bobvault:{chainid}: collecting 24h volume from snapshot')
                now = int(time())
                now_minus_24h = now - ONE_DAY
                vol = _get_bobvault_volume_for_timeframe(chainid, logs, now_minus_24h, now)
                info(f'bobvault:{chainid}: discovered volume {vol}')
            ret.update({chainid: vol})
        return ret

    def get_volume(self) -> Dict[str, Decimal]:
        info(f'bobvault: getting volume through {"/".join(self._chainids)} chains')
        return self._get_volume_24h()


