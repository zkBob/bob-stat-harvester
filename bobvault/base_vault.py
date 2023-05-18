from decimal import Decimal

from time import time
from json import load

from utils.logging import info, debug, error
from utils.constants import BOB_TOKEN_ADDRESS, ONE_DAY

from .models import BobVaultTradesSnapshot

class BaseBobVault:
    _full_filename: str
    _chainid: str

    def __init__(self, chainid: str, snapshot_dir: str, snapshot_suffix: str):
        self._full_filename = f'{snapshot_dir}/{chainid}-{snapshot_suffix}'
        self._chainid = chainid

    def _get_bobvault_volume_for_timeframe(self, logs, ts_start, ts_end):
        info(f'bobvault:{self._chainid}: getting volume between {ts_start} and {ts_end}')
        logs_len = len(logs)
        prev_indices = [-1, logs_len]
        first_index = sum(prev_indices) // 2
        no_error = True
        while no_error:
            debug(f'bobvault:{self._chainid}: binary search: {prev_indices} - {first_index}')
            if (logs[first_index].timestamp >= ts_start):
                if (first_index == 0) or (logs[first_index-1].timestamp < ts_start):
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
            info(f'bobvault:{self._chainid}: no events for last required time frame')
        else:
            for trade in logs[first_index:]:
                if trade.timestamp < ts_end:
                    if trade.args.inToken == BOB_TOKEN_ADDRESS:
                        trade_volume = trade.args.amountIn
                    elif trade.args.outToken == BOB_TOKEN_ADDRESS:
                        trade_volume = trade.args.amountOut
                    else:
                        info(f'bobvault:{self._chainid}: swap operations skipped')
                        trade_volume = 0
                    volume_tf += trade_volume
                else:
                    break
        return volume_tf

    def _load(self) -> dict:
        info(f'bobvault:{self._chainid}: looking for snapshot {self._full_filename}')
        try:
            with open(self._full_filename, 'r') as json_file:
                data = ''.join(json_file.readlines())
                snapshot = BobVaultTradesSnapshot.parse_raw(data)
        except IOError:
            error(f'bobvault:{self._chainid}: snapshot not found')
            snapshot = BobVaultTradesSnapshot()
        return snapshot

    def _get_logs_from_snapshot(self) -> dict:
        return self._load().logs

    def get_volume_24h(self) -> Decimal:
        vol = Decimal(0)
        logs = self._get_logs_from_snapshot()
        if len(logs) != 0:
            info(f'bobvault:{self._chainid}: collecting 24h volume from snapshot')
            now = int(time())
            now_minus_24h = now - ONE_DAY
            vol = self._get_bobvault_volume_for_timeframe(logs, now_minus_24h, now)
            info(f'bobvault:{self._chainid}: discovered volume {vol}')
        return vol
    
    def getChainId(self) -> str:
        return self._chainid