from decimal import Decimal
from typing import Tuple

from time import time
from json import dump

from utils.web3 import Web3Provider, ERC20Token
from utils.logging import info, error
from utils.constants import ONE_DAY, BOB_TOKEN_ADDRESS, ONE_ETHER
from utils.misc import CustomJSONEncoder, InitException
from utils.settings.models import BobVaultInventory

from .feeding import CoinGeckoFeedingServiceConnector

from bobvault.contract import BobVaultContract
from bobvault.settings import Settings, discover_inventory
from bobvault.abs_processor import BobVaultLogsProcessor
from bobvault.models import BobVaultTrade, BobVaultCollateral

from .models import PairOrderbookModel, PairTradesModel, PairDataModelInterim, \
                    BobVaultTradeModel, BobVaultDataModel

ZERO = Decimal("0.0")
DEFAULT_SMALL_VALUE = ZERO
DEFAULT_BIG_VALUE = Decimal(10 ** 9)
ONE = Decimal("1.0")
TEN = Decimal("10.0")

class CoinGeckoAdapter(BobVaultLogsProcessor):
    _chainid: str
    _w3prov: Web3Provider
    _contract: BobVaultContract
    _pool_id: str
    _full_filename: str
    _connector: CoinGeckoFeedingServiceConnector
    _feeding_service_health_container: str
    _feeding_service_monitor_interval: int
    _feeding_service_monitor_attempts_for_info: int

    def __init__(self, chainid: str, settings: Settings):
        def inventory_setup(inv: BobVaultInventory):
            self._contract = BobVaultContract(
                self._w3prov,
                inv.address,
                inv.start_block,
                settings.chains[chainid].rpc.history_block_range
            )
            self._pool_id = inv.coingecko_poolid
            self._connector = CoinGeckoFeedingServiceConnector(
                base_url=settings.feeding_service_url,
                upload_path=inv.feeding_service_path,
                upload_token=settings.feeding_service_upload_token,
                health_path=settings.feeding_service_health_path,
                health_container=inv.feeding_service_health_container
            )

        self._chainid = chainid
        self._full_filename = f'{settings.snapshot_dir}/{chainid}-{settings.coingecko_file_suffix}'
        self._w3prov = settings.w3_providers[chainid]
        self._feeding_service_monitor_interval = settings.feeding_service_monitor_interval
        self._feeding_service_monitor_attempts_for_info = settings.feeding_service_monitor_attempts_for_info
        if not discover_inventory(settings.chains[chainid].inventories, inventory_setup):
            error(f'coingecko:{self._chainid}: inventory is not found')
            raise InitException

    def __repr__(self):
        return type(self).__name__

    def pre(self, snapshot: dict) -> bool:
        now = int(time())
        self._max_log_index = 0
        self._cg_data = BobVaultDataModel(__root__={'timestamp': now})
        self._ts_start = now - ONE_DAY
        self._ts_end = now
        self._snapshot_startblock = snapshot.start_block
        self._snapshot_lastblock = snapshot.last_block
        self._collaterals = {}
        self._vault_balance = Decimal(-1)
        info(f'coingecko:{self._chainid}: preparation to transform snapshot for usage by CG (24h interval: {self._ts_start} - {self._ts_end})')
        return True

    def _new_pair_init(self, ticker_id: str, base: Tuple[str, str], target: Tuple[str, str]):
        ob_template = PairOrderbookModel(bids=[[0.0, 0.0]], asks=[[0.0, 0.0]])
        tr_template = PairTradesModel(buy=[], sell=[])
        self._cg_data[ticker_id] = PairDataModelInterim(
            pool_id=self._pool_id,
            base_address=base[0],
            target_address=target[0],
            base_currency=base[1],
            target_currency=target[1],
            timestamp=0.0, # receive from timestamp of the last_block
            last_price=0.0, # receive from the last trade
            base_volume=0.0,
            target_volume=0.0,
            bid=0.0, # receive from bobvault based on fees
            ask=0.0, # receive from bobvault based on fees
            high=0.0,
            low=0.0,
            high_buy=DEFAULT_SMALL_VALUE,
            high_sell=DEFAULT_SMALL_VALUE,
            low_buy=DEFAULT_BIG_VALUE,
            low_sell=DEFAULT_BIG_VALUE,
            orderbook=ob_template, # for BOB bids receive from bobvault
            trades=tr_template
        )
        info(f'coingecko:{self._chainid}: init {ticker_id}')

    def _count_for_timeframe(self, ts: int, ticker_id: str, action_type: str, price: int, base_volume: Decimal, target_volume: Decimal):
        if (ts >= self._ts_start) and (ts < self._ts_end):
            self._cg_data[ticker_id].base_volume += base_volume
            self._cg_data[ticker_id].target_volume += target_volume
            if price != 0:
                if action_type == 'buy':
                    self._cg_data[ticker_id].high_buy = max(price, self._cg_data[ticker_id].high_buy)
                    self._cg_data[ticker_id].low_buy = min(price, self._cg_data[ticker_id].low_buy)
                if action_type == 'sell':
                    self._cg_data[ticker_id].high_sell = max(price, self._cg_data[ticker_id].high_sell)
                    self._cg_data[ticker_id].low_sell = min(price, self._cg_data[ticker_id].low_sell)

    def process(self, trade: BobVaultTrade) -> bool:
        if trade.name == 'Swap':
            token1 = trade.args.inToken
            token2 = trade.args.outToken
            if token1 < token2:
                action_type = 'buy'
                base = token1
                target = token2
                base_volume = trade.args.amountIn
                target_volume = trade.args.amountOut
            else:
                action_type = 'sell'
                base = token2
                target = token1
                base_volume = trade.args.amountOut
                target_volume = trade.args.amountIn
        elif trade.name == 'Buy':
            # BOB is base, another stable is target: user sells target for base
            action_type = 'sell'
            base = trade.args.outToken
            target = trade.args.inToken
            base_volume = trade.args.amountOut
            target_volume = trade.args.amountIn
        elif trade.name == 'Sell':
            # BOB is base, another stable is target: user buys target for base
            action_type = 'buy'
            base = trade.args.inToken
            target = trade.args.outToken
            base_volume = trade.args.amountIn
            target_volume = trade.args.amountOut
        
        base_sym = ERC20Token(self._w3prov, base).symbol()
        target_sym = ERC20Token(self._w3prov, target).symbol()
        ticker_id = f'{base_sym}_{target_sym}'
        
        if not ticker_id in self._cg_data.pairs():
            self._new_pair_init(ticker_id, (base, base_sym), (target, target_sym))

        if base_volume != 0:
            price = (target_volume / base_volume) * ONE # Multiply by Decimal(1.0) to keep fraction point in case of equal values
        else:
            price = ZERO

        self._count_for_timeframe(trade.timestamp, ticker_id, action_type, price, base_volume, target_volume)    

        self._max_log_index = max(trade.logIndex, self._max_log_index)
        xtrade = BobVaultTradeModel(
            trade_id=(trade.blockNumber - self._snapshot_startblock) * (self._max_log_index + 1) + trade.logIndex,
            price=price,
            base_volume=base_volume,
            target_volume=target_volume,
            trade_timestamp=trade.timestamp,
            type=action_type
        )

        if action_type == "buy":
            self._cg_data[ticker_id].trades.buy.append(xtrade)
        else:
            self._cg_data[ticker_id].trades.sell.append(xtrade)

    def _fill_high_and_low(self, ticker_id: str):
        # BOB is base, another stable is target: sell target for base
        # BOB is base, another stable is target: buy target for base
        # 'high_buy': 0.9999000016536099,
        # 'high_sell': 1.0000800064005122,
        # 'low_buy': 0.9998999989355484,
        # 'low_sell': 1.000079993223124
        if self._cg_data[ticker_id].high_sell != DEFAULT_SMALL_VALUE:
            self._cg_data[ticker_id].high = self._cg_data[ticker_id].high_sell
        elif self._cg_data[ticker_id].high_buy != DEFAULT_SMALL_VALUE:
            self._cg_data[ticker_id].high = self._cg_data[ticker_id].high_buy
        if self._cg_data[ticker_id].low_buy != DEFAULT_BIG_VALUE:
            self._cg_data[ticker_id].low = self._cg_data[ticker_id].low_buy
        elif self._cg_data[ticker_id].low_sell != DEFAULT_BIG_VALUE:
            self._cg_data[ticker_id].low = self._cg_data[ticker_id].low_sell
        self._cg_data[ticker_id].high_sell = None
        self._cg_data[ticker_id].high_buy = None
        self._cg_data[ticker_id].low_sell = None
        self._cg_data[ticker_id].low_buy = None

    def _collateral(self, token: str) -> BobVaultCollateral:
        if not token in self._collaterals:
            self._collaterals[token] = self._contract.get_collateral(token, self._snapshot_lastblock)
        return self._collaterals[token]

    def _bob_balance_for_vault(self) -> Decimal:
        if self._vault_balance == -1:
            self._vault_balance = ERC20Token(self._w3prov, BOB_TOKEN_ADDRESS).balanceOf(
                self._contract.address(), 
                bn = self._snapshot_lastblock
            )
        return self._vault_balance

    def _fill_orderbook(self, ticker_id: str):
        col_info = self._collateral(self._cg_data[ticker_id].target_address)
        token2 = ERC20Token(self._w3prov, self._cg_data[ticker_id].target_address)
        token2_one = TEN ** Decimal(token2.decimals())
        token2_balance = token2.normalize(col_info.balance)
        token2_price = Decimal(col_info.price)
        token2_inFee = Decimal(col_info.inFee)
        token2_outFee = Decimal(col_info.outFee)
        if self._cg_data[ticker_id].base_address == BOB_TOKEN_ADDRESS:
            token1_balance = self._bob_balance_for_vault()
            token1_one = TEN ** Decimal(ERC20Token(self._w3prov, BOB_TOKEN_ADDRESS).decimals())
            self._cg_data[ticker_id].orderbook.bids[0][1] = ONE - (token1_one / ONE_ETHER * token2_price / token2_one * token2_outFee / ONE_ETHER)
            self._cg_data[ticker_id].orderbook.asks[0][1] = token1_one / token2_one * token2_price / (ONE_ETHER - token2_inFee)
        else:
            col_info = self._collateral(self._cg_data[ticker_id].base_address)
            token1 = ERC20Token(self._w3prov, self._cg_data[ticker_id].base_address)
            token1_one = TEN ** Decimal(token1.decimals())
            token1_balance = token1.normalize(col_info.balance)
            token1_price = Decimal(col_info.price)
            token1_inFee = Decimal(col_info.inFee)
            token1_outFee = Decimal(col_info.outFee)
            
            self._cg_data[ticker_id].orderbook.bids[0][1] = token2_one / (token1_one * (1 - token2_inFee / ONE_ETHER) * token1_price * (1 - token1_outFee / ONE_ETHER) / token2_price)
            self._cg_data[ticker_id].orderbook.asks[0][1] = token1_one / (token2_one * (1 - token1_inFee / ONE_ETHER) * token2_price * (1 - token2_outFee / ONE_ETHER) / token1_price)

        self._cg_data[ticker_id].base_address = None
        self._cg_data[ticker_id].target_address = None
            
        self._cg_data[ticker_id].orderbook.bids[0][0] = token2_balance
        self._cg_data[ticker_id].orderbook.asks[0][0] = token1_balance

        self._cg_data[ticker_id].bid = self._cg_data[ticker_id].orderbook.bids[0][1]
        self._cg_data[ticker_id].ask = self._cg_data[ticker_id].orderbook.asks[0][1]

    def _fill_last_price(self, ticker_id: str):
        if len(self._cg_data[ticker_id].trades.buy) > 0:
            if len(self._cg_data[ticker_id].trades.sell) > 0:
                if self._cg_data[ticker_id].trades.buy[-1].trade_id > self._cg_data[ticker_id].trades.sell[-1].trade_id:
                    self._cg_data[ticker_id].last_price = self._cg_data[ticker_id].trades.buy[-1].price
                else:
                    self._cg_data[ticker_id].last_price = self._cg_data[ticker_id].trades.sell[-1].price
            else:
                self._cg_data[ticker_id].last_price = self._cg_data[ticker_id].trades.buy[-1].price
        elif len(self._cg_data[ticker_id].trades.sell) > 0:
            self._cg_data[ticker_id].last_price = self._cg_data[ticker_id].trades.sell[-1].price

    def post(self) -> bool:
        if len(self._cg_data.pairs()) > 0:
            one_timestamp = Decimal(
                self._w3prov.make_call(
                    self._w3prov.w3.eth.get_block,
                    self._snapshot_lastblock
                ).timestamp
            )

        for ticker_id in self._cg_data.pairs():
            self._cg_data[ticker_id].timestamp = one_timestamp

            self._fill_high_and_low(ticker_id)

            self._fill_orderbook(ticker_id)

            self._fill_last_price(ticker_id)

        info(f'coingecko:{self._chainid}: saving CG data snapshot to {self._full_filename}')
        data_as_dict = self._cg_data.dict(exclude_none = True)
        retval = False
        try:
            with open(self._full_filename, 'w') as json_file:
                dump(data_as_dict, json_file, cls=CustomJSONEncoder)
                retval = True
        except Exception as e:
            error(f'coingecko:{self._chainid}: cannot save CG data with the reason {e}')

        retval &= self._connector.upload_cg_data(data_as_dict)

        return True