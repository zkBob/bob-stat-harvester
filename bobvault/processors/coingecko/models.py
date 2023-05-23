from typing import Dict, List, Union, Optional
from decimal import Decimal

import copy

from pydantic import Extra, BaseModel, root_validator, ValidationError

# this is required to encode fields when the model is returned in HTTP
class ModelWithJSONEncoder(BaseModel):
    class Config:
        json_encoders = {
            Decimal: lambda v: str(v)
        }

class PairOrderbookModel(ModelWithJSONEncoder):
    bids: List[List[Decimal]] = [[]] #TODO Limit the list with two elements only
    asks: List[List[Decimal]] = [[]] #TODO Limit the list with two elements only

class BobVaultTradeModel(ModelWithJSONEncoder):
    trade_id: int
    price: Decimal
    base_volume: Decimal
    target_volume: Decimal
    trade_timestamp: Decimal # in fact, this is str(int)
    type: str #TODO use enum here

class PairTradesModel(ModelWithJSONEncoder):
    buy: Optional[List[BobVaultTradeModel]]
    sell: Optional[List[BobVaultTradeModel]]

class TickerBaseModel(ModelWithJSONEncoder):
    pool_id: str
    base_currency: str
    target_currency: str
    last_price: Decimal
    base_volume: Decimal
    target_volume: Decimal
    bid: Decimal
    ask: Decimal
    high: Decimal
    low: Decimal

class PairDataModel(TickerBaseModel):
    timestamp: Decimal # in fact, this is str(int)
    orderbook: PairOrderbookModel
    trades: PairTradesModel

class PairDataModelInterim(PairDataModel):
    base_address: str
    target_address: str
    high_buy: Decimal
    high_sell: Decimal
    low_buy: Decimal
    low_sell: Decimal

class BobVaultDataModel(ModelWithJSONEncoder):
    # Dynamic models are not easy: https://github.com/pydantic/pydantic/discussions/4938
    __root__: Dict[str, Union[Union[PairDataModel, PairDataModelInterim], int]]

    @root_validator(pre=True)
    def check_timestamp(cls, values):
        timestamp_exist = False
        for k in values['__root__'].keys():
            if isinstance(values['__root__'][k], int):
                if k == 'timestamp':
                    timestamp_exist = True
                else:
                    raise ValidationError(f'Found the "{k}" field. Only one int field - "timestamp" is allowed')
        if not timestamp_exist:
            raise ValidationError(f'The field "timestamp" is not found')
        return values

    class Config:
        extra = Extra.forbid

    def dict(self, **kwargs):
        output = super().dict(**kwargs)
        return output['__root__']

    def pairs(self) -> List:
        keys = set(self.keys())
        keys.discard('timestamp')
        return list(keys)

    def __iter__(self):
        return iter(self.__root__)

    def __getitem__(self, item):
        return self.__root__[item]

    def __setitem__(self, item, value):
        if type(value) != PairDataModelInterim:
            raise TypeError("'BobVaultDataModel' object does not support item assignment besides 'PairDataModelInterim'")
        self.__root__[item] = value

    def keys(self):
        return self.__root__.keys()

    def values(self):
        return self.__root__.values()

    def items(self):
        return self.__root__.items()

    def __copy__(self):
        return self.__root__.__copy__

    def __repr__(self):
        return self.__root__.__repr__()

    def __deepcopy__(self, memodict={}):
        return copy.deepcopy(self.__root__)