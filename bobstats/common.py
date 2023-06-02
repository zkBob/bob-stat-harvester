from typing import List, Dict, Optional
from decimal import Decimal
from pydantic import BaseModel

class OneTokenAcc(BaseModel):
    symbol: str
    amount: Decimal

YieldSet = List[OneTokenAcc]

class GainStats(BaseModel):
    fees: YieldSet
    interest: Optional[YieldSet]

    def is_empty(self) -> bool:
        retval = True
        if not self.is_fees_empty():
            retval = False
        if not self.is_interest_empty():
            retval = False
        return retval
    
    def is_fees_empty(self) -> bool:
        retval = True
        if self.fees and len(self.fees) > 0:
            retval = False
        return retval

    def is_interest_empty(self) -> bool:
        retval = True
        if self.interest and len(self.interest) > 0:
            retval = False
        return retval

    def adjust(self, source):
        def adjust_gain_set(fees_source: YieldSet, fees_target: YieldSet):
            for f in fees_source:
                found = False
                for target in fees_target:
                    if target.symbol == f.symbol:
                        found = True
                        target.amount += f.amount
                        break
                if not found:
                    fees_target.append(f)
            
        if not source.is_fees_empty():
            if not self.fees:
                self.fees = []
            adjust_gain_set(source.fees, self.fees)
        if not source.is_interest_empty():
            if not self.interest:
                self.interest = []
            adjust_gain_set(source.interest, self.interest)
        
class ChainStats(BaseModel):
    dt: int
    chain: str
    totalSupply: Decimal
    colCirculatingSupply: Decimal
    volumeUSD: Decimal
    holders: int
    gain: Optional[GainStats]

    def _serialize(self, output: dict):
        for k, v in output.items():
            if isinstance(v, Decimal):
                output[k] = float(v)
            if isinstance(v, dict):
                self._serialize(v)
            if isinstance(v, list):
                for i in v:
                    self._serialize(i)

    def dict(self, **kwargs):
        output = super().dict(**kwargs)
        self._serialize(output)
        return output

StatsByChains = List[Dict[str, ChainStats]] 