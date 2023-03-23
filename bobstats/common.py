from typing import List, Dict, Optional
from decimal import Decimal
from pydantic import BaseModel

class ChainStats(BaseModel):
    dt: int
    chain: str
    totalSupply: Decimal
    colCirculatingSupply: Decimal
    volumeUSD: Decimal
    holders: int
    fees: Optional[Dict[str, Decimal]]

    def _serialize(self, output: dict):
        for k, v in output.items():
            if isinstance(v, Decimal):
                output[k] = float(v)
            if isinstance(v, dict):
                self._serialize(v)

    def dict(self, **kwargs):
        output = super().dict(**kwargs)
        self._serialize(output)
        return output

StatsByChains = List[Dict[str, ChainStats]] 