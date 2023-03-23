from typing import Optional, Dict, Union
from pydantic import BaseModel

class WorkerHealthModelBase(BaseModel):
    status: str
    lastSuccessTimestamp: int
    lastErrorTimestamp: int
    dataTimestamp: Optional[int]

class WorkerHealthModelOut(WorkerHealthModelBase):
    lastSuccessDatetime: Optional[str]
    secondsSinceLastSuccess: Optional[int]
    lastErrorDatetime: Optional[str]
    secondsSinceLastError: Optional[int]

class HealthOut(BaseModel):
    currentDatetime: str
    modules: Dict[str, Union[WorkerHealthModelOut, Dict[str, WorkerHealthModelOut]]]