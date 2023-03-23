from pydantic import BaseModel

class TimestampedBaseModel(BaseModel):
    timestamp: int
