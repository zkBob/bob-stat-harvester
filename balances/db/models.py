from pydantic import BaseModel
from typing import Optional

class DBAConfig(BaseModel):
    chainid: str
    snapshot_dir: str
    snapshot_file_suffix: str
    init_block: int
    tsdb_dir: Optional[str]
    tsdb_file_suffix: Optional[str]
