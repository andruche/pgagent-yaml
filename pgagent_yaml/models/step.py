from pydantic import BaseModel
from typing import Union
from enum import Enum


class Kind(str, Enum):
    sql = "sql"
    batch = "batch"


class OnError(str, Enum):
    success = "success"
    fail = "fail"
    ignore = "ignore"


class Step(BaseModel):
    enabled: bool
    description: Union[str, None]
    kind: Kind
    on_error: OnError
    connection_string: Union[str, None]
    local_database: Union[str, None]
    code: str
