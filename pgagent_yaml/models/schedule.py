from pydantic import BaseModel, Field, field_validator
from typing import List, Union, Literal, Annotated
from enum import Enum


class Weekday(str, Enum):
    monday = "monday"
    tuesday = "tuesday"
    wednesday = "wednesday"
    thursday = "thursday"
    friday = "friday"
    saturday = "saturday"
    sunday = "sunday"


Minute = Annotated[int, Field(ge=0, le=59)]
Hour = Annotated[int, Field(ge=0, le=23)]
Monthday = Union[Annotated[int, Field(ge=1, le=31)], Literal["last day"]]
Month = Annotated[int, Field(ge=1, le=12)]


class Schedule(BaseModel):
    enabled: bool
    description: Union[str, None]
    minutes: Union[List[Minute], Literal["*"], Literal["-"]]
    hours: Union[List[Hour], Literal["*"], Literal["-"]]
    monthdays: Union[List[Monthday], Literal["*"], Literal["-"]]
    months: Union[List[Month], Literal["*"], Literal["-"]]
    weekdays: Union[List[Weekday], Literal["*"], Literal["-"]]

    @field_validator("minutes", "hours", "monthdays", "months", "weekdays", mode="after")
    @classmethod
    def check_duplicates_int_lists(cls, v):
        if len(set(v)) != len(v):
            raise ValueError("Duplicate values are not allowed")
        return v
