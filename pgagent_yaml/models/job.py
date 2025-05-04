from pydantic import BaseModel
from typing import Union

from pgagent_yaml.models.schedule import Schedule
from pgagent_yaml.models.step import Step


class Job(BaseModel):
    enabled: bool
    description: Union[str, None]
    schedules: dict[str, Schedule]
    steps: dict[str, Step]
