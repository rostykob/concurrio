from enum import Enum
from typing import Any, Optional, TypeVar

from pydantic import BaseModel


class CommandType(Enum):
    return_result = 0
    process = 1
    reschedule = 2
    force_cancel = 3
    finalize = 4
    exception_ignore = 5
    exception_fail = 6


class Command(BaseModel):
    command_type: CommandType
    item: Optional[Any]


ProcessingCommandType = TypeVar("ProcessingCommandType", bound=Command, covariant=True)
