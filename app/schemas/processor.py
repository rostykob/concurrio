from enum import Enum
from typing import Any, Awaitable, Dict, Optional

from pydantic import BaseModel


class ProcessingStates(Enum):
    success = "success"
    failed = "failed"


class Processor(BaseModel):
    current_fn: Any
    workload: Any
    additional_params: Optional[Dict[str, Any]] = {}
    next_processor: Optional["Processor"]
