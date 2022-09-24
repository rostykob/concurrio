from enum import Enum
from typing import Any, Dict, Optional, TypeVar
from uuid import uuid4

from pydantic import BaseModel, Field


class ProcessingStates(Enum):
    success = "success"
    failed = "failed"


class Processor(BaseModel):
    current_fn: Any
    workload: Any
    additional_params: Optional[Dict[str, Any]] = {}
    next_processor: Optional["Processor"]
    flow_id: str = Field(str(uuid4()))


ProcessorType = TypeVar("ProcessorType", bound=Processor, contravariant=True)
