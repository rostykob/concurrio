from queue import Queue
from typing import Any, Awaitable, Callable, Dict, Tuple

QueueCollection = Dict[str, Dict[str, Queue]]
FunctionWithParameters = Tuple[Callable | Awaitable, Dict[str, Any]]
