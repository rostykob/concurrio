import signal
from abc import ABCMeta
from typing import Any, Callable, List, Optional

from schemas.processor import ProcessorType


class AbsWorker(metaclass=ABCMeta):
    async def process_workload(self) -> None:
        ...


class CancellableAbsWorker(metaclass=ABCMeta):
    async def process_workload(self, signum: signal) -> None:
        ...


class AbsProcessWorker(AbsWorker):
    def run_async(
        self, init_fn: Optional[Callable] = None, init_args: Optional[List[Any]] = None
    ) -> None:
        ...


class AbsCoroutineWorker(CancellableAbsWorker):
    async def handle_processing(self, processor: ProcessorType) -> None:
        ...
