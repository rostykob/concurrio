import multiprocessing
from abc import ABCMeta
from logging import Logger
from multiprocessing.context import BaseContext
from typing import Any, Callable, Dict, Iterable, List, Literal, Optional

from schemas.alliases import FunctionWithParameters, QueueCollection
from schemas.processor import Processor

from .abs_shedulers import AbsScheduler


class AbsExecutor(metaclass=ABCMeta):
    def __init__(
        self,
        ctx: Literal["fork", "spawn"] = "fork",
        processes: int = 0,
        concurrency: int = 10,
        init_fn: Optional[Callable] = None,
        init_args: Optional[Iterable[Any]] = None,
        logger: Optional[Logger] = None,
    ):
        self.queues: QueueCollection = None
        self.context: BaseContext = None
        self.processes = None
        self.result_q: Dict[str, multiprocessing.Queue] = None
        self.init_fn: Optional[Callable] = None
        self.init_args: Optional[Iterable[Any]] = None
        self.logger = None
        self.concurrency = None
        self._process_workers: List[multiprocessing.Process] = None
        self.common_q_scheduler: AbsScheduler = None

    def get_max_allowed_processes(self, processes: int) -> int:
        ...

    def initialize_queues_dict(self) -> QueueCollection:
        ...

    def register_queue(self, q_type: str) -> None:
        ...

    def get_queue_id(self, q_type: str) -> str:
        ...

    async def process(
        self,
        work_items: Iterable,
        function_processor: Processor,
        pre_processing_setting: Optional[FunctionWithParameters] = None,
        post_processing_setting: Optional[FunctionWithParameters] = None,
        ignore_errors: bool = False,
        timeout: float = 120,
        result_q: multiprocessing.Queue = None,
    ) -> Optional[Iterable]:
        ...

    def release_resources(self) -> None:
        ...

    def start_processes(self) -> None:
        ...

    def setup_processes(
        self,
        ignore_errors: bool,
        timeout: float,
        bulk_return: bool,
        work_items_size: int,
    ) -> None:
        ...

    async def _preprocess_items(
        self,
        processing_setting: FunctionWithParameters,
        work_items: Iterable,
        prefix: str,
    ):
        ...

    async def wait_for_results(self):
        ...
