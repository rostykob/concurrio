from abc import ABCMeta
from itertools import cycle
from logging import Logger
from multiprocessing import Queue
from multiprocessing.connection import Connection
from signal import SIGINT
from typing import Any, Dict, List, Optional, TypeVar

import anyio
from schemas.alliases import QueueCollection
from schemas.command import ProcessingCommandType

from interfaces.abs_shedulers import SchedulerType


class CommandHandler(metaclass=ABCMeta):
    async def handle_command(self, command: ProcessingCommandType) -> None:
        ...


class AbsResultHandler(CommandHandler):
    def __init__(
        self,
        in_qs: Dict[str, QueueCollection],
        out_q: Queue,
        worker_name: str,
        logger: Logger,
        work_items_size,
        common_q_scheduler: SchedulerType,
        f_cancel_pipes: List[Connection],
        result_queue: Queue = None,
        bulk_return: bool = True,
    ) -> None:
        self.in_qs: Dict[str, QueueCollection]
        self.out_q: Queue
        self.worker_name: str
        self.result_queue: Queue
        self.bulk_return: bool
        self.results: List[Any] = []
        self.common_qids_iter: cycle
        self.signum: Optional[SIGINT]
        self.logger: Logger
        self.common_q_scheduler: SchedulerType
        self._work_items_size: int
        self.processed: Dict[str, int]
        self._lock: anyio.Lock
        self._f_cancel_pipes: List[Connection]
        self.errors: List[str]

    async def finalize(self) -> Dict[str, int]:
        ...

    async def register_processed_item(self, processing_state: str) -> None:
        ...

    def force_cancel_processing(self, error: str):
        ...


ResultHandlerType = TypeVar("ResultHandlerType", bound=AbsResultHandler, covariant=True)
