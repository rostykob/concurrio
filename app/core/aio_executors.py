import inspect
import multiprocessing
import os
from logging import Logger
from multiprocessing.context import BaseContext
from multiprocessing.queues import Queue
from typing import Any, Callable, Dict, Iterable, List, Literal, Optional

import anyio
from interfaces.abs_executors import AbsExecutor
from interfaces.abs_shedulers import AbsScheduler
from schemas.alliases import FunctionWithParameters, QueueCollection
from schemas.command import Command, CommandType
from schemas.processor import ProcessorType
from schemas.queues import QueueType
from utils import common_utils

from core.aio_workers import (
    AioCoroutineResultHandler,
    AioMainProcessWorker,
    AioProcessWorker,
)
from core.schedulers import Scheduler


class AioExecutorBase(AbsExecutor):
    def __init__(
        self,
        ctx: Literal["fork", "spawn"] = "fork",
        processes: int = 0,
        concurrency: int = 10,
        init_fn: Optional[Callable] = None,
        init_args: Optional[Iterable[Any]] = None,
        logger: Optional[Logger] = None,
    ):
        self.logger = logger if logger else common_utils.logger
        self.queues: QueueCollection = self.initialize_queues_dict()
        self.context: BaseContext = multiprocessing.get_context(ctx)
        self.processes = self.get_max_allowed_processes(processes)
        self.result_q: Dict[str, multiprocessing.Queue] = None
        self.init_fn: Optional[Callable] = init_fn
        self.init_args: Optional[Iterable[Any]] = init_args
        self.concurrency = concurrency
        self._process_workers: List[multiprocessing.Process] = []
        self.common_q_scheduler: AbsScheduler = None

    def initialize_queues_dict(self) -> QueueCollection:
        q = {}
        for qt in QueueType:
            q[qt.value] = {}
        return q

    def get_queue_id(self, q_type: str) -> str:
        return f"q_{len(self.queues[QueueType[q_type].value])}"

    def register_queue(self, q_type: str) -> None:
        id = self.get_queue_id(q_type)
        self.queues[QueueType[q_type].value][id] = self.context.Queue()
        self.logger.info(f"Queue registered with id {id} type {q_type}")
        return id

    def get_max_allowed_processes(self, processes: int) -> int:
        max_processes = os.cpu_count() - 1 if os.cpu_count() > 1 else 1
        if processes > max_processes:
            self.logger.warning(
                f"Cannot use so many processes {processes}, I will use max possible value {max_processes}"
            )
            processes = max_processes
        elif processes < 0:
            self.logger.warning(
                f"Negative numbers not allowed {max_processes} will be used"
            )
            processes = max_processes
        elif not processes or processes == 0:
            processes = max_processes
            self.logger.info(f"{max_processes} will be used")
        else:
            self.logger.info(f"{processes} will be used")
        return processes

    def release_resources(self) -> None:
        self._process_workers = []
        self.common_q_scheduler = None
        self.queues = self.initialize_queues_dict()
        self.result_q = None

    def start_processes(self):
        for process in self._process_workers:
            process.start()

    async def _preprocess_items(
        self,
        processing_setting: FunctionWithParameters,
        work_items: Iterable,
        prefix: str,
    ):
        self.logger.debug(
            f"{prefix}processing settings defined. Starting {prefix.lower()}processing using {processing_setting[0].__name__}"
        )
        if inspect.iscoroutinefunction(processing_setting[0]):
            work_items = await processing_setting[0](
                work_items, **processing_setting[1]
            )
        else:
            work_items = processing_setting[0](work_items, **processing_setting[1])
        m = f"{prefix}processing done"
        if prefix == "Pre":
            m = m + f", {len(work_items)} work_items defined"
        self.logger.info(m)
        return work_items


class ParallelConcurrentExecutor(AioExecutorBase):
    def __init__(
        self,
        ctx: Literal["fork", "spawn"] = "fork",
        processes: int = 0,
        concurrency: int = 10,
        init_fn: Optional[Callable] = None,
        init_args: Optional[Iterable[Any]] = None,
        logger: Optional[Logger] = None,
    ):
        super().__init__(ctx, processes, concurrency, init_fn, init_args, logger)
        # if processes > 1:
        #     self.register_queue("priority")

    async def process(
        self,
        work_items: Iterable,
        function_processor: ProcessorType,
        pre_processing_setting: FunctionWithParameters = None,
        post_processing_setting: FunctionWithParameters = None,
        ignore_errors: bool = False,
        timeout: float = 120,
        result_q: Queue = None,
    ) -> Optional[Iterable]:
        self.result_q = result_q if result_q else self.context.Queue()
        if not result_q and post_processing_setting:
            # TODO: Custom exception
            raise Exception(
                "Cannot have postprocessing setting without bulk output return"
            )
        if pre_processing_setting:
            work_items = await self._preprocess_items(
                processing_setting=pre_processing_setting,
                prefix="Pre",
                work_items=work_items,
            )

        self.setup_processes(
            ignore_errors=ignore_errors,
            bulk_return=True if not result_q else False,
            timeout=timeout,
            work_items_size=len(work_items),
        )
        for item in work_items:
            processor = function_processor.copy(deep=True)
            processor.workload = item
            cmd = Command(command_type=CommandType.process, item=processor)
            qid = self.common_q_scheduler.schedule()
            self.queues[QueueType.common.value][qid].put_nowait(cmd)

        await self.wait_for_results()

        results = None
        if not result_q:
            # TODO: Wrap results into Some Bulk results schema to provide additional metadata
            results = self.result_q.get()
            if isinstance(results, BaseException):
                raise results
            if post_processing_setting and results:
                work_items = await self._preprocess_items(
                    processing_setting=pre_processing_setting,
                    prefix="Post",
                    work_items=work_items,
                )
        self.release_resources()
        return results

    async def wait_for_results(self):
        async with anyio.create_task_group() as tg:
            for process in self._process_workers:
                tg.start_soon(anyio.to_thread.run_sync, process.join)

    def setup_processes(
        self,
        ignore_errors: bool,
        timeout: float,
        bulk_return: bool,
        work_items_size: int,
    ) -> None:
        out_q_id = self.register_queue("output")
        pipes = list()
        for x in range(self.processes):
            q_id = self.register_queue("common")
            if x != self.processes - 1:
                f_cancel_sender, f_cancel_reciever = self.context.Pipe()
            if self.processes == 1 or x == self.processes - 1:
                self.common_q_scheduler = Scheduler(
                    list(self.queues[QueueType.common.value].keys())
                )
                rh = AioCoroutineResultHandler(
                    in_qs=self.queues[QueueType.common.value],
                    out_q=self.queues[QueueType.output.value][out_q_id],
                    worker_name=f"{AioCoroutineResultHandler.__class__.__name__}",
                    result_queue=self.result_q,
                    bulk_return=bulk_return,
                    f_cancel_pipes=pipes,
                    logger=self.logger,
                    common_q_scheduler=self.common_q_scheduler,
                    work_items_size=work_items_size,
                )
                w = AioMainProcessWorker(
                    q_id=q_id,
                    in_q=self.queues[QueueType.common.value][q_id],
                    out_q=self.queues[QueueType.output.value][out_q_id],
                    result_handler=rh,
                    concurrency_level=self.concurrency,
                    ignore_errors=ignore_errors,
                    timeout=timeout,
                    logger=self.logger,
                )
            else:
                w = AioProcessWorker(
                    q_id=q_id,
                    f_cancel_pipe=f_cancel_reciever,
                    in_q=self.queues[QueueType.common.value][q_id],
                    out_q=self.queues[QueueType.output.value][out_q_id],
                    concurrency_level=self.concurrency,
                    ignore_errors=ignore_errors,
                    timeout=timeout,
                    logger=self.logger,
                )
            pipes.append(f_cancel_sender)
            p: multiprocessing.Process = self.context.Process(
                target=w.run_async,
                kwargs={"init_fn": self.init_fn, "init_args": self.init_args},
            )
            self._process_workers.append(p)
        self._process_workers.reverse()
        self.start_processes()
