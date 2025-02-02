import inspect
import signal
from datetime import datetime
from itertools import cycle
from logging import Logger
from multiprocessing import Queue
from multiprocessing.connection import Connection
from queue import Empty
from signal import SIGINT
from typing import Any, Callable, Dict, List, Optional

import anyio
import sniffio._impl as sniff
from exceptions.worker_exceptions import ForceCancelException, UnknownCommandException
from interfaces.abs_handler import AbsResultHandler, CommandHandler, ResultHandlerType
from interfaces.abs_shedulers import SchedulerType
from interfaces.abs_worker import AbsCoroutineWorker, AbsProcessWorker
from schemas.alliases import QueueCollection
from schemas.command import Command, CommandType, ProcessingCommandType
from schemas.processor import ProcessingStates, ProcessorType
from utils import common_utils


class ResultHandlerBase(AbsResultHandler):
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
        super().__init__(
            in_qs,
            out_q,
            worker_name,
            logger,
            work_items_size,
            common_q_scheduler,
            f_cancel_pipes,
            result_queue,
            bulk_return,
        )
        self.in_qs: Dict[str, QueueCollection] = in_qs
        self.out_q: Queue = out_q
        self.worker_name: str = worker_name
        self.result_queue: Queue = result_queue
        self.bulk_return: bool = bulk_return
        self.results: List[Any] = []
        self.common_qids_iter: cycle = cycle(list(self.in_qs.keys()))
        self.signum: Optional[SIGINT] = None
        self.logger = logger if logger else common_utils.logger
        self.common_q_scheduler = common_q_scheduler
        self._work_items_size = work_items_size
        self.processed: Dict[str, int] = {}
        self.processed[ProcessingStates.success.value] = 0
        self.processed[ProcessingStates.failed.value] = 0
        self._lock: anyio.Lock = None
        self._f_cancel_pipes: List[Connection] = f_cancel_pipes
        self.errors: List[str] = list()


class AioProcessBase(AbsProcessWorker):
    def __init__(
        self,
        q_id: str,
        in_q: Queue,
        out_q: Queue,
        logger: Logger,
        concurrency_level: int = 10,
        ignore_errors: bool = False,
        timeout: float = 120,
    ):
        self.q_id = q_id
        self.in_q: Queue = in_q
        self.out_q: Queue = out_q
        self.finished: anyio.Event = None
        self.concurrency_level: int = concurrency_level
        self.ignore_errors: bool = ignore_errors
        self.timeout: float = timeout
        self.logger = logger if logger else common_utils.logger


class AioChildProcessBase(AioProcessBase):
    def __init__(
        self,
        q_id: str,
        in_q: Queue,
        out_q: Queue,
        logger: Logger,
        f_cancel_pipe: Connection,
        concurrency_level: int = 10,
        ignore_errors: bool = False,
        timeout: float = 120,
    ):
        super().__init__(
            q_id,
            in_q,
            out_q,
            logger,
            concurrency_level,
            ignore_errors,
            timeout,
        )
        self.f_cancel_pipe: Connection = f_cancel_pipe


class AioMainProcessBase(AioProcessBase):
    def __init__(
        self,
        q_id: str,
        in_q: Queue,
        out_q: Queue,
        result_handler: ResultHandlerType,
        logger: Logger,
        concurrency_level: int = 10,
        ignore_errors: bool = False,
        timeout: float = 120,
        # prioritize: bool = True,
    ):
        super().__init__(
            q_id,
            in_q,
            out_q,
            logger,
            concurrency_level,
            ignore_errors,
            timeout,
        )
        self.result_handler: ResultHandlerType = result_handler
        self.lock: anyio.Lock = None
        # TODO: Implement logic for force cancel
        # self.prioritize: bool = prioritize


class AioCoroutineWorkerBase(AbsCoroutineWorker, CommandHandler):
    def __init__(
        self,
        in_q: Queue,
        out_q: Queue,
        finished: anyio.Event,
        ignore_errors: bool,
        worker_name: str,
        scope: anyio.CancelScope,
        logger: Logger,
        f_cancel_pipe: Optional[Connection] = None,
    ) -> None:
        self.in_q: Queue = in_q
        self.out_q: Queue = out_q
        self.finished: anyio.Event = finished
        self.ignore_errors: bool = ignore_errors
        self.worker_name: str = worker_name
        self.scope: anyio.CancelScope = scope
        self.signum: signal.SIGINT = None
        self.logger = logger if logger else common_utils.logger
        self.f_cancel_pipe: Connection = f_cancel_pipe


class AioMainProcessWorker(AioMainProcessBase):
    def __init__(
        self,
        q_id: str,
        in_q: Queue,
        out_q: Queue,
        result_handler: ResultHandlerType,
        logger: Logger,
        concurrency_level: int = 10,
        ignore_errors: bool = False,
        timeout: float = 120,
    ):
        super().__init__(
            q_id,
            in_q,
            out_q,
            result_handler,
            logger,
            concurrency_level,
            ignore_errors,
            timeout,
        )

    async def process_workload(self) -> None:
        self.finished = anyio.Event()
        self.lock = anyio.Lock()
        self.result_handler._lock = self.lock
        async with anyio.create_task_group() as tg:
            with anyio.open_signal_receiver(signal.SIGINT) as signum:

                # TODO: Think on priority queue
                # if self.prioritize:
                #     worker = AioCoroutineWorker(
                #         in_q=self.in_q,
                #         out_q=self.out_q,
                #         finished=self.finished,
                #         ignore_errors=self.ignore_errors,
                #         worker_name=f"{self.__class__.__name__}_prio",
                #         scope=scope,
                #         signum=signum,
                #     )
                #     tg.start_soon(worker.process_workload)
                # else:
                with anyio.CancelScope(deadline=self.timeout) as scope:
                    tg.start_soon(self.result_handler.process_workload, signum)
                    for x in range(self.concurrency_level):
                        worker = AioCoroutineWorker(
                            in_q=self.in_q,
                            out_q=self.out_q,
                            finished=self.finished,
                            ignore_errors=self.ignore_errors,
                            worker_name=f"{self.__class__.__name__}_{x}",
                            scope=scope,
                            logger=self.logger,
                        )
                        tg.start_soon(worker.process_workload, signum)

    def run_async(
        self, init_fn: Optional[Callable] = None, init_args: Optional[List[Any]] = None
    ) -> None:
        # process_worker = AioProcessWorker(
        #     in_q=self.in_q,
        #     out_q=self.out_q,
        #     concurrency_level=self.concurrency_level,
        #     ignore_errors=self.ignore_errors,
        #     timeout=self.timeout,
        # )
        if init_fn:
            if init_args:
                init_fn(*init_args)
            else:
                init_fn()
        sniff.current_async_library_cvar.set(None)
        anyio.run(
            self.process_workload,
            backend="asyncio",
            backend_options={"use_uvloop": True},
        )


class AioProcessWorker(AioChildProcessBase):
    def __init__(
        self,
        q_id: str,
        in_q: Queue,
        out_q: Queue,
        logger: Logger,
        f_cancel_pipe: Connection,
        concurrency_level: int = 10,
        ignore_errors: bool = False,
        timeout: float = 120,
    ):
        super().__init__(
            q_id,
            in_q,
            out_q,
            logger,
            f_cancel_pipe,
            concurrency_level,
            ignore_errors,
            timeout,
        )

    async def process_workload(self) -> None:
        self.finished = anyio.Event()
        async with anyio.create_task_group() as tg:
            with anyio.open_signal_receiver(signal.SIGINT) as signum:
                with anyio.CancelScope(deadline=self.timeout) as scope:
                    for x in range(self.concurrency_level):
                        worker = AioCoroutineWorker(
                            in_q=self.in_q,
                            out_q=self.out_q,
                            finished=self.finished,
                            f_cancel_pipe=self.f_cancel_pipe,
                            ignore_errors=self.ignore_errors,
                            worker_name=f"{self.__class__.__name__}_{x}",
                            scope=scope,
                            logger=self.logger,
                        )
                        tg.start_soon(worker.process_workload, signum)
                        if self.finished.is_set():
                            tg.start_soon(anyio.to_thread.run_sync, self.force_close)

    # TODO: Think of main functions in class private and this fn static
    def run_async(
        self, init_fn: Optional[Callable] = None, init_args: Optional[List[Any]] = None
    ) -> None:
        # process_worker = AioProcessWorker(
        #     in_q=self.in_q,
        #     out_q=self.out_q,
        #     concurrency_level=self.concurrency_level,
        #     ignore_errors=self.ignore_errors,
        #     timeout=self.timeout,
        # )
        # loop = asyncio.get_running_loop()
        # loop.run_until_complete(self.process_workload)
        if init_fn:
            if init_args:
                init_fn(*init_args)
            else:
                init_fn()
        sniff.current_async_library_cvar.set(None)
        anyio.run(
            self.process_workload,
            backend="asyncio",
            backend_options={"use_uvloop": True},
        )


class AioCoroutineWorker(AioCoroutineWorkerBase):
    def __init__(
        self,
        in_q: Queue,
        out_q: Queue,
        finished: anyio.Event,
        ignore_errors: bool,
        worker_name: str,
        scope: anyio.CancelScope,
        logger: Logger,
        f_cancel_pipe: Optional[Connection] = None,
    ) -> None:
        super().__init__(
            in_q,
            out_q,
            finished,
            ignore_errors,
            worker_name,
            scope,
            logger,
            f_cancel_pipe,
        )

    async def process_workload(self, signum: signal) -> None:
        self.signum = signum
        start = datetime.now()
        self.logger.debug(f"Worker {self.worker_name} starts execution at {str(start)}")
        try:
            while not self.finished.is_set():
                if self.signum == signal.SIGINT:
                    self.logger.warning(
                        f"Ctrl+C pressed, cancelling processing on worker {self.worker_name}"
                    )
                    self.scope.cancel()
                    return
                if self.f_cancel_pipe and self.f_cancel_pipe.poll(0):
                    cmd = self.f_cancel_pipe.recv()
                    await self.handle_command(command=cmd)
                try:
                    self.logger.debug(
                        f"Worker {self.worker_name} getting item from a queue"
                    )
                    cmd: ProcessingCommandType = self.in_q.get_nowait()
                    await self.handle_command(command=cmd)
                except ForceCancelException:
                    raise anyio.get_cancelled_exc_class()()
                except Empty:
                    await anyio.sleep(0)
                    continue
                except anyio.get_cancelled_exc_class():
                    self.logger.warning(
                        f"Cancellation recieved. Processing cancelled on worker {self.worker_name}"
                    )
                    stop = datetime.now()
                    self.logger.debug(
                        f"Worker {self.worker_name} finished execution at {str(stop)}. Executed in {str((stop - start).total_seconds())} sec."
                    )
                    return
                except ForceCancelException:
                    raise anyio.get_cancelled_exc_class()()
        except:
            raise
        stop = datetime.now()
        self.logger.debug(
            f"Worker {self.worker_name} finished execution at {str(stop)}. Executed in {str((stop - start).total_seconds())} sec."
        )

    async def handle_processing(self, processor: ProcessorType) -> None:
        self.logger.debug(
            f"Worker {self.worker_name} starting processing item from a queue"
        )
        try:
            if inspect.iscoroutinefunction(processor.current_fn):
                processed_workload = await processor.current_fn(
                    processor.workload, **processor.additional_params
                )
            else:
                args = [processor.workload]
                if processor.additional_params:
                    args.extend([x for x in processor.additional_params.values()])
                processed_workload = await anyio.to_thread.run_sync(
                    processor.current_fn, *args
                )
            self.logger.debug(
                f"Worker {self.worker_name} finished processing item from a queue, no errors"
            )
            if processor.next_processor:
                processor.next_processor.workload = processed_workload
                command = Command(
                    command_type=CommandType.reschedule,
                    item=processor.next_processor,
                )
            else:
                command = Command(
                    command_type=CommandType.return_result, item=processed_workload
                )
            self.out_q.put_nowait(command)
        except Exception as ex:
            # if we will get any exception, it will be caught and forwarded to main thread. we can pick in on task,result()
            self.logger.error(
                f"Above exception raised during processing on worker {self.worker_name}. Exception details {str(ex)}"
            )
            if self.ignore_errors:
                self.logger.warning(f"Ignore errors is defined so I will continue")
                command = Command(
                    command_type=CommandType.exception_ignore, item=str(ex)
                )
            else:
                command = Command(command_type=CommandType.exception_fail, item=str(ex))
            self.out_q.put_nowait(command)

    async def handle_command(self, command: ProcessingCommandType) -> None:
        match command.command_type.value:
            case CommandType.process.value:
                await self.handle_processing(processor=command.item)
            case CommandType.finalize.value:
                await self.finished.set()
            case CommandType.force_cancel.value:
                raise ForceCancelException()
            case _:
                raise UnknownCommandException()


class AioCoroutineResultHandler(ResultHandlerBase):
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
        super().__init__(
            in_qs,
            out_q,
            worker_name,
            logger,
            work_items_size,
            common_q_scheduler,
            f_cancel_pipes,
            result_queue,
            bulk_return,
        )

    async def process_workload(self, signum: signal) -> None:
        start = datetime.now()
        self.signum == signum
        self.logger.debug(f"Worker {self.worker_name} starts execution at {str(start)}")
        while True:
            if self.signum == signal.SIGINT:
                self.logger.info("Ctrl+C pressed!")
                stop = datetime.now()
                self.logger.debug(
                    f"Worker {self.worker_name} finished execution at {str(stop)}. Executed in {str((stop - start).total_seconds())} sec."
                )
                await self.finalize()
                break
            if (
                self._work_items_size
                == self.processed["success"] + self.processed["failed"]
            ):
                await self.finalize()
                break
            try:
                self.logger.debug(
                    f"Worker {self.worker_name} getting item from a queue"
                )
                cmd: Command = self.out_q.get_nowait()
                await self.handle_command(command=cmd)
            except ForceCancelException as ex:
                raise anyio.get_cancelled_exc_class()(str(ex))
            except Empty:
                await anyio.sleep(0)
                continue
            except anyio.get_cancelled_exc_class():
                self.logger.warning("Cancellation recieved")
                stop = datetime.now()
                self.logger.debug(
                    f"Worker {self.worker_name} finished execution at {str(stop)}. Executed in {str((stop - start).total_seconds())} sec."
                )
                return
            except Exception as ex:
                # if we will get any exception, it will be caught and forwarded to main thread. we can pick in on task,result()
                self.logger.error(
                    f"Above exception raised during processing on worker {self.worker_name} "
                )
                raise
            stop = datetime.now()
            self.logger.debug(
                f"Worker {self.worker_name} finished execution at {str(stop)}. Executed in {str((stop - start).total_seconds())} sec."
            )

    async def handle_command(self, command: ProcessingCommandType) -> None:
        match command.command_type.value:
            case CommandType.return_result.value:
                if self.bulk_return:
                    self.results.append(command.item)
                else:
                    self.result_queue.put_nowait(command.item)
                await self.register_processed_item(ProcessingStates.success.value)
            case CommandType.reschedule.value:
                cmd = Command(command_type=CommandType.process, item=command.item)
                self.in_qs[self.common_q_scheduler.schedule()].put_nowait(cmd)
            case CommandType.exception_ignore.value:
                await self.register_processed_item(
                    ProcessingStates.failed.value, error=command.item
                )
            case CommandType.exception_fail.value:
                self.force_cancel_processing(command.item)
            case _:
                raise UnknownCommandException()

    def force_cancel_processing(self, error: str):
        for fcp in self._f_cancel_pipes:
            cmd = Command(command_type=CommandType.force_cancel)
            fcp.send(cmd)
        self.result_queue.put_nowait(error)
        raise ForceCancelException(f"Cancelled due to exception {error}")

    async def register_processed_item(
        self, processing_state: str, error: str = None
    ) -> None:
        async with self._lock:
            self.processed[processing_state] = self.processed[processing_state] + 1
            if error:
                self.errors.append(error)

    async def finalize(self) -> Dict[str, int]:
        for q in self.in_qs.values():
            cmd = Command(command_type=CommandType.finalize)
            q.put_nowait(cmd)
        if self.bulk_return:
            self.result_queue.put_nowait(self.results)
        return self.processed
