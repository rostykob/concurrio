import inspect
import signal
from datetime import datetime
from itertools import cycle
from logging import Logger
from multiprocessing import Pipe, Queue
from multiprocessing.connection import Connection
from queue import Empty
from typing import Any, Callable, Dict, List, Optional
from unittest import result

import anyio
import loguru
import sniffio._impl as sniff
from exceptions.worker_exceptions import ForceCancelException, UnknownCommandException
from schemas.command import Command, CommandType
from schemas.processor import ProcessingStates, Processor

from core.scheduler import Scheduler


class AioMainProcessWorker:
    def __init__(
        self,
        q_id: str,
        in_q: Queue,
        out_q: Queue,
        result_handler: "AioCoroutineResultHandler",
        logger: Logger,
        concurrency_level: int = 10,
        ignore_errors: bool = False,
        timeout: float = 120,
        # prioritize: bool = True,
    ):
        self.q_id = q_id
        self.in_q: Queue = in_q
        self.out_q: Queue = out_q
        self.finished: anyio.Event = None
        self.concurrency_level: int = concurrency_level
        self.ignore_errors: bool = ignore_errors
        self.timeout: float = timeout
        self.result_handler: AioCoroutineResultHandler = result_handler
        self.logger = logger if logger else loguru.logger
        self.lock: anyio.Lock = None
        # TODO: Implement logic for force cancel
        # self.prioritize: bool = prioritize

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


class AioProcessWorker:
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
        self.q_id = q_id
        self.in_q: Queue = in_q
        self.out_q: Queue = out_q
        self.finished: anyio.Event = None
        self.concurrency_level: int = concurrency_level
        self.ignore_errors: bool = ignore_errors
        self.timeout: float = timeout
        self.logger = logger if logger else loguru.logger
        self.f_cancel_pipe: Connection = f_cancel_pipe

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


# TODO:1) Derrive from Interface 2) Add Generic on Processor and Command
class AioCoroutineWorker:
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
        self.logger = logger if logger else loguru.logger
        self.f_cancel_pipe: Connection = f_cancel_pipe

    async def process_workload(self, signum: signal) -> None:
        self.signum = signum
        start = datetime.now()
        self.logger.debug(f"Worker {self.worker_name} starts execution at {str(start)}")
        try:
            while not self.finished.is_set():
                if self.signum == signal.SIGINT:
                    self.logger("Ctrl+C pressed!")
                    self.scope.cancel()
                    return
                if self.f_cancel_pipe and self.f_cancel_pipe.poll(0):
                    cmd = self.f_cancel_pipe.recv()
                    await self.handle_command(command=cmd)
                try:
                    self.logger.debug(
                        f"Worker {self.worker_name} getting item from a queue"
                    )
                    cmd: Command = self.in_q.get_nowait()
                    await self.handle_command(command=cmd)
                except ForceCancelException:
                    raise anyio.get_cancelled_exc_class()()
                except Empty:
                    self.logger.debug(f"No Items in queue at the moment, waiting...")
                    await anyio.sleep(0)
                    continue
                except anyio.get_cancelled_exc_class():
                    self.logger.warning("Cancellation recieved")
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

    async def handle_processing(self, processor: Processor) -> None:
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
                command = Command(command_type=CommandType.exception_ignore)
            else:
                command = Command(command_type=CommandType.exception_fail, item=ex)
            self.out_q.put_nowait(command)

    async def handle_command(self, command: Command) -> None:
        match command.command_type.value:
            case CommandType.process.value:
                await self.handle_processing(processor=command.item)
            case CommandType.finalize.value:
                await self.finished.set()
            case CommandType.force_cancel.value:
                raise ForceCancelException()
            case _:
                raise UnknownCommandException()


class AioCoroutineResultHandler:
    def __init__(
        self,
        in_qs: Dict[str, Dict[str, Queue]],
        out_q: Queue,
        worker_name: str,
        logger: Logger,
        work_items_size,
        common_q_scheduler: Scheduler,
        f_cancel_pipes: List[Pipe],
        result_queue: Queue = None,
        bulk_return: bool = True,
    ) -> None:
        self.in_qs: Dict[str, Queue] = in_qs
        self.out_q: Queue = out_q
        self.worker_name: str = worker_name
        self.result_queue: Queue = result_queue
        self.bulk_return: bool = bulk_return
        self.results: List[Any] = []
        self.common_qids_iter: cycle = cycle(list(self.in_qs.keys()))
        self.signum: Optional[signal.signal] = None
        self.logger = logger if logger else loguru.logger
        self.common_q_scheduler = common_q_scheduler
        self._work_items_size = work_items_size
        self.processed: Dict[str, int] = {}
        self.processed[ProcessingStates.success.value] = 0
        self.processed[ProcessingStates.failed.value] = 0
        self._lock: anyio.Lock = None
        self._f_cancel_pipes: List[Connection] = f_cancel_pipes

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
                self.logger.debug(f"No Items in queue at the moment, waiting...")
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

    async def handle_command(self, command: Command) -> None:
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
                await self.register_processed_item(ProcessingStates.failed.value)
            case CommandType.exception_fail.value:
                self.force_cancel_processing(command.item)
            case _:
                raise UnknownCommandException()

    def force_cancel_processing(self, error: str):
        for fcp in self._f_cancel_pipes:
            cmd = Command(command_type=CommandType.force_cancel)
            fcp.send(cmd)
        self.result_queue.put_nowait(error)
        raise ForceCancelException(f"Cancelled due to exception {str(error)}")

    async def register_processed_item(self, processing_state: str) -> None:
        async with self._lock:
            self.processed[processing_state] = self.processed[processing_state] + 1

    async def finalize(self) -> Dict[str, int]:
        for q in self.in_qs.values():
            cmd = Command(command_type=CommandType.finalize)
            q.put_nowait(cmd)
        if self.bulk_return:
            self.result_queue.put_nowait(self.results)
        return self.processed
