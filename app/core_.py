# import asyncio
# from datetime import datetime
# from contextlib import suppress


# def execution_time(func):
#     async def process(func, *args, **params):
#         if asyncio.iscoroutinefunction(func):
#             return await func(*args, **params)
#         else:
#             return func(*args, **params)

#     async def helper(*args, **params):
#         start = datetime.now()
#         logger.info(f"{func.__name__} starts execution at {str(start.time())}")
#         result = await process(func, *args, **params)
#         stop = datetime.now()
#         logger.info(
#             f"Function {func.__name__} finished execution at {str(stop.time())}. Executed in {str((stop - start).total_seconds())} sec."
#         )
#         return result

#     return helper


# async def parallel_concurrent_executor(
#     work_items: Iterable,
#     work_function: Awaitable,
#     additional_kwargs: Dict[str, Any] = {},
#     concurrency_level: int = 5,
#     processes: int = MAX_PROCESSES,
#     post_processing_setting: Optional[
#         Tuple[Union[Callable, Awaitable], Dict[str, Any]]
#     ] = None,
#     pre_processing_setting: Optional[
#         Tuple[Union[Callable, Awaitable], Dict[str, Any]],
#     ] = None,
#     ignore_errors: bool = False,
#     timeout: int = 60,
# ) -> Optional[Any]:
#     """Executes provided `Awaitable` concurrently on separate processes,  to process `work_items`. Items and `Awaitable` will be placed in `Queue` to be spreaded between workers.
#     Items can be preprocessed or results can be postprocessed if defined. Use it for heavy loads that need to be executed.

#     Args:
#     ---
#         * `work_items` (Iterable): Iterable of items to be processed
#         * `work_function` (Awaitable): Courotine used to process the items. Should have atleast 1 parameter which accepts item to process
#         * `additional_kwargs` (Dict[str, Any]): Additional parameters defined for `work_function` if signature of `work_function` contains more that 1 parameter. Defaults to None.
#         * `concurrency_level` (int, optional): Defines of concurrency level in one process. Defaults to 5.
#         * `processes` (int, optional): Defines how many processes will be used to process the work items. Defaults to max cpu processes - 1.
#         * `post_processing_setting` (Optional[ Tuple[Union[Callable, Awaitable], Dict[str, Any]] ], optional): Tuple that contains a `post_processing` function in position 0 and dictionary of parameters in position 1 for that function
#         Function should have at least 1 paramiter that accepts a Iterable, and must return a Iterable. If function has only 1 parameter, additional parameters on position 1 should be empty dict {}. Defaults to None.
#         * `pre_processing_setting` (Optional[ Tuple[Union[Callable, Awaitable], Dict[str, Any]] ], optional): Tuple that contains a `pre_processing` function in position 0 and dictionary of parameters in position 1 for that function
#         Function should have at least 1 paramiter that accepts a Iterable. If function has only 1 parameter, additional parameters on position 1 should be empty dict {}. Defaults to None.
#         * `timeout` (int, optional): Delay defined for execution before Timeout exception is thrown and execution stopped. Defaults to 60 seconds.
#         * `ignore_errors` (bool, optional): Defines whether errors inside worker should be ignered or execution shoul be stopped on errors. Defaults to False.

#     Raises:
#     ---
#         `task.exception`: Rethrown exception from the task

#     Returns:
#     ---
#         Optional[Any]: Result on processing
#     """
#     if pre_processing_setting:
#         work_items = await _preprocess_items(
#             pre_processing_setting=pre_processing_setting,
#             prefix="Pre",
#             work_items=work_items,
#         )
#     logger.trace(
#         f"Starting setup of executor and queues with {concurrency_level} concurrency level"
#     )
#     if processes > MAX_PROCESSES:
#         logger.warning(
#             f"Cannot use so many processes {processes}, I will use max possible value {MAX_PROCESSES}"
#         )
#         processes = MAX_PROCESSES
#     elif processes < 0:
#         logger.warning(f"Babe please, be serious. {MAX_PROCESSES} will be used")
#         processes = MAX_PROCESSES
#     elif processes == 0:
#         logger.info(f"{MAX_PROCESSES} will be used")
#     else:
#         logger.info(f"{processes} will be used")
#     tasks: List[Task] = list()
#     async with Pool(
#         processes=processes,
#         initializer=CustomizeLogger.customize_logging,
#         initargs=[settings.ENV],
#         maxtasksperchild=concurrency_level,
#     ) as pool:
#         for item in enumerate(work_items):
#             task = pool.apply(
#                 _task_processor,
#                 args=[item[1], work_function],
#                 kwds={
#                     "ignore_errors": ignore_errors,
#                     "fn_kwargs": additional_kwargs,
#                     "item_name": f"{work_function.__name__}_item_{item[0]}",
#                 },
#             )
#             tasks.append(asyncio.create_task(task))
#         results = await _results_waiter(timeout, tasks)
#         logger.success(
#             f"Processing done. Collected {len(results)} items"
#         ) if results else logger.success(f"Processing done.")
#         if post_processing_setting:
#             results = await _preprocess_items(
#                 pre_processing_setting=post_processing_setting,
#                 prefix="Post",
#                 work_items=results,
#             )
#         logger.success(
#             f"Processing finished with {len(results)} items to return"
#         ) if results else logger.success(f"Processing finished.")
#     return results


# async def _preprocess_items(
#     pre_processing_setting: Tuple[Union[Callable, Awaitable], Dict[str, Any]],
#     work_items: Iterable,
#     prefix: str,
# ):
#     logger.trace(
#         f"{prefix}processing settings defined (͡° ͜ʖ ͡°) starting {prefix.lower()}processing using {pre_processing_setting[0].__name__}"
#     )
#     if asyncio.iscoroutinefunction(pre_processing_setting[0]):
#         work_items = await pre_processing_setting[0](
#             work_items, **pre_processing_setting[1]
#         )
#     else:
#         work_items = pre_processing_setting[0](work_items, **pre_processing_setting[1])
#     m = f"{prefix}processing done"
#     if prefix == "Pre":
#         m = m + f", {len(work_items)} work_items defined"
#     logger.success(m)
#     return work_items


# async def concurrent_executor(
#     work_items: Iterable,
#     work_function: Awaitable,
#     additional_kwargs: Dict[str, Any] = {},
#     concurrency_level: int = 5,
#     post_processing_setting: Optional[
#         Tuple[Union[Callable, Awaitable], Dict[str, Any]]
#     ] = None,
#     pre_processing_setting: Optional[
#         Tuple[Union[Callable, Awaitable], Dict[str, Any]]
#     ] = None,
#     loop: Optional[asyncio.AbstractEventLoop] = None,
#     timeout: int = 60,
#     ignore_errors: bool = False,
# ) -> Optional[Any]:
#     """Executes provided `Awaitable` concurrently to process `work_items`. Items will be placed in `Queue` to be spreaded between workers.
#     Items can be preprocessed or results can be postprocessed if defined.

#     Args:
#     ---
#         * `work_items` (Iterable): Iterable of items to be processed
#         * `work_function` (Awaitable): Courotine used to process the items. Should have atleast 1 parameter which accepts item to process
#         * `additional_kwargs` (Dict[str, Any]): Additional parameters defined for `work_function` if signature of `work_function` contains more that 1 parameter. Defaults to None.
#         * `concurrency_level` (int, optional): Defines a ammount of worker that will be spinned up to execute processing concurrently. Defaults to 5.
#         * `post_processing_setting` (Optional[ Tuple[Union[Callable, Awaitable], Dict[str, Any]] ], optional): Tuple that contains a `post_processing` function in position 0 and dictionary of parameters in position 1 for that function
#         Function should have at least 1 paramiter that accepts a Iterable, and must return a Iterable. If function has only 1 parameter, additional parameters on position 1 should be empty dict {}. Defaults to None.
#         * `pre_processing_setting` (Optional[ Tuple[Union[Callable, Awaitable], Dict[str, Any]] ], optional): Tuple that contains a `pre_processing` function in position 0 and dictionary of parameters in position 1 for that function
#         Function should have at least 1 paramiter that accepts a Iterable. If function has only 1 parameter, additional parameters on position 1 should be empty dict {}. Defaults to None.
#         * `loop` (Optional[asyncio.AbstractEventLoop], optional): Event loop where executor should start execution. If no loop provided will use currently running loop. Defaults to None.
#         * `timeout` (int, optional): Delay defined for execution before Timeout exception is thrown and execution stopped. Defaults to 60 seconds.
#         * `ignore_errors` (bool, optional): Defines whether errors inside worker should be ignered or execution shoul be stopped on errors. Defaults to False.

#     Raises:
#     ---
#         `task.exception`: Rethrown exception from the task

#     Returns:
#     ---
#         Optional[Any]: Result on processing
#     """
#     if pre_processing_setting:
#         work_items = await _preprocess_items(
#             pre_processing_setting=pre_processing_setting,
#             prefix="Pre",
#             work_items=work_items,
#         )
#     logger.trace(
#         f"Starting setup of executor and queues with {concurrency_level} concurrency level"
#     )
#     q = asyncio.Queue(loop=loop if loop else None)
#     for wi in work_items:
#         q.put_nowait(item=wi)
#     results: List[Any] = list()
#     tasks = [
#         asyncio.create_task(
#             _processing_queue_worker(
#                 q=q,
#                 fn=work_function,
#                 fn_kwargs=additional_kwargs,
#                 ignore_errors=ignore_errors,
#                 worker_name=f"worker_{work_function.__name__}_{x}",
#             )
#         )
#         for x in range(concurrency_level)
#     ]
#     results = await _results_waiter(timeout=timeout, tasks=tasks)
#     logger.success(
#         f"Processing done. Collected {len(results)} items"
#     ) if results else logger.success(f"Processing done.")
#     if post_processing_setting:
#         results = await _preprocess_items(
#             pre_processing_setting=post_processing_setting,
#             prefix="Post",
#             work_items=results,
#         )
#     logger.success(
#         f"Processing finished with {len(results)} items to return"
#     ) if results else logger.success(f"Processing finished.")
#     return results


# async def _results_waiter(timeout: float, tasks: List[Task]) -> Optional[List[Any]]:
#     results: List[Any] = list()
#     try:
#         await asyncio.wait(
#             tasks,
#             timeout=timeout,
#             return_when=asyncio.FIRST_EXCEPTION,
#         )
#         for task in tasks:
#             if task.done():
#                 if task.exception():
#                     raise task.exception()
#                 results.append(task.result())
#             else:
#                 task.cancel()
#                 #  Once cancel called, it will raise an exception inside the worker which will be exit point for tasks
#                 #  How ever task.done() will be called on empty queue, so we will need to suppress value error
#                 with suppress(ValueError):
#                     await task
#     except asyncio.TimeoutError:
#         logger.warning(
#             f"Processing stopped due to timeout limit reached. Limit is {timeout} sec"
#         )
#         ...
#     except Exception as ex:
#         logger.opt(exception=ex).error("Processing failed, something went wrong")
#         raise
#     return results


# async def _task_processor(
#     i: Any,
#     fn: Awaitable,
#     ignore_errors: bool,
#     item_name: str,
#     fn_kwargs: Dict[str, Any] = {},
# ) -> List[Any]:
#     """Asynchronous worker that has connection to `queue` will execute the `processing function` on recieved `item` from queue

#     Args:
#     ---
#         * `i` (Iterable): Item to be processed inside the worker
#         * `fn` (Awaitable): processing function to be executed towards `i`.
#         *`fn_kwargs` (Dict[str, Any] ): Additional processing function parameters. Defaults to empty dict
#         * `ignore_errors` (bool, optional): Defines whether errors inside worker should be ignered or execution shoul be stopped on errors. Defaults to False.
#         * `worker_name` (str): Name of this worker

#     Returns:
#     ---
#         List[Any]: List of processing results
#     """
#     start = datetime.now()
#     logger.trace(f"Worker starts execution  of item  {item_name} at {str(start)}")
#     try:
#         r = await fn(i, **fn_kwargs)
#         logger.trace(
#             f"Worker finished processing item item_name {item_name}, no errors"
#         )
#     #  Exit point from infinite loop, will be triggered once processing is done
#     except asyncio.CancelledError:
#         logger.warning("Cancellation recieved")
#         return r
#     except Exception as ex:
#         # if we will get any exception, it will be caught and forwarded to main thread. we can pick in on task,result()
#         logger.opt(exception=ex).error(
#             f"Above exception raised during processing on worker item with name {item_name} "
#         )
#         if ignore_errors:
#             logger.warning(f"Ignore errors is defined so I will continue")
#             ...
#         else:
#             raise
#     stop = datetime.now()
#     logger.trace(
#         f"Worker finished execution of item {item_name} at {str(stop)}. Executed in {str((stop - start).total_seconds())} sec."
#     )
#     return r


# async def _processing_queue_worker(
#     q: asyncio.Queue,
#     fn: Awaitable,
#     ignore_errors: bool,
#     worker_name: str,
#     fn_kwargs: Dict[str, Any] = {},
# ) -> List[Any]:
#     """Asynchronous worker that has connection to `queue` will execute the `processing function` on recieved `item` from queue

#     Args:
#     ---
#         * `q` (asyncio.Queue): Queue with work items
#         * `fn` (Awaitable): processing function to be executed towards `item` from `queue`
#         *`fn_kwargs` (Dict[str, Any] ): Additional processing function parameters. Defaults to empty dict
#         * `ignore_errors` (bool, optional): Defines whether errors inside worker should be ignered or execution shoul be stopped on errors. Defaults to False.
#         * `worker_name` (str): Name of this worker

#     Returns:
#     ---
#         List[Any]: List of processing results
#     """
#     results: List[Any] = list()
#     start = datetime.now()
#     logger.trace(f"Worker {worker_name} starts execution at {str(start)}")
#     try:
#         while not q.empty():
#             try:
#                 logger.trace(f"Worker {worker_name} getting item from a queue")
#                 workload = await q.get()
#                 logger.trace(
#                     f"Worker {worker_name} starting processing item from a queue"
#                 )
#                 r = await fn(workload, **fn_kwargs)
#                 logger.trace(
#                     f"Worker {worker_name} finished processing item from a queue, no errors"
#                 )
#                 results.append(r)
#             #  Exit point from infinite loop, will be triggered once processing is done
#             except asyncio.CancelledError:
#                 logger.warning("Cancellation recieved")
#                 return results
#             except Exception as ex:
#                 # if we will get any exception, it will be caught and forwarded to main thread. we can pick in on task,result()
#                 logger.opt(exception=ex).error(
#                     f"Above exception raised during processing on worker {worker_name} "
#                 )
#                 if ignore_errors:
#                     logger.warning(f"Ignore errors is defined so I will continue")
#                     ...
#                 else:
#                     raise
#             finally:
#                 q.task_done()
#     except:
#         raise
#     stop = datetime.now()
#     logger.trace(
#         f"Worker {worker_name} finished execution at {str(stop)}. Executed in {str((stop - start).total_seconds())} sec."
#     )
#     return results
