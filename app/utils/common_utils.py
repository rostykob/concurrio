import inspect
import logging
from datetime import datetime
from itertools import islice
from typing import Any, Awaitable, Callable, Dict, Generator, Iterable, List, Tuple

from schemas.processor import Processor

logging.basicConfig(
    format="[%(levelname)s][PID:%(process)d][ThreadID:%(thread)d]: %(asctime)s - %(module)s %(funcName)s: %(lineno)d - %(message)s",
    # level=logging.DEBUG,
)
logger = logging.getLogger(__name__)


def execution_time(func):
    async def process(func, *args, **params):
        if inspect.iscoroutinefunction(func):
            return await func(*args, **params)
        else:
            return func(*args, **params)

    async def helper(*args, **params):
        start = datetime.now()
        logger.info(f"{func.__name__} starts execution at {str(start.time())}")
        result = await process(func, *args, **params)
        stop = datetime.now()
        logger.info(
            f"Function {func.__name__} finished execution at {str(stop.time())}. Executed in {str((stop - start).total_seconds())} sec."
        )
        return result

    return helper


def create_processor(
    *args: List[Tuple[Awaitable | Callable, Dict[str, Any]] | Awaitable | Callable]
) -> Processor:
    args = list(args)
    head_fn = args.pop(0)
    if isinstance(head_fn, tuple):
        processor = Processor(
            current_fn=head_fn[0],
            workload=None,
            additional_params=head_fn[1] if head_fn[1] else {},
        )
    else:
        processor = Processor(
            current_fn=head_fn,
            workload=None,
            additional_params={},
        )
    next_processor: Processor = processor
    for fn in args:
        if isinstance(fn, tuple):
            nxt = Processor(
                current_fn=fn[0],
                workload=None,
                additional_params=fn[1] if fn[1] else {},
            )
        else:
            nxt = Processor(current_fn=fn, workload=None, additional_params={})
        next_processor.next_processor = nxt
        next_processor = nxt
    return processor


def batch_generator(iterable: Iterable, batch_size=50) -> Generator:
    if isinstance(iterable, list):
        l = len(iterable)
        for ndx in range(0, l, batch_size):
            yield iterable[ndx : min(ndx + batch_size, l)]
    elif isinstance(iterable, dict):
        it = iter(iterable)
        for i in range(0, len(iterable), batch_size):
            yield {k: iterable[k] for k in islice(it, batch_size)}
    else:
        raise NotImplementedError()


def batch_list(iterable: Iterable, batch_size: int = 50) -> List[Any]:
    return list(batch_generator(iterable=iterable, batch_size=batch_size))


def execution_time(func):
    async def process(func, *args, **params):
        if inspect.iscoroutinefunction(func):
            return await func(*args, **params)
        else:
            return func(*args, **params)

    async def helper(*args, **params):
        start = datetime.now()
        print(f"{func.__name__} starts execution at {str(start.time())}")
        logger.info(f"{func.__name__} starts execution at {str(start.time())}")
        result = await process(func, *args, **params)
        stop = datetime.now()
        print(
            f"Function {func.__name__} finished execution at {str(stop.time())}. Executed in {str((stop - start).total_seconds())} sec."
        )
        logger.info(
            f"Function {func.__name__} finished execution at {str(stop.time())}. Executed in {str((stop - start).total_seconds())} sec."
        )
        return result

    return helper
