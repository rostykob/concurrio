import logging
import multiprocessing
import multiprocessing.managers
import os
import random
from signal import raise_signal
from time import sleep

import anyio

from core.aio_executors import ParallelConcurrentExecutor
from utils.common_utils import create_processor

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.WARNING)
logger = logging.getLogger(__name__)


async def add(item):
    v = item + 1
    print(f"Function ADD inside {os.getpid()}  and item was {item} item now {v}")
    await anyio.sleep(random.randint(3, 5))
    return v


async def mul(item):
    v = item * 2
    print(f"Function MUL inside {os.getpid()}  and item was {item} item now {v}")
    await anyio.sleep(random.randint(3, 5))
    return v


async def mul_bad(item):
    v = item * 2
    print(f"Function MUL inside {os.getpid()}  and item was {item} item now {v}")
    if random.randint(1, 4) == 4:
        raise Exception("poop")
    await anyio.sleep(random.randint(3, 5))
    return v


def random_add(item, add):
    v = item + random.randint(3, 5)
    print(
        f"Function RANDOM_ADD inside {os.getpid()}  and item was {item} item now {v}, additional param is {add}"
    )
    sleep(random.randint(3, 5))
    return v


async def main():
    processor = create_processor(
        add, mul_bad, (random_add, {"add": random.randint(1, 5)})
    )
    executor = ParallelConcurrentExecutor(logger=logger, processes=0)
    workload = [z for z in range(10)]
    res = await executor.process(work_items=workload, function_processor=processor)
    print(res)
    workload = [z for z in range(10)]
    processor = create_processor(
        add, mul_bad, (random_add, {"add": random.randint(1, 5)})
    )
    # res1 = await executor.process(work_items=workload, function_processor=processor)


if __name__ == "__main__":
    ctx = multiprocessing.get_context("fork")
    anyio.run(main)
