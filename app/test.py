import logging
import multiprocessing
import multiprocessing.managers
import os
import random
from time import sleep

import anyio
from aiomultiprocess import Pool

from core.aio_executors import ParallelConcurrentExecutor
from utils.common_utils import create_processor, execution_time


async def add(item):
    v = item + 1
    # print(f"Function ADD inside {os.getpid()}  and item was {item} item now {v}")
    await anyio.sleep(3)
    return v


async def mul(item):
    v = item * 2
    # print(f"Function MUL inside {os.getpid()}  and item was {item} item now {v}")
    await anyio.sleep(3)
    return v


async def mul_bad(item):
    v = item * 2
    print(f"Function MUL inside {os.getpid()}  and item was {item} item now {v}")
    if random.randint(1, 4) == 4:
        raise Exception("poop")
    await anyio.sleep(5)
    return v


async def power_async(item, add):
    v = item**5
    # print(
    #     f"Function RANDOM_ADD inside {os.getpid()}  and item was {item} item now {v}, additional param is {add}"
    # )
    sleep(3)
    return v


async def power_async_no_param(item):
    v = item**5
    # print(
    #     f"Function RANDOM_ADD inside {os.getpid()}  and item was {item} item now {v}, additional param is {add}"
    # )
    sleep(3)
    return v


def power_sync(item, add):
    v = item**5
    print(
        f"Function RANDOM_ADD inside {os.getpid()}  and item was {item} item now {v}, additional param is {add}"
    )
    sleep(3)
    return v


async def main():
    workload = [z for z in range(10)]
    await parallel_executor_test(workload)
    await aio_multiprocess_test(workload)
    await sync_execution_test(workload)


@execution_time
async def aio_multiprocess_test(workload):
    async with Pool(processes=5, childconcurrency=10) as pool:
        res = await pool.map(add, workload)
        res = list(res)
        res = await pool.map(mul, res)
        res = list(res)
        # no possibility to combine sync with async fn, no additional params in map (you can do it of cource but will require more codding)
        res = await pool.map(power_async_no_param, res)
        res = list(res)
    print(res)


@execution_time
async def sync_execution_test(workload):
    results = list()
    for item in workload:
        r = await add(item)
        r = await mul(r)
        r = await power_async(r, 5)
        results.append(r)
    print(results)


@execution_time
async def parallel_executor_test(workload):
    processor = create_processor(add, mul, (power_async, {"add": 5}))
    executor = ParallelConcurrentExecutor(processes=5, concurrency=10)
    res = await executor.process(work_items=workload, function_processor=processor)
    print(res)


if __name__ == "__main__":
    ctx = multiprocessing.get_context("fork")
    anyio.run(main)
