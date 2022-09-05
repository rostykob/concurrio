from itertools import cycle
from multiprocessing.queues import Queue
from typing import Dict, List

from loguru import logger
from schemas.queues import QueueType


class Scheduler:
    def __init__(self, q_ids: str):
        self.qids_iter: cycle = cycle(q_ids)

    def schedule(self) -> None:
        return next(self.qids_iter)
