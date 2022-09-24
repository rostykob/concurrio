from itertools import cycle

from interfaces.abs_shedulers import AbsScheduler

class Scheduler(AbsScheduler):
    def __init__(self, q_ids: str):
        super().__init__()
        self.qids_iter: cycle = cycle(q_ids)

    def schedule(self) -> None:
        return next(self.qids_iter)
