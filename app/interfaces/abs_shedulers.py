from abc import ABCMeta
from typing import TypeVar


class AbsScheduler(metaclass=ABCMeta):
    def schedule(self) -> None:
        ...


SchedulerType = TypeVar("SchedulerType", bound=AbsScheduler, covariant=True)
