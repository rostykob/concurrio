from abc import ABCMeta


class AbsScheduler(metaclass=ABCMeta):
    def schedule(self) -> None:
        ...
