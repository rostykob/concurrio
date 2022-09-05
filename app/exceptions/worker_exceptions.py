class ForceCancelException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class FinishedlException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class UnknownCommandException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
