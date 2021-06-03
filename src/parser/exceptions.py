class PrefixURLNotFoundError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class TwoDataFrameNotSameDateError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
