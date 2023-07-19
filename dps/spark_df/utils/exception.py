

class DpsException(Exception):
    """
    Base exception
     :param msg: an output exception message string, for which the format method
       is called
    """
    def __init__(self, msg: str, *args):
        super().__init__(msg.format(*args) if args else msg)


class SetupException(DpsException):
    """
    An exception triggered during process setup
    """
    pass


class ProcException(DpsException):
    """
    An exception triggered during processing
    """
    pass

