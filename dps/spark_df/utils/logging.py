"""
A wrapper for the standard Python logging module that adds a new "TRACE" level
"""

from functools import partial, partialmethod

import logging

from logging import CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET  # noqa: F401
from logging import getLevelName   # noqa: F401


# The name & numeric value of the new level we are defining
TRACE_NAME = 'TRACE'
TRACE = DEBUG - 5


def _check_levels():
    """
    Check if the new level is there, define it if not
    """
    if not hasattr(logging, "trace"):
        logging.addLevelName(TRACE, TRACE_NAME)
        logging.Logger.trace = partialmethod(logging.Logger.log, TRACE)
        logging.trace = partial(logging.log, TRACE)


def basicConfig(**kwargs):
    """
    wrapper for logging.basicConfig
    """
    _check_levels()
    return logging.basicConfig(**kwargs)


def getLogger(name: str = None):
    """
    wrapper for logging.getLogger
    """
    _check_levels()
    return logging.getLogger(name)
