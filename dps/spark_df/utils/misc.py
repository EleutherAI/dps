"""
Miscellaneous utilities
"""

from itertools import islice
import importlib
from collections.abc import Mapping, Sequence

from typing import Dict, Iterable, Type

from .exception import ProcException, SetupException


def recursive_update(dst: Dict, src: Dict, lev: int = 0) -> Dict:
    """
    Retursively add a dict onto another, updating fields as needed
    """
    dst = dst.copy()
    if not src:
        return dst

    try:
        for k, v in src.items():
            if k not in dst:
                dst[k] = v
            elif isinstance(v, Mapping):
                dst[k] = recursive_update(dst[k].copy() or {}, v, lev+1)
            elif isinstance(v, Sequence):
                dst[k] = dst[k].copy() + v
            else:
                dst[k] = v
    except Exception as e:
        raise ProcException("cannot update dictionary: {!r}", e) from e
    return dst


def import_object(objpath: str) -> Type:
    """
    Import a Python object (a function or a class) given its fully qualified
    name
    """
    try:
        modname, oname = objpath.rsplit(".", 1)
        mod = importlib.import_module(modname)
        return getattr(mod, oname)
    except Exception as e:
        raise SetupException("cannot import object '{}': {}", objpath, e) from e


def chunker(it: Iterable[str], size: int) -> Iterable[str]:
    """
    Take a string iterator and join strings into groups
     :param it: iterator to group
     :param size: maximum size of each group (number of items)
    """
    it = iter(it)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield "".join(chunk)
