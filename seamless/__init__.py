import sys


class CacheMissError(Exception):
    """Exception for when a checksum cannot be mapped to a buffer"""


_IS_WORKER = False
_closed = False
_require_close = False


def set_is_worker(value: bool = True) -> None:
    """Mark the current process as a Seamless worker process."""

    global _IS_WORKER
    _IS_WORKER = bool(value)


def is_worker() -> bool:
    """Return True when running inside a Seamless worker process."""

    return _IS_WORKER


def ensure_open(op: str | None = None, *, mark_required: bool = True) -> None:
    """Raise RuntimeError if Seamless was closed; optionally mark that close is required."""

    global _require_close
    if mark_required:
        _require_close = True
    if _closed:
        action = f" for {op}" if op else ""
        raise RuntimeError(
            f"Seamless has been closed; cannot perform further operations{action}."
        )


from .checksum_class import Checksum as _Checksum
from .buffer_class import Buffer as _Buffer
from .shutdown import close

# Expose classes under the top-level module so their repr shows seamless.Checksum/Buffer
Checksum = _Checksum
Checksum.__module__ = __name__
Buffer = _Buffer
Buffer.__module__ = __name__

from .checksum.expression import Expression as _Expression

Expression = _Expression
Expression.__module__ = __name__

__all__ = [
    "Checksum",
    "Buffer",
    "Expression",
    "CacheMissError",
    "set_is_worker",
    "is_worker",
    "ensure_open",
    "close",
]

try:
    import seamless_config as config

    __all__.append("config")
    sys.modules["seamless.config"] = config
except ImportError:
    pass
