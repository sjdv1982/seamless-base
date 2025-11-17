import sys

from .checksum_class import Checksum as _Checksum
from .buffer_class import Buffer as _Buffer

# Expose classes under the top-level module so their repr shows seamless.Checksum/Buffer
Checksum = _Checksum
Checksum.__module__ = __name__
Buffer = _Buffer
Buffer.__module__ = __name__

from .checksum.expression import Expression as _Expression

Expression = _Expression
Expression.__module__ = __name__


class CacheMissError(Exception):
    """Exception for when a checksum cannot be mapped to a buffer"""


SEAMLESS_WORKFLOW_IMPORTED = False

__all__ = ["Checksum", "Buffer", "Expression", "CacheMissError"]

try:
    import seamless_config as config

    __all__.append("config")
    sys.modules["seamless.config"] = config
except ImportError:
    pass
