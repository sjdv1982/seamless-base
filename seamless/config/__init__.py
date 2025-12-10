try:
    from seamless_config import *

    __all__ = [
        "init",
        "set_stage",
        "set_substage",
        "set_workdir",
        "collect_remote_clients",
        "set_remote_clients",
    ]
except ImportError:
    __all__ = []
