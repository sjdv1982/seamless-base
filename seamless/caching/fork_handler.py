from __future__ import annotations

import os
import threading
import warnings
from typing import Callable, List

_before_fork_subhooks: List[Callable[[], None]] = []
_after_fork_child_subhooks: List[Callable[[], None]] = []
_after_fork_parent_subhooks: List[Callable[[], None]] = []


def register_before_fork_subhook(subhook: Callable[[], None]) -> None:
    _before_fork_subhooks.append(subhook)


def register_after_fork_child_subhook(subhook: Callable[[], None]) -> None:
    _after_fork_child_subhooks.append(subhook)


def register_after_fork_parent_subhook(subhook: Callable[[], None]) -> None:
    _after_fork_parent_subhooks.append(subhook)


def _run_subhooks(subhooks: List[Callable[[], None]]) -> None:
    for subhook in list(subhooks):
        subhook()


def _before_fork() -> None:
    _run_subhooks(_before_fork_subhooks)
    alive_threads = [
        t
        for t in threading.enumerate()
        if t.is_alive() and t != threading.current_thread()
    ]
    if alive_threads:
        warnings.warn(
            f"This process (pid={os.getpid()}) is multi-threaded, "
            "use of fork() may lead to deadlocks in the child.",
            DeprecationWarning,
            stacklevel=2,
        )


def _after_fork_child() -> None:
    _run_subhooks(_after_fork_child_subhooks)


def _after_fork_parent() -> None:
    _run_subhooks(_after_fork_parent_subhooks)


if hasattr(os, "register_at_fork"):  # pragma: no cover - platform dependent
    os.register_at_fork(
        before=_before_fork,
        after_in_child=_after_fork_child,
        after_in_parent=_after_fork_parent,
    )


__all__ = [
    "register_before_fork_subhook",
    "register_after_fork_child_subhook",
    "register_after_fork_parent_subhook",
]
