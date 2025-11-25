"""Centralized shutdown routine for Seamless."""

from __future__ import annotations

import atexit
import asyncio
import sys
from typing import Any, Dict, List

from . import is_worker

_closing = False
_closed = False


def _module(name: str):
    """Return a module from sys.modules without importing it."""

    return sys.modules.get(name)


def _run_coro(coro: Any, loop: asyncio.AbstractEventLoop | None, timeout: float | None):
    """Run an async callable on the provided loop."""

    if coro is None:
        return None
    if loop and loop.is_running():
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        return fut.result(timeout=timeout)
    return asyncio.run(coro) if timeout is None else asyncio.run(asyncio.wait_for(coro, timeout))


def _compute_flush_timeout(entries: Dict[Any, Any], *, multiplier: float = 1.0) -> float:
    total_bytes = 0
    for entry in entries.values():
        buf = getattr(entry, "buffer", None)
        if buf is None:
            continue
        try:
            total_bytes += len(buf.content)
        except Exception:
            pass
    seconds = total_bytes / 1_000_000.0
    return multiplier * max(3.0, seconds if seconds > 0 else 3.0)


def _flush_buffers_short_then_long(failures: List[str]) -> List[str]:
    remaining: List[str] = []
    bw = _module("seamless.caching.buffer_writer")
    if bw is None:
        return remaining
    entries = getattr(bw, "_entries", {})
    if not entries:
        return remaining

    timeout1 = _compute_flush_timeout(entries, multiplier=1.0)
    try:
        bw.flush(timeout=timeout1)
    except Exception as exc:
        failures.append(f"buffer flush (short) raised: {exc}")

    entries = getattr(bw, "_entries", {})
    if entries:
        timeout2 = _compute_flush_timeout(entries, multiplier=2.0)
        try:
            bw.flush(timeout=timeout2)
        except Exception as exc:
            failures.append(f"buffer flush (long) raised: {exc}")
        entries = getattr(bw, "_entries", {})

    remaining = [str(cs) for cs in getattr(bw, "_entries", {}).keys()]
    return remaining


def _close_remote_clients():
    client_mod = _module("seamless_remote.client")
    if client_mod is None:
        return
    close_clients = getattr(client_mod, "close_all_clients", None)
    if close_clients is not None:
        try:
            close_clients()
        except Exception:
            pass


def _sweep_worker_shared_memory(worker_manager: Any, failures: List[str]) -> None:
    if worker_manager is None:
        return
    pointers = list(getattr(worker_manager, "_pointers", {}).values())
    manager = getattr(worker_manager, "_manager", None)
    loop = getattr(worker_manager, "loop", None)
    memory_registry = getattr(manager, "memory_registry", None)
    handles = list(getattr(worker_manager, "_handles", []) or [])
    pids = [getattr(h, "pid", None) for h in handles if getattr(h, "pid", None)]

    if memory_registry is not None:
        for pid in pids:
            try:
                _run_coro(memory_registry.reset_pid(pid), loop, timeout=1.0)
            except Exception as exc:
                failures.append(f"reset_pid failed for PID {pid}: {exc}")
        try:
            _run_coro(memory_registry.close(), loop, timeout=1.0)
        except Exception as exc:
            failures.append(f"memory_registry.close failed: {exc}")

    for ptr in pointers:
        try:
            ptr.shm.close()
        except Exception:
            pass
        try:
            ptr.shm.unlink()
        except Exception:
            pass
    try:
        getattr(worker_manager, "_pointers", {}).clear()
    except Exception:
        pass


def close(*, from_atexit: bool = False) -> None:
    """Close Seamless subsystems to avoid leaks and hangs."""

    global _closing, _closed
    if _closed or _closing:
        return
    if is_worker():
        return

    _closing = True
    failures: List[str] = []
    pending_buffers: List[str] = []
    worker_shutdown = False
    try:
        try:
            import seamless as _self

            setattr(_self, "_closed", True)
        except Exception:
            pass

        worker_mod = _module("seamless_transformer.worker")
        worker_manager = getattr(worker_mod, "_worker_manager", None) if worker_mod else None
        has_workers = bool(worker_mod and getattr(worker_mod, "has_spawned", False))

        # Phase 1: cancel/call manager cleanup
        if worker_mod and has_workers:
            try:
                worker_mod.shutdown_workers()
                worker_shutdown = True
            except Exception as exc:
                failures.append(f"shutdown_workers raised: {exc}")

        # Phase 2: buffer flush attempts
        pending_buffers = _flush_buffers_short_then_long(failures)

        # Close remote client sessions/keepalive
        _close_remote_clients()

        # Sweep shared memory even if workers were force-killed
        if worker_manager is not None:
            _sweep_worker_shared_memory(worker_manager, failures)

    finally:
        summary_parts: List[str] = []
        if pending_buffers:
            summary_parts.append(
                f"unwritten buffers: {', '.join(pending_buffers)}"
            )
        if failures:
            summary_parts.extend(failures)
        if worker_shutdown and not failures:
            summary_parts.append("workers shut down")
        if summary_parts:
            print(
                "[seamless.close] " + " ; ".join(summary_parts),
                file=sys.stderr,
            )
        _closed = True
        _closing = False


def _atexit_close():
    try:
        close(from_atexit=True)
    except Exception:
        pass


atexit.register(_atexit_close)
