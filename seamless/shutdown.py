"""Centralized shutdown routine for Seamless."""

from __future__ import annotations

import atexit
import asyncio
import logging
import os
import sys
import multiprocessing as mp
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
    return (
        asyncio.run(coro)
        if timeout is None
        else asyncio.run(asyncio.wait_for(coro, timeout))
    )


def _compute_flush_timeout(
    entries: Dict[Any, Any], *, multiplier: float = 1.0
) -> float:
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


def _quiet_workers(worker_manager: Any, failures: List[str]) -> None:
    if worker_manager is None:
        return
    handles = list(getattr(worker_manager, "_handles", []) or [])
    loop = getattr(worker_manager, "loop", None)
    debug = bool(os.environ.get("SEAMLESS_DEBUG_TRANSFORMATION"))
    for handle in handles:
        try:
            if debug:
                print(
                    f"[seamless.close] sending quiet to {getattr(handle, 'name', '?')}",
                    file=sys.stderr,
                )
            _run_coro(handle.request("quiet", None, timeout=0.5), loop, timeout=0.5)
            if debug:
                print(
                    f"[seamless.close] quiet ack from {getattr(handle, 'name', '?')}",
                    file=sys.stderr,
                )
        except Exception as exc:
            failures.append(
                f"quiet request failed for {getattr(handle, 'name', '?')}: {exc}"
            )


def _wait_workers_ready(
    worker_manager: Any, failures: List[str], timeout: float = 0.2
) -> None:
    """Give workers a brief chance to finish their ready handshake to avoid broken pipes."""

    if worker_manager is None:
        return
    loop = getattr(worker_manager, "loop", None)
    handles = list(getattr(worker_manager, "_handles", []) or [])
    for handle in handles:
        try:
            if handle.ready_event.is_set():
                continue
        except Exception:
            continue
        try:
            assert loop is not None
            fut = asyncio.run_coroutine_threadsafe(handle.wait_until_ready(), loop)
            fut.result(timeout=timeout)
        except Exception as exc:
            failures.append(
                f"wait_until_ready timed out for {getattr(handle, 'name', '?')}: {exc}"
            )


def close(*, from_atexit: bool = False) -> None:
    """Close Seamless subsystems to avoid leaks and hangs."""

    global _closing, _closed
    if _closed or _closing:
        return
    if is_worker():
        return

    in_child_process = False
    try:
        in_child_process = mp.parent_process() is not None
    except Exception:
        pass

    _closing = True
    failures: List[str] = []
    pending_buffers: List[str] = []
    worker_shutdown = False
    try:
        if from_atexit and not getattr(
            sys.modules.get("seamless"), "_require_close", False
        ):
            # Nothing in Seamless was used; skip shutdown noise.
            _closed = True
            _closing = False
            return
        if from_atexit:
            # Quiet noisy worker/pipe logging when called late in interpreter teardown.
            for logger_name in (
                "seamless_transformer.process.manager",
                "seamless_transformer.process.channel",
            ):
                try:
                    logging.getLogger(logger_name).setLevel(logging.ERROR)
                except Exception:
                    pass
            if not in_child_process:
                print(
                    "[seamless.close] called via atexit; call seamless.close() earlier to ensure all buffers/workers shut down cleanly (atexit is best-effort/ noisier)",
                    file=sys.stderr,
                )
        try:
            import seamless as _self

            setattr(_self, "_closed", True)
        except Exception:
            pass

        worker_mod = _module("seamless_transformer.worker")
        worker_manager = (
            getattr(worker_mod, "_worker_manager", None) if worker_mod else None
        )
        has_workers = bool(worker_mod and getattr(worker_mod, "has_spawned", False))

        if worker_manager is not None:
            try:
                setattr(worker_manager, "_closing", True)
                for handle in getattr(worker_manager, "_handles", []):
                    try:
                        handle.restart = False
                    except Exception:
                        pass
            except Exception:
                pass

        if from_atexit and worker_manager is not None:
            _wait_workers_ready(worker_manager, failures)
            _quiet_workers(worker_manager, failures)

        # Phase 1: cancel/call manager cleanup
        if worker_mod and has_workers:
            try:
                worker_mod.shutdown_workers()
                worker_shutdown = True
            except Exception as exc:
                if isinstance(exc, RuntimeError) and "Seamless has been closed" in str(
                    exc
                ):
                    # If close() already marked the session closed, suppress noisy retry error.
                    pass
                else:
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
        debug = bool(os.environ.get("SEAMLESS_DEBUG_TRANSFORMATION"))
        write_clients = False
        try:
            br_mod = _module("seamless_remote.buffer_remote")
            if br_mod is not None:
                clients = getattr(br_mod, "_write_server_clients", []) or []
                write_clients = any(not getattr(c, "readonly", True) for c in clients)
        except Exception:
            pass
        if pending_buffers and write_clients:
            summary_parts.append(f"unwritten buffers: {', '.join(pending_buffers)}")
        if failures:
            summary_parts.extend(failures)
        if worker_shutdown and (failures or pending_buffers or debug):
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
