"""Background buffer writer
Whenever a buffer (not just a checksum) is entered into the strong buffer_cache.py cache,
buffer_writer.register(buffer) will be invoked. This adds the buffer to a queue,
unless the buffer is already in the queue (note that the checksum of the buffer is always known).

In the background, a dedicated thread hosts an asyncio event loop that runs the writer coroutine.
All queued buffers are written remotely/asynchronously in the same way that Buffer.write does.
The asynchronous tasks are stored in the queue.

Whenever Buffer.write is invoked, it checks first with the buffer writer if the asynchronous task
for that buffer is already in the queue. If so, it awaits that task and returns.

Whenever the process is forked:
  - In the parent: we stop the worker thread before fork and start a fresh one afterwards,
    preserving the queue so pending writes continue after the fork.
  - In the child: buffer_writer.purge() is called to drop all state, leaving the child without
    a background writer unless it later calls init().
During the parent’s before-fork hook we emit the standard multi-threaded fork warning if any
foreign threads remain. run.py suppresses Python's own warning only when this hook is installed.
init() is automatically called on module import to start the thread and associated loop if
they are not already running.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
import os
import traceback
import warnings
from http.client import HTTPConnection
from urllib.parse import urlsplit
from dataclasses import dataclass
from typing import Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from seamless.buffer_class import Buffer
    from seamless.checksum_class import Checksum


@dataclass(slots=True)
class _QueueEntry:
    checksum: "Checksum"
    buffer: "Buffer"
    future: concurrent.futures.Future
    queued: bool = False


_entries: Dict["Checksum", _QueueEntry] = {}
_loop: Optional[asyncio.AbstractEventLoop] = None
_queue: Optional[asyncio.Queue[_QueueEntry]] = None
_thread: Optional[threading.Thread] = None
_lock = threading.RLock()


def init() -> None:
    """Ensure that the background writer thread is running."""
    _ensure_worker()


def register(buffer: "Buffer") -> None:
    """Register a buffer for background writing."""
    checksum = buffer.checksum
    with _lock:
        entry = _entries.get(checksum)
        if entry is not None:
            entry.buffer = buffer
        else:
            future: concurrent.futures.Future = concurrent.futures.Future()
            entry = _QueueEntry(checksum=checksum, buffer=buffer, future=future)
            _entries[checksum] = entry
    _enqueue_entry(entry)


async def await_existing_task(checksum: "Checksum") -> Optional[bool]:
    """Await the shared write task for checksum if it exists."""
    entry = _entries.get(checksum)
    if entry is None:
        return None
    future = entry.future
    if future.cancelled():
        return None
    _enqueue_entry(entry)
    if future.done():
        return future.result()
    wrapped = asyncio.wrap_future(future)
    return await wrapped


def purge() -> None:
    """Stop the worker thread and clear all queued buffers."""
    _stop_worker()
    with _lock:
        entries = list(_entries.values())
        _entries.clear()
    for entry in entries:
        if not entry.future.done():
            entry.future.cancel()


def flush(timeout: Optional[float] = None) -> None:
    """Synchronously write all queued buffers using direct HTTP calls.
    This is normally called upon interpreter shutdown.
    We can't use aiohttp because we can't create new futures at shutdown.
    """

    try:
        import seamless_remote.buffer_remote as buffer_remote
    except ImportError:
        return

    buffers = {checksum: entry.buffer for checksum, entry in _entries.items()}

    clients = []
    for c in getattr(buffer_remote, "_write_server_clients", []):
        try:
            init_sync = getattr(c, "ensure_initialized_sync", None)
            if init_sync is not None:
                init_sync()
        except Exception:
            continue
        if not getattr(c, "url", None):
            continue
        try:
            setattr(c, "_shutdown", True)
        except Exception:
            pass
        clients.append(c)
    if not clients:
        return

    start = time.time()

    def remaining() -> Optional[float]:
        if timeout is None:
            return None
        return max(0.0, timeout - (time.time() - start))

    health_ok: Dict[str, bool] = {}
    for checksum, buffer in buffers.items():
        success = False
        for client in clients:
            url = client.url.rstrip("/")
            ok = health_ok.get(url)
            if ok is None:
                ok = _healthcheck_sync(url, remaining())
                health_ok[url] = ok
            if not ok:
                continue
            if _put_sync(url, checksum, buffer, remaining()):
                success = True

        if success:
            entry = _entries.get(checksum)
            if entry is not None:
                fut = entry.future
                if not fut.done():
                    fut.set_result(success)
                with _lock:
                    _entries.pop(checksum, None)

    for client in clients:
        try:
            close_sessions = getattr(client, "_close_sessions", None)
            if close_sessions is not None:
                close_sessions()
        except Exception:
            pass

    _stop_worker()


# --- worker thread management -------------------------------------------------
def _ensure_worker() -> None:
    global _thread
    with _lock:
        thread = _thread
        if thread is not None and thread.is_alive():
            return
        worker = threading.Thread(
            target=_thread_main,
            name="SeamlessBufferWriter",
            daemon=True,
        )
        _thread = worker
    worker.start()


def _stop_worker() -> None:
    thread = None
    with _lock:
        loop = _loop
        queue = _queue
        thread = _thread
    if loop is None or queue is None or thread is None:
        return

    def _request_stop() -> None:
        queue.put_nowait(None)

    loop.call_soon_threadsafe(_request_stop)
    thread.join()
    with _lock:
        for entry in _entries.values():
            entry.queued = False
        if _idle_event is not None:
            _idle_event.set()


def _close_worker_thread_sessions(thread: threading.Thread) -> None:
    """Ensure aiohttp sessions owned by the worker thread get closed."""
    try:
        import seamless_remote.buffer_remote as buffer_remote
    except ImportError:
        return
    clients = getattr(buffer_remote, "_write_server_clients", [])
    for client in clients:
        close_for_thread = getattr(client, "_close_sessions_for_thread", None)
        if close_for_thread is None:
            continue
        try:
            close_for_thread(thread)
        except Exception:
            traceback.print_exc()


def _thread_main() -> None:
    global _loop, _queue, _thread
    loop = asyncio.new_event_loop()
    queue: asyncio.Queue[_QueueEntry] = asyncio.Queue()
    try:
        asyncio.set_event_loop(loop)
        with _lock:
            _loop = loop
            _queue = queue
            pending = [
                entry
                for entry in _entries.values()
                if not entry.future.done()
                and not entry.future.cancelled()
                and not entry.queued
            ]
            for entry in pending:
                entry.queued = True
        for entry in pending:
            queue.put_nowait(entry)
        loop.run_until_complete(_worker_loop(queue))
    finally:
        try:
            _close_worker_thread_sessions(threading.current_thread())
        except Exception:
            traceback.print_exc()
        asyncio.set_event_loop(None)
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        loop.close()
        with _lock:
            _loop = None
            _queue = None
            _thread = None
            for entry in _entries.values():
                entry.queued = False


async def _worker_loop(queue: asyncio.Queue[_QueueEntry]) -> None:
    loop = asyncio.get_running_loop()
    pending: set[asyncio.Task] = set()

    def _on_done(task: asyncio.Task) -> None:
        pending.discard(task)

    while True:
        entry = await queue.get()
        if entry is None:
            break
        if entry.future.cancelled() or entry.future.done():
            continue
        task = loop.create_task(_process_entry(entry))
        pending.add(task)
        task.add_done_callback(_on_done)
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


async def _process_entry(entry: _QueueEntry) -> None:
    try:
        import seamless_remote.buffer_remote as buffer_remote
    except ImportError:
        result = False
        error = None
    else:
        try:
            result = await buffer_remote.write_buffer(entry.checksum, entry.buffer)
            error = None
        except Exception as exc:  # pragma: no cover - network errors propagated
            result = None
            error = exc

    future = entry.future
    if not future.done():
        if error is None:
            future.set_result(result)
        else:
            future.set_exception(error)
    with _lock:
        _entries.pop(entry.checksum, None)


# --- queue submission helpers -------------------------------------------------
def _enqueue_entry(entry: _QueueEntry) -> None:
    _ensure_worker()
    with _lock:
        loop = _loop
        queue = _queue
        already_queued = entry.queued
    if loop is None or queue is None or already_queued or entry.future.done():
        return

    def _put(e: _QueueEntry) -> None:
        if e.future.cancelled() or e.future.done() or e.queued:
            return
        e.queued = True
        queue.put_nowait(e)

    loop.call_soon_threadsafe(_put, entry)


# --- fork handling ------------------------------------------------------------
def _before_fork() -> None:
    _stop_worker()
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
    purge()


def _after_fork_parent() -> None:
    with _lock:
        for entry in _entries.values():
            entry.queued = False
    init()


def _healthcheck_sync(url: str, timeout: Optional[float]) -> bool:
    parts = urlsplit(url)
    host = parts.hostname
    port = parts.port or (80 if parts.scheme == "http" else 443)
    try:
        conn = HTTPConnection(host, port, timeout=timeout or 1.0)
        conn.request("GET", "/healthcheck")
        resp = conn.getresponse()
        ok = 200 <= resp.status < 300
        conn.close()
        return ok
    except Exception:
        return False


def _put_sync(
    url: str, checksum: Checksum, buffer: Buffer, timeout: Optional[float]
) -> bool:
    parts = urlsplit(url)
    host = parts.hostname
    port = parts.port or (80 if parts.scheme == "http" else 443)
    path = "/" + str(checksum)
    try:
        conn = HTTPConnection(host, port, timeout=timeout or 1.0)
        conn.request("PUT", path, body=buffer.content)
        resp = conn.getresponse()
        ok = 200 <= resp.status < 300
        conn.close()
        return ok
    except Exception:
        import traceback

        traceback.print_exc()
        return False


try:
    import os

    if hasattr(os, "register_at_fork"):
        os.register_at_fork(
            before=_before_fork,
            after_in_child=_after_fork_child,
            after_in_parent=_after_fork_parent,
        )
except ImportError:  # pragma: no cover - minimal Python builds
    pass

try:
    from seamless_transformer import run as transformer_run
except ImportError:  # pragma: no cover - optional dependency
    transformer_run = None
else:
    hook_setter = getattr(transformer_run, "mark_buffer_writer_hook_installed", None)
    if hook_setter is not None:
        hook_setter()


__all__ = ["register", "await_existing_task", "init", "purge", "flush"]


init()
