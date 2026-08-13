"""Test helpers for exercising buffer-cache lifetime boundaries.

The helpers in this module deliberately operate on cache state only.  Callers
must keep the checksum unique and remove any Python-owned Buffer references
they created before asking the cache to evict it.
"""

from __future__ import annotations

import gc
import importlib
from typing import Any


def cache_entry_state(cache: Any, checksum: Any) -> dict[str, Any]:
    """Return the observable state used by lifecycle tests.

    This is intentionally a small test-only snapshot of the current cache
    implementation.  The public lifecycle snapshot uses ``manual_refs``.
    internal lifecycle snapshot API.
    """

    with cache.lock:
        entry = cache.strong_cache.get(checksum)
        if entry is None:
            return {
                "strong": False,
                "manual_refs": 0,
                "tempref": None,
                "buffer": None,
            }
        return {
            "strong": True,
            "manual_refs": entry.manual_refs,
            "tempref": entry.tempref,
            "buffer": entry.buffer,
        }


def force_expiry(checksum: Any, *, cache: Any = None) -> tuple[int, int]:
    """Clear cache interest and run one low-cap eviction pass.

    A lifecycle test should pass a unique checksum and remove its own strong
    Buffer references first.  The helper clears the known process-global
    Expression result map when present, clears the cache tempref, and uses a
    zero-byte soft cap to make an unprotected entry eligible for demotion.
    Remote resolution, recomputation, and caller-owned Buffer references are
    intentionally outside this helper so tests can disable and count them
    explicitly as negative controls.
    """

    if cache is None:
        from seamless.caching.buffer_cache import get_buffer_cache

        cache = get_buffer_cache()

    expression_module = importlib.import_module("seamless.checksum.expression")
    result_buffers = getattr(expression_module, "_expression_result_buffers", None)
    if result_buffers is not None:
        result_buffers.pop(checksum, None)

    with cache.lock:
        entry = cache.strong_cache.get(checksum)
        if entry is not None and entry.tempref is not None:
            entry.tempref.clear()
        old_soft_cap = cache.soft_cap
        old_hard_cap = cache.hard_cap
        cache.soft_cap = 0
        cache.hard_cap = 0

    try:
        result = cache.run_eviction_once()
        gc.collect()
        return result
    finally:
        cache.soft_cap = old_soft_cap
        cache.hard_cap = old_hard_cap
