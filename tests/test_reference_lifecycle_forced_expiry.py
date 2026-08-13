from __future__ import annotations

import gc

import pytest

from seamless import Buffer, Cell, CacheMissError
from seamless.caching.buffer_cache import get_buffer_cache


def _force_zero_cap(cache, checksum) -> None:
    with cache.lock:
        old_soft_cap = cache.soft_cap
        old_hard_cap = cache.hard_cap
        cache.soft_cap = 0
        cache.hard_cap = 0
        entry = cache.strong_cache.get(checksum)
        if entry is not None and entry.tempref is not None:
            entry.tempref.clear()
    try:
        cache.run_eviction_once()
    finally:
        cache.soft_cap = old_soft_cap
        cache.hard_cap = old_hard_cap


def test_cell_owned_checksum_survives_forced_expiry_then_releases():
    cache = get_buffer_cache()
    source = Buffer(b"forced-expiry-cell-unique", "bytes")
    checksum = source.get_checksum()
    cell = Cell(checksum)
    del source
    gc.collect()

    _force_zero_cap(cache, checksum)
    assert checksum in cache.strong_cache
    assert checksum.resolve().content == b"forced-expiry-cell-unique"

    cell._release_refholds()
    gc.collect()
    # Remove weak-cache survival so the negative control tests only the
    # refholder bridge, not an unrelated weak-cache hit.
    with cache.lock:
        cache.weak_cache.pop(checksum, None)
    _force_zero_cap(cache, checksum)
    with pytest.raises(CacheMissError):
        checksum.resolve()
