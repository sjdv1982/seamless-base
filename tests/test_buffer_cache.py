import os
import sys
import importlib.util

# Import seamless module
from seamless import Buffer
from seamless.checksum_class import Checksum
from seamless.caching import eviction_cost


def load_buffer_cache_module():
    # load the file directly to avoid importing the top-level seamless package
    p = os.path.join(
        os.path.dirname(__file__), "..", "seamless", "caching", "buffer_cache.py"
    )
    p = os.path.abspath(p)
    name = "seamless_caching_buffer_cache_test"
    spec = importlib.util.spec_from_file_location(name, p)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_register_refs_and_eviction():
    mod = load_buffer_cache_module()
    BufferCache = mod.BufferCache

    soft_cap = 5 * 1024 * 1024
    hard_cap = 100 * 1024 * 1024
    cache = BufferCache(soft_cap=soft_cap, hard_cap=hard_cap)

    # Create real Buffer and Checksum instances from seamless
    buf1 = Buffer(b"a" * 1024)
    buf2 = Buffer(b"b" * (10 * 1024 * 1024))
    size_small = 1024  # 1 KB
    size_big = 10 * 1024 * 1024  # 10 MB

    # c1 is small but expensive; c2 is big and cheap -> c2 should be evicted first
    c1 = Checksum(bytes.fromhex("01" * 32))
    c2 = Checksum(bytes.fromhex("02" * 32))
    eviction_cost.set_download_profile(c1, "remote_hashserver")
    eviction_cost.set_download_profile(c2, "read_buffer")
    cache.register(c1, buf1, size=size_small)
    cache.register(c2, buf2, size=size_big)
    cache.incref(c1)
    cache.incref(c2)

    stats_before = cache.stats()
    assert stats_before["strong_count"] == 2
    assert stats_before["strong_bytes"] >= size_big

    before, after = cache.run_eviction_once()
    stats_after = cache.stats()
    # should be at or below soft cap
    assert stats_after["strong_bytes"] <= soft_cap

    # c2 (big & cheap) should have been demoted to weak cache
    assert c2 not in cache.strong_cache
    assert c2 in cache.weak_cache
    # c1 should still be present in strong cache
    assert c1 in cache.strong_cache

    cache.decref(c1)


def test_eviction_loop_start_stop():
    mod = load_buffer_cache_module()
    BufferCache = mod.BufferCache
    cache = BufferCache(soft_cap=1, hard_cap=10)
    # start a quick background eviction loop (async)
    import asyncio

    async def run_loop():
        await cache.start_eviction_loop(interval=0.05)
        try:
            assert cache._eviction_task is not None and not cache._eviction_task.done()
        finally:
            await cache.stop_eviction_loop()
        assert cache._eviction_task is None

    asyncio.run(run_loop())
