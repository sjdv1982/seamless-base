import importlib.util
import os
import sys

import pytest

from seamless import Buffer
from seamless.checksum_class import Checksum


def load_buffer_cache_module():
    path = os.path.join(
        os.path.dirname(__file__), "..", "seamless", "caching", "buffer_cache.py"
    )
    path = os.path.abspath(path)
    name = "seamless_caching_buffer_cache_lifecycle_test"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def make_cache():
    return load_buffer_cache_module().BufferCache(soft_cap=0, hard_cap=0)


def test_many_refholders_share_one_bridge_and_interest():
    cache = make_cache()
    buf = Buffer(b"refholder-bridge")
    checksum = buf.get_checksum()
    cache.register(checksum, buf, size=len(buf.content))

    cache.incref_refholder(checksum, buffer=buf)
    one_interest = cache.strong_cache[checksum].interest()
    cache.incref_refholder(checksum)
    cache.incref_refholder(checksum)

    entry = cache.strong_cache[checksum]
    assert cache.refholder_counts[checksum] == 3
    assert entry.has_refholder_bridge is True
    assert entry.interest() == one_interest

    assert cache.decref_refholder(checksum) is True
    assert cache.decref_refholder(checksum) is True
    assert cache.refholder_counts[checksum] == 1
    assert cache.strong_cache[checksum].has_refholder_bridge is True
    assert cache.decref_refholder(checksum) is True
    assert checksum not in cache.refholder_counts
    assert checksum not in cache.strong_cache


def test_manual_refs_and_refholder_bridge_are_independent():
    cache = make_cache()
    buf = Buffer(b"manual-and-refholder")
    checksum = buf.get_checksum()
    cache.register(checksum, buf, size=len(buf.content))

    cache.incref(checksum)
    cache.incref_refholder(checksum)
    assert cache.reference_snapshot()[checksum] == (1, 1, True)

    assert cache.decref_refholder(checksum) is True
    assert cache.reference_snapshot()[checksum] == (0, 1, False)
    assert checksum in cache.strong_cache
    assert cache.decref(checksum) is True
    assert checksum not in cache.strong_cache


def test_protected_entries_survive_zero_cap_and_unprotected_entries_demote():
    cache = make_cache()
    buf = Buffer(b"protected-by-bridge")
    checksum = buf.get_checksum()
    cache.register(checksum, buf, size=len(buf.content))
    cache.incref_refholder(checksum)

    before, after = cache.run_eviction_once()
    assert before == after == len(buf.content)
    assert checksum in cache.strong_cache

    cache.decref_refholder(checksum)
    assert checksum not in cache.strong_cache
    assert checksum in cache.weak_cache


def test_equal_checksum_acquire_before_release_keeps_bridge():
    cache = make_cache()
    buf = Buffer(b"same-checksum-replacement")
    checksum = buf.get_checksum()
    cache.register(checksum, buf, size=len(buf.content))
    cache.incref_refholder(checksum)

    # This is the observable state during an acquire-before-release replace.
    cache.incref_refholder(checksum)
    assert cache.reference_snapshot()[checksum] == (2, 0, True)
    cache.decref_refholder(checksum)
    assert cache.reference_snapshot()[checksum] == (1, 0, True)
    cache.decref_refholder(checksum)


def test_absent_buffer_refholder_balances():
    cache = make_cache()
    checksum = Checksum("a" * 64)

    cache.incref_refholder(checksum)
    assert cache.reference_snapshot()[checksum] == (1, 0, True)
    assert cache.decref_refholder(checksum) is True
    assert checksum not in cache.reference_snapshot()


def test_both_decrement_underflows_warn(caplog):
    cache = make_cache()
    checksum = Checksum("b" * 64)
    with caplog.at_level("WARNING", logger="seamless.references"):
        assert cache.decref(checksum) is False
        assert cache.decref_refholder(checksum) is False
    messages = [record.message for record in caplog.records]
    assert any("Manual decref ignored" in message for message in messages)
    assert any("Refholder decref ignored" in message for message in messages)


def test_forced_cleanup_clears_manual_refs_and_refholder_bridges():
    cache = make_cache()
    buffer = Buffer(b"forced-accounting-cleanup")
    checksum = buffer.get_checksum()
    cache.register(checksum, buffer, size=len(buffer.content))
    cache.incref(checksum)
    cache.incref(checksum)
    cache.incref_refholder(checksum)
    cache.force_clear_reference_accounting()
    assert cache.reference_snapshot() == {}
