import sys
import asyncio
from types import ModuleType

import pytest

from seamless import Buffer
from seamless.checksum.hash_type import (
    get_hash_type_cache,
    get_hash_type_remote,
    register_hash_type_for_buffer_async,
)


def _install_fake_database_remote(monkeypatch, rows):
    seamless_remote = ModuleType("seamless_remote")
    database_remote = ModuleType("seamless_remote.database_remote")

    async def get_hash_type(checksum):
        return rows.get(checksum.hex())

    async def set_hash_type(checksum, hash_type):
        rows[checksum.hex()] = hash_type
        return True

    database_remote.get_hash_type = get_hash_type
    database_remote.set_hash_type = set_hash_type
    seamless_remote.database_remote = database_remote
    monkeypatch.setitem(sys.modules, "seamless_remote", seamless_remote)
    monkeypatch.setitem(sys.modules, "seamless_remote.database_remote", database_remote)


def test_hash_type_async_lookup_writes_and_reads_remote(monkeypatch):
    rows = {}
    _install_fake_database_remote(monkeypatch, rows)
    buffer = Buffer(b"hello")
    checksum = buffer.get_checksum()
    get_hash_type_cache().clear()

    computed = asyncio.run(register_hash_type_for_buffer_async(checksum, buffer))
    assert rows[checksum.hex()] == computed.word

    get_hash_type_cache().clear()
    loaded = asyncio.run(get_hash_type_remote(checksum))

    assert loaded == computed
    assert get_hash_type_cache()[checksum] == computed.word


def test_hash_type_async_lookup_rejects_invalid_remote_word(monkeypatch):
    buffer = Buffer(b"hello")
    checksum = buffer.get_checksum()
    rows = {checksum.hex(): 8192}
    _install_fake_database_remote(monkeypatch, rows)
    get_hash_type_cache().clear()

    with pytest.raises(ValueError):
        asyncio.run(get_hash_type_remote(checksum))
