import asyncio
import sys
from types import ModuleType

from seamless import Buffer, Checksum
from seamless.checksum.expression import (
    evaluate_expression_remote,
    get_expression_cache,
)


def _install_fake_remotes(monkeypatch, expression_rows, jobserver_results, calls):
    seamless_remote = ModuleType("seamless_remote")
    database_remote = ModuleType("seamless_remote.database_remote")
    jobserver_remote = ModuleType("seamless_remote.jobserver_remote")

    def key(input_checksum, path, celltype, target_celltype):
        return (
            Checksum(input_checksum).hex(),
            path,
            celltype,
            target_celltype,
        )

    async def get_expression_result(input_checksum, path, celltype, target_celltype):
        calls.append("database:get")
        return expression_rows.get(key(input_checksum, path, celltype, target_celltype))

    async def set_expression_result(
        input_checksum, path, celltype, target_celltype, result_checksum
    ):
        calls.append("database:set")
        expression_rows[key(input_checksum, path, celltype, target_celltype)] = Checksum(
            result_checksum
        )
        return True

    async def run_expression(input_checksum, path, celltype, target_celltype):
        calls.append("jobserver:run")
        return Checksum(jobserver_results[key(input_checksum, path, celltype, target_celltype)])

    database_remote.get_expression_result = get_expression_result
    database_remote.set_expression_result = set_expression_result
    jobserver_remote.run_expression = run_expression
    seamless_remote.database_remote = database_remote
    seamless_remote.jobserver_remote = jobserver_remote
    monkeypatch.setitem(sys.modules, "seamless_remote", seamless_remote)
    monkeypatch.setitem(sys.modules, "seamless_remote.database_remote", database_remote)
    monkeypatch.setitem(sys.modules, "seamless_remote.jobserver_remote", jobserver_remote)


def test_remote_expression_dispatch_writes_expression_cache(monkeypatch):
    get_expression_cache().clear()
    source_checksum = Checksum("1" * 64)
    result_checksum = Buffer("remote", "str").get_checksum()
    expression_rows = {}
    calls = []
    key = (source_checksum.hex(), "a", "plain", "str")
    _install_fake_remotes(
        monkeypatch,
        expression_rows,
        {key: result_checksum.hex()},
        calls,
    )

    result = asyncio.run(
        evaluate_expression_remote(
            source_checksum,
            "a",
            "plain",
            "str",
            execution="remote",
        )
    )

    assert result == result_checksum
    assert expression_rows[key] == result_checksum
    assert calls == ["database:get", "jobserver:run", "database:set"]


def test_remote_expression_cache_hit_skips_dispatch(monkeypatch):
    get_expression_cache().clear()
    source_checksum = Checksum("2" * 64)
    result_checksum = Buffer("cached", "str").get_checksum()
    key = (source_checksum.hex(), "a", "plain", "str")
    expression_rows = {key: result_checksum}
    calls = []
    _install_fake_remotes(monkeypatch, expression_rows, {}, calls)

    result = asyncio.run(
        evaluate_expression_remote(
            source_checksum,
            "a",
            "plain",
            "str",
            execution="remote",
        )
    )

    assert result == result_checksum
    assert calls == ["database:get"]


def test_auto_expression_uses_local_buffer_before_remote_dispatch(monkeypatch):
    get_expression_cache().clear()
    source_checksum = Buffer({"a": "local"}, "plain").get_checksum()
    result_checksum = Buffer("local", "str").get_checksum()
    key = (source_checksum.hex(), "a", "plain", "str")
    expression_rows = {}
    calls = []
    _install_fake_remotes(monkeypatch, expression_rows, {key: result_checksum.hex()}, calls)

    result = asyncio.run(
        evaluate_expression_remote(
            source_checksum,
            "a",
            "plain",
            "str",
            execution="auto",
        )
    )

    assert result == result_checksum
    assert calls == ["database:get", "database:set"]
