import gzip
import os
import subprocess
import sys
from pathlib import Path

import zstandard

from seamless.checksum.calculate_checksum import calculate_checksum
from seamless.checksum.serialize import serialize_sync as serialize


ROOT = Path(__file__).resolve().parent.parent


def _run_script(script_name: str, *args: str, cwd: Path | None = None) -> str:
    env = os.environ.copy()
    pythonpath = str(ROOT)
    if env.get("PYTHONPATH"):
        pythonpath += os.pathsep + env["PYTHONPATH"]
    env["PYTHONPATH"] = pythonpath
    result = subprocess.run(
        [sys.executable, str(ROOT / "bin" / script_name), *args],
        check=True,
        text=True,
        capture_output=True,
        cwd=cwd,
        env=env,
    )
    return result.stdout.strip()


def test_seamless_checksum_uses_canonical_zstd_checksum(tmp_path):
    payload = b"canonical payload" * 100
    compressed = tmp_path / "input.bin.zst"
    compressed.write_bytes(zstandard.ZstdCompressor().compress(payload))

    checksum = _run_script("seamless-checksum", compressed.as_posix())

    assert checksum == calculate_checksum(payload)


def test_seamless_checksum_file_strips_zstd_suffix_for_sidecar(tmp_path):
    payload = b"sidecar payload" * 100
    compressed = tmp_path / "input.npy.zst"
    compressed.write_bytes(zstandard.ZstdCompressor().compress(payload))

    _run_script("seamless-checksum-file", compressed.as_posix())

    checksum_file = tmp_path / "input.npy.CHECKSUM"
    assert checksum_file.read_text(encoding="utf-8").strip() == calculate_checksum(
        payload
    )
    assert not (tmp_path / "input.npy.zst.CHECKSUM").exists()


def test_seamless_checksum_file_supports_gzip(tmp_path):
    payload = b"gzip payload" * 100
    compressed = tmp_path / "input.bin.gz"
    compressed.write_bytes(gzip.compress(payload))

    _run_script("seamless-checksum-file", compressed.as_posix())

    assert (tmp_path / "input.bin.CHECKSUM").read_text(
        encoding="utf-8"
    ).strip() == calculate_checksum(payload)


def test_seamless_checksum_index_uses_canonical_names_and_checksums(tmp_path):
    dirname = tmp_path / "dataset"
    dirname.mkdir()
    payload = b"indexed payload" * 100
    (dirname / "array.npy.zst").write_bytes(
        zstandard.ZstdCompressor().compress(payload)
    )

    _run_script("seamless-checksum-index", dirname.as_posix())

    expected_index = {"array.npy": calculate_checksum(payload)}
    expected_index_buffer = serialize(expected_index, "plain")
    assert (tmp_path / "dataset.INDEX").read_bytes() == expected_index_buffer
    assert (tmp_path / "dataset.CHECKSUM").read_text(
        encoding="utf-8"
    ).strip() == calculate_checksum(expected_index_buffer)
