from __future__ import annotations

import gzip
import io

import zstandard

COMPRESSION_SUFFIXES = (".zst", ".gz")


def strip_compression_suffix(name: str) -> tuple[str, str | None]:
    for suffix in COMPRESSION_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)], suffix
    return name, None


def decompress_bytes(data: bytes, suffix: str) -> bytes:
    if suffix == ".zst":
        decompressor = zstandard.ZstdDecompressor()
        try:
            return decompressor.decompress(data)
        except zstandard.ZstdError:
            with decompressor.stream_reader(io.BytesIO(data)) as reader:
                return reader.read()
    if suffix == ".gz":
        return gzip.decompress(data)
    raise ValueError(suffix)
