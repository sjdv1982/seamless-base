from __future__ import annotations

import gzip

import zstandard

COMPRESSION_SUFFIXES = (".zst", ".gz")


def strip_compression_suffix(name: str) -> tuple[str, str | None]:
    for suffix in COMPRESSION_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)], suffix
    return name, None


def decompress_bytes(data: bytes, suffix: str) -> bytes:
    if suffix == ".zst":
        return zstandard.ZstdDecompressor().decompress(data)
    if suffix == ".gz":
        return gzip.decompress(data)
    raise ValueError(suffix)
