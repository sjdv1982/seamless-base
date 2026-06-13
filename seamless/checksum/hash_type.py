"""Packed checksum HashType words.

This module intentionally starts with the structural representation only. Later
phases add producers, caches, and query methods on top of the same packed word.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum, IntFlag


class Kind(IntEnum):
    RAW_BYTES = 0
    NUMPY = 1
    MIXED_OBJECT = 2
    MIXED_ARRAY = 3
    RAW_TEXT = 4
    JSON_OBJECT = 5
    JSON_ARRAY = 6
    JSON_STRING = 7
    JSON_NUMBER = 8


class Length(IntEnum):
    SHORT = 0
    EQ64 = 1
    MEDIUM = 2
    LONG = 3


class DType(IntEnum):
    NA = 0
    NUMERIC = 1
    NONNUMERIC = 2
    STRUCTURED = 3


class Rank(IntEnum):
    SCALAR = 0
    D1 = 1
    D2 = 2
    D3PLUS = 3


class Flag(IntFlag):
    NUMERIC_SCALAR = 1 << 0
    NUMPY_BYTES = 1 << 1
    SEMANTIC = 1 << 2


_KIND_SHIFT = 0
_LENGTH_SHIFT = 4
_DTYPE_SHIFT = 6
_RANK_SHIFT = 8
_FLAG_SHIFT = 10
_MAX_WORD = 1 << 13


@dataclass(frozen=True, slots=True)
class HashType:
    """Decoded representation of a packed HashType word."""

    kind: Kind
    length: Length
    dtype: DType = DType.NA
    rank: Rank = Rank.SCALAR
    flags: Flag = Flag(0)

    @property
    def word(self) -> int:
        return pack(self.kind, self.length, self.dtype, self.rank, self.flags)

    @property
    def is_utf8(self) -> bool:
        return self.kind >= Kind.RAW_TEXT

    @property
    def is_json(self) -> bool:
        return self.kind >= Kind.JSON_OBJECT

    @property
    def mic(self) -> str:
        return MIC_BY_KIND[self.kind]

    @classmethod
    def unpack(cls, word: int) -> "HashType":
        return unpack(word)

    @staticmethod
    def is_valid_word(word: int) -> bool:
        return is_valid_word(word)


MIC_BY_KIND = {
    Kind.RAW_BYTES: "bytes",
    Kind.RAW_TEXT: "text",
    Kind.NUMPY: "binary",
    Kind.MIXED_OBJECT: "mixed",
    Kind.MIXED_ARRAY: "mixed",
    Kind.JSON_OBJECT: "plain",
    Kind.JSON_ARRAY: "plain",
    Kind.JSON_STRING: "str",
    Kind.JSON_NUMBER: "float",
}


def pack(
    kind: Kind,
    length: Length,
    dtype: DType = DType.NA,
    rank: Rank = Rank.SCALAR,
    flags: Flag = Flag(0),
) -> int:
    """Pack HashType fields into the 13-bit integer representation."""

    kind = Kind(kind)
    length = Length(length)
    dtype = DType(dtype)
    rank = Rank(rank)
    flags = Flag(flags)
    return (
        (kind << _KIND_SHIFT)
        | (length << _LENGTH_SHIFT)
        | (dtype << _DTYPE_SHIFT)
        | (rank << _RANK_SHIFT)
        | (flags << _FLAG_SHIFT)
    )


def unpack(word: int) -> HashType:
    """Decode a packed HashType word."""

    if not isinstance(word, int):
        raise TypeError(type(word))
    if word < 0 or word >= _MAX_WORD:
        raise ValueError(word)
    kind = Kind((word >> _KIND_SHIFT) & 0xF)
    length = Length((word >> _LENGTH_SHIFT) & 0x3)
    dtype = DType((word >> _DTYPE_SHIFT) & 0x3)
    rank = Rank((word >> _RANK_SHIFT) & 0x3)
    flags = Flag((word >> _FLAG_SHIFT) & 0x7)
    return HashType(kind, length, dtype, rank, flags)


def is_valid_word(word: int) -> bool:
    """Return whether a word satisfies the HashType well-formedness rules."""

    try:
        decoded = unpack(word)
    except (TypeError, ValueError):
        return False

    kind = decoded.kind
    dtype = decoded.dtype
    rank = decoded.rank
    flags = decoded.flags

    if (dtype != DType.NA) != (kind == Kind.NUMPY):
        return False
    if rank != Rank.SCALAR and kind != Kind.NUMPY:
        return False
    if flags & Flag.NUMPY_BYTES:
        if not (
            kind == Kind.NUMPY
            and dtype == DType.NONNUMERIC
            and rank == Rank.SCALAR
        ):
            return False
    if kind == Kind.JSON_NUMBER and not (flags & Flag.NUMERIC_SCALAR):
        return False
    if flags & Flag.NUMERIC_SCALAR:
        if kind not in (Kind.JSON_NUMBER, Kind.JSON_STRING):
            return False
    if flags & Flag.SEMANTIC:
        if kind != Kind.RAW_TEXT:
            return False
    return True


__all__ = [
    "DType",
    "Flag",
    "HashType",
    "Kind",
    "Length",
    "MIC_BY_KIND",
    "Rank",
    "is_valid_word",
    "pack",
    "unpack",
]
