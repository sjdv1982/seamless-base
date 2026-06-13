"""HashType-based structural validation helpers."""

from __future__ import annotations

from typing import Any

from seamless.buffer_class import Buffer
from seamless.checksum_class import Checksum

from .conversion import (
    conversion_chain,
    conversion_equivalent,
    conversion_forbidden,
    conversion_possible,
    conversion_reformat,
    conversion_reinterpret,
    conversion_trivial,
    conversion_values,
)
from .hash_type import HashType, get_hash_type, register_hash_type_for_buffer


class HashTypeValidationError(ValueError):
    """Raised when HashType proves a checksum/celltype operation impossible."""


def ensure_hash_type(
    checksum: Checksum | str | bytes,
    *,
    buffer: Buffer | bytes | bytearray | memoryview | None = None,
) -> HashType | None:
    """Return cached HashType, computing it from a local buffer when available."""

    checksum = Checksum(checksum)
    hash_type = get_hash_type(checksum)
    if hash_type is not None:
        return hash_type
    if buffer is None:
        return None
    return register_hash_type_for_buffer(checksum, buffer)


def validate_deserializable_as(
    checksum: Checksum | str | bytes,
    celltype: str,
    *,
    buffer: Buffer | bytes | bytearray | memoryview | None = None,
) -> HashType | None:
    """Reject when HashType proves checksum cannot deserialize as celltype."""

    checksum = Checksum(checksum)
    hash_type = ensure_hash_type(checksum, buffer=buffer)
    if hash_type is None:
        return None
    if not hash_type.deserializable_as(celltype, checksum=checksum):
        raise HashTypeValidationError(
            _message(
                "Cannot deserialize checksum as requested celltype",
                checksum,
                hash_type,
                celltype=celltype,
            )
        )
    return hash_type


def validate_expression(
    checksum: Checksum | str | bytes,
    *,
    buffer: Buffer | bytes | bytearray | memoryview | None,
    source_celltype: str,
    path_steps: tuple[tuple[str, Any], ...],
    target_celltype: str,
) -> HashType | None:
    """Validate expression source, path capability, and empty-path conversion."""

    checksum = Checksum(checksum)
    hash_type = validate_deserializable_as(
        checksum, source_celltype, buffer=buffer
    )
    if hash_type is None:
        return None

    _validate_path_capability(checksum, hash_type, source_celltype, path_steps)
    if not path_steps:
        feasible = conversion_feasible(
            hash_type,
            source_celltype,
            target_celltype,
            checksum=checksum,
        )
        if feasible is False:
            raise HashTypeValidationError(
                _message(
                    "Cannot convert expression source to target celltype",
                    checksum,
                    hash_type,
                    celltype=source_celltype,
                    target_celltype=target_celltype,
                )
            )
    return hash_type


def conversion_feasible(
    hash_type: HashType | int,
    source_celltype: str,
    target_celltype: str,
    *,
    checksum: Checksum | str | bytes | None = None,
) -> bool | None:
    """Return False only when HashType proves conversion impossible."""

    hash_type = hash_type if isinstance(hash_type, HashType) else HashType.unpack(hash_type)
    if source_celltype == target_celltype:
        return True
    if not hash_type.deserializable_as(source_celltype, checksum=checksum):
        return False
    conv = (source_celltype, target_celltype)
    conv = conversion_equivalent.get(conv, conv)
    if conv in conversion_chain:
        return None
    if conv in conversion_forbidden:
        return False
    if conv in conversion_trivial or conv in conversion_reformat:
        return True
    if conv in conversion_reinterpret:
        return hash_type.deserializable_as(target_celltype)
    if conv in conversion_possible:
        return _possible_conversion_feasible(hash_type, source_celltype, target_celltype)
    if conv in conversion_values:
        return _value_conversion_feasible(hash_type, source_celltype, target_celltype)
    return None


def _validate_path_capability(
    checksum: Checksum,
    hash_type: HashType,
    source_celltype: str,
    path_steps: tuple[tuple[str, Any], ...],
) -> None:
    caps = hash_type.capabilities(source_celltype)
    for kind, payload in path_steps:
        needs = "SEQ"
        if kind == "item" and isinstance(payload, str):
            needs = "MAP"
        if needs not in caps:
            raise HashTypeValidationError(
                _message(
                    f"Expression path requires {needs} capability",
                    checksum,
                    hash_type,
                    celltype=source_celltype,
                    path_step=(kind, payload),
                )
            )


def _possible_conversion_feasible(
    hash_type: HashType,
    source_celltype: str,
    target_celltype: str,
) -> bool | None:
    if target_celltype in ("int", "float"):
        if hash_type.length.name == "LONG":
            return False
        if source_celltype == "binary":
            return hash_type.dtype.name == "NUMERIC" and hash_type.rank.name == "SCALAR"
        return True if hash_type.is_json_numeric_scalar else False
    if target_celltype == "str" and source_celltype in ("plain", "mixed"):
        if hash_type.kind.name in ("JSON_OBJECT", "JSON_ARRAY"):
            return False
        return True
    return None


def _value_conversion_feasible(
    hash_type: HashType,
    source_celltype: str,
    target_celltype: str,
) -> bool | None:
    if target_celltype == "checksum":
        return hash_type.kind.name == "RAW_TEXT" and hash_type.length.name == "EQ64"
    if source_celltype == "checksum":
        return None
    if source_celltype == "plain" and target_celltype == "binary":
        if hash_type.kind.name == "JSON_OBJECT":
            return False
    if source_celltype == "binary" and target_celltype == "plain":
        if hash_type.dtype.name in ("NUMERIC", "STRUCTURED"):
            return True
    return None


def _message(
    reason: str,
    checksum: Checksum,
    hash_type: HashType,
    **details,
) -> str:
    detail = ", ".join(f"{key}={value!r}" for key, value in details.items())
    suffix = f", {detail}" if detail else ""
    return (
        f"{reason}: checksum={checksum.hex()}, hash_type={hash_type.word} "
        f"({hash_type.kind.name}/{hash_type.length.name}/"
        f"{hash_type.dtype.name}/{hash_type.rank.name}, flags={int(hash_type.flags)})"
        f"{suffix}"
    )


__all__ = [
    "HashTypeValidationError",
    "conversion_feasible",
    "ensure_hash_type",
    "validate_deserializable_as",
    "validate_expression",
]
