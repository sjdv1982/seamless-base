"""JSON helpers for mixed-form serialization."""

from __future__ import annotations

import json
from typing import Any, Callable

import numpy as np

from . import _float_types, _integer_types


def _default_encoder(obj: Any) -> Any:
    """Fallback encoder that understands numpy scalars and wrappers with ``value``."""
    if hasattr(obj, "value"):
        value = obj.value  # Seamless MixedScalar-style wrapper
        if value is obj:
            raise TypeError(f"{type(obj).__name__} is not JSON serializable")
        return value
    if isinstance(obj, np.generic):
        if isinstance(obj, _integer_types):  # type: ignore
            return int(obj)
        if isinstance(obj, _float_types):  # type: ignore
            return float(obj)
        if isinstance(obj, (np.bytes_, np.str_)):
            return str(obj)
    raise TypeError(f"{type(obj).__name__} is not JSON serializable")


def json_encode(
    obj: Any,
    *,
    skipkeys: bool = False,
    ensure_ascii: bool = True,
    check_circular: bool = True,
    allow_nan: bool = True,
    cls: type | None = None,
    indent: int | None = None,
    separators: tuple[str, str] | None = None,
    default: Callable[[Any], Any] | None = None,
    sort_keys: bool = False,
    **kw: Any,
) -> str:
    """Serialize ``obj`` to a JSON formatted ``str`` with Seamless defaults."""
    if default is None:
        default = _default_encoder
    return json.dumps(
        obj,
        skipkeys=skipkeys,
        ensure_ascii=ensure_ascii,
        check_circular=check_circular,
        allow_nan=allow_nan,
        cls=cls,
        indent=indent,
        separators=separators,
        default=default,
        sort_keys=sort_keys,
        **kw,
    )


__all__ = ["json_encode"]
