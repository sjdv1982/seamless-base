"""Container class for Seamless structural expressions."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from .checksum_class import Checksum


def normalize_path(path: str | None) -> str:
    """Normalize the expression path container field.

    Path parsing, validation, and application are intentionally deferred.
    """

    if path is None:
        return ""
    if not isinstance(path, str):
        raise TypeError("Expression path must be a string")
    return path


def append_item_path(path: str, key: Any) -> str:
    if isinstance(key, str) and key.isidentifier():
        prefix = "." if path else ""
        return f"{path}{prefix}{key}"
    return f"{path}[{key!r}]"


def append_slice_path(
    path: str,
    start: Any = None,
    stop: Any = None,
    step: Any = None,
) -> str:
    parts = [
        "" if start is None else repr(start),
        "" if stop is None else repr(stop),
    ]
    if step is not None:
        parts.append(repr(step))
    return path + "[" + ":".join(parts) + "]"


def _input_ref_key(input_ref: Any) -> tuple[str, Any]:
    if isinstance(input_ref, Checksum):
        return ("checksum", input_ref.hex())
    if isinstance(input_ref, Expression):
        return ("expression", input_ref.identity_key)
    try:
        checksum = Checksum(input_ref)
    except (TypeError, ValueError):
        return ("object", id(input_ref))
    else:
        return ("checksum", checksum.hex())


@dataclass(frozen=True, slots=True, eq=False)
class Expression:
    """Immutable structural expression definition.

    This class intentionally carries only the definition shape. Resolution,
    caching, reverse lookup, cancellation, path validation, and value
    materialization are later mechanics.
    """

    input_ref: Any
    path: str | None = ""
    celltype: str = "mixed"
    target_celltype: str | None = None
    validator: Checksum | str | bytes | None = None
    validator_language: str | None = None
    result: Checksum | str | bytes | None = None

    def __post_init__(self) -> None:
        path = normalize_path(self.path)
        target_celltype = (
            self.celltype if self.target_celltype is None else self.target_celltype
        )
        validator = None if self.validator is None else Checksum(self.validator)
        result = None if self.result is None else Checksum(self.result)
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "target_celltype", target_celltype)
        object.__setattr__(self, "validator", validator)
        object.__setattr__(self, "result", result)

    @property
    def input_checksum(self) -> Checksum | None:
        try:
            return Checksum(self.input_ref)
        except (TypeError, ValueError):
            return self.input_ref if isinstance(self.input_ref, Checksum) else None

    @property
    def identity_key(self) -> tuple[Any, str, str, str]:
        return (
            _input_ref_key(self.input_ref),
            self.path,
            self.celltype,
            self.target_celltype,
        )

    @property
    def path_python(self) -> str:
        return self.path

    @property
    def database_key(self) -> tuple[str, str, str, str]:
        input_checksum = self.input_checksum
        if input_checksum is None:
            raise ValueError("Expression input is not a concrete checksum yet")
        return (
            input_checksum.hex(),
            self.path,
            self.celltype,
            self.target_celltype,
        )

    def with_result(self, result: Checksum | str | bytes | None) -> "Expression":
        return replace(self, result=result)

    def item(self, key: Any) -> "Expression":
        return replace(self, path=append_item_path(self.path, key))

    def slice(
        self,
        start: Any = None,
        stop: Any = None,
        step: Any = None,
    ) -> "Expression":
        return replace(self, path=append_slice_path(self.path, start, stop, step))

    def as_celltype(self, target_celltype: str) -> "Expression":
        return replace(self, target_celltype=target_celltype)

    def __getitem__(self, item: Any) -> "Expression":
        if isinstance(item, slice):
            return self.slice(item.start, item.stop, item.step)
        return self.item(item)

    def __getattr__(self, name: str) -> "Expression":
        if name.startswith("_"):
            raise AttributeError(name)
        return self.item(name)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Expression):
            return False
        return self.identity_key == other.identity_key

    def __hash__(self) -> int:
        return hash(self.identity_key)

    def __repr__(self) -> str:
        cls = type(self).__name__
        return (
            f"{cls}(input_ref={self.input_ref!r}, path={self.path!r}, "
            f"celltype={self.celltype!r}, target_celltype={self.target_celltype!r})"
        )

    async def compute_async(self, *, execution: str = "local") -> Checksum | None:
        from .checksum.expression import evaluate_expression_async, evaluate_expression_remote

        input_checksum = self.input_checksum
        if input_checksum is None:
            raise ValueError("Expression input is not a concrete checksum yet")
        if execution == "local":
            return await evaluate_expression_async(
                input_checksum,
                self.path,
                self.celltype,
                self.target_celltype,
                validator=self.validator,
                validator_language=self.validator_language,
            )
        return await evaluate_expression_remote(
            input_checksum,
            self.path,
            self.celltype,
            self.target_celltype,
            validator=self.validator,
            validator_language=self.validator_language,
            execution=execution,
        )

    def compute(self, *, execution: str = "local") -> Checksum | None:
        from .checksum.expression import evaluate_expression

        input_checksum = self.input_checksum
        if input_checksum is None:
            raise ValueError("Expression input is not a concrete checksum yet")
        if execution != "local":
            import asyncio

            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return asyncio.run(self.compute_async(execution=execution))
            raise RuntimeError(
                "Cannot block on remote expression evaluation in a running loop"
            )
        return evaluate_expression(
            input_checksum,
            self.path,
            self.celltype,
            self.target_celltype,
            validator=self.validator,
            validator_language=self.validator_language,
        )

    def run(self) -> Any:
        from .checksum.expression import resolve_expression_value

        result = self.compute()
        if result is None:
            return None
        return resolve_expression_value(result, self.target_celltype)

    __call__ = run

    def cancel(self, *, recursive: bool = True) -> bool:
        raise NotImplementedError("Expression cancellation is not implemented yet")


__all__ = [
    "Expression",
    "append_item_path",
    "append_slice_path",
    "normalize_path",
]
