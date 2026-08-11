"""Mutable structural Cell builder."""

from __future__ import annotations

import copy
from typing import Any

from .expression_class import (
    Expression,
    append_item_path,
    append_slice_path,
    normalize_path,
)

_UNSET = object()


class Cell:
    """Mutable structural expression builder.

    Navigation creates derived Cell builders. ``build()`` snapshots the current
    builder state into an immutable ``Expression`` container.
    """

    __slots__ = (
        "_workflow_backend",
        "_input_ref",
        "_path",
        "_celltype",
        "_target_celltype",
        "_validator",
        "_validator_language",
    )

    def __init__(
        self,
        input_ref: Any = None,
        *,
        path: str | None = None,
        celltype: str = "mixed",
        target_celltype: str | None = None,
        validator: Any = None,
        validator_language: str | None = None,
    ) -> None:
        self._workflow_backend = None
        self._input_ref = input_ref
        self._path = normalize_path(path)
        self._celltype = celltype
        self._target_celltype = celltype if target_celltype is None else target_celltype
        self._validator = validator
        self._validator_language = validator_language

    @classmethod
    def _from_backend(cls, backend) -> "Cell":
        """Create a canonical Cell whose state is entirely backend-owned."""

        self = cls.__new__(cls)
        object.__setattr__(self, "_workflow_backend", backend)
        object.__setattr__(self, "_input_ref", None)
        object.__setattr__(self, "_path", "")
        object.__setattr__(self, "_celltype", "mixed")
        object.__setattr__(self, "_target_celltype", "mixed")
        object.__setattr__(self, "_validator", None)
        object.__setattr__(self, "_validator_language", None)
        return self

    @property
    def input_ref(self) -> Any:
        if self._workflow_backend is not None:
            return self._workflow_backend.input_ref
        return self._input_ref

    @input_ref.setter
    def input_ref(self, input_ref: Any) -> None:
        if self._workflow_backend is not None:
            raise _bound_state_error("input_ref")
        self._input_ref = input_ref

    @property
    def path(self) -> str:
        if self._workflow_backend is not None:
            return self._workflow_backend.path
        return self._path

    @path.setter
    def path(self, path: str | None) -> None:
        if self._workflow_backend is not None:
            raise _bound_state_error("path")
        self._path = normalize_path(path)

    @property
    def path_python(self) -> str:
        if self._workflow_backend is not None:
            return self._workflow_backend.path_python
        return self._path

    @property
    def celltype(self) -> str:
        if self._workflow_backend is not None:
            return self._workflow_backend.celltype
        return self._celltype

    @celltype.setter
    def celltype(self, celltype: str) -> None:
        if self._workflow_backend is not None:
            self._workflow_backend.celltype = celltype
            return
        self._celltype = celltype

    @property
    def target_celltype(self) -> str:
        if self._workflow_backend is not None:
            return self._workflow_backend.target_celltype
        return self._target_celltype

    @target_celltype.setter
    def target_celltype(self, target_celltype: str | None) -> None:
        if self._workflow_backend is not None:
            self._workflow_backend.target_celltype = target_celltype
            return
        self._target_celltype = (
            self._celltype if target_celltype is None else target_celltype
        )

    @property
    def validator(self) -> Any:
        if self._workflow_backend is not None:
            return self._workflow_backend.validator
        return self._validator

    @validator.setter
    def validator(self, validator: Any) -> None:
        if self._workflow_backend is not None:
            self._workflow_backend.validator = validator
            return
        self._validator = validator

    @property
    def validator_language(self) -> str | None:
        if self._workflow_backend is not None:
            return self._workflow_backend.validator_language
        return self._validator_language

    @validator_language.setter
    def validator_language(self, validator_language: str | None) -> None:
        if self._workflow_backend is not None:
            self._workflow_backend.validator_language = validator_language
            return
        self._validator_language = validator_language

    @property
    def checksum(self):
        if self._workflow_backend is None:
            raise AttributeError("checksum is only available for bound workflow cells")
        return self._workflow_backend.checksum

    @property
    def buffer(self):
        if self._workflow_backend is None:
            raise AttributeError("buffer is only available for bound workflow cells")
        return self._workflow_backend.buffer

    @property
    def value(self):
        if self._workflow_backend is None:
            raise AttributeError("value is only available for bound workflow cells")
        return self._workflow_backend.value

    def set(self, value: Any) -> None:
        if self._workflow_backend is not None:
            self._workflow_backend.set(value)
            return None
        value = _capture_workflow_source(value)
        self.input_ref = value
        return None

    def set_checksum(self, checksum) -> None:
        if self._workflow_backend is not None:
            self._workflow_backend.set_checksum(checksum)
            return None
        from .checksum_class import Checksum

        self.input_ref = Checksum(checksum)
        return None

    def _derive(self, **updates: Any) -> "Cell":
        if self._workflow_backend is not None:
            return type(self)._from_backend(self._workflow_backend.derive(**updates))
        clone = type(self)(
            self._input_ref,
            path=self._path,
            celltype=self._celltype,
            target_celltype=self._target_celltype,
            validator=self._validator,
            validator_language=self._validator_language,
        )
        for name, value in updates.items():
            setattr(clone, name, value)
        return clone

    def item(self, key: Any) -> "Cell":
        if self._workflow_backend is not None:
            return type(self)._from_backend(self._workflow_backend.derive_item(key))
        return self._derive(path=append_item_path(self.path_python, key))

    def slice(self, start: Any = None, stop: Any = None, step: Any = None) -> "Cell":
        if self._workflow_backend is not None:
            return type(self)._from_backend(
                self._workflow_backend.derive_slice(start, stop, step)
            )
        return self._derive(path=append_slice_path(self.path_python, start, stop, step))

    def as_celltype(self, target_celltype: str) -> "Cell":
        return self._derive(target_celltype=target_celltype)

    def with_input(self, input_ref: Any) -> "Cell":
        return self._derive(input_ref=input_ref)

    def with_validator(
        self, validator: Any, *, language: str | None = None
    ) -> "Cell":
        return self._derive(validator=validator, validator_language=language)

    def build(self, input_ref: Any = _UNSET) -> Expression:
        if self._workflow_backend is not None:
            return self._workflow_backend.build(input_ref)
        if input_ref is _UNSET:
            input_ref = self._input_ref
        input_ref = _capture_workflow_source(input_ref)
        return Expression(
            _snapshot_input_ref(input_ref),
            path=self._path,
            celltype=self._celltype,
            target_celltype=self._target_celltype,
            validator=self._validator,
            validator_language=self._validator_language,
    )

    expression = build

    def __call__(self, input_ref: Any = _UNSET) -> Expression:
        return self.build(input_ref)

    def compute(self, input_ref: Any = _UNSET):
        if self._workflow_backend is not None:
            return self._workflow_backend.compute(input_ref)
        return self.build(input_ref).compute()

    def run(self, input_ref: Any = _UNSET):
        if self._workflow_backend is not None:
            return self._workflow_backend.run(input_ref)
        return self.build(input_ref).run()

    async def compute_async(self, input_ref: Any = _UNSET):
        if self._workflow_backend is not None:
            return await self._workflow_backend.compute_async(input_ref)
        return await self.build(input_ref).compute_async()

    def prune(self):
        if self._workflow_backend is None:
            raise AttributeError("prune is only available for bound workflow cells")
        return self._workflow_backend.prune()

    def clear_exception(self):
        if self._workflow_backend is None:
            raise AttributeError(
                "clear_exception is only available for bound workflow cells"
            )
        return self._workflow_backend.clear_exception()

    def _workflow_endpoint(self):
        backend = self._workflow_backend
        return backend._workflow_endpoint() if backend is not None else None

    def _workflow_capture_source(self):
        backend = self._workflow_backend
        if backend is None:
            return self
        return backend.capture_source()

    def __getitem__(self, item: Any) -> "Cell":
        if isinstance(item, slice):
            return self.slice(item.start, item.stop, item.step)
        return self.item(item)

    def __getattr__(self, name: str) -> "Cell":
        if name.startswith("_"):
            raise AttributeError(name)
        # A class-defined API member is authoritative even when its getter raises
        # a deliberate bound-only AttributeError.  Only genuinely unknown names
        # participate in structural projection.
        if _class_attribute(type(self), name) is not None:
            raise AttributeError(name)
        return self.item(name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_") or _class_attribute(type(self), name) is not None:
            object.__setattr__(self, name, value)
            return
        if self._workflow_backend is None:
            raise AttributeError(name)
        self._workflow_backend.assign(self.path_python, name, value)

    def __delattr__(self, name: str) -> None:
        if name.startswith("_") or _class_attribute(type(self), name) is not None:
            object.__delattr__(self, name)
            return
        if self._workflow_backend is None:
            raise AttributeError(name)
        self._workflow_backend.delete(self.path_python, name)

    def __setitem__(self, key: Any, value: Any) -> None:
        if self._workflow_backend is None:
            raise TypeError("Standalone Cell item assignment is not supported")
        self._workflow_backend.assign_item(self.path_python, key, value)

    def __delitem__(self, key: Any) -> None:
        if self._workflow_backend is None:
            raise TypeError("Standalone Cell item deletion is not supported")
        self._workflow_backend.delete_item(self.path_python, key)

    def __iadd__(self, value: Any) -> "Cell":
        return self._augmented(value, "add")

    def __isub__(self, value: Any) -> "Cell":
        return self._augmented(value, "sub")

    def __imul__(self, value: Any) -> "Cell":
        return self._augmented(value, "mul")

    def __itruediv__(self, value: Any) -> "Cell":
        return self._augmented(value, "truediv")

    def _augmented(self, value: Any, operation: str) -> "Cell":
        if self._workflow_backend is None:
            raise TypeError("Augmented Cell updates require a bound Cell")
        self._workflow_backend.augmented(self.path_python, operation, value)
        return self

    def __repr__(self) -> str:
        cls = type(self).__name__
        return (
            f"{cls}(input_ref={self.input_ref!r}, path={self.path_python!r}, "
            f"celltype={self.celltype!r}, target_celltype={self.target_celltype!r})"
        )


def _class_attribute(cls, name: str):
    """Return a statically defined member without invoking descriptors."""

    for parent in cls.__mro__:
        if name in parent.__dict__:
            return parent.__dict__[name]
    return None


def _bound_state_error(name: str):
    from .cell_errors import BoundStateError

    return BoundStateError(f"{name} is standalone-only for bound Cells")


def _snapshot_input_ref(input_ref: Any) -> Any:
    if isinstance(input_ref, (dict, list, set, bytearray)):
        return copy.deepcopy(input_ref)
    return input_ref


def _capture_workflow_source(value: Any) -> Any:
    # This is intentionally a duck-typed protocol.  Core must remain importable
    # without the workflow package and must not know workflow view classes.
    capture = getattr(value, "_workflow_capture_source", None)
    if callable(capture):
        return capture()
    return value


__all__ = ["Cell"]
