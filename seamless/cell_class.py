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
        self._input_ref = input_ref
        self._path = normalize_path(path)
        self._celltype = celltype
        self._target_celltype = celltype if target_celltype is None else target_celltype
        self._validator = validator
        self._validator_language = validator_language

    @property
    def input_ref(self) -> Any:
        return self._input_ref

    @input_ref.setter
    def input_ref(self, input_ref: Any) -> None:
        self._input_ref = input_ref

    @property
    def path(self) -> str:
        return self._path

    @path.setter
    def path(self, path: str | None) -> None:
        self._path = normalize_path(path)

    @property
    def path_python(self) -> str:
        return self._path

    @property
    def celltype(self) -> str:
        return self._celltype

    @celltype.setter
    def celltype(self, celltype: str) -> None:
        self._celltype = celltype

    @property
    def target_celltype(self) -> str:
        return self._target_celltype

    @target_celltype.setter
    def target_celltype(self, target_celltype: str | None) -> None:
        self._target_celltype = (
            self._celltype if target_celltype is None else target_celltype
        )

    @property
    def validator(self) -> Any:
        return self._validator

    @validator.setter
    def validator(self, validator: Any) -> None:
        self._validator = validator

    @property
    def validator_language(self) -> str | None:
        return self._validator_language

    @validator_language.setter
    def validator_language(self, validator_language: str | None) -> None:
        self._validator_language = validator_language

    def _derive(self, **updates: Any) -> "Cell":
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
        return self._derive(path=append_item_path(self._path, key))

    def slice(self, start: Any = None, stop: Any = None, step: Any = None) -> "Cell":
        return self._derive(path=append_slice_path(self._path, start, stop, step))

    def as_celltype(self, target_celltype: str) -> "Cell":
        return self._derive(target_celltype=target_celltype)

    def with_input(self, input_ref: Any) -> "Cell":
        return self._derive(input_ref=input_ref)

    def with_validator(
        self, validator: Any, *, language: str | None = None
    ) -> "Cell":
        return self._derive(validator=validator, validator_language=language)

    def build(self, input_ref: Any = _UNSET) -> Expression:
        if input_ref is _UNSET:
            input_ref = self._input_ref
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
        return self.build(input_ref).compute()

    def run(self, input_ref: Any = _UNSET):
        return self.build(input_ref).run()

    def __getitem__(self, item: Any) -> "Cell":
        if isinstance(item, slice):
            return self.slice(item.start, item.stop, item.step)
        return self.item(item)

    def __getattr__(self, name: str) -> "Cell":
        if name.startswith("_"):
            raise AttributeError(name)
        return self.item(name)

    def __repr__(self) -> str:
        cls = type(self).__name__
        return (
            f"{cls}(input_ref={self._input_ref!r}, path={self.path_python!r}, "
            f"celltype={self._celltype!r}, target_celltype={self._target_celltype!r})"
        )


def _snapshot_input_ref(input_ref: Any) -> Any:
    if isinstance(input_ref, (dict, list, set, bytearray)):
        return copy.deepcopy(input_ref)
    return input_ref


__all__ = ["Cell"]
