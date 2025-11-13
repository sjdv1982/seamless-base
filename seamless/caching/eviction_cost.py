"""Eviction cost coordination module.

This module keeps track of buffer lengths (which are never forgotten) and
pre-computed eviction costs for all checksums of interest. "Interest" covers
checksums that reside in the strong cache and every checksum in their upstream
dependency graph. Costs are recomputed lazily: either on the first request
after a checksum becomes interesting or when a subsystem notifies the
coordinator that relevant data changed.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Sequence, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from seamless.checksum_class import Checksum

BYTES_IN_GB = 1024**3

# Default download cost profiles
DEFAULT_READ_BUFFER_PROFILE = ("read_buffer", 1.0, 0.0)  # units/GB, min units
DEFAULT_LOCAL_HASH_PROFILE = ("local_hashserver", 10.0, 0.001)
DEFAULT_REMOTE_HASH_PROFILE = ("remote_hashserver", 100.0, 0.01)

# Default transformation settings
DEFAULT_CPU_COST_PER_SECOND = 0.1
DEFAULT_TRANSFORMATION_SECONDS = 3600


@dataclass(frozen=True)
class CostProfile:
    """Container for basic per-GB cost information."""

    units_per_gb: float
    min_cost: float = 0.0


class DownloadCostSubsystem:
    """Stub implementation that can map a checksum to a download cost profile."""

    def __init__(self) -> None:
        self._profiles: Dict[str, CostProfile] = {
            DEFAULT_READ_BUFFER_PROFILE[0]: CostProfile(
                DEFAULT_READ_BUFFER_PROFILE[1], DEFAULT_READ_BUFFER_PROFILE[2]
            ),
            DEFAULT_LOCAL_HASH_PROFILE[0]: CostProfile(
                DEFAULT_LOCAL_HASH_PROFILE[1], DEFAULT_LOCAL_HASH_PROFILE[2]
            ),
            DEFAULT_REMOTE_HASH_PROFILE[0]: CostProfile(
                DEFAULT_REMOTE_HASH_PROFILE[1], DEFAULT_REMOTE_HASH_PROFILE[2]
            ),
        }
        self._checksum_profiles: Dict[Checksum, str] = {}
        self._manual_costs: Dict[Checksum, float] = {}

    def set_profile(self, checksum: Checksum, profile: str) -> None:
        """Assign a download profile to a checksum."""
        self._checksum_profiles[checksum] = profile

    def set_manual_cost(self, checksum: Checksum, cost: float) -> None:
        """Override the download cost entirely for tests or advanced use-cases."""
        self._manual_costs[checksum] = max(cost, 0.0)

    def clear(self, checksum: Checksum) -> None:
        """Forget cost information for a checksum."""
        self._checksum_profiles.pop(checksum, None)
        self._manual_costs.pop(checksum, None)

    def notify_upload(self, checksum: Checksum) -> None:
        """Placeholder: would invalidate cached download data after uploads."""
        self.clear(checksum)

    def estimate_cost(self, checksum: Checksum, length_bytes: int) -> float:
        """Return download cost for the checksum."""
        if checksum in self._manual_costs:
            return self._manual_costs[checksum]
        profile_name = self._checksum_profiles.get(checksum)
        if profile_name is None:
            profile = self._profiles["read_buffer"]
        else:
            profile = self._profiles.get(profile_name, self._profiles["read_buffer"])
        length_gb = length_bytes / BYTES_IN_GB
        cost = length_gb * profile.units_per_gb
        return max(cost, profile.min_cost)


class TransformationSubsystem:
    """Stub that tracks CPU time metadata for transformation results."""

    def __init__(self) -> None:
        self._cpu_seconds: Dict[Checksum, float] = {}

    def record(self, checksum: Checksum, cpu_seconds: Optional[float]) -> None:
        if cpu_seconds is None:
            self._cpu_seconds.pop(checksum, None)
        else:
            self._cpu_seconds[checksum] = max(cpu_seconds, 0.0)

    def notify_transformation(self, checksum: Checksum, metadata: dict) -> None:
        """Placeholder for integration with execution machinery."""
        cpu_seconds = metadata.get("cpu_seconds")
        if cpu_seconds is not None:
            self.record(checksum, cpu_seconds)

    def estimate_cost(self, checksum: Checksum) -> float:
        cpu_seconds = self._cpu_seconds.get(checksum)
        if cpu_seconds is None:
            return 0.0
        return cpu_seconds * DEFAULT_CPU_COST_PER_SECOND


class ConversionSubsystem:
    """Stub for expression/join/syn2sem conversions (zero cost by default)."""

    def __init__(self) -> None:
        self._metadata: Dict[Checksum, dict] = {}

    def record(self, checksum: Checksum, metadata: Optional[dict]) -> None:
        if metadata is None:
            self._metadata.pop(checksum, None)
        else:
            self._metadata[checksum] = metadata

    def estimate_cost(self, checksum: Checksum) -> float:
        """Conversions are assumed to be free; override to customize."""
        _ = self._metadata.get(checksum)
        return 0.0


class BufferLengthSubsystem:
    """Tracks known buffer lengths. Entries are never forgotten."""

    def __init__(self) -> None:
        self._lengths: Dict[Checksum, int] = {}

    def notify_length(self, checksum: Checksum, length: Optional[int]) -> bool:
        """Record a buffer length. Returns True if something changed."""
        if length is None:
            return False
        normalized = max(int(length), 0)
        old = self._lengths.get(checksum)
        if old == normalized:
            return False
        self._lengths[checksum] = normalized
        return True

    def get_length(self, checksum: Checksum) -> Optional[int]:
        return self._lengths.get(checksum)


class CostModule:
    """Central coordinator that holds subsystem stubs and exposes get_cost."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.download = DownloadCostSubsystem()
        self.transformations = TransformationSubsystem()
        self.conversions = ConversionSubsystem()
        self.buffer_lengths = BufferLengthSubsystem()
        self._dependencies: Dict[Checksum, Sequence[Checksum]] = {}
        self._reverse_dependencies: Dict[Checksum, Set[Checksum]] = {}
        self._dependency_interest: Dict[Checksum, int] = {}
        self._direct_interest: Dict[Checksum, int] = {}
        self._cost_cache: Dict[Checksum, float] = {}
        self._computing: Set[Checksum] = set()

    # --- interest tracking -------------------------------------------------
    def add_interest(self, checksum: Checksum) -> None:
        with self._lock:
            count = self._direct_interest.get(checksum, 0) + 1
            self._direct_interest[checksum] = count
            if count == 1 and self._dependency_interest.get(checksum, 0) == 0:
                self._on_interest_start(checksum)

    def remove_interest(self, checksum: Checksum) -> None:
        with self._lock:
            count = self._direct_interest.get(checksum)
            if count is None:
                return
            if count <= 1:
                self._direct_interest.pop(checksum, None)
                self._maybe_release(checksum)
            else:
                self._direct_interest[checksum] = count - 1

    def _increment_dependency_interest(self, checksum: Checksum) -> None:
        count = self._dependency_interest.get(checksum, 0) + 1
        self._dependency_interest[checksum] = count
        if count == 1 and self._direct_interest.get(checksum, 0) == 0:
            self._on_interest_start(checksum)

    def _decrement_dependency_interest(self, checksum: Checksum) -> None:
        count = self._dependency_interest.get(checksum)
        if count is None:
            return
        if count <= 1:
            self._dependency_interest.pop(checksum, None)
            self._maybe_release(checksum)
        else:
            self._dependency_interest[checksum] = count - 1

    def _total_interest(self, checksum: Checksum) -> int:
        return self._direct_interest.get(checksum, 0) + self._dependency_interest.get(
            checksum, 0
        )

    def _is_interesting(self, checksum: Checksum) -> bool:
        return self._total_interest(checksum) > 0

    def _on_interest_start(self, checksum: Checksum) -> None:
        """Hook for batching subsystem queries. No-op in this stub."""
        # Real implementation would schedule background fetches here.
        _ = checksum

    def _maybe_release(self, checksum: Checksum) -> None:
        if self._is_interesting(checksum):
            return
        self._cost_cache.pop(checksum, None)
        self._detach_dependencies(checksum)

    # --- dependency management --------------------------------------------
    def register_dependencies(
        self, checksum: Checksum, dependencies: Optional[Iterable[Checksum]]
    ) -> None:
        deps_tuple: Sequence[Checksum] = tuple(dependencies) if dependencies else ()
        with self._lock:
            self._detach_dependencies(checksum)
            if deps_tuple:
                self._dependencies[checksum] = deps_tuple
                for dep in deps_tuple:
                    self._reverse_dependencies.setdefault(dep, set()).add(checksum)
                    self._increment_dependency_interest(dep)
            else:
                self._dependencies.pop(checksum, None)
            if self._is_interesting(checksum):
                self._mark_dirty(checksum)

    def _detach_dependencies(self, checksum: Checksum) -> None:
        old_deps = self._dependencies.pop(checksum, ())
        if not old_deps:
            return
        for dep in old_deps:
            rev = self._reverse_dependencies.get(dep)
            if rev is not None:
                rev.discard(checksum)
                if not rev:
                    self._reverse_dependencies.pop(dep, None)
            self._decrement_dependency_interest(dep)

    # --- buffer length registration ---------------------------------------
    def register_buffer_length(self, checksum: Checksum, length: Optional[int]) -> None:
        changed = self.buffer_lengths.notify_length(checksum, length)
        if changed and self._is_interesting(checksum):
            self._mark_dirty(checksum)

    # --- dirty propagation -------------------------------------------------
    def mark_dirty(self, checksum: Checksum) -> None:
        with self._lock:
            self._mark_dirty(checksum)

    def _mark_dirty(self, checksum: Checksum) -> None:
        pending = [checksum]
        seen: Set[Checksum] = set()
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            self._cost_cache.pop(current, None)
            for parent in self._reverse_dependencies.get(current, ()):
                pending.append(parent)

    # --- cost computation --------------------------------------------------
    def get_cost(
        self, checksum: Checksum, buffer_length: Optional[int] = None
    ) -> float:
        with self._lock:
            if buffer_length is not None:
                changed = self.buffer_lengths.notify_length(checksum, buffer_length)
                if changed and self._is_interesting(checksum):
                    self._mark_dirty(checksum)
            if not self._is_interesting(checksum):
                return float("inf")
            return self._ensure_cost(checksum)

    def _ensure_cost(self, checksum: Checksum) -> float:
        cached = self._cost_cache.get(checksum)
        if cached is not None:
            return cached
        cost = self._compute_cost(checksum)
        self._cost_cache[checksum] = cost
        return cost

    def _compute_cost(self, checksum: Checksum) -> float:
        if checksum in self._computing:
            return float("inf")
        self._computing.add(checksum)
        try:
            length = self.buffer_lengths.get_length(checksum)
            if length is None:
                return float("inf")
            download_cost = self.download.estimate_cost(checksum, length)
            deps = self._dependencies.get(checksum)
            dependency_cost: Optional[float] = None
            if deps:
                dependency_cost = 0.0
                for dep in deps:
                    dep_cost = self._ensure_cost(dep)
                    if dep_cost == float("inf"):
                        dependency_cost = float("inf")
                        break
                    dependency_cost += dep_cost
            if dependency_cost == float("inf"):
                return float("inf")
            if dependency_cost is None:
                base_cost = download_cost
            else:
                base_cost = max(download_cost, dependency_cost)
            transformation_cost = self.transformations.estimate_cost(checksum)
            conversion_cost = self.conversions.estimate_cost(checksum)
            return base_cost + transformation_cost + conversion_cost
        finally:
            self._computing.discard(checksum)


_cost_module = CostModule()


def get_cost(checksum: Checksum, buffer_length: Optional[int] = None) -> float:
    """Public entry point for eviction cost calculation."""
    return _cost_module.get_cost(checksum, buffer_length)


def register_buffer_length(checksum: Checksum, buffer_length: Optional[int]) -> None:
    """Expose buffer length updates."""
    _cost_module.register_buffer_length(checksum, buffer_length)


def register_dependencies(
    checksum: Checksum, dependencies: Optional[Iterable[Checksum]]
) -> None:
    _cost_module.register_dependencies(checksum, dependencies)


def set_download_profile(checksum: Checksum, profile: str) -> None:
    _cost_module.download.set_profile(checksum, profile)
    _cost_module.mark_dirty(checksum)


def override_download_cost(checksum: Checksum, cost: float) -> None:
    _cost_module.download.set_manual_cost(checksum, cost)
    _cost_module.mark_dirty(checksum)


def record_transformation_cost(
    checksum: Checksum, cpu_seconds: Optional[float]
) -> None:
    _cost_module.transformations.record(checksum, cpu_seconds)
    _cost_module.mark_dirty(checksum)


def add_interest(checksum: Checksum) -> None:
    _cost_module.add_interest(checksum)


def remove_interest(checksum: Checksum) -> None:
    _cost_module.remove_interest(checksum)


__all__ = [
    "get_cost",
    "register_buffer_length",
    "register_dependencies",
    "set_download_profile",
    "override_download_cost",
    "record_transformation_cost",
    "add_interest",
    "remove_interest",
    "DEFAULT_CPU_COST_PER_SECOND",
    "DEFAULT_TRANSFORMATION_SECONDS",
    "DEFAULT_READ_BUFFER_PROFILE",
    "DEFAULT_LOCAL_HASH_PROFILE",
    "DEFAULT_REMOTE_HASH_PROFILE",
]
