"""Buffer cache implementation.

This module implements a simplified but functional central buffer cache following the
design notes in caching.txt. It provides:
- a weak cache (WeakValueDictionary) for general registrations
- a strong cache (dict) for buffers that have refs (interest)
- normal refs (incref/decref) and one tempref per checksum with decaying interest
- an eviction procedure that moves buffers from strong to weak based on a cost-per-GB
  ordering and configured soft/hard memory caps

The implementation focuses on the main behaviors and provides hooks for cost and
subsystem integrations.
"""

from __future__ import annotations

import threading
import time
import weakref
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

DEFAULT_SOFT_CAP = 5 * 1024**3
DEFAULT_HARD_CAP = 50 * 1024**3
DEFAULT_BENEFIT_PER_GB = 2.0


@dataclass
class TempRef:
    interest: float
    fade_factor: float = 2.0
    fade_interval: float = 2.0
    created: float = field(default_factory=time.time)
    last_refreshed: float = field(default_factory=time.time)

    def refresh(self):
        self.last_refreshed = time.time()

    def current_interest(self) -> float:
        """Calculate decayed interest based on elapsed time since last_refresh."""
        elapsed = time.time() - self.last_refreshed
        if elapsed <= 0 or self.fade_interval <= 0:
            return self.interest
        # number of fade steps
        steps = elapsed / self.fade_interval
        return self.interest / (self.fade_factor**steps)


@dataclass
class StrongEntry:
    buffer: Any = None
    size: Optional[int] = None  # bytes
    cost_per_gb: float = 1.0  # cost units per GB
    normal_refs: int = 0
    tempref: Optional[TempRef] = None

    def interest(self) -> float:
        i = float(self.normal_refs)
        if self.tempref is not None:
            i += self.tempref.current_interest()
        return i


class BufferCache:
    """Central buffer cache with weak and strong caches.

    Typical usage:
      cache = BufferCache()
      cache.register(buffer, checksum, size=1234)
      cache.add_ref(checksum)
      cache.run_eviction_once()

    The implementation intentionally keeps dependencies and subsystem hooks minimal.
    """

    def __init__(
        self,
        soft_cap: int = DEFAULT_SOFT_CAP,
        hard_cap: int = DEFAULT_HARD_CAP,
        benefit_per_gb: float = DEFAULT_BENEFIT_PER_GB,
    ) -> None:
        self.weak_cache: "weakref.WeakValueDictionary[str, Any]" = (
            weakref.WeakValueDictionary()
        )
        self.strong_cache: Dict[str, StrongEntry] = {}
        self.lock = threading.RLock()

        self.soft_cap = soft_cap
        self.hard_cap = hard_cap
        self.benefit_per_gb = benefit_per_gb
        # eviction background thread controls
        self._eviction_thread = None
        self._eviction_interval = None
        self._eviction_stop_event = None
        # store objects that cannot be weak-referenced
        self._non_weak = {}
        # metadata for checksums (size, cost)
        self._meta = {}

    # --- registration & lookup ---
    def register(
        self,
        checksum: str,
        buffer: Any,
        size: Optional[int] = None,
        cost_per_gb: Optional[float] = None,
    ) -> None:
        """Register a buffer with the weak cache and move to strong if the checksum has refs.

        - checksum: unique identifier
        - buffer: object to store (can be any Python object)
        - size: optional length in bytes. If None, treated as unknown (infinite cost)
        - cost_per_gb: override cost units/GB for this buffer
        """
        if cost_per_gb is None:
            cost = 1.0
        else:
            cost = cost_per_gb

        with self.lock:
            # store in weak cache first; if object cannot be weak-referenced,
            # keep a strong reference in _non_weak
            try:
                self.weak_cache[checksum] = buffer
                # ensure not present in non_weak
                if checksum in self._non_weak:
                    del self._non_weak[checksum]
            except TypeError:
                self._non_weak[checksum] = buffer
            # store metadata so later promotions know size and cost
            self._meta[checksum] = {"size": size, "cost_per_gb": cost}

            # If there is already a strong entry (refs exist), move/update it
            entry = self.strong_cache.get(checksum)
            if entry is not None:
                entry.buffer = buffer
                entry.size = size
                entry.cost_per_gb = cost

    def get(self, checksum: str) -> Optional[Any]:
        """Return buffer if present in strong or weak caches (promotes to strong if refs exist)."""
        with self.lock:
            entry = self.strong_cache.get(checksum)
            if entry is not None and entry.buffer is not None:
                return entry.buffer
            # try weak cache then non_weak
            buf = self.weak_cache.get(checksum)
            if buf is None:
                buf = self._non_weak.get(checksum)
            if buf is None:
                return None
            # if this checksum currently has interest, promote to strong
            if checksum in self.strong_cache:
                self.strong_cache[checksum].buffer = buf
                return buf
            return buf

    # --- refs management ---
    def add_ref(self, checksum: str) -> None:
        """Increment normal refcount for checksum. Creates a strong entry if needed."""
        with self.lock:
            entry = self.strong_cache.get(checksum)
            if entry is None:
                # create strong entry; buffer may be in weak cache or _non_weak
                buf = self.weak_cache.get(checksum)
                if buf is None:
                    buf = self._non_weak.get(checksum)
                meta = self._meta.get(checksum, {})
                size = meta.get("size", getattr(buf, "length", None))
                cost = meta.get("cost_per_gb", 1.0)
                entry = StrongEntry(buffer=buf, size=size, cost_per_gb=cost)
                self.strong_cache[checksum] = entry
            entry.normal_refs += 1

    def remove_ref(self, checksum: str) -> None:
        """Decrement normal refcount. If no refs remain (and no tempref), demote to weak."""
        with self.lock:
            entry = self.strong_cache.get(checksum)
            if entry is None:
                return
            entry.normal_refs = max(0, entry.normal_refs - 1)
            if entry.normal_refs == 0 and entry.tempref is None:
                # demote
                buf = entry.buffer
                if buf is not None:
                    try:
                        self.weak_cache[checksum] = buf
                        if checksum in self._non_weak:
                            del self._non_weak[checksum]
                    except TypeError:
                        self._non_weak[checksum] = buf
                del self.strong_cache[checksum]

    def add_tempref(
        self,
        checksum: str,
        interest: float = 128.0,
        fade_factor: float = 2.0,
        fade_interval: float = 2.0,
    ) -> None:
        """Add or refresh a single tempref for checksum. Only one tempref allowed per checksum."""
        with self.lock:
            entry = self.strong_cache.get(checksum)
            if entry is None:
                buf = self.weak_cache.get(checksum)
                if buf is None:
                    buf = self._non_weak.get(checksum)
                meta = self._meta.get(checksum, {})
                size = meta.get("size", getattr(buf, "length", None))
                cost = meta.get("cost_per_gb", 1.0)
                entry = StrongEntry(buffer=buf, size=size, cost_per_gb=cost)
                self.strong_cache[checksum] = entry
            if entry.tempref is None:
                entry.tempref = TempRef(
                    interest=interest,
                    fade_factor=fade_factor,
                    fade_interval=fade_interval,
                )
            else:
                entry.tempref.interest = interest
                entry.tempref.fade_factor = fade_factor
                entry.tempref.fade_interval = fade_interval
                entry.tempref.refresh()

    def refresh_tempref(self, checksum: str) -> None:
        with self.lock:
            entry = self.strong_cache.get(checksum)
            if entry is not None and entry.tempref is not None:
                entry.tempref.refresh()

    # --- eviction ---
    def _strong_memory_usage(self) -> int:
        total = 0
        for entry in self.strong_cache.values():
            if entry.size:
                total += int(entry.size)
        return total

    def _candidate_score(self, checksum: str, entry: StrongEntry) -> float:
        """Return cost-per-GB score used to pick eviction candidates.

        Lower scores are evicted first. If size unknown or zero, return +inf to avoid eviction.
        """
        if entry.size is None or entry.size <= 0:
            return float("inf")
        size_gb = entry.size / (1024**3)
        if size_gb <= 0:
            return float("inf")
        loss = entry.cost_per_gb * entry.interest()
        # cost-per-GB
        return loss / size_gb

    def run_eviction_once(self) -> Tuple[int, int]:
        """Run a single eviction pass.

        Returns (before_bytes, after_bytes) strong-cache totals.
        """
        with self.lock:
            before = self._strong_memory_usage()
            if before <= self.soft_cap:
                return before, before

            # Build candidate list (checksum, score)
            candidates = []
            for k, e in self.strong_cache.items():
                score = self._candidate_score(k, e)
                candidates.append((score, k, e))

            # Sort by score ascending (lowest cost-per-GB first)
            candidates.sort(key=lambda x: x[0])

            # Evict until under caps as required
            current = before
            i = 0
            while current > self.soft_cap and i < len(candidates):
                score, k, e = candidates[i]
                i += 1
                # If score is +inf, skip (unknown sizes)
                if score == float("inf"):
                    continue
                # Evict this candidate
                buf = e.buffer
                if buf is not None:
                    try:
                        self.weak_cache[k] = buf
                        if k in self._non_weak:
                            del self._non_weak[k]
                    except TypeError:
                        self._non_weak[k] = buf
                # subtract size
                if e.size:
                    current -= int(e.size)
                # remove strong entry but keep refs info
                del self.strong_cache[k]
                # If we are above the hard cap, keep evicting aggressively (loop continues)

            # If still above hard cap (because inf-sized entries prevented full eviction), we can't do more
            after = max(0, current)
            return before, after

    # --- background eviction loop ---
    def _eviction_worker(self, interval: float, stop_event: threading.Event) -> None:
        while not stop_event.wait(interval):
            try:
                self.run_eviction_once()
            except Exception:
                # swallow exceptions to keep the thread alive
                pass

    def start_eviction_loop(self, interval: float = 5.0) -> None:
        """Start a background thread that runs eviction every `interval` seconds.

        If already running, this is a no-op.
        """
        with self.lock:
            if self._eviction_thread is not None and self._eviction_thread.is_alive():
                return
            stop_event = threading.Event()
            t = threading.Thread(
                target=self._eviction_worker, args=(interval, stop_event), daemon=True
            )
            self._eviction_thread = t
            self._eviction_interval = interval
            self._eviction_stop_event = stop_event
            t.start()

    def stop_eviction_loop(self, timeout: Optional[float] = 2.0) -> None:
        """Stop the background eviction thread and wait up to `timeout` seconds."""
        with self.lock:
            ev = self._eviction_stop_event
            th = self._eviction_thread
            self._eviction_stop_event = None
            self._eviction_thread = None
            self._eviction_interval = None
        if ev is not None:
            ev.set()
        if th is not None:
            try:
                th.join(timeout=timeout)
            except Exception:
                pass

    # --- utilities & hooks ---
    def stats(self) -> Dict[str, Any]:
        with self.lock:
            strong = self._strong_memory_usage()
            weak_count = len(self.weak_cache) + len(self._non_weak)
            strong_count = len(self.strong_cache)
            return {
                "strong_bytes": strong,
                "strong_count": strong_count,
                "weak_count": weak_count,
                "soft_cap": self.soft_cap,
                "hard_cap": self.hard_cap,
            }


__all__ = ["BufferCache", "StrongEntry", "TempRef"]
