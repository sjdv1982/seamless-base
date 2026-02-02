"""Class for Seamless checksums. Seamless checksums are calculated as SHA-256 hashes of buffers."""

import asyncio
import threading
from typing import Union

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from seamless.caching.buffer_cache import TempRef

from seamless import CacheMissError, is_worker


def _run_coro_in_new_loop(coro):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        asyncio.set_event_loop(None)
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        try:
            loop.run_until_complete(loop.shutdown_default_executor())
        except Exception:
            pass
        loop.close()


def _run_coro_in_worker_thread(coro):
    result: dict[str, object] = {}
    error: dict[str, BaseException] = {}

    def _runner():
        try:
            result["value"] = asyncio.run(coro)
        except BaseException as exc:
            error["exc"] = exc

    thread = threading.Thread(target=_runner, name="ChecksumResolveLoop")
    thread.start()
    thread.join()
    if error:
        raise error["exc"]
    return result.get("value")


def validate_checksum(v):
    """Validate a checksum, list or dict recursively"""
    if isinstance(v, Checksum):
        pass
    elif isinstance(v, str):
        if len(v) != 64:
            msg = v
            if len(v) > 1000:
                msg = v[:920] + "..." + v[-50:]
            raise ValueError(msg)
        Checksum(v)
    elif isinstance(v, list):
        for vv in v:
            validate_checksum(vv)
    elif isinstance(v, dict):
        for vv in v.values():
            validate_checksum(vv)
    else:
        raise TypeError(type(v))


class Checksum:
    """Class for Seamless checksums.
    Seamless checksums are calculated as SHA-256 hashes of buffers."""

    __slots__ = ["_value"]

    def __init__(self, checksum: Union["Checksum", str, bytes]):
        if checksum is None:
            raise TypeError("Checksum cannot be constructed from None")

        if isinstance(checksum, Checksum):
            self._value = checksum.bytes()
        else:
            if isinstance(checksum, str):
                checksum = bytes.fromhex(checksum)
            if isinstance(checksum, bytes):
                if len(checksum) != 32:
                    raise ValueError(f"Incorrect length: {len(checksum)}, must be 32")
            else:
                raise TypeError(type(checksum))

            self._value = checksum

    @classmethod
    def load(cls, filename: str) -> "Checksum":
        """Loads the checksum from a .CHECKSUM file.

        If the filename doesn't have a .CHECKSUM extension, it is added"""
        if not filename.endswith(".CHECKSUM"):
            filename2 = filename + ".CHECKSUM"
        else:
            filename2 = filename
        with open(filename2, "rt") as f:
            checksum = f.read(100).rstrip()
        try:
            if len(checksum) != 64:
                raise ValueError
            self = cls(checksum)
        except (TypeError, ValueError):
            raise ValueError("File does not contain a SHA-256 checksum") from None
        return self

    def bytes(self) -> bytes:
        """Returns the checksum as a 32-byte bytes object"""
        return self._value

    def hex(self) -> str:
        """Returns the checksum as a 64-byte hexadecimal string"""
        return self._value.hex()

    def __eq__(self, other):
        if isinstance(other, bool):
            return bool(self) == other
        elif isinstance(other, int):
            return False
        if not isinstance(other, Checksum):
            other = Checksum(other)
        return self.bytes() == other.bytes()

    def __gt__(self, other):
        if isinstance(other, Checksum):
            return self._value > other._value
        elif isinstance(other, str):
            return self.hex() > other
        elif isinstance(other, bytes):
            return self._value > other
        else:
            raise NotImplementedError

    def __lt__(self, other):
        if isinstance(other, Checksum):
            return self._value < other._value
        elif isinstance(other, str):
            return self.hex() < other
        elif isinstance(other, bytes):
            return self._value < other
        else:
            raise NotImplementedError

    def save(self, filename):
        """Saves the checksum to a .CHECKSUM file.

        If the filename doesn't have a .CHECKSUM extension, it is added"""
        if not filename.endswith(".CHECKSUM"):
            filename2 = filename + ".CHECKSUM"
        else:
            filename2 = filename
        with open(filename2, "wt") as f:
            f.write(self.hex() + "\n")

    def resolve(self, celltype=None):
        """Returns the data buffer that corresponds to the checksum.
        If celltype is provided, a value is returned instead.

        The buffer is retrieved from buffer cache"""

        from . import Buffer
        from seamless.checksum.calculate_checksum import TRIVIAL_CHECKSUMS

        btriv = TRIVIAL_CHECKSUMS.get(self.hex())
        if btriv is not None:
            buf = Buffer(btriv)
        else:
            buf = get_buffer_cache().get(self)
        if buf is None:
            try:
                import seamless_remote.buffer_remote
            except ImportError:
                pass
            else:
                coro = seamless_remote.buffer_remote.get_buffer(self)
                if is_worker():
                    buf = asyncio.run(coro)
                else:
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = None
                    if loop is None:
                        buf = _run_coro_in_new_loop(coro)
                    elif loop.is_running():
                        buf = _run_coro_in_worker_thread(coro)
                    else:
                        buf = loop.run_until_complete(coro)

        if buf is None:
            raise CacheMissError(self)
        assert isinstance(buf, Buffer)
        if celltype is not None:
            return buf.get_value(celltype)
        else:
            return buf

    async def resolution(self, celltype=None):
        """Returns the data buffer that corresponds to the checksum.
        If celltype is provided, a value is returned instead."""

        buf = get_buffer_cache().get(self)

        if buf is None:
            try:
                import seamless_remote.buffer_remote
            except ImportError:
                pass
            else:
                buf = await seamless_remote.buffer_remote.get_buffer(self)

        if buf is None:
            raise CacheMissError(self)
        if celltype is not None:
            return await buf.get_value_async(celltype)
        else:
            return buf

    def incref(self, *, scratch: bool = False) -> None:
        """Increment normal refcount in the buffer cache.

        If scratch is True, keep the ref scratch-only (no remote registration).
        """

        get_buffer_cache().incref(self, scratch=scratch)

    def decref(self):
        """Decrement normal refcount in the buffer cache. If no refs remain (and no tempref), may be uncached."""
        get_buffer_cache().decref(self)

    def tempref(
        self,
        interest: float = 128.0,
        fade_factor: float = 2.0,
        fade_interval: float = 2.0,
        scratch: bool = False,
    ) -> "TempRef":
        """Add or refresh a single tempref. Only one tempref allowed per checksum.

        If scratch is True, keep the tempref scratch-only (no remote registration).
        """

        return get_buffer_cache().tempref(
            self,
            interest=interest,
            fade_factor=fade_factor,
            fade_interval=fade_interval,
            scratch=scratch,
        )

    async def fingertip(self, celltype=None):
        """Return a resolvable buffer/value, recomputing locally if needed."""

        try:
            return await self.resolution(celltype)
        except CacheMissError:
            pass

        candidates: list[str] = []
        seen: set[str] = set()
        try:
            from seamless_transformer.transformation_cache import (
                get_transformation_cache,
            )

            cache = get_transformation_cache()
            for tf_checksum in cache.get_reverse_transformations(self):
                tf_hex = Checksum(tf_checksum).hex()
                if tf_hex not in seen:
                    seen.add(tf_hex)
                    candidates.append(tf_hex)
        except Exception:
            pass

        try:
            from seamless_remote import database_remote

            tf_checksums = await database_remote.get_rev_transformations(self)
        except Exception:
            tf_checksums = None
        if tf_checksums:
            for tf_checksum in tf_checksums:
                tf_hex = Checksum(tf_checksum).hex()
                if tf_hex not in seen:
                    seen.add(tf_hex)
                    candidates.append(tf_hex)

        if not candidates:
            raise CacheMissError(self)

        try:
            from seamless_transformer.transformation_cache import (
                recompute_from_transformation_checksum,
            )
        except Exception as exc:
            raise CacheMissError(self) from exc

        for tf_hex in candidates:
            try:
                result = await recompute_from_transformation_checksum(
                    tf_hex, scratch=True, require_value=True
                )
            except Exception:
                continue
            if result is None:
                continue
            try:
                if Checksum(result) != self:
                    continue
            except Exception:
                continue
            try:
                return await self.resolution(celltype)
            except CacheMissError:
                continue

        raise CacheMissError(self)

    def fingertip_sync(self, celltype=None):
        """Synchronously resolve or recompute the checksum buffer/value."""

        coro = self.fingertip(celltype=celltype)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is None:
            return _run_coro_in_new_loop(coro)
        if loop.is_running():
            return _run_coro_in_worker_thread(coro)
        return loop.run_until_complete(coro)

    def find(self, verbose: bool = False) -> list | None:
        """Returns a list of URL infos to download the underlying buffer.
        An URL info can be an URL string, or a dict with additional information."""
        # from seamless.util.fair import find_url_info
        raise NotImplementedError(
            "Checksum.find is disabled: FAIR lookup support not available"
        )

    def __str__(self):
        return str(self.hex())

    def __repr__(self):
        return repr(self.hex())

    def __hash__(self):
        return hash(self._value)


from seamless.caching.buffer_cache import get_buffer_cache
