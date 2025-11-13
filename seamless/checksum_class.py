"""Class for Seamless checksums. Seamless checksums are calculated as SHA3-256 hashes of buffers."""

from typing import Union


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
    Seamless checksums are calculated as SHA3-256 hashes of buffers."""

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
            raise ValueError("File does not contain a SHA3-256 checksum") from None
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

        # local import to avoid importing caching at module import time
        from seamless.caching.buffer_cache import get_cache
        from seamless import CacheMissError

        buf = get_cache().get(self)
        if buf is None:
            raise CacheMissError
        return buf

    async def resolution(self, celltype=None):
        """Returns the data buffer that corresponds to the checksum.
        If celltype is provided, a value is returned instead.

        This imports seamless.workflow"""
        # local import to avoid importing caching at module import time
        from seamless.caching.buffer_cache import get_cache
        from seamless import CacheMissError

        buf = get_cache().get(self)
        if buf is None:
            raise CacheMissError
        return buf

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
