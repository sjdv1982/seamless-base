"""Calculate SHA3-256 checksum, with a small LRU cache"""

from seamless import Checksum, Buffer
from .calculate_checksum import (
    calculate_checksum as calculate_checksum_func,
)

from ..util import lrucache2

# calculate_checksum_cache: maps id(buffer) to (checksum, buffer).
# Need to store (a ref to) buffer,
#  because id(buffer) is only unique while buffer does not die!!!

calculate_checksum_cache = lrucache2(10)

checksum_cache = lrucache2(10)


async def cached_calculate_checksum(buffer: Buffer) -> Checksum:
    """Calculate SHA3-256 checksum, with a small LRU cache"""

    buffer2 = buffer.content
    assert isinstance(buffer2, bytes)
    buf_id = id(buffer2)
    cached_checksum, _ = calculate_checksum_cache.get(buf_id, (None, None))  # type: ignore
    if cached_checksum is not None:
        cached_checksum = Checksum(cached_checksum)
        checksum_cache[cached_checksum] = buffer2
        return cached_checksum
    """
    # ThreadPoolExecutor does not work... ProcessPoolExecutor is slow. To experiment with later
    loop = asyncio.get_event_loop()
    with ProcessPoolExecutor() as executor:
        checksum = await loop.run_in_executor(
            executor, calculate_checksum_func, buffer2
        )
    """
    checksum = Checksum(calculate_checksum_func(buffer2))
    calculate_checksum_cache[buf_id] = checksum, buffer2
    checksum_cache[checksum] = buffer2
    return checksum


def cached_calculate_checksum_sync(buffer: Buffer) -> Checksum:
    """Calculate SHA3-256 checksum, with a small LRU cache.
    This function can be executed if the asyncio event loop is already running"""
    buffer2 = buffer.content
    assert isinstance(buffer2, bytes)
    buf_id = id(buffer2)
    cached_checksum, _ = calculate_checksum_cache.get(buf_id, (None, None))  # type: ignore
    if cached_checksum is not None:
        cached_checksum = Checksum(cached_checksum)
        checksum_cache[cached_checksum] = buffer2
        return cached_checksum
    checksum = Checksum(calculate_checksum_func(buffer2))
    calculate_checksum_cache[buf_id] = checksum, buffer2
    checksum_cache[checksum] = buffer2
    return checksum
