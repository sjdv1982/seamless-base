import os
import shutil

from seamless import Buffer, Checksum, CacheMissError


def write_to_directory(directory, data, *, cleanup, deep, text_only):
    """Writes (deep) folder data into a directory.

    Note: This does not do buffer bookkeeping (incref/cache/remote upload). Restoring
    parity with old Seamless likely requires resolving deep checksums, ensuring buffers
    exist in storage, and managing reference counts for data that is written.

    Long version:

    Bookkeeping can be restored later, but “just call Buffer.incref()” is not sufficient by itself. A few gaps to keep in mind:

    Buffer.incref() requires a Buffer object; for deep structures you often only have checksum strings, so you’d need to Checksum.resolve() each referenced buffer first and then incref it. If the buffer isn’t already in storage, resolve() will fail.

    In old Seamless, write_to_directory (deep mode) implicitly ensured each file buffer was present in storage (and cached/registered) as it walked the structure. Simply incref‑ing after the fact doesn’t magically populate missing buffers.

    For text_only or decoded variants, the bytes written to disk may differ from the canonical buffer content; if you later incref the original buffer, you’re not necessarily recording the exact bytes that were written.

    For remote setups, incref alone doesn’t guarantee the data is uploaded or discoverable by other processes unless the backing storage is available and configured. The old code used buffer_cache utilities to guarantee buffer info; without that, resolution across processes can be fragile.

    If you intend to allow re‑reading written directories as Seamless deep folders later, you’d need a read‑back path (or explicit caching) that creates/checksums buffers from the filesystem content, not just incref existing buffers.

    So: incref can be part of the fix, but you’ll likely also need “resolve or create buffer per file” + explicit caching/metadata steps to match old behavior. If you want, I can sketch the minimal bookkeeping pipeline when you decide to add parity.
    """
    abs_dir = os.path.abspath(directory)
    os.makedirs(abs_dir, exist_ok=True)
    if not isinstance(data, dict):
        raise TypeError("Data must be a dict of files")
    all_files = set()
    all_dirs = set()
    for relpath, value in data.items():
        rel_dir = os.path.dirname(relpath)
        if rel_dir:
            rel_dir = os.path.join(abs_dir, rel_dir)
            if rel_dir not in all_dirs:
                all_dirs.add(rel_dir)
                os.makedirs(rel_dir, exist_ok=True)
        filename = os.path.join(abs_dir, relpath)
        all_files.add(filename)
        if deep:
            checksum = Checksum(value)
            buffer = checksum.resolve()
            if buffer is None:
                raise CacheMissError(checksum.hex())
        else:
            if isinstance(value, Buffer):
                buffer = value
            elif isinstance(value, bytes):
                buffer = Buffer(value)
            elif isinstance(value, str):
                buffer = Buffer(value, celltype="text")
            else:
                buffer = Buffer(value, celltype="mixed")
        if text_only:
            try:
                text_value = buffer.get_value("text")
            except Exception:
                continue
            with open(filename, "w") as handle:
                handle.write(text_value)
        else:
            with open(filename, "wb") as handle:
                handle.write(buffer.value)
    if cleanup:
        with os.scandir(abs_dir) as it:
            for entry in it:
                path = os.path.abspath(entry.path)
                if entry.is_file():
                    if path not in all_files:
                        os.unlink(path)
                elif entry.is_dir():
                    if path not in all_dirs:
                        shutil.rmtree(path, ignore_errors=True)


__all__ = ["write_to_directory"]
