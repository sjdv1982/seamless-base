"""Helper to obtain or create an asyncio event loop without deprecation warnings."""

from __future__ import annotations

import asyncio
import warnings


def get_event_loop() -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        pass

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    return loop
