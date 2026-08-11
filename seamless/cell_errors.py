"""Errors used by the canonical Cell builder API."""


class BoundStateError(AttributeError):
    """Raised when standalone-only Cell state is changed on a bound Cell."""


__all__ = ["BoundStateError"]
