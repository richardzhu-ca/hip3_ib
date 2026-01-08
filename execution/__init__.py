"""Execution backends for order execution."""

from .base import ExecutionBackend, ExecutionResult

# Conditional import for pquant backend
try:
    from .pquant import PQuantBackend, PQUANT_AVAILABLE
except ImportError:
    PQuantBackend = None  # type: ignore
    PQUANT_AVAILABLE = False

__all__ = [
    "ExecutionBackend",
    "ExecutionResult",
    "PQuantBackend",
    "PQUANT_AVAILABLE",
]