"""Abstract base class for execution backends."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ExecutionResult:
    """Result of an order execution."""
    
    success: bool
    action_id: str
    exchange: str = ""
    symbol: str = ""
    side: str = ""
    requested_size: float = 0.0
    filled_amount: Optional[float] = None
    avg_price: Optional[float] = None
    order_count: int = 0
    message: str = ""
    error: Optional[str] = None
    raw_result: dict = field(default_factory=dict)


class ExecutionBackend(ABC):
    """
    Abstract interface for order execution backends.
    
    Implementations can include:
    - PQuantBackend: Uses pquant_sdk for execution
    - MockBackend: For testing and dry-run mode
    - DirectAPIBackend: Direct exchange API calls
    
    The executor injects a backend instance and delegates
    actual order execution to it.
    """
    
    @abstractmethod
    async def connect(self) -> bool:
        """
        Connect to execution service.
        
        Returns:
            True if connected successfully
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from execution service."""
        pass
    
    @abstractmethod
    async def execute_order(
        self,
        exchange: str,
        symbol: str,
        side: str,
        size: float,
        reduce_only: bool = False,
        **kwargs: Any,
    ) -> ExecutionResult:
        """
        Execute a single order.
        
        Args:
            exchange: Exchange name (e.g., "hyperliquid_linear")
            symbol: Trading symbol (e.g., "BTCUSDC")
            side: Order side ("buy" or "sell")
            size: Order size in base currency
            reduce_only: Whether order can only reduce position
            **kwargs: Backend-specific parameters
        
        Returns:
            ExecutionResult with fill details
        """
        pass
    
    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if backend is connected."""
        pass
    
    @property
    def name(self) -> str:
        """Backend name for logging."""
        return self.__class__.__name__
