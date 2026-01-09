"""PQuant SDK execution backend."""

import asyncio
import logging
from typing import Any, Optional

from .base import ExecutionBackend, ExecutionResult

logger = logging.getLogger(__name__)

# Lazy import - SDK may not be installed
try:
    from pquant_sdk import (
        ExecutionClient,
        Exchange,
        InstrumentType,
        Side,
    )
    PQUANT_AVAILABLE = True
except ImportError:
    PQUANT_AVAILABLE = False
    ExecutionClient = None
    Exchange = None
    InstrumentType = None
    Side = None


class PQuantBackend(ExecutionBackend):
    """
    Execution backend using pquant_sdk.
    
    Features:
    - Lazy registration: Only registers with exchanges when first order is sent
    - DCA execution: Uses taker DCA for better fills
    - Blocking mode: Waits for order completion
    
    Example:
        backend = PQuantBackend(
            server_addr="http://localhost:8080",
            credentials={
                "hyperliquid": {"api_key": "wallet_addr", "api_secret": "private_key"},
            },
        )
        await backend.connect()
        result = await backend.execute_order("hyperliquid", "BTCUSDC", "buy", 0.1)
    """
    
    # Exchange name mapping to SDK enums
    EXCHANGE_MAP = {
        "hyperliquid": "HYPERLIQUID",
        "binance": "BINANCE",
        "drift": "DRIFT",
    }
    
    def __init__(
        self,
        server_addr: str,
        credentials: dict[str, dict[str, str]],
        strategy_name: str = "risk_engine",
        use_dca: bool = True,
        default_dca_size: Optional[float] = None,
        timeout_seconds: int = 60,
        best_effort_completion: bool = True,
    ):
        """
        Initialize PQuant backend.
        
        Args:
            server_addr: PQuant execution server address
            credentials: Dict mapping exchange name to credentials
                         {"hyperliquid": {"api_key": "...", "api_secret": "..."}}
            strategy_name: Strategy name for registration
            use_dca: Whether to use DCA execution (recommended)
            default_dca_size: Default DCA chunk size (None = full size)
            timeout_seconds: Order timeout in seconds
            best_effort_completion: Try to complete order even if partial fills
        """
        if not PQUANT_AVAILABLE:
            raise ImportError(
                "pquant_sdk not installed. Install with: pip install pquant-sdk"
            )
        
        self.server_addr = server_addr
        self.credentials = credentials
        self.strategy_name = strategy_name
        self.use_dca = use_dca
        self.default_dca_size = default_dca_size
        self.timeout_seconds = timeout_seconds
        self.best_effort_completion = best_effort_completion
        
        self._client: Optional[ExecutionClient] = None
        self._registered_exchanges: set[str] = set()
        self._connected = False
    
    @property
    def is_connected(self) -> bool:
        return self._connected and self._client is not None
    
    async def connect(self) -> bool:
        """Connect to PQuant execution server."""
        try:
            # ExecutionClient uses context manager, but we need persistent connection
            self._client = ExecutionClient(self.server_addr)
            self._client.__enter__()
            self._connected = True
            logger.info(f"Connected to PQuant server: {self.server_addr}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PQuant server: {e}")
            self._connected = False
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from PQuant server."""
        if self._client:
            try:
                self._client.shutdown()
                self._client.__exit__(None, None, None)
            except Exception as e:
                logger.warning(f"Error during disconnect: {e}")
            finally:
                self._client = None
                self._connected = False
                self._registered_exchanges.clear()
                logger.info("Disconnected from PQuant server")
    
    def _get_exchange_enum(self, exchange: str) -> Any:
        """Map exchange string to SDK enum."""
        # Extract base exchange name (e.g., "hyperliquid_linear" -> "hyperliquid")
        base = exchange.split("_")[0].lower()
        enum_name = self.EXCHANGE_MAP.get(base)
        
        if not enum_name:
            raise ValueError(f"Unknown exchange: {exchange}")
        
        return getattr(Exchange, enum_name)
    
    def _ensure_registered(self, exchange: str) -> None:
        """
        Ensure exchange is registered (lazy registration).
        
        Only registers with an exchange when we actually need to execute there.
        """
        base_exchange = exchange.split("_")[0].lower()
        
        if base_exchange in self._registered_exchanges:
            return
        
        creds = self.credentials.get(base_exchange) or self.credentials.get(exchange)
        if not creds:
            raise ValueError(
                f"No credentials for {exchange}. "
                f"Available: {list(self.credentials.keys())}"
            )
        
        exchange_enum = self._get_exchange_enum(exchange)
        
        logger.info(f"Registering with {exchange}...")
        result = self._client.register(
            strategy_name=self.strategy_name,
            exchange=exchange_enum,
            api_key=creds["api_key"],
            api_secret=creds["api_secret"],
        )
        
        self._registered_exchanges.add(base_exchange)
        logger.info(f"Registered with {exchange}: worker_id={result.worker_id}")
    
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
        Execute order via PQuant SDK.
        
        Additional kwargs:
            dca_size: Override DCA chunk size
            timeout: Override timeout seconds
            instrument_type: "perp" or "spot" (default: "perp")
        """
        if not self.is_connected:
            return ExecutionResult(
                success=False,
                action_id="",
                exchange=exchange,
                symbol=symbol,
                side=side,
                requested_size=size,
                error="Not connected to PQuant server",
                message="Backend not connected",
            )
        
        try:
            # Ensure registered with this exchange
            self._ensure_registered(exchange)
            
            # Map parameters
            sdk_side = Side.BUY if side.lower() == "buy" else Side.SELL
            # DCA size should not exceed total size
            dca_size_raw = kwargs.get("dca_size", self.default_dca_size) or size
            dca_size = min(dca_size_raw, size)  # Ensure DCA size doesn't exceed total size
            timeout = kwargs.get("timeout", self.timeout_seconds)
            
            # Instrument type
            inst_type_str = kwargs.get("instrument_type", "perp").lower()
            inst_type = InstrumentType.PERP if inst_type_str == "perp" else InstrumentType.SPOT
            
            logger.info(
                f"Executing order: {side.upper()} {size} {symbol} on {exchange} "
                f"(dca_size={dca_size}, reduce_only={reduce_only})"
            )
            
            # Execute via SDK
            if self.use_dca:
                result = self._client.execute_taker_dca(
                    symbol=symbol,
                    side=sdk_side,
                    total_size=str(size),
                    dca_size=str(dca_size),
                    reduce_only=reduce_only,
                    best_effort_completion=self.best_effort_completion,
                    instrument_type=inst_type,
                    wait_for_completion=True,
                    timeout_seconds=timeout,
                )
            else:
                # Direct taker order (if SDK supports it)
                result = self._client.execute_taker_dca(
                    symbol=symbol,
                    side=sdk_side,
                    total_size=str(size),
                    dca_size=str(size),  # Single order
                    reduce_only=reduce_only,
                    best_effort_completion=self.best_effort_completion,
                    instrument_type=inst_type,
                    wait_for_completion=True,
                    timeout_seconds=timeout,
                )
            
            # Parse result
            filled = float(result.filled_amount) if result.filled_amount else None
            avg_price = float(result.avg_price) if result.avg_price else None
            
            exec_result = ExecutionResult(
                success=result.success,
                action_id=result.action_id or "",
                exchange=exchange,
                symbol=symbol,
                side=side,
                requested_size=size,
                filled_amount=filled,
                avg_price=avg_price,
                order_count=result.order_count if hasattr(result, "order_count") else 1,
                message=result.message or "",
                raw_result={
                    "status": result.status.name if hasattr(result.status, "name") else str(result.status),
                    "total_volume": str(result.total_volume) if hasattr(result, "total_volume") else None,
                },
            )
            
            if result.success:
                logger.info(
                    f"Order completed: filled={filled} @ avg_price={avg_price} "
                    f"(orders={exec_result.order_count})"
                )
            else:
                logger.warning(f"Order failed: {result.message}")
            
            return exec_result
            
        except Exception as e:
            logger.error(f"Order execution error: {e}")
            return ExecutionResult(
                success=False,
                action_id="",
                exchange=exchange,
                symbol=symbol,
                side=side,
                requested_size=size,
                error=str(e),
                message=f"Execution error: {e}",
            )
