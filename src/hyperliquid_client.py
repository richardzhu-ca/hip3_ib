"""
Hyperliquid API Client

Connects to Hyperliquid to fetch real-time market data for HIP3.
"""

import asyncio
import logging
from typing import Dict, Optional

try:
    from hyperliquid.info import Info
    from hyperliquid.utils import constants
    HYPERLIQUID_AVAILABLE = True
except ImportError:
    HYPERLIQUID_AVAILABLE = False
    logging.warning("hyperliquid-python-sdk not installed. Install with: pip install hyperliquid-python-sdk")

logger = logging.getLogger(__name__)


class HyperliquidClient:
    """
    Hyperliquid API client for fetching market data.
    """
    
    def __init__(self, base_url: Optional[str] = None):
        """
        Initialize Hyperliquid client.
        
        :param base_url: Base URL for Hyperliquid API (default: mainnet)
        """
        if not HYPERLIQUID_AVAILABLE:
            raise ImportError(
                "hyperliquid-python-sdk is required. Install with: pip install hyperliquid-python-sdk"
            )
        
        # Use mainnet by default, or custom URL if provided
        if base_url is None:
            base_url = constants.MAINNET_API_URL
        
        self.info = Info(base_url, skip_ws=True, perp_dexs=["xyz"])
        self.connected = True  # Hyperliquid Info API doesn't require explicit connection
    
    async def get_price(self, symbol: str) -> Optional[float]:
        """
        Get the current mark price for a symbol.
        
        :param symbol: Symbol name (e.g., "HIP3")
        :return: Current mark price or None if not available
        """
        try:
            # Get all market data
            meta = self.info.meta()
            
            # Find the symbol in the universe
            for coin_info in meta.get("universe", []):
                if coin_info.get("name") == symbol:
                    # Get the latest price from the market data
                    market_data = self.info.all_mids()
                    if market_data and symbol in market_data:
                        return float(market_data[symbol])
            
            logger.warning(f"Symbol {symbol} not found in Hyperliquid universe")
            return None
            
        except Exception as e:
            logger.error(f"Error getting price for {symbol}: {e}")
            return None
    
    async def get_orderbook(self, symbol: str) -> Optional[Dict]:
        """
        Get orderbook data (best bid and ask) for a symbol.
        
        :param symbol: Symbol name (e.g., "xyz:AAPL")
        :return: Dictionary with best_bid, best_ask, best_bid_qty, best_ask_qty, or None
        """
        try:
            # Get orderbook snapshot
            # Structure: {'coin': 'xyz:AAPL', 'time': ..., 'levels': [[bids...], [asks...]]}
            orderbook = self.info.l2_snapshot(symbol)
            
            if not orderbook or "levels" not in orderbook:
                return None
            
            levels = orderbook["levels"]
            
            # levels is a list: [bids_list, asks_list]
            if not isinstance(levels, list) or len(levels) < 2:
                return None
            
            bids = levels[0] if isinstance(levels[0], list) else []
            asks = levels[1] if isinstance(levels[1], list) else []
            
            # Extract best bid and ask
            best_bid = None
            best_bid_qty = None
            best_ask = None
            best_ask_qty = None
            
            if len(bids) > 0:
                best_bid_data = bids[0]
                if isinstance(best_bid_data, dict):
                    best_bid = float(best_bid_data.get("px", 0))
                    best_bid_qty = float(best_bid_data.get("sz", 0))
            
            if len(asks) > 0:
                best_ask_data = asks[0]
                if isinstance(best_ask_data, dict):
                    best_ask = float(best_ask_data.get("px", 0))
                    best_ask_qty = float(best_ask_data.get("sz", 0))
            
            if best_bid is None and best_ask is None:
                return None
            
            return {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "best_bid_qty": best_bid_qty,
                "best_ask_qty": best_ask_qty,
            }
            
        except Exception as e:
            logger.error(f"Error getting orderbook for {symbol}: {e}")
            return None
    
    def is_connected(self) -> bool:
        """Check if client is ready (Hyperliquid Info API is always available)."""
        return self.connected


# Example usage and testing
async def test_hyperliquid_connection():
    """Test Hyperliquid connection."""
    client = HyperliquidClient()
    
    try:
        # Test getting price for HIP3
        logger.info("Testing HIP3 price fetch...")
        price = await client.get_price("HIP3")
        if price:
            logger.info(f"HIP3 price: ${price:.2f}")
        
        # Test getting orderbook
        logger.info("Testing HIP3 orderbook fetch...")
        orderbook = await client.get_orderbook("HIP3")
        if orderbook:
            logger.info(
                f"HIP3 orderbook: Bid=${orderbook['best_bid']:.2f}, "
                f"Ask=${orderbook['best_ask']:.2f}"
            )
        
    except Exception as e:
        logger.error(f"Test failed: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_hyperliquid_connection())

