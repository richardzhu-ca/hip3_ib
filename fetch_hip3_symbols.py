"""
Fetch all HIP3 available symbols from Hyperliquid and store them in a JSON file.

HIP3 symbols on Hyperliquid typically follow the pattern "xyz:SYMBOL" where SYMBOL
is the underlying stock symbol (e.g., "xyz:AAPL", "xyz:TSLA").
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import hyperliquid SDK
try:
    from hyperliquid.info import Info
    from hyperliquid.utils import constants
except ImportError as e:
    import traceback
    print(f"ERROR: Failed to import hyperliquid-python-sdk")
    print(f"Import error: {e}")
    print("\nTraceback:")
    traceback.print_exc()
    print("\nTroubleshooting:")
    print("1. Make sure you're in the virtual environment (you should see '(venv)' in your prompt)")
    print("2. Verify installation: pip show hyperliquid-python-sdk")
    print("3. Try reinstalling: pip install --upgrade hyperliquid-python-sdk")
    sys.exit(1)


# Known non-stock symbols to exclude (crypto, commodities, etc.)
NON_STOCK_SYMBOLS = {"GOLD", "SILVER", "USDJPY", "XYZ100", "JPY", "EUR", "SKHX", "CL"}

def is_stock_symbol(base_symbol: str) -> bool:
    """
    Determine if a symbol is likely a stock symbol.
    
    Stock symbols typically:
    - Are 1-5 characters long
    - Are all uppercase letters (sometimes with numbers)
    - Are not in the known non-stock list
    
    Args:
        base_symbol: The symbol without the "xyz:" prefix
        
    Returns:
        True if likely a stock symbol, False otherwise
    """
    # Check if in known non-stock list
    if base_symbol in NON_STOCK_SYMBOLS:
        return False
    
    return True


def fetch_hip3_symbols() -> List[Dict[str, Any]]:
    """
    Fetch all HIP3 symbols from Hyperliquid using the xyz DEX filter.
    
    Returns:
        List of dictionaries containing symbol information
    """
    # Get metadata from xyz DEX explicitly
    info_xyz = Info(constants.MAINNET_API_URL, skip_ws=True, perp_dexs=["xyz"])
    meta_xyz = info_xyz.meta(dex="xyz")
    
    hip3_symbols = []
    
    # All symbols in universe are HIP3 symbols from the xyz DEX
    universe = meta_xyz.get("universe", [])
    
    for coin_info in universe:
        name = coin_info.get("name", "")
        
        symbol_data = {
            "symbol": name,
            "index": coin_info.get("index"),
            "maxLeverage": coin_info.get("maxLeverage"),
            "onlyIsolated": coin_info.get("onlyIsolated", False),
        }
        hip3_symbols.append(symbol_data)
    
    # Sort by symbol name
    hip3_symbols.sort(key=lambda x: x["symbol"])
    
    return hip3_symbols




async def main():
    """Main function to fetch and save HIP3 symbols."""
    output_file = Path(__file__).parent / "hip3_symbols.json"
    
    logger.info("Fetching HIP3 symbols from Hyperliquid...")
    
    try:
        # Fetch all HIP3 symbols from universe
        all_symbols = fetch_hip3_symbols()
        
        if not all_symbols:
            logger.warning("No HIP3 symbols found!")
            return
        
        logger.info(f"Found {len(all_symbols)} total symbols in xyz DEX")
        
        # Filter by stock symbols
        candidate_symbols = []
        excluded_non_stock = 0
        
        for symbol_data in all_symbols:
            symbol = symbol_data["symbol"]
            
            # Extract base symbol (remove "xyz:" prefix if present)
            if symbol.startswith("xyz:"):
                base_symbol = symbol.replace("xyz:", "")
            else:
                base_symbol = symbol
            
            # Check if it's a stock symbol (not in non-stock list)
            if not is_stock_symbol(base_symbol):
                excluded_non_stock += 1
                continue
            
            candidate_symbols.append(symbol_data)
        
        # Initialize Info client for xyz DEX (for orderbook checks)
        info_xyz = Info(constants.MAINNET_API_URL, skip_ws=True, perp_dexs=["xyz"])
        
        # Check orderbook data (l2_snapshot) asynchronously to verify tradeability
        async def check_orderbook(symbol_data: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
            """Check if symbol has valid orderbook data."""
            symbol = symbol_data["symbol"]
            try:
                # Run l2_snapshot in executor since it's a blocking call
                loop = asyncio.get_event_loop()
                orderbook = await loop.run_in_executor(
                    None, 
                    lambda: info_xyz.l2_snapshot(symbol)
                )
                
                # Check if orderbook has valid bid/ask data
                # Structure: {'coin': 'xyz:AMD', 'time': ..., 'levels': [[bids...], [asks...]]}
                if orderbook and "levels" in orderbook:
                    levels = orderbook["levels"]
                    
                    # levels is a list: [bids_list, asks_list]
                    if isinstance(levels, list) and len(levels) >= 2:
                        bids = levels[0] if isinstance(levels[0], list) else []
                        asks = levels[1] if isinstance(levels[1], list) else []
                        
                        has_bids = len(bids) > 0
                        has_asks = len(asks) > 0
                        
                        if has_bids or has_asks:
                            # Extract best bid/ask if available
                            best_bid = None
                            best_ask = None
                            
                            if has_bids:
                                bid_data = bids[0]
                                if isinstance(bid_data, dict):
                                    best_bid = float(bid_data.get("px", bid_data.get("price", 0)))
                            
                            if has_asks:
                                ask_data = asks[0]
                                if isinstance(ask_data, dict):
                                    best_ask = float(ask_data.get("px", ask_data.get("price", 0)))
                            
                            if best_bid or best_ask:
                                symbol_data["best_bid"] = best_bid
                                symbol_data["best_ask"] = best_ask
                                return symbol_data, True
                
                return symbol_data, False
            except Exception as e:
                return symbol_data, False
        
        # Check all orderbooks concurrently (with semaphore to limit concurrent requests)
        semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests
        
        async def check_with_semaphore(symbol_data):
            async with semaphore:
                return await check_orderbook(symbol_data)
        
        # Run all orderbook checks concurrently
        results = await asyncio.gather(*[
            check_with_semaphore(sym) for sym in candidate_symbols
        ], return_exceptions=True)
        
        # Filter symbols with valid orderbook data
        symbols = []
        excluded_no_orderbook = 0
        
        for result in results:
            if isinstance(result, Exception):
                excluded_no_orderbook += 1
                continue
            
            symbol_data, has_orderbook = result
            if has_orderbook:
                symbols.append(symbol_data)
            else:
                excluded_no_orderbook += 1
        
        # Sort by symbol name
        symbols.sort(key=lambda x: x["symbol"])
        
        if not symbols:
            logger.warning("No tradeable stock symbols with orderbook data found!")
            return
                
        # Create output data structure
        output_data = {
            "last_updated": datetime.now().isoformat(),
            "total_symbols": len(symbols),
            "total_before_filtering": len(all_symbols),
            "filtering_applied": {
                "excluded_non_stock": excluded_non_stock,
                "excluded_no_orderbook": excluded_no_orderbook,
            },
            "symbols": symbols
        }
        
        # Save to JSON file
        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=2)
        
        logger.info(f"Saved {len(symbols)} HIP3 symbols to {output_file}")
        
        # Also create a simple list file for easy access
        simple_list_file = Path(__file__).parent / "hip3_symbols_list.json"
        simple_list = [sym["symbol"] for sym in symbols]
        with open(simple_list_file, "w") as f:
            json.dump(simple_list, f, indent=2)
        
    except Exception as e:
        logger.error(f"Error fetching HIP3 symbols: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

