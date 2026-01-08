"""
Interactive Brokers TWS API Client

Connects to Interactive Brokers Trader Workstation (TWS) or IB Gateway
to fetch real-time market data.
"""

import asyncio
import logging
import math
from typing import Dict, Optional, List
from datetime import datetime

try:
    from ib_insync import IB, Stock, Contract, util
    IB_INSYNC_AVAILABLE = True
except ImportError:
    IB_INSYNC_AVAILABLE = False
    logging.warning("ib_insync not installed. Install with: pip install ib_insync")

logger = logging.getLogger(__name__)


class IBClient:
    """
    Interactive Brokers TWS API client for fetching market data.
    """
    
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,  # TWS paper trading port (7497), live trading (7496)
        client_id: int = 3,
        use_paper_trading: bool = True,
    ):
        """
        Initialize IB client.
        
        :param host: TWS/Gateway host (default: localhost)
        :param port: TWS/Gateway port
                     - Paper trading: 7497 (TWS) or 4002 (Gateway)
                     - Live trading: 7496 (TWS) or 4001 (Gateway)
        :param client_id: Client ID (must be unique, 1-32)
        :param use_paper_trading: Whether to use paper trading account
        """
        if not IB_INSYNC_AVAILABLE:
            raise ImportError(
                "ib_insync is required. Install with: pip install ib_insync"
            )
        
        self.ib = IB()
        self.host = host
        self.port = port
        self.client_id = client_id
        self.use_paper_trading = use_paper_trading
        self.connected = False
        self.subscribed_contracts: Dict[str, Contract] = {}
        self._error_handler_setup = False
        
        # Set up error filtering immediately (before any connections)
        self._setup_error_handler()
    
    def _setup_error_handler(self):
        """Set up error handler to filter expected market data subscription errors."""
        if self._error_handler_setup:
            return
        
        # Store original error handlers
        original_handlers = list(self.ib.errorEvent.handlers) if hasattr(self.ib.errorEvent, 'handlers') else []
        
        def error_handler(reqId, errorCode, errorString, contract):
            # Filter out expected errors that spam the logs
            # Error 354: Market data not subscribed (expected for accounts without subscriptions)
            # Error 300: Can't find EId (often follows error 354)
            # Error 10275: Positions info not available (expected for some accounts)
            # Error 200: No security definition found (symbol doesn't exist on IB, e.g., indices like NDX)
            # Warning 2104, 2106, 2158: Market data farm connection is OK (informational, not actual warnings)
            if errorCode in [354, 300, 10275, 200, 2104, 2106, 2158]:
                # These are expected/informational - don't log them
                return
            
            # Log other errors
            if errorCode >= 1000:  # Warnings (1000+) are usually informational
                logger.debug(f"IB Warning {errorCode}: {errorString}")
            else:  # Errors (< 1000)
                logger.error(f"IB Error {errorCode}, reqId {reqId}: {errorString}")
        
        # Clear existing handlers and add our filtered one
        self.ib.errorEvent.clear()
        self.ib.errorEvent += error_handler
        # Re-add original handlers if any
        for handler in original_handlers:
            if handler != error_handler:
                self.ib.errorEvent += handler
        
        # Suppress ib_insync's internal logging for filtered errors using a logging filter
        # Apply to multiple loggers that might log these errors
        loggers_to_filter = [
            logging.getLogger('ib_insync'),
            logging.getLogger('ib_insync.wrapper'),
            logging.getLogger('ib_insync.client'),
            logger,  # Our own logger
            logging.root,  # Root logger in case errors are logged there
        ]
        
        class IBErrorFilter(logging.Filter):
            """Filter to suppress expected IB errors and position logging."""
            def filter(self, record):
                msg = record.getMessage()
                # Filter out error 354, 300, 10275, and 200 messages (check various formats)
                if any(pattern in msg for pattern in [
                    'Error 354', 'Error 300', 'Error 10275', 'Error 200',
                    'errorCode=354', 'errorCode=300', 'errorCode=10275', 'errorCode=200',
                    'error 354', 'error 300', 'error 10275', 'error 200'
                ]):
                    return False
                # Filter out position logging
                if 'position:' in msg.lower():
                    return False
                return True
        
        # Apply filter to all relevant loggers
        for log in loggers_to_filter:
            # Remove any existing filter of this type
            for f in log.filters[:]:
                if isinstance(f, IBErrorFilter):
                    log.removeFilter(f)
            log.addFilter(IBErrorFilter())
        
        self._error_handler_setup = True
        
    async def connect_with_auto_client_id(self, max_retries: int = 5) -> bool:
        """
        Connect to TWS, automatically trying different client_ids if the preferred one is in use.
        
        :param max_retries: Maximum number of different client_ids to try (default: 5)
        :return: True if connected successfully
        """
        original_client_id = self.client_id
        client_id_error_detected = False
        
        for attempt in range(max_retries):
            client_id_to_try = (original_client_id + attempt) % 32 + 1  # Cycle through 1-32
            
            if attempt > 0:
                logger.info(f"Trying client_id {client_id_to_try} (attempt {attempt + 1}/{max_retries})...")
                self.client_id = client_id_to_try
            
            # Always disconnect first
            try:
                if self.ib.isConnected():
                    self.ib.disconnect()
                    await asyncio.sleep(0.3)
            except Exception:
                pass
            
            try:
                logger.info(f"Connecting to IB TWS/Gateway at {self.host}:{self.port} with clientId {self.client_id}...")
                
                # Try to connect with a timeout
                # If TWS sends error 326 (client_id in use), it will close the connection
                # and connectAsync will timeout, so we treat timeout as potential client_id conflict
                try:
                    await asyncio.wait_for(
                        self.ib.connectAsync(
                            self.host,
                            self.port,
                            clientId=self.client_id
                        ),
                        timeout=4.0  # Timeout to detect if connection fails
                    )
                except asyncio.TimeoutError:
                    # Timeout often means TWS rejected the connection (e.g., error 326)
                    # Check if we're connected - if not, it's likely a client_id issue
                    await asyncio.sleep(0.3)  # Brief wait for any error messages
                    if not self.ib.isConnected():
                        # Treat timeout + not connected as client_id conflict
                        raise Exception("Connection timeout - client ID may be in use")
                
                # Wait a moment to see if TWS rejects the connection
                await asyncio.sleep(0.5)
                
                # Check if we're actually connected
                if not self.ib.isConnected():
                    raise Exception("Connection was rejected by TWS - client ID may be in use")
                
                self.connected = True
                
                # Set up error handler to filter expected market data errors
                self._setup_error_handler()
                
                # Set market data type to LIVE (1) for real-time data
                # Market data types: 1=Live, 2=Frozen, 3=Delayed, 4=Delayed-Frozen
                try:
                    self.ib.reqMarketDataType(1)  # Request live market data
                    logger.info("Market data type set to LIVE (requires market data subscriptions)")
                except Exception as e:
                    logger.warning(f"Could not set market data type: {e}")
                
                if attempt > 0:
                    logger.info(f"Successfully connected using client_id {self.client_id} (original was {original_client_id})")
                else:
                    logger.info("Successfully connected to IB TWS/Gateway")
                
                # Get account info
                accounts = self.ib.accountValues()
                if accounts:
                    logger.info(f"Connected to account(s): {[a.account for a in accounts[:5]]}")
                
                logger.info(
                    "Note: To get live market data, ensure you have market data subscriptions enabled.\n"
                    "Check: Account → Market Data Subscriptions in TWS.\n"
                    "Error 354 means market data is not subscribed for the requested instruments."
                )
                
                return True
                
            except Exception as e:
                error_msg = str(e).lower()
                is_client_id_error = (
                    isinstance(e, asyncio.TimeoutError) or
                    "client id" in error_msg or 
                    "clientid" in error_msg or
                    "error 326" in error_msg or
                    "client id may be in use" in error_msg or
                    "connection timeout" in error_msg
                )
                
                # If it's a client_id error and we have more retries, try next client_id
                if is_client_id_error and attempt < max_retries - 1:
                    logger.warning(f"Client ID {self.client_id} is in use, trying next...")
                    try:
                        if self.ib.isConnected():
                            self.ib.disconnect()
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)  # Wait before trying next client_id
                    continue
                elif is_client_id_error:
                    # Out of retries
                    logger.error(f"All client_ids tried. Client ID {self.client_id} is still in use.")
                    return False
                else:
                    # If it's not a client_id error, raise it
                    raise
        
        # If we get here, all retries failed
        return False
    
    async def connect(self, auto_find_client_id: bool = True) -> bool:
        """
        Connect to TWS or IB Gateway.
        
        Make sure TWS or IB Gateway is running and API is enabled:
        1. In TWS: Configure -> API -> Settings
           - Enable ActiveX and Socket Clients: ✓
           - Socket port: 7497 (paper) or 7496 (live)
           - Read-Only API: Unchecked (if you need to trade)
        
        :param auto_find_client_id: If True, automatically try different client_ids if the preferred one is in use
        :return: True if connected successfully
        """
        if auto_find_client_id:
            return await self.connect_with_auto_client_id()
        
        # Original connect logic (without auto client_id)
        try:
            # Always try to disconnect first to clean up any stale connections
            try:
                if self.ib.isConnected():
                    logger.info("Disconnecting existing connection...")
                    self.ib.disconnect()
                    await asyncio.sleep(0.5)
            except Exception:
                pass
            
            logger.info(f"Connecting to IB TWS/Gateway at {self.host}:{self.port} with clientId {self.client_id}...")
            
            await self.ib.connectAsync(
                self.host,
                self.port,
                clientId=self.client_id
            )
            
            await asyncio.sleep(0.5)
            
            if not self.ib.isConnected():
                raise Exception("Connection was rejected by TWS (client_id may be in use)")
            
            self.connected = True
            
            # Set up error handler to filter expected market data errors
            self._setup_error_handler()
            
            # Set market data type to LIVE (1) for real-time data
            # Market data types: 1=Live, 2=Frozen, 3=Delayed, 4=Delayed-Frozen
            try:
                self.ib.reqMarketDataType(1)  # Request live market data
                logger.info("Market data type set to LIVE (requires market data subscriptions)")
            except Exception as e:
                logger.warning(f"Could not set market data type: {e}")
            
            logger.info("Successfully connected to IB TWS/Gateway")
            
            accounts = self.ib.accountValues()
            if accounts:
                logger.info(f"Connected to account(s): {[a.account for a in accounts[:5]]}")
            
            logger.info(
                "Note: To get live market data, ensure you have market data subscriptions enabled.\n"
                "Check: Account → Market Data Subscriptions in TWS.\n"
                "Error 354 means market data is not subscribed for the requested instruments."
            )
            
            return True
            
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"Failed to connect to IB: {e}")
            
            try:
                if self.ib.isConnected():
                    self.ib.disconnect()
            except Exception:
                pass
            
            if "client id" in error_msg or "clientid" in error_msg:
                logger.error(
                    f"Client ID {self.client_id} is already in use. "
                    f"Set auto_find_client_id=True to automatically try different client_ids."
                )
            else:
                logger.error(
                    "Make sure TWS or IB Gateway is running and API is enabled:\n"
                    "1. TWS -> Configure -> API -> Settings\n"
                    "2. Enable 'Enable ActiveX and Socket Clients'\n"
                    "3. Set correct port (7497 for paper, 7496 for live)"
                )
            self.connected = False
            return False
    
    def disconnect(self):
        """Disconnect from TWS/Gateway."""
        try:
            # Try to disconnect regardless of connection state
            # This helps release the client_id in TWS
            if hasattr(self.ib, 'isConnected') and self.ib.isConnected():
                self.ib.disconnect()
                logger.info("Disconnected from IB TWS/Gateway")
            else:
                # Even if not connected, try to disconnect to clean up
                try:
                    self.ib.disconnect()
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"Error during disconnect (may already be disconnected): {e}")
        finally:
            self.connected = False
    
    def _hip3_symbol_to_ib_contract(self, hip3_symbol: str) -> Optional[Contract]:
        """
        Convert HIP3 symbol to IB Contract.
        
        HIP3 symbols are like: "xyz:AAPL", "xyz:TSLA"
        IB needs: Stock contract with symbol, secType, exchange, currency
        
        :param hip3_symbol: HIP3 symbol (e.g., "xyz:AAPL")
        :return: IB Contract or None
        """
        try:
            # Parse HIP3 symbol: "xyz:AAPL" -> symbol="AAPL", quote="USD"
            parts = hip3_symbol.split(":")
            if len(parts) >= 2:
                symbol = parts[1]  # e.g., "AAPL"
                quote = "USD"
                
                # Create IB Contract object (matching the example format)
                contract = Contract()
                contract.symbol = symbol
                contract.secType = "STK"  # Stock
                contract.exchange = "SMART"  # Smart routing
                contract.currency = quote
                # Note: primaryExchange can be set if known (e.g., "NASDAQ", "NYSE")
                # But SMART routing will find the best exchange automatically
                
                return contract
            else:
                logger.warning(f"Could not parse HIP3 symbol: {hip3_symbol}")
                return None
        except Exception as e:
            logger.error(f"Error converting symbol {hip3_symbol}: {e}")
            return None
    
    async def get_orderbook(self, hip3_symbol: str) -> Optional[Dict]:
        """
        Get orderbook data (best bid and ask) for a symbol using snapshot request.
        
        Uses reqMktData with snapshot=True to get a one-time snapshot instead of
        continuous subscription. This is more efficient and may work with delayed data.
        
        :param hip3_symbol: HIP3 symbol (e.g., "xyz:AAPL")
        :return: Dictionary with best_bid, best_ask, best_bid_qty, best_ask_qty, or None
        """
        if not self.connected:
            logger.error("Not connected to IB")
            return None
        
        try:
            # Helper function to check if value is valid (not None, not nan, and not -1)
            # IB uses -1 as a sentinel value meaning "no data available"
            def is_valid_price(value):
                if value is None:
                    return False
                if isinstance(value, (int, float)):
                    # Check for nan using math.isnan
                    if math.isnan(value):
                        return False
                    # Check for -1 (IB's sentinel for "no data")
                    if value == -1.0 or value == -1:
                        return False
                return True
            
            # Convert Vest symbol to IB contract
            contract = self._hip3_symbol_to_ib_contract(hip3_symbol)
            if not contract:
                return None
            
            ticker = self.ib.reqMktData(contract, "", True, False)  # snapshot=True, empty tick list for bid/ask
            
            # Wait for bid/ask data to arrive using ib_insync's util.waitForUpdate
            # This properly waits for the ticker to be updated with real data
            try:
                # Wait for ticker update (with timeout)
                await util.waitForUpdate(ticker, timeout=5.0)
                # After update, check if we have valid data (not -1)
                # IB might return -1 immediately, so wait a bit more if we only have -1
                if not is_valid_price(ticker.bid) and not is_valid_price(ticker.ask):
                    # Only have -1 or invalid data, wait a bit more
                    await asyncio.sleep(0.5)
            except Exception:
                # If waitForUpdate fails, fall back to polling
                max_wait = 50  # 5 seconds max
                waited = 0
                while waited < max_wait:
                    # Check if we have valid bid or ask data (not None, not nan, and not -1)
                    # Don't break early if we only have -1 (IB's "no data" sentinel)
                    bid_valid = is_valid_price(ticker.bid)
                    ask_valid = is_valid_price(ticker.ask)
                    if bid_valid or ask_valid:
                        break
                    await asyncio.sleep(0.1)
                    waited += 1
            
            # Get best bid and ask, filtering out nan values
            best_bid = ticker.bid if is_valid_price(ticker.bid) else None
            best_ask = ticker.ask if is_valid_price(ticker.ask) else None
            best_bid_size = ticker.bidSize if hasattr(ticker, 'bidSize') and is_valid_price(ticker.bidSize) else None
            best_ask_size = ticker.askSize if hasattr(ticker, 'askSize') and is_valid_price(ticker.askSize) else None
            
            if best_bid is None and best_ask is None:
                # No valid data available - snapshot request may have failed
                # Error 354 is already filtered by error handler, so just return None silently
                # Cancel market data request (even though it's a snapshot, we should clean up)
                try:
                    self.ib.cancelMktData(contract)
                except Exception:
                    pass
                return None
            
            # Double-check: ensure we don't return -1 values (IB's "no data" sentinel)
            if best_bid is not None and (best_bid == -1.0 or best_bid == -1):
                best_bid = None
            if best_ask is not None and (best_ask == -1.0 or best_ask == -1):
                best_ask = None
            
            # If both are invalid after filtering, return None
            if best_bid is None and best_ask is None:
                try:
                    self.ib.cancelMktData(contract)
                except Exception:
                    pass
                return None
            
            result = {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "best_bid_qty": best_bid_size,
                "best_ask_qty": best_ask_size,
            }
            
            # Cancel market data request (cleanup, though snapshot is one-time)
            try:
                self.ib.cancelMktData(contract)
            except Exception:
                pass
            
            return result
            
        except asyncio.TimeoutError:
            logger.warning(f"Timeout waiting for orderbook snapshot for {hip3_symbol}")
            return None
        except Exception as e:
            logger.error(f"Error getting orderbook snapshot for {hip3_symbol}: {e}")
            return None
    
    async def get_orderbooks_batch(
        self,
        hip3_symbols: List[str],
        max_concurrent: int = 10
    ) -> Dict[str, Dict]:
        """
        Get orderbook data (bid/ask) for multiple symbols concurrently.
        
        :param hip3_symbols: List of HIP3 symbols
        :param max_concurrent: Maximum concurrent requests (IB has rate limits)
        :return: Dictionary mapping symbol to {best_bid, best_ask, best_bid_qty, best_ask_qty}
        """
        if not self.connected:
            logger.error("Not connected to IB")
            return {}
        
        logger.info(f"Fetching IB orderbooks for {len(hip3_symbols)} symbols...")
        
        # Use semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def get_orderbook_with_semaphore(symbol):
            async with semaphore:
                orderbook = await self.get_orderbook(symbol)
                return symbol, orderbook
        
        # Fetch all orderbooks concurrently
        tasks = [get_orderbook_with_semaphore(symbol) for symbol in hip3_symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Build result dictionary
        orderbooks = {}
        errors = 0
        no_data = 0
        
        for result in results:
            if isinstance(result, Exception):
                errors += 1
                logger.warning(f"Error in batch fetch: {result}")
                continue
            
            symbol, orderbook = result
            if orderbook is not None:
                # Check if orderbook actually has bid/ask data
                if orderbook.get("best_bid") is not None or orderbook.get("best_ask") is not None:
                    orderbooks[symbol] = orderbook
                else:
                    no_data += 1
            else:
                no_data += 1
        
        logger.info(
            f"IB orderbooks: {len(orderbooks)} with data, {no_data} no data, {errors} errors "
            f"(out of {len(hip3_symbols)} total)"
        )
        
        # Log a sample of successful fetches if any
        if orderbooks:
            sample_count = min(3, len(orderbooks))
            sample_symbols = list(orderbooks.keys())[:sample_count]
            for sym in sample_symbols:
                ob = orderbooks[sym]
                logger.debug(
                    f"Sample {sym}: Bid=${ob.get('best_bid', 'N/A')}, "
                    f"Ask=${ob.get('best_ask', 'N/A')}"
                )
        else:
            logger.warning(
                "No IB orderbook data received. This may indicate:\n"
                "1. Market data subscriptions are not enabled\n"
                "2. Snapshot requests are not working\n"
                "3. Symbols are not available on IB"
            )
        
        return orderbooks
    
    def is_connected(self) -> bool:
        """Check if connected to IB."""
        return self.connected and self.ib.isConnected()


# Example usage and testing
async def test_ib_connection():
    """Test IB connection."""
    client = IBClient(use_paper_trading=True)
    
    try:
        # Connect
        connected = await client.connect()
        if not connected:
            return
        
        # Test getting orderbook for a single symbol
        logger.info("Testing single orderbook fetch...")
        orderbook = await client.get_orderbook("xyz:AAPL")
        if orderbook:
            logger.info(f"AAPL orderbook: Bid=${orderbook['best_bid']:.2f}, Ask=${orderbook['best_ask']:.2f}")
        
        # Test batch fetch
        logger.info("Testing batch orderbook fetch...")
        symbols = ["xyz:AAPL", "xyz:TSLA", "xyz:MSFT"]
        orderbooks = await client.get_orderbooks_batch(symbols)
        for symbol, ob in orderbooks.items():
            logger.info(f"{symbol}: Bid=${ob['best_bid']:.2f}, Ask=${ob['best_ask']:.2f}")
        
    finally:
        client.disconnect()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_ib_connection())


