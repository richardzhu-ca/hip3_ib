"""
Mispricing Strategy - Monitor prices between Hyperliquid (HIP3) and Interactive Brokers.

Detects price differences and executes trades on Hyperliquid when mispricing is detected.
"""

import asyncio
import logging
import argparse
import os
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Add src and execution directories to path for imports
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "execution"))

from hyperliquid_client import HyperliquidClient
from ib import IBClient

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import discord
try:
    import discord
    DISCORD_AVAILABLE = True
except ImportError:
    DISCORD_AVAILABLE = False

# Try to import PQuant backend
try:
    from execution.pquant import PQuantBackend
    PQUANT_AVAILABLE = True
except ImportError:
    PQUANT_AVAILABLE = False
    logger.warning("PQuant backend not available")


class MispricingStrategy:
    """
    Strategy to find mispricing opportunities between Hyperliquid HIP3 and Interactive Brokers.
    Executes trades only on Hyperliquid when mispricing is detected.
    
    Filters out market-wide moves by adjusting for NASDAQ index movements.
    """
    
    # NASDAQ index symbols
    NASDAQ_HL_SYMBOL = "xyz:XYZ100"
    NASDAQ_IB_SYMBOL = "NDX-USD-PERP"
    
    # Trading parameters
    NOTIONAL_VALUE_USD = 200.0  # Fixed notional value per trade
    
    def __init__(
        self,
        hyperliquid_client: HyperliquidClient,
        ib_client: Optional[IBClient] = None,
        execution_backend: Optional['PQuantBackend'] = None,
        min_price_diff_percent: float = 2.0,
        discord_config: Optional[Dict[str, str]] = None,
        enable_trading: bool = False,
    ):
        """
        Initialize mispricing strategy.
        
        :param hyperliquid_client: HyperliquidClient instance
        :param ib_client: Interactive Brokers client
        :param execution_backend: PQuantBackend for order execution (optional)
        :param min_price_diff_percent: Minimum price difference percentage to consider (default 2.0%)
        :param discord_config: Optional dict with 'token' and 'channel_id' for Discord notifications
        :param enable_trading: Whether to enable actual trading (default False for monitoring only)
        """
        self.hyperliquid_client = hyperliquid_client
        self.ib_client = ib_client
        self.execution_backend = execution_backend
        self.hip3_symbols: List[str] = []
        self.running = False
        self.min_price_diff_percent = min_price_diff_percent
        self.discord_config = discord_config
        self.discord_client: Optional[discord.Client] = None
        self.discord_channel: Optional[discord.TextChannel] = None
        self.enable_trading = enable_trading
        
        # Data cache with timestamps
        self.cache = {
            'hyperliquid': {'data': {}, 'timestamp': None},
            'ib': {'data': {}, 'timestamp': None}
        }
        
    def load_hip3_symbols(self) -> List[str]:
        """
        Load HIP3 symbols from hip3_symbols_list.json.
        Ensures NASDAQ index symbol is included for market adjustment.
        
        :return: List of HIP3 symbol names
        """
        symbols_file = Path(__file__).parent.parent / "hip3_symbols_list.json"
        
        if not symbols_file.exists():
            logger.warning(f"HIP3 symbols file not found: {symbols_file}")
            logger.info("Run fetch_hip3_symbols.py to generate the symbols list")
            return []
        
        try:
            with open(symbols_file, 'r') as f:
                symbols = json.load(f)
            
            # Ensure NASDAQ index is in the list for market adjustment
            if self.NASDAQ_HL_SYMBOL not in symbols:
                symbols.append(self.NASDAQ_HL_SYMBOL)
                logger.info(f"Added NASDAQ index symbol {self.NASDAQ_HL_SYMBOL} for market adjustment")
            
            logger.info(f"Loaded {len(symbols)} HIP3 symbols from {symbols_file}")
            return symbols
        except Exception as e:
            logger.error(f"Error loading HIP3 symbols: {e}")
            return []
    
    async def fetch_hyperliquid_orderbooks(self, symbols: List[str]) -> Dict[str, Dict]:
        """
        Fetch orderbook data from Hyperliquid for multiple symbols.
        
        :param symbols: List of symbols to fetch
        :return: Dictionary mapping symbol to {best_bid, best_ask, best_bid_qty, best_ask_qty}
        """
        logger.info(f"Fetching Hyperliquid orderbooks for {len(symbols)} symbols...")
        
        # Fetch all orderbooks concurrently
        tasks = [
            self.hyperliquid_client.get_orderbook(symbol)
            for symbol in symbols
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        orderbooks = {}
        for symbol, result in zip(symbols, results):
            if isinstance(result, Exception):
                logger.warning(f"Error fetching {symbol}: {result}")
                continue
            
            if result is None:
                logger.debug(f"No orderbook data for {symbol}")
                continue
            
            orderbooks[symbol] = result
        
        logger.info(f"Successfully fetched Hyperliquid orderbooks for {len(orderbooks)} symbols")
        return orderbooks
    
    async def fetch_ib_orderbooks(self, symbols: List[str]) -> Dict[str, Dict]:
        """
        Fetch orderbook data (bid/ask) from Interactive Brokers TWS API.
        
        :param symbols: List of symbols to fetch
        :return: Dictionary mapping symbol to {best_bid, best_ask, best_bid_qty, best_ask_qty}
        """
        if not self.ib_client:
            logger.warning("IB client not initialized - returning empty orderbooks")
            return {}
        
        if not self.ib_client.is_connected():
            logger.warning("IB client not connected - attempting to reconnect...")
            connected = await self.ib_client.connect()
            if not connected:
                logger.error("Failed to connect to IB - returning empty orderbooks")
                return {}
        
        # Fetch orderbooks using batch method
        orderbooks = await self.ib_client.get_orderbooks_batch(symbols, max_concurrent=10)
        return orderbooks
    
    def calculate_mispricing_opportunities(
        self,
        hyperliquid_orderbooks: Dict[str, Dict],
        ib_orderbooks: Dict[str, Dict],
    ) -> List[Dict]:
        """
        Compare Hyperliquid and IB orderbooks to find mispricing opportunities.
        Adjusts for market-wide moves by comparing to NASDAQ index.
        
        Adjustment Logic:
        1. Calculate stock_gap = (HL_stock - IB_stock) / IB_stock
        2. Calculate index_gap = (HL_NASDAQ - IB_NASDAQ) / IB_NASDAQ
        3. adjusted_gap = stock_gap - index_gap
        
        This filters out market-wide moves (e.g., during pre-market when IB is stale).
        
        Mispricing Logic (using actual executable prices):
        1. If IB_best_bid > Hyperliquid_best_ask + threshold:
           - Action: BUY on Hyperliquid at ask price
           
        2. If IB_best_ask < Hyperliquid_best_bid - threshold:
           - Action: SELL on Hyperliquid at bid price
        
        :param hyperliquid_orderbooks: Hyperliquid orderbooks
        :param ib_orderbooks: IB orderbooks
        :return: List of mispricing opportunities (adjusted for market moves)
        """
        opportunities = []
        
        # First, calculate the NASDAQ index gap for market adjustment
        index_gap_percent = 0.0
        nasdaq_hl_data = hyperliquid_orderbooks.get(self.NASDAQ_HL_SYMBOL)
        nasdaq_ib_data = ib_orderbooks.get("xyz:NDX")
        
        if nasdaq_hl_data and nasdaq_ib_data:
            nasdaq_hl_bid = nasdaq_hl_data.get("best_bid")
            nasdaq_hl_ask = nasdaq_hl_data.get("best_ask")
            nasdaq_ib_bid = nasdaq_ib_data.get("best_bid")
            nasdaq_ib_ask = nasdaq_ib_data.get("best_ask")
            
            if all([nasdaq_hl_bid, nasdaq_hl_ask, nasdaq_ib_bid, nasdaq_ib_ask]):
                # Use mid prices for index comparison
                nasdaq_hl_mid = (nasdaq_hl_bid + nasdaq_hl_ask) / 2
                nasdaq_ib_mid = (nasdaq_ib_bid + nasdaq_ib_ask) / 2
                index_gap_percent = ((nasdaq_hl_mid - nasdaq_ib_mid) / nasdaq_ib_mid) * 100
                logger.info(f"📊 NASDAQ index gap: {index_gap_percent:.2f}% (HL: ${nasdaq_hl_mid:.2f}, IB: ${nasdaq_ib_mid:.2f})")
        else:
            logger.warning("⚠️  NASDAQ index data not available - cannot adjust for market moves")
        
        # Now check each stock and adjust for index movement
        for symbol, hl_data in hyperliquid_orderbooks.items():
            # Skip the index itself
            if symbol == self.NASDAQ_HL_SYMBOL:
                continue
            
            if symbol not in ib_orderbooks:
                continue
            
            ib_data = ib_orderbooks[symbol]
            hl_bid = hl_data.get("best_bid")
            hl_ask = hl_data.get("best_ask")
            ib_bid = ib_data.get("best_bid")
            ib_ask = ib_data.get("best_ask")
            
            if hl_bid is None or hl_ask is None:
                continue
            if ib_bid is None or ib_ask is None:
                continue
            
            # Calculate stock-specific gap using mid prices for comparison
            hl_mid = (hl_bid + hl_ask) / 2
            ib_mid = (ib_bid + ib_ask) / 2
            stock_gap_percent = ((hl_mid - ib_mid) / ib_mid) * 100
            
            # Adjust for market-wide movement
            adjusted_gap_percent = stock_gap_percent - index_gap_percent
            
            # Opportunity 1: IB best bid > HL best ask (HL underpriced)
            # But adjusted for market moves
            if ib_bid > hl_ask:
                diff_absolute = ib_bid - hl_ask
                diff_percent = (diff_absolute / hl_ask) * 100
                
                # Use adjusted gap to filter
                if adjusted_gap_percent >= self.min_price_diff_percent:
                    opportunities.append({
                        "symbol": symbol,
                        "type": "BUY_HL",
                        "hl_bid": hl_bid,
                        "hl_ask": hl_ask,
                        "ib_bid": ib_bid,
                        "ib_ask": ib_ask,
                        "diff_absolute": diff_absolute,
                        "diff_percent": diff_percent,
                        "stock_gap_percent": stock_gap_percent,
                        "index_gap_percent": index_gap_percent,
                        "adjusted_gap_percent": adjusted_gap_percent,
                        "action": f"BUY on Hyperliquid at ${hl_ask:.2f} (IB bid: ${ib_bid:.2f})",
                        "execute_price": hl_ask,
                        "hl_ask_qty": hl_data.get("best_ask_qty"),
                        "ib_bid_qty": ib_data.get("best_bid_qty"),
                    })
            
            # Opportunity 2: IB best ask < HL best bid (HL overpriced)
            # But adjusted for market moves
            elif ib_ask < hl_bid:
                diff_absolute = hl_bid - ib_ask
                diff_percent = (diff_absolute / ib_ask) * 100
                
                # Use adjusted gap to filter (negative for SELL)
                if adjusted_gap_percent <= -self.min_price_diff_percent:
                    opportunities.append({
                        "symbol": symbol,
                        "type": "SELL_HL",
                        "hl_bid": hl_bid,
                        "hl_ask": hl_ask,
                        "ib_bid": ib_bid,
                        "ib_ask": ib_ask,
                        "diff_absolute": diff_absolute,
                        "diff_percent": diff_percent,
                        "stock_gap_percent": stock_gap_percent,
                        "index_gap_percent": index_gap_percent,
                        "adjusted_gap_percent": adjusted_gap_percent,
                        "action": f"SELL on Hyperliquid at ${hl_bid:.2f} (IB ask: ${ib_ask:.2f})",
                        "execute_price": hl_bid,
                        "hl_bid_qty": hl_data.get("best_bid_qty"),
                        "ib_ask_qty": ib_data.get("best_ask_qty"),
                    })
        
        # Sort by absolute adjusted gap (highest first)
        opportunities.sort(key=lambda x: abs(x["adjusted_gap_percent"]), reverse=True)
        return opportunities
    
    async def _init_discord(self) -> bool:
        """
        Initialize Discord client and connect to channel.
        
        :return: True if successfully initialized, False otherwise
        """
        if not DISCORD_AVAILABLE:
            logger.debug("Discord not available (discord.py not installed)")
            return False
        
        if not self.discord_config:
            logger.debug("Discord config not provided")
            return False
        
        token = self.discord_config.get("token")
        channel_id_str = self.discord_config.get("channel_id")
        
        if not token or not channel_id_str:
            logger.debug("Discord token or channel_id missing from config")
            return False
        
        try:
            channel_id = int(channel_id_str)
        except ValueError:
            logger.warning(f"Invalid Discord channel_id: {channel_id_str}")
            return False
        
        try:
            intents = discord.Intents.default()
            intents.message_content = True
            self.discord_client = discord.Client(intents=intents)
            
            connection_ready = asyncio.Event()
            
            @self.discord_client.event
            async def on_ready():
                channel = self.discord_client.get_channel(channel_id)
                if channel is None:
                    logger.warning(f"Discord channel {channel_id} not found")
                    await self.discord_client.close()
                    self.discord_client = None
                    connection_ready.set()
                    return
                self.discord_channel = channel
                logger.info(f"Discord bot connected: {self.discord_client.user}")
                logger.info(f"Discord channel: {channel.name}")
                connection_ready.set()
            
            # Start connection in background task
            asyncio.create_task(self.discord_client.start(token))
            
            # Wait for connection to establish (with timeout)
            try:
                await asyncio.wait_for(connection_ready.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("Discord connection timeout")
                return False
            
            if self.discord_channel is None:
                logger.warning("Discord channel not available after connection")
                return False
            
            return True
            
        except discord.LoginFailure:
            logger.warning("Discord bot token invalid")
            return False
        except Exception as e:
            logger.warning(f"Failed to initialize Discord: {e}")
            return False
    
    async def _send_discord_message(self, message: str) -> bool:
        """
        Send a message to Discord channel.
        
        :param message: Message content to send
        :return: True if sent successfully, False otherwise
        """
        if not self.discord_client or not self.discord_channel:
            return False
        
        try:
            await self.discord_channel.send(message)
            return True
        except Exception as e:
            logger.warning(f"Failed to send Discord message: {e}")
            return False
    
    async def _send_opportunities_to_discord(
        self,
        opportunities: List[Dict],
        hl_cached: bool = False,
        ib_cached: bool = False
    ):
        """
        Format and send mispricing opportunities to Discord.
        
        :param opportunities: List of mispricing opportunity dictionaries
        :param hl_cached: Whether Hyperliquid data is using cached data
        :param ib_cached: Whether IB data is using cached data
        """
        if not opportunities:
            return
        
        if not self.discord_client or not self.discord_channel:
            return
        
        # Build header message with cache status
        header_parts = [
            f"🔔 **{len(opportunities)} Mispricing Opportunity{'ies' if len(opportunities) != 1 else ''} Found**",
            f"Threshold: {self.min_price_diff_percent}%",
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Trading: {'ENABLED ✅' if self.enable_trading else 'DISABLED (monitoring only)'}",
        ]
        
        # Add cache status warnings
        cache_warnings = []
        if hl_cached and self.cache['hyperliquid']['timestamp']:
            hl_age = datetime.now() - self.cache['hyperliquid']['timestamp']
            cache_warnings.append(
                f"⚠️ **Hyperliquid**: Using cached data from {self.cache['hyperliquid']['timestamp'].strftime('%H:%M:%S')} "
                f"({hl_age.total_seconds() / 60:.0f} minutes old)"
            )
        if ib_cached and self.cache['ib']['timestamp']:
            ib_age = datetime.now() - self.cache['ib']['timestamp']
            cache_warnings.append(
                f"⚠️ **IB**: Using cached data from {self.cache['ib']['timestamp'].strftime('%H:%M:%S')} "
                f"({ib_age.total_seconds() / 60:.0f} minutes old)"
            )
        
        if cache_warnings:
            header_parts.append("")
            header_parts.extend(cache_warnings)
        
        header = "\n".join(header_parts)
        await self._send_discord_message(header)
        
        # Send opportunities
        DISCORD_MAX_LENGTH = 1900
        current_message_parts = []
        current_length = 0
        
        for opp in opportunities:
            emoji = "📈" if opp['type'] == "BUY_HL" else "📉"
            opp_text = (
                f"\n{emoji} **{opp['symbol']}** - {abs(opp['adjusted_gap_percent']):.2f}% adjusted gap\n"
                f"   {opp['type']} - {opp['action']}\n"
                f"   Hyperliquid: ${opp['hl_bid']:.2f} / ${opp['hl_ask']:.2f}\n"
                f"   IB:          ${opp['ib_bid']:.2f} / ${opp['ib_ask']:.2f}\n"
                f"   📊 Stock gap: {opp['stock_gap_percent']:.2f}%, Index gap: {opp['index_gap_percent']:.2f}%\n"
                f"   ⭐ **Adjusted gap: {opp['adjusted_gap_percent']:.2f}%**"
            )
            
            opp_length = len(opp_text)
            
            if current_length + opp_length > DISCORD_MAX_LENGTH and current_message_parts:
                message = "\n".join(current_message_parts)
                await self._send_discord_message(message)
                current_message_parts = []
                current_length = 0
            
            current_message_parts.append(opp_text)
            current_length += opp_length
        
        if current_message_parts:
            message = "\n".join(current_message_parts)
            await self._send_discord_message(message)
    
    async def execute_trade(self, opportunity: Dict, order_value: float) -> bool:
        """
        Execute a trade on Hyperliquid based on the opportunity.
        Uses fixed notional value of $200 USD and calculates order size.
        
        :param opportunity: Opportunity dictionary with symbol, type, execute_price, etc.
        :return: True if trade executed successfully, False otherwise
        """
        if not self.execution_backend:
            logger.error("No execution backend configured - cannot execute trade")
            return False
        
        if not self.execution_backend.is_connected:
            logger.error("Execution backend not connected")
            return False
        
        symbol = opportunity["symbol"]
        trade_type = opportunity["type"]
        execute_price = opportunity["execute_price"]
        
        # Calculate order size from notional value
        # order_size = notional_value / price
        order_size = int(order_value / execute_price)
        
        # Determine side: BUY_HL -> buy, SELL_HL -> sell
        side = "buy" if trade_type == "BUY_HL" else "sell"
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🔥 EXECUTING TRADE: {side.upper()} {symbol}")
        logger.info(f"   Notional value: ${self.NOTIONAL_VALUE_USD:.2f}")
        logger.info(f"   Execute price: ${execute_price:.2f}")
        logger.info(f"   Order size: {order_size:.4f}")
        logger.info(f"   Adjusted gap: {opportunity['adjusted_gap_percent']:.2f}%")
        logger.info(f"{'='*60}")
        
        try:
            # Execute order via PQuant backend
            result = await self.execution_backend.execute_order(
                exchange="hyperliquid",
                symbol=symbol,
                side=side,
                size=order_size,
                reduce_only=False,
                instrument_type="perp",
            )
            
            if result.success:
                logger.info(f"✅ Trade executed successfully!")
                logger.info(f"   Filled: {result.filled_amount} @ ${result.avg_price:.2f}")
                logger.info(f"   Notional: ${result.filled_amount * result.avg_price:.2f}")
                logger.info(f"   Orders: {result.order_count}")
                logger.info(f"   Action ID: {result.action_id}")
                
                # Send to Discord
                if self.discord_client and self.discord_channel:
                    emoji = "📈" if side == "buy" else "📉"
                    discord_msg = (
                        f"{emoji} **TRADE EXECUTED** {emoji}\n"
                        f"Symbol: **{symbol}**\n"
                        f"Side: **{side.upper()}**\n"
                        f"Size: {result.filled_amount:.4f}\n"
                        f"Avg Price: ${result.avg_price:.2f}\n"
                        f"Notional: ${result.filled_amount * result.avg_price:.2f}\n"
                        f"Orders: {result.order_count}\n"
                        f"Adjusted Gap: {opportunity['adjusted_gap_percent']:.2f}%"
                    )
                    await self._send_discord_message(discord_msg)
                
                return True
            else:
                logger.error(f"❌ Trade failed: {result.message}")
                if result.error:
                    logger.error(f"   Error: {result.error}")
                
                # Send failure to Discord
                if self.discord_client and self.discord_channel:
                    discord_msg = (
                        f"❌ **TRADE FAILED**\n"
                        f"Symbol: {symbol}\n"
                        f"Side: {side.upper()}\n"
                        f"Size: {order_size:.4f}\n"
                        f"Error: {result.message}"
                    )
                    await self._send_discord_message(discord_msg)
                
                return False
                
        except Exception as e:
            logger.error(f"❌ Exception during trade execution: {e}", exc_info=True)
            
            # Send exception to Discord
            if self.discord_client and self.discord_channel:
                discord_msg = (
                    f"❌ **TRADE EXCEPTION**\n"
                    f"Symbol: {symbol}\n"
                    f"Side: {side.upper()}\n"
                    f"Error: {str(e)}"
                )
                await self._send_discord_message(discord_msg)
            
            return False
    
    async def run_cycle(self):
        """Run one cycle of price fetching and comparison."""
        logger.info(f"\n{'='*80}")
        logger.info(f"Running mispricing check at {datetime.now().isoformat()}")
        logger.info(f"{'='*80}")
        
        # Track which data sources are using cache
        hl_cached = False
        ib_cached = False
        
        # Fetch Hyperliquid orderbooks
        hl_orderbooks = await self.fetch_hyperliquid_orderbooks(self.hip3_symbols)
        
        if hl_orderbooks:
            self.cache['hyperliquid']['data'] = hl_orderbooks
            self.cache['hyperliquid']['timestamp'] = datetime.now()
            logger.info(f"✅ Updated Hyperliquid cache with {len(hl_orderbooks)} symbols")
        else:
            if self.cache['hyperliquid']['data']:
                hl_orderbooks = self.cache['hyperliquid']['data']
                hl_cached = True
                cache_age = datetime.now() - self.cache['hyperliquid']['timestamp']
                logger.warning(
                    f"⚠️  Using cached Hyperliquid data "
                    f"({cache_age.total_seconds() / 60:.0f} minutes old)"
                )
            else:
                logger.error("No Hyperliquid data available")
                return
        
        # Fetch IB orderbooks
        ib_orderbooks = await self.fetch_ib_orderbooks(self.hip3_symbols)
        
        if ib_orderbooks:
            self.cache['ib']['data'] = ib_orderbooks
            self.cache['ib']['timestamp'] = datetime.now()
            logger.info(f"✅ Updated IB cache with {len(ib_orderbooks)} symbols")
        else:
            if self.cache['ib']['data']:
                ib_orderbooks = self.cache['ib']['data']
                ib_cached = True
                cache_age = datetime.now() - self.cache['ib']['timestamp']
                logger.warning(
                    f"⚠️  Using cached IB data "
                    f"({cache_age.total_seconds() / 60:.0f} minutes old)"
                )
            else:
                logger.warning("No IB data available - skipping mispricing calculation")
                return
        
        # Calculate mispricing opportunities
        opportunities = self.calculate_mispricing_opportunities(hl_orderbooks, ib_orderbooks)
        
        # Log results
        if opportunities:
            logger.info(
                f"\nFound {len(opportunities)} mispricing opportunities "
                f"(threshold: {self.min_price_diff_percent}%):"
            )
            for opp in opportunities:
                logger.info(f"\n{opp['symbol']}: {opp['type']}")
                logger.info(f"  Hyperliquid: Bid=${opp['hl_bid']:.2f}, Ask=${opp['hl_ask']:.2f}")
                logger.info(f"  IB:          Bid=${opp['ib_bid']:.2f}, Ask=${opp['ib_ask']:.2f}")
                logger.info(f"  Price diff: ${opp['diff_absolute']:.2f} ({opp['diff_percent']:.2f}%)")
                logger.info(f"  Stock gap: {opp['stock_gap_percent']:.2f}%, Index gap: {opp['index_gap_percent']:.2f}%")
                logger.info(f"  Adjusted gap: {opp['adjusted_gap_percent']:.2f}% ⭐")
                logger.info(f"  Action: {opp['action']}")
            
            # Send to Discord if configured
            await self._send_opportunities_to_discord(opportunities, hl_cached, ib_cached)
            
            # Execute trades if enabled
            if self.enable_trading:
                logger.info(f"\n{'='*80}")
                logger.info("🔥 TRADING ENABLED - Executing trades...")
                logger.info(f"{'='*80}")
                order_value = int(self.NOTIONAL_VALUE_USD/len(opportunities))
                for opp in opportunities:
                    success = await self.execute_trade(opp, order_value)
                    if success:
                        logger.info(f"✅ Successfully traded {opp['symbol']}")
                    else:
                        logger.error(f"❌ Failed to trade {opp['symbol']}")
                    
                    # Wait between trades to avoid rate limiting
                    if len(opportunities) > 1:
                        await asyncio.sleep(2)
            else:
                logger.info("\nℹ️  Trading is DISABLED (monitoring only)")
        else:
            logger.info(
                f"No mispricing opportunities found "
                f"(threshold: {self.min_price_diff_percent}%)"
            )
    
    async def start(self, interval_seconds: int = 10):
        """
        Start the mispricing strategy, running cycles every interval_seconds.
        
        :param interval_seconds: Interval between cycles in seconds (default 10)
        """
        logger.info("Starting mispricing strategy...")
        
        # Initialize Discord if configured
        if self.discord_config:
            logger.info("Initializing Discord integration...")
            discord_ready = await self._init_discord()
            if discord_ready:
                logger.info("✅ Discord integration ready")
            else:
                logger.warning("⚠️  Discord integration failed, continuing without notifications")
        
        # Load HIP3 symbols
        self.hip3_symbols = self.load_hip3_symbols()
        if not self.hip3_symbols:
            logger.error("No HIP3 symbols found - exiting")
            return
        
        logger.info(f"Monitoring {len(self.hip3_symbols)} HIP3 symbols")
        logger.info(f"Running cycles every {interval_seconds} seconds")
        logger.info(f"Notional value per trade: ${self.NOTIONAL_VALUE_USD:.2f}")
        logger.info(f"Trading: {'ENABLED ✅' if self.enable_trading else 'DISABLED (monitoring only)'}")
        
        if self.enable_trading and not self.execution_backend:
            logger.error("⚠️  Trading is ENABLED but no execution backend configured!")
            logger.error("     Continuing in monitoring mode only")
            self.enable_trading = False
        
        self.running = True
        
        # Run initial cycle
        await self.run_cycle()
        
        # Run cycles periodically
        while self.running:
            await asyncio.sleep(interval_seconds)
            if self.running:
                await self.run_cycle()
    
    def stop(self):
        """Stop the mispricing strategy."""
        logger.info("Stopping mispricing strategy...")
        self.running = False
    
    async def cleanup(self):
        """Clean up resources, including Discord connection."""
        if self.discord_client:
            try:
                await self.discord_client.close()
                logger.info("Discord client disconnected")
            except Exception as e:
                logger.warning(f"Error closing Discord client: {e}")


async def main():
    """Main entry point."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="HIP3-IB Mispricing Strategy - Monitor price differences and trade on Hyperliquid",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                            # Use defaults (2% threshold, 10s interval, monitoring only)
  python main.py --threshold 3.0            # 3% threshold
  python main.py --interval 30              # 30s interval
  python main.py --enable-trading           # Enable actual trading (USE WITH CAUTION!)
  python main.py --threshold 2.5 --interval 15 --enable-trading
        """
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=2.0,
        help="Minimum price difference percentage to consider (default: 2.0%%)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Interval between cycles in seconds (default: 10)"
    )
    parser.add_argument(
        "--enable-trading",
        action="store_true",
        help="Enable actual trading (default: False, monitoring only)"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.threshold <= 0:
        parser.error("--threshold must be greater than 0")
    if args.interval < 1:
        parser.error("--interval must be at least 1 second")
    
    logger.info(f"Starting with threshold: {args.threshold}%%, interval: {args.interval}s")
    logger.info(f"Trading: {'ENABLED ✅' if args.enable_trading else 'DISABLED (monitoring only)'}")
    
    # Load Discord config from environment
    discord_config = None
    discord_token = os.getenv("DISCORD_TOKEN")
    discord_channel_id = os.getenv("DISCORD_CHANNEL_ID")
    
    if discord_token and discord_channel_id:
        discord_config = {
            "token": discord_token,
            "channel_id": discord_channel_id,
        }
        logger.info("Discord notifications enabled")
    else:
        logger.info("Discord notifications disabled (set DISCORD_TOKEN and DISCORD_CHANNEL_ID in .env to enable)")
    
    # Initialize Hyperliquid client
    hyperliquid_client = HyperliquidClient()
    
    # Initialize execution backend if trading is enabled
    execution_backend = None
    if args.enable_trading:
        if not PQUANT_AVAILABLE:
            logger.error("PQuant backend not available - install required packages")
            logger.error("Continuing in monitoring mode only")
            args.enable_trading = False
        else:
            logger.info("Initializing execution backend (PQuant)...")
            
            # Get PQuant config from environment
            pquant_server = os.getenv("PQUANT_SERVER", "")
            hyperliquid_wallet = os.getenv("HYPERLIQUID_WALLET")
            hyperliquid_private_key = os.getenv("HYPERLIQUID_PRIVATE_KEY")
            
            if not hyperliquid_wallet or not hyperliquid_private_key:
                logger.error("HYPERLIQUID_WALLET and HYPERLIQUID_PRIVATE_KEY must be set in .env for trading")
                logger.error("Continuing in monitoring mode only")
                args.enable_trading = False
            else:
                execution_backend = PQuantBackend(
                    server_addr=pquant_server,
                    credentials={
                        "hyperliquid": {
                            "api_key": hyperliquid_wallet,
                            "api_secret": hyperliquid_private_key,
                        }
                    },
                    strategy_name="hip3_ib_mispricing",
                    use_dca=True,
                    default_dca_size=0.01,  # Will be overridden per trade
                    timeout_seconds=60,
                    best_effort_completion=True,
                )
                
                # Connect to PQuant server
                connected = await execution_backend.connect()
                if not connected:
                    logger.error("Failed to connect to PQuant server - continuing in monitoring mode only")
                    args.enable_trading = False
                    execution_backend = None
                else:
                    logger.info("✅ Execution backend connected")
    
    # Initialize IB client
    ib_client = IBClient(
        host="127.0.0.1",
        port=7496,  # TWS port (7496 for live, 7497 for paper trading)
        client_id=3,
        use_paper_trading=True
    )
    
    strategy = None
    try:
        # Connect to IB
        logger.info("Connecting to Interactive Brokers...")
        ib_connected = await ib_client.connect()
        if not ib_connected:
            logger.error("Failed to connect to IB. Make sure TWS/Gateway is running.")
            logger.error("Continuing without IB connection for testing...")
            ib_client = None
        
        # Initialize strategy
        strategy = MispricingStrategy(
            hyperliquid_client,
            ib_client,
            execution_backend=execution_backend,
            min_price_diff_percent=args.threshold,
            discord_config=discord_config,
            enable_trading=args.enable_trading,
        )
        
        # Start strategy
        await strategy.start(interval_seconds=args.interval)
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal - shutting down...")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        if strategy:
            await strategy.cleanup()
        if ib_client:
            ib_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
