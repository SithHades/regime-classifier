import asyncio
import json
import logging
from datetime import datetime

import websockets
from common.models import Candle

from .config import settings
from .db import db
from .health import health_monitor
from .producer import producer

logger = logging.getLogger(__name__)


class BinanceSentinel:
    def __init__(self):
        self.url = settings.EXCHANGE_WEBSOCKET_URL
        self.running = False

    async def start(self):
        self.running = True
        backoff = 1
        while self.running:
            try:
                async with websockets.connect(self.url) as ws:
                    logger.info(f"Connected to Binance WebSocket: {self.url}")
                    backoff = 1  # Reset backoff on successful connection
                    while self.running:
                        msg = await ws.recv()
                        await self.handle_message(msg)
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")
                logger.info(f"Reconnecting in {backoff} seconds...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)  # Cap at 60s

    async def stop(self):
        self.running = False

    async def handle_message(self, msg: str):
        # Binance combined stream payload structure:
        # {
        #   "stream": "<streamName>",
        #   "data": <rawPayload>
        # }
        try:
            data = json.loads(msg)
            if "data" in data:
                payload = data["data"]
            else:
                payload = data  # Handle single stream case if URL is different

            # Parse kline data
            # Binance kline payload:
            # {
            #   "e": "kline",     // Event type
            #   "E": 123456789,   // Event time
            #   "s": "BTCUSDT",   // Symbol
            #   "k": {
            #     "t": 123400000, // Kline start time
            #     "T": 123460000, // Kline close time
            #     "s": "BTCUSDT", // Symbol
            #     "i": "1m",      // Interval
            #     "f": 100,       // First trade ID
            #     "L": 200,       // Last trade ID
            #     "o": "0.0010",  // Open price
            #     "c": "0.0020",  // Close price
            #     "h": "0.0025",  // High price
            #     "l": "0.0015",  // Low price
            #     "v": "1000",    // Base asset volume
            #     "n": 100,       // Number of trades
            #     "x": false,     // Is this kline closed?
            #     "q": "1.0000",  // Quote asset volume
            #     ...
            #   }
            # }

            if "k" not in payload:
                return

            kline = payload["k"]
            is_closed = kline.get("x", False)

            # We only care about closed candles as per requirements ("Pushes raw 'candle closed' events")
            if not is_closed:
                return

            # Normalize symbol: BTCUSDT -> BTC-USD (Assumption based on reqs)
            raw_symbol = kline["s"]
            # A simple heuristic for now. In prod we'd map this properly.
            symbol = raw_symbol.replace("USDT", "-USD")

            # Timestamp: "t" is start time in ms.
            ts = datetime.fromtimestamp(kline["t"] / 1000.0)

            candle = Candle(
                event_type="candle_close",
                symbol=symbol,
                exchange="BINANCE",
                timestamp=ts,
                open=float(kline["o"]),
                high=float(kline["h"]),
                low=float(kline["l"]),
                close=float(kline["c"]),
                volume=float(kline["v"]),
            )

            # Deduplication handled by DB constraint (time, symbol, exchange).
            # For Redis, we simply publish. The consumer (Quant Engine) might receive dupes if we restart and re-process,
            # but websocket usually sends once per close.
            # If we reconnect and receive the same close event again?
            # Binance sends kline update every 2s (approx). The "x": true only happens once at the end.

            # Persist to DB
            await db.insert_candle(candle)

            # Publish to Redis
            await producer.publish_candle(candle)

            # Update Health Monitor
            health_monitor.update_heartbeat()

            logger.info(f"Processed candle: {symbol} @ {ts}")

        except Exception as e:
            logger.error(f"Error processing message: {e} | Msg: {msg}")


connector = BinanceSentinel()
