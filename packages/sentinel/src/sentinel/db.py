import logging

import asyncpg

from .config import settings

logger = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        logger.info(f"Connecting to database: {settings.DATABASE_URL}")
        try:
            self.pool = await asyncpg.create_pool(settings.DATABASE_URL)
            await self.init_db()
            logger.info("Database connected successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def init_db(self):
        async with self.pool.acquire() as conn:
            # Create table if not exists
            # We assume TimescaleDB extension is enabled on the DB.
            # "stores the last few months of OHLCV data"
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_candles (
                    time        TIMESTAMPTZ       NOT NULL,
                    symbol      TEXT              NOT NULL,
                    exchange    TEXT              NOT NULL,
                    open        DOUBLE PRECISION  NOT NULL,
                    high        DOUBLE PRECISION  NOT NULL,
                    low         DOUBLE PRECISION  NOT NULL,
                    close       DOUBLE PRECISION  NOT NULL,
                    volume      DOUBLE PRECISION  NOT NULL,
                    UNIQUE(time, symbol, exchange)
                );
            """)

            # Convert to hypertable if not already (TimescaleDB specific)
            # We catch error in case it's already a hypertable or extension missing
            try:
                await conn.execute("SELECT create_hypertable('raw_candles', 'time', if_not_exists => TRUE);")
            except Exception as e:
                logger.warning(f"Could not create hypertable (might not be TimescaleDB or already exists): {e}")

    async def insert_candle(self, candle):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO raw_candles (time, symbol, exchange, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (time, symbol, exchange) DO NOTHING
            """,
                candle.timestamp,
                candle.symbol,
                candle.exchange,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
            )


db = Database()
