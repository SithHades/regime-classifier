import logging
import ssl

import asyncpg

from .config import settings

logger = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        logger.info(f"Connecting to database: {settings.database_url.split('@')[-1]}")  # masking auth info
        try:
            # Parse the URL to handle sslmode
            from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

            parsed = urlparse(settings.database_url)
            query_params = parse_qs(parsed.query)

            ssl_option = query_params.pop("sslmode", [None])[0]

            # Reconstruct URL without sslmode
            new_query = urlencode(query_params, doseq=True)
            clean_url = urlunparse(parsed._replace(query=new_query))

            # Map sslmode to asyncpg ssl argument
            # asyncpg accepts 'require', 'verify-ca', 'verify-full' as strings, or an SSLContext
            # Common postgres url values are 'disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'
            if ssl_option == "require":
                # Explicitly create an SSL context that ignores verification
                ssl_ctx = ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE
            elif ssl_option in ("verify-ca", "verify-full"):
                ssl_ctx = ssl.create_default_context()
                ssl_ctx.check_hostname = True
                ssl_ctx.verify_mode = ssl.CERT_REQUIRED
            elif ssl_option == "disable":
                ssl_ctx = False
            else:
                ssl_ctx = None

            # If user didn't specify sslmode, we default to None (let asyncpg decide)
            # OR if the user provided unexpected value.

            logger.debug(f"Connecting with ssl={ssl_ctx}")

            self.pool = await asyncpg.create_pool(clean_url, ssl=ssl_ctx)
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
                CREATE TABLE IF NOT EXISTS regime_classifier.raw_candles (
                    time        TIMESTAMPTZ       NOT NULL,
                    symbol      TEXT              NOT NULL,
                    exchange    TEXT              NOT NULL,
                    timeframe   TEXT              NOT NULL,
                    open        DOUBLE PRECISION  NOT NULL,
                    high        DOUBLE PRECISION  NOT NULL,
                    low         DOUBLE PRECISION  NOT NULL,
                    close       DOUBLE PRECISION  NOT NULL,
                    volume      DOUBLE PRECISION  NOT NULL,
                    UNIQUE(time, symbol, exchange, timeframe)
                );
            """)

            # Convert to hypertable if not already (TimescaleDB specific)
            # We catch error in case it's already a hypertable or extension missing
            try:
                await conn.execute("SELECT create_hypertable('regime_classifier.raw_candles', 'time', if_not_exists => TRUE);")
            except Exception as e:
                logger.warning(f"Could not create hypertable (might not be TimescaleDB or already exists): {e}")

            # MIGRATION: Attempt to add timeframe column and update constraint if they don't exist
            try:
                # Add timeframe column
                await conn.execute(f"""
                    ALTER TABLE regime_classifier.raw_candles 
                    ADD COLUMN IF NOT EXISTS timeframe TEXT DEFAULT '{settings.kline_interval}' NOT NULL;
                """)

                # Drop old unique constraint if it exists (heuristic name)
                await conn.execute("""
                    ALTER TABLE regime_classifier.raw_candles 
                    DROP CONSTRAINT IF EXISTS raw_candles_time_symbol_exchange_key;
                """)

                # Add new unique constraint
                try:
                    await conn.execute("""
                        ALTER TABLE regime_classifier.raw_candles 
                        ADD CONSTRAINT raw_candles_unique_idx UNIQUE (time, symbol, exchange, timeframe);
                    """)
                except Exception:
                    # Constraint likely already exists
                    pass
            except Exception as e:
                logger.debug(f"Migration step error (safe to ignore if schema is up to date): {e}")

    async def insert_candle(self, candle):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO regime_classifier.raw_candles (time, symbol, exchange, timeframe, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (time, symbol, exchange, timeframe) DO NOTHING
            """,
                candle.timestamp,
                candle.symbol,
                candle.exchange,
                candle.timeframe,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
            )


db = Database()
