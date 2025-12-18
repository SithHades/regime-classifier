from typing import List, Optional, Dict, Any, TypedDict
import json
import redis.asyncio as redis
from sqlalchemy import create_engine, text
# select, desc, Session, sessionmaker removed as we are using Core
from common.models import Candle, RegimeResult
from datetime import datetime, timedelta
from pydantic_settings import BaseSettings

class Config(BaseSettings):
    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql://user:password@localhost:5432/timescaledb"
    # Mode: RULE_BASED or ML_CLUSTERING
    mode: str = "RULE_BASED"

    # Thresholds for Rule Based
    volatility_threshold: float = 0.02
    trend_threshold: float = 0.0

    # Stream
    stream_key: str = "market_data_feed"
    consumer_group: str = "quant_group"
    consumer_name: str = "quant_processor_1"

    # Features
    feature_names: List[str] = ["volatility", "sma_slope", "rsi"]

class CentroidData(TypedDict):
    centroids: List[List[float]]
    labels: List[str]
    scaler_mean: List[float]
    scaler_scale: List[float]

class Repository:
    def __init__(self, config: Config):
        self.config = config
        self.redis_client = redis.from_url(config.redis_url, decode_responses=True)
        # SQLAlchemy async engine is preferred for async but user didn't explicitly demand async DB, just "async function" for processing stream.
        # However, calling sync DB in async loop blocks.
        # But for now, to minimize diffs and stick to requirements "We should have this function asynchronously" (referring to main loop/processing),
        # I'll keep sync DB (or use run_in_executor if needed) or just acknowledge the constraint.
        # Ideally should use asyncpg + sqlalchemy async.
        # Given dependencies installed (psycopg2-binary), we only have sync driver.
        # So I will keep sync engine.
        self.engine = create_engine(config.database_url)

    def get_recent_candles(self, symbol: str, timeframe: str, limit: int = 100) -> List[Candle]:
        """
        Fetches the last N candles for a symbol from TimescaleDB.
        """
        # Assuming schema is 'market_data' based on comments
        query = text("""
            SELECT symbol, timestamp, open, high, low, close, volume, timeframe
            FROM market_data.candles
            WHERE symbol = :symbol AND timeframe = :timeframe
            ORDER BY timestamp DESC
            LIMIT :limit
        """)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {'symbol': symbol, 'timeframe': timeframe, 'limit': limit})
                rows = result.fetchall()
                # Sort chronologically (oldest first)
                candles = []
                for row in reversed(rows):
                    candles.append(Candle(
                        symbol=row.symbol,
                        timestamp=row.timestamp,
                        open=float(row.open),
                        high=float(row.high),
                        low=float(row.low),
                        close=float(row.close),
                        volume=float(row.volume),
                        timeframe=row.timeframe
                    ))
                return candles
        except Exception as e:
            print(f"Error fetching candles: {e}")
            return []

    async def save_regime(self, result: RegimeResult, timeframe: str):
        """
        Writes the classification result to Redis.
        Key: regime:{symbol}:{timeframe}
        """
        key = f"regime:{result.symbol}:{timeframe}"
        value = result.model_dump_json()
        # Set with expiration (e.g., 2x the timeframe or fixed 1 hour). Let's say 1 hour (3600s).
        await self.redis_client.set(key, value, ex=3600)
        print(f"Saved regime to Redis: {key} -> {value}")

    def get_latest_centroids(self) -> Optional[CentroidData]:
        """
        Fetches the active cluster centroids from the model_registry table.
        """
        query = text("""
            SELECT model_params
            FROM model_registry
            WHERE is_active = TRUE
            ORDER BY created_at DESC
            LIMIT 1
        """)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query)
                row = result.fetchone()
                if row:
                    # Assuming model_params is a JSON column
                    return row.model_params
                return None
        except Exception as e:
            print(f"Error fetching centroids: {e}")
            return None
