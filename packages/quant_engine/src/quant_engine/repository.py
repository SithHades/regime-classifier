from typing import List, Optional, Dict, Any
import json
import redis
from sqlalchemy import create_engine, text
# select, desc, Session, sessionmaker removed as we are using Core
from common.models import Candle, RegimeResult
from datetime import datetime, timedelta
from pydantic_settings import BaseSettings

class Config(BaseSettings):
    REDIS_URL: str = "redis://localhost:6379/0"
    DATABASE_URL: str = "postgresql://user:password@localhost:5432/timescaledb"
    # Mode: RULE_BASED or ML_CLUSTERING
    MODE: str = "RULE_BASED"

    # Thresholds for Rule Based
    VOLATILITY_THRESHOLD: float = 0.02
    TREND_THRESHOLD: float = 0.0

    # Stream
    STREAM_KEY: str = "market_data_feed"
    CONSUMER_GROUP: str = "quant_group"
    CONSUMER_NAME: str = "quant_processor_1"

class Repository:
    def __init__(self, config: Config):
        self.config = config
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=True)
        self.engine = create_engine(config.DATABASE_URL)

    def get_recent_candles(self, symbol: str, limit: int = 100) -> List[Candle]:
        """
        Fetches the last N candles for a symbol from TimescaleDB.
        """
        query = text("""
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM candles
            WHERE symbol = :symbol
            ORDER BY timestamp DESC
            LIMIT :limit
        """)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {'symbol': symbol, 'limit': limit})
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
                        volume=float(row.volume)
                    ))
                return candles
        except Exception as e:
            print(f"Error fetching candles: {e}")
            return []

    def save_regime(self, result: RegimeResult, timeframe: str = "1h"):
        """
        Writes the classification result to Redis.
        Key: regime:{symbol}:{timeframe}
        """
        key = f"regime:{result.symbol}:{timeframe}"
        value = result.model_dump_json()
        self.redis_client.set(key, value)
        print(f"Saved regime to Redis: {key} -> {value}")

    def get_latest_centroids(self) -> List[Dict[str, Any]]:
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
                return []
        except Exception as e:
            print(f"Error fetching centroids: {e}")
            return []
