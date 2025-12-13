from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # App
    APP_NAME: str = "sentinel"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_STREAM_KEY: str = "market_data_feed"

    # TimescaleDB / Postgres
    DATABASE_URL: str = "postgresql://postgres:password@localhost:5432/quant"

    # Exchange
    EXCHANGE_WEBSOCKET_URL: str = "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1h/ethusdt@kline_1h"
    # Example stream URL for 1h candles for BTC and ETH.
    # Binance stream name format: <symbol>@kline_<interval>
    # Note: user mentioned configurable subscription lists.
    # For v1 we can use a comma separated string or just default to this URL which supports multiple streams.

    # Health Check
    HEALTH_CHECK_PORT: int = 8000
    LIVENESS_THRESHOLD_SECONDS: int = 60


settings = Settings()
