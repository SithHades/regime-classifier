from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    # App
    APP_NAME: str = "sentinel"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_STREAM_KEY: str = "market_data_feed"

    # TimescaleDB / Postgres
    # TimescaleDB / Postgres
    database_user: str = "postgres"
    database_password: str = "password"
    database_host: str = "localhost"
    database_port: int = 5432
    database_name: str = "quant"
    database_url: str | None = None

    @model_validator(mode="after")
    def assemble_db_url(self) -> "Settings":
        if self.database_url is None:
            self.database_url = (
                f"postgresql://{self.database_user}:{self.database_password}"
                f"@{self.database_host}:{self.database_port}/{self.database_name}"
            )
        return self

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
