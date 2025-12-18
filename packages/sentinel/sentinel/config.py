from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    # App
    app_name: str = "sentinel"

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_stream_key: str = "market_data_feed"
    redis_stream_max_len: int = 10000

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
    # Exchange
    watch_symbols: list[str] = ["btcusdt", "ethusdt"]
    kline_interval: str = "1h"
    # Base WebSocket URL for Binance
    binance_ws_base_url: str = "wss://stream.binance.com:9443/stream?streams="

    # Health Check
    health_check_port: int = 8000
    liveness_threshold_seconds: int = 60


settings = Settings()
