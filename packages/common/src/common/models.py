from datetime import datetime

from pydantic import BaseModel


class Candle(BaseModel):
    event_type: str = "candle_close"
    symbol: str
    exchange: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    timeframe: str
