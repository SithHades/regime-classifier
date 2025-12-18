from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field

class Candle(BaseModel):
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    timeframe: str

    class Config:
        from_attributes = True

class RegimeMetrics(BaseModel):
    trend_score: float
    volatility: float
    additional_metrics: Dict[str, Any] = Field(default_factory=dict)

class RegimeResult(BaseModel):
    symbol: str
    regime_label: str
    regime_id: Optional[int] = None
    confidence: float
    metrics: RegimeMetrics
    updated_at: datetime
