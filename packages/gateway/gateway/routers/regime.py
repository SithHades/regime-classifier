import json

from fastapi import APIRouter, HTTPException, Query, Request

from ..limiter import limiter
from ..services.redis import redis_service

router = APIRouter()


@router.get("/v1/regime")
@limiter.limit("60/minute")
async def get_regime(
    request: Request,
    symbol: str = Query(..., description="Symbol to query, e.g., BTC-USD"),
    timeframe: str = Query(..., description="Timeframe, e.g., 1h"),
    mock: bool = Query(False, description="Mock the response"),
):
    key = f"regime:{symbol}:{timeframe}"
    data = await redis_service.get(key)

    if not data and not mock:
        # If not found in Redis, return 404
        raise HTTPException(status_code=404, detail=f"Regime data not found for {symbol} {timeframe}")
    if not data and mock:
        data = '{"regime": "BULL", "timestamp": "2025-12-14T21:19:49.123456"}'

    try:
        # Assuming data is stored as JSON string in Redis
        return json.loads(data)
    except json.JSONDecodeError:
        return {"raw_data": data}
