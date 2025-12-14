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
):
    key = f"regime:{symbol}:{timeframe}"
    data = await redis_service.get(key)

    if not data:
        # If not found in Redis, return 404
        raise HTTPException(status_code=404, detail=f"Regime data not found for {symbol} {timeframe}")

    try:
        # Assuming data is stored as JSON string in Redis
        return json.loads(data)
    except json.JSONDecodeError:
        return {"raw_data": data}
