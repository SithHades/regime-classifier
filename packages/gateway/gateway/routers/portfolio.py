from typing import Annotated, Dict, List

from fastapi import APIRouter, Body, Request

from ..limiter import limiter

router = APIRouter()


@router.post("/v1/portfolio/risk")
@limiter.limit("60/minute")
async def calculate_portfolio_risk(
    request: Request,
    holdings: Annotated[
        List[Dict],
        Body(..., description="List of holdings, e.g. [{'symbol': 'BTC', 'amount': 1.0}]"),
    ],
):
    # This endpoint mimics a high-tier computation
    # In a real system, it would fetch covariance matrices and volatilities
    # Here we return a mock response as calculation is out of scope

    # Simple mock logic: sum of amounts * dummy risk factor
    total_value = 0
    risk_score = 0

    for item in holdings:
        amount = item.get("amount", 0)
        symbol = item.get("symbol", "UNKNOWN")
        # Dummy risk contribution
        risk_contribution = amount * 0.5
        risk_score += risk_contribution
        total_value += amount

    # Normalize risk score roughly
    final_risk = min(100, risk_score) if total_value > 0 else 0

    return {"portfolio_risk_score": final_risk, "details": "Calculated using mock engine (Gateway MVP)", "symbol": symbol}
