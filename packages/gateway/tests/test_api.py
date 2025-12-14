import os
import sys

import pytest
from httpx import ASGITransport, AsyncClient

# Add packages/gateway to sys.path to allow importing 'gateway' package
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from gateway.main import app
from gateway.services.redis import redis_service


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture(autouse=True)
async def mock_redis():
    # Pre-fill mock redis data
    await redis_service.set("regime:BTC-USD:1h", '{"state": "BULL", "score": 90}')
    yield
    # Cleanup if necessary


@pytest.mark.anyio
async def test_health_check(client):
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.anyio
async def test_regime_endpoint_no_payment(client):
    response = await client.get("/v1/regime?symbol=BTC-USD&timeframe=1h")
    assert response.status_code == 402
    assert "WWW-Authenticate" in response.headers
    assert "L402" in response.headers["WWW-Authenticate"]


@pytest.mark.anyio
async def test_regime_endpoint_with_payment(client):
    # Mock valid credentials
    # The middleware currently accepts any preimage > 0 length
    headers = {"Authorization": "L402 mock_macaroon:valid_preimage"}

    response = await client.get("/v1/regime?symbol=BTC-USD&timeframe=1h", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["state"] == "BULL"
    assert data["score"] == 90


@pytest.mark.anyio
async def test_regime_endpoint_not_found(client):
    headers = {"Authorization": "L402 mock_macaroon:valid_preimage"}
    response = await client.get("/v1/regime?symbol=ETH-USD&timeframe=1h", headers=headers)
    assert response.status_code == 404


@pytest.mark.anyio
async def test_portfolio_risk(client):
    headers = {"Authorization": "L402 mock_macaroon:valid_preimage"}
    payload = [{"symbol": "BTC", "amount": 10}]
    response = await client.post("/v1/portfolio/risk", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert "portfolio_risk_score" in data
