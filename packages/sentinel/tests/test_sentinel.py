import json
from datetime import datetime
from unittest.mock import AsyncMock

import pytest
from common.models import Candle
from httpx import ASGITransport, AsyncClient

# Import sentinel modules
# We need to make sure we can import src.sentinel
from src.sentinel.connector import BinanceSentinel
from src.sentinel.db import db
from src.sentinel.health import app
from src.sentinel.producer import producer


@pytest.fixture
def mock_db():
    db.pool = AsyncMock()
    db.insert_candle = AsyncMock()
    return db


@pytest.fixture
def mock_producer():
    producer.redis = AsyncMock()
    producer.publish_candle = AsyncMock()
    return producer


@pytest.mark.asyncio
async def test_binance_connector_handle_message(mock_db, mock_producer):
    connector = BinanceSentinel()

    # Sample Binance kline message
    msg = json.dumps(
        {
            "stream": "btcusdt@kline_1h",
            "data": {
                "e": "kline",
                "E": 123456789,
                "s": "BTCUSDT",
                "k": {
                    "t": 1698400800000,
                    "T": 1698404399999,
                    "s": "BTCUSDT",
                    "i": "1h",
                    "f": 100,
                    "L": 200,
                    "o": "34000.00",
                    "c": "34050.00",
                    "h": "34100.00",
                    "l": "33900.00",
                    "v": "105.5",
                    "n": 100,
                    "x": True,  # Closed
                    "q": "1000.0000",
                    "V": "50.0",
                    "Q": "500.0",
                    "B": "0",
                },
            },
        }
    )

    await connector.handle_message(msg)

    # Verify DB insertion
    assert mock_db.insert_candle.called
    call_args = mock_db.insert_candle.call_args[0][0]
    assert isinstance(call_args, Candle)
    assert call_args.symbol == "BTC-USD"
    assert call_args.close == 34050.0
    assert call_args.volume == 105.5
    assert call_args.exchange == "BINANCE"

    # Verify Producer publish
    assert mock_producer.publish_candle.called
    assert mock_producer.publish_candle.call_args[0][0] == call_args


@pytest.mark.asyncio
async def test_binance_connector_ignores_open_candle(mock_db, mock_producer):
    connector = BinanceSentinel()

    # Open candle (x: False)
    msg = json.dumps(
        {
            "stream": "btcusdt@kline_1h",
            "data": {
                "e": "kline",
                "s": "BTCUSDT",
                "k": {
                    "t": 1698400800000,
                    "s": "BTCUSDT",
                    "o": "34000.00",
                    "c": "34050.00",
                    "h": "34100.00",
                    "l": "33900.00",
                    "v": "105.5",
                    "x": False,  # Not closed
                },
            },
        }
    )

    await connector.handle_message(msg)

    assert not mock_db.insert_candle.called
    assert not mock_producer.publish_candle.called


@pytest.mark.asyncio
async def test_health_check_healthy():
    # Simulate recent heartbeat
    from src.sentinel.health import health_monitor

    health_monitor.update_heartbeat()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_health_check_unhealthy():
    from datetime import timedelta

    from src.sentinel.health import health_monitor

    # Simulate old heartbeat
    health_monitor.last_heartbeat = datetime.now() - timedelta(seconds=120)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/health")

    assert response.status_code == 503
