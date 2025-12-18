import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import json
import asyncio
from common.models import Candle
from quant_engine.main import parse_candle_from_msg, process_candle
from quant_engine.repository import Repository
from quant_engine.classifier import RegimeClassifier
from datetime import datetime

class TestStreamIntegration(unittest.TestCase):
    def test_parse_candle_from_msg_dict(self):
        data = {
            "symbol": "BTC-USD",
            "timestamp": "2023-10-27T10:00:00Z",
            "open": 30000.0,
            "high": 30100.0,
            "low": 29900.0,
            "close": 30050.0,
            "volume": 100.0,
            "timeframe": "1h"
        }
        candle = parse_candle_from_msg(data)
        self.assertEqual(candle.symbol, "BTC-USD")
        self.assertEqual(candle.close, 30050.0)

    def test_parse_candle_from_msg_json_payload(self):
        payload = json.dumps({
            "symbol": "BTC-USD",
            "timestamp": "2023-10-27T10:00:00Z",
            "open": 30000.0,
            "high": 30100.0,
            "low": 29900.0,
            "close": 30050.0,
            "volume": 100.0,
            "timeframe": "1h"
        })
        data = {"payload": payload}
        candle = parse_candle_from_msg(data)
        self.assertEqual(candle.symbol, "BTC-USD")

    def test_process_candle(self):
        # process_candle is async, so we need to run it in a loop
        repository = MagicMock(spec=Repository)
        # Mock get_recent_candles (sync method called in executor)
        repository.get_recent_candles.return_value = []
        # Mock save_regime (async method)
        repository.save_regime = AsyncMock()

        classifier = MagicMock(spec=RegimeClassifier)

        # Mock classifier result
        mock_result = MagicMock()
        mock_result.regime_label = "TEST_REGIME"
        classifier.classify.return_value = mock_result

        candle = Candle(
            symbol="BTC-USD",
            timestamp=datetime.utcnow(),
            open=100, high=101, low=99, close=100, volume=10,
            timeframe="1h"
        )

        async def run_test():
            await process_candle(candle, repository, classifier)

        asyncio.run(run_test())

        # Verify interactions
        repository.get_recent_candles.assert_called_with("BTC-USD", "1h", 100)
        classifier.classify.assert_called()
        repository.save_regime.assert_called_with(mock_result, timeframe="1h")

if __name__ == '__main__':
    unittest.main()
