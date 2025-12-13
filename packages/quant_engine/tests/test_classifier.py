import unittest
from unittest.mock import MagicMock
import pandas as pd
import numpy as np
from datetime import datetime
from common.models import Candle
from quant_engine.classifier import RegimeClassifier
from quant_engine.repository import Repository, Config

class TestClassifier(unittest.TestCase):
    def setUp(self):
        self.config = Config(MODE="RULE_BASED", VOLATILITY_THRESHOLD=0.01)
        self.repository = MagicMock(spec=Repository)
        self.classifier = RegimeClassifier(self.repository, self.config)

        self.candles = []
        base_time = datetime.utcnow()
        for i in range(101):
            price = 100.0 + (i % 5) # some movement
            self.candles.append(Candle(
                symbol="BTC-USD",
                timestamp=base_time,
                open=price, high=price, low=price, close=price, volume=100
            ))

    def test_rule_based_classification(self):
        # With low volatility
        result = self.classifier.classify(self.candles)
        self.assertIsNotNone(result)
        self.assertEqual(result.regime_label.split('_')[-1], "VOL")
        # Since volatility is likely high due to modulo oscillation

    def test_ml_based_fallback(self):
        self.config.MODE = "ML_CLUSTERING"
        self.repository.get_latest_centroids.return_value = None # Simulate no model

        result = self.classifier.classify(self.candles)
        self.assertIsNotNone(result)
        # Should fallback to rule based
        self.assertTrue("VOL" in result.regime_label)

    def test_ml_based_classification(self):
        self.config.MODE = "ML_CLUSTERING"
        # Mock centroids: 2 clusters, 3 dims [vol, slope, rsi]
        self.repository.get_latest_centroids.return_value = {
            'centroids': [[0.0, 0.0, 50.0], [1.0, 1.0, 80.0]],
            'labels': ['CALM', 'PANIC'],
            'scaler_mean': [0.0, 0.0, 0.0],
            'scaler_scale': [1.0, 1.0, 1.0]
        }

        result = self.classifier.classify(self.candles)
        self.assertIsNotNone(result)
        self.assertIn(result.regime_label, ['CALM', 'PANIC'])

if __name__ == '__main__':
    unittest.main()
