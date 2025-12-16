import unittest
import pandas as pd
from datetime import datetime, timedelta
from common.models import Candle
from quant_engine.analytics import calculate_technical_indicators, calculate_rsi

class TestAnalytics(unittest.TestCase):
    def setUp(self):
        # Create a mock sequence of candles
        self.candles = []
        base_time = datetime(2023, 1, 1, 10, 0)
        base_price = 100.0

        for i in range(150):
            # Create a trend
            price = base_price + (i * 0.5)
            if i % 2 == 0:
                price += 1.0 # Add some volatility
            else:
                price -= 1.0

            self.candles.append(Candle(
                symbol="BTC-USD",
                timestamp=base_time + timedelta(hours=i),
                open=price, high=price+1, low=price-1, close=price,
                volume=1000
            ))

    def test_calculate_technical_indicators(self):
        df = calculate_technical_indicators(self.candles, window_size=10)

        self.assertFalse(df.empty)
        self.assertIn('volatility', df.columns)
        self.assertIn('sma', df.columns)
        self.assertIn('sma_slope', df.columns)
        self.assertIn('rsi', df.columns)

        # Check if values are calculated (not all NaN)
        self.assertFalse(df['volatility'].dropna().empty)
        self.assertFalse(df['rsi'].dropna().empty)

    def test_rsi_calculation(self):
        # Test RSI on known data
        prices = pd.Series([
            44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.10, 45.42,
            45.84, 46.08, 45.89, 46.03, 45.61, 46.28, 46.28, 46.00
        ])
        # Simple test to check it runs and produces 0-100
        rsi = calculate_rsi(prices, window=14)
        self.assertTrue((rsi.dropna() >= 0).all() and (rsi.dropna() <= 100).all())

if __name__ == '__main__':
    unittest.main()
