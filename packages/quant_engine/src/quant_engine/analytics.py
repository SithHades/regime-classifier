import pandas as pd
import numpy as np
from typing import List
from common.models import Candle

def calculate_technical_indicators(candles: List[Candle], window_size: int = 100) -> pd.DataFrame:
    """
    Calculates technical indicators for the regime classifier.
    Expects a list of Candles sorted by timestamp (oldest first).
    """
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame([c.model_dump() for c in candles])
    df.set_index('timestamp', inplace=True)
    df.sort_index(inplace=True)

    # Calculate Returns
    df['returns'] = df['close'].pct_change()

    # Volatility: Standard deviation of returns over window
    df['volatility'] = df['returns'].rolling(window=window_size).std()

    # Trend: SMA Slope
    # We can approximate trend by comparing current SMA vs SMA X periods ago,
    # or by linear regression slope. PRD says "SMA Slope".
    # Let's use the slope of the SMA over the window.
    # Or simply: (SMA_now - SMA_prev) / SMA_prev
    # Let's compute SMA first
    df['sma'] = df['close'].rolling(window=window_size).mean()

    # Calculate slope of SMA (change over 1 period)
    df['sma_slope'] = df['sma'].diff()

    # Normalize slope? The PRD mentions "Trend (SMA Slope)".
    # For a rule based system, we might want a normalized value or just the raw slope.
    # Let's keep 'sma_slope'.

    # We might also want RSI as PRD mentions "Calculates technicals (Volatility, RSI, Moving Averages)"
    df['rsi'] = calculate_rsi(df['close'], window=14)

    return df

def calculate_rsi(series: pd.Series, window: int = 14) -> pd.Series:
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()

    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi
