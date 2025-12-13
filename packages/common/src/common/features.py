import pandas as pd
import numpy as np

def calculate_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates technical features for the regime classifier.
    Expects a DataFrame with OHLCV columns: 'open', 'high', 'low', 'close', 'volume'.
    """
    df = df.copy()

    # Ensure sorted by time
    df = df.sort_values('time')

    # 1. Log Returns
    df['log_return'] = np.log(df['close'] / df['close'].shift(1))

    # 2. Volatility (Rolling Standard Deviation of Log Returns) - e.g. 24h window
    # Assuming hourly candles.
    df['volatility_24h'] = df['log_return'].rolling(window=24).std()

    # 3. Simple Moving Average (Trend) - e.g. 50h
    df['sma_50'] = df['close'].rolling(window=50).mean()

    # 4. Momentum / RSI (simplified calculation)
    # Relative Strength Index (RSI) - 14 period
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()

    # Avoid division by zero
    rs = gain / loss.replace(0, np.nan)
    df['rsi_14'] = 100 - (100 / (1 + rs))
    df['rsi_14'] = df['rsi_14'].fillna(100) # If loss is 0, RSI is 100

    return df
