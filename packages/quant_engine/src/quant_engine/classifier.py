import numpy as np
import pandas as pd
from typing import List, Optional, Dict, Any
from common.models import Candle, RegimeResult, RegimeMetrics
from .analytics import calculate_technical_indicators
from .repository import Repository, Config
from datetime import datetime

class RegimeClassifier:
    def __init__(self, repository: Repository, config: Config):
        self.repository = repository
        self.config = config

    def classify(self, candles: List[Candle]) -> Optional[RegimeResult]:
        """
        Main entry point for classification.
        1. Calculate features.
        2. Apply Rule-Based or ML-Based logic.
        3. Return RegimeResult.
        """
        if not candles:
            return None

        # Calculate Indicators
        # We need a window of data. The last candle is the one we are classifying.
        df = calculate_technical_indicators(candles)

        if df.empty or 'volatility' not in df.columns:
            return None

        # Get the latest row (the candle we just received/closed)
        current_state = df.iloc[-1]

        # Ensure we have enough data (volatility might be NaN if not enough history)
        if pd.isna(current_state['volatility']):
            return None

        if self.config.MODE == "ML_CLUSTERING":
            return self._classify_ml(current_state, candles[-1])
        else:
            return self._classify_rule_based(current_state, candles[-1])

    def _classify_rule_based(self, state: pd.Series, last_candle: Candle) -> RegimeResult:
        """
        Simple rule-based logic:
        High Volatility if vol > threshold.
        Bull/Bear based on Trend/Slope.
        """
        vol = state['volatility']
        trend = state.get('sma_slope', 0)

        vol_high = vol > self.config.VOLATILITY_THRESHOLD

        if trend > self.config.TREND_THRESHOLD:
            direction = "BULL"
        elif trend < -self.config.TREND_THRESHOLD:
            direction = "BEAR"
        else:
            direction = "SIDEWAYS"

        vol_label = "HIGH_VOL" if vol_high else "LOW_VOL"

        label = f"{direction}_{vol_label}"

        metrics = RegimeMetrics(
            trend_score=float(trend) if not pd.isna(trend) else 0.0,
            volatility=float(vol),
            additional_metrics={"rsi": float(state.get('rsi', 0))}
        )

        return RegimeResult(
            symbol=last_candle.symbol,
            regime_label=label,
            regime_id=None, # No ID for rule based
            confidence=1.0,
            metrics=metrics,
            updated_at=datetime.utcnow()
        )

    def _classify_ml(self, state: pd.Series, last_candle: Candle) -> RegimeResult:
        """
        ML-Based logic:
        Load centroids, calculate distance, assign nearest cluster.
        """
        centroids_data = self.repository.get_latest_centroids()

        if not centroids_data:
            # Fallback to rule based if no model loaded
            print("No centroids found, falling back to rule-based.")
            return self._classify_rule_based(state, last_candle)

        # Assuming centroids_data structure: {'centroids': [[v1, v2], ...], 'labels': ['BULL', ...], 'scaler_mean': ..., 'scaler_scale': ...}
        # We need to standardize the input vector using the same scaler used during training.

        try:
            centroids = np.array(centroids_data.get('centroids', []))
            labels = centroids_data.get('labels', [])
            scaler_mean = np.array(centroids_data.get('scaler_mean', []))
            scaler_scale = np.array(centroids_data.get('scaler_scale', []))

            if len(centroids) == 0:
                return self._classify_rule_based(state, last_candle)

            # Feature vector: [volatility, sma_slope, rsi] - needs to match training features!
            # Let's assume the training used these 3 features in this order.
            # In a real system, feature names should be part of the model config.
            features = [state['volatility'], state.get('sma_slope', 0), state.get('rsi', 50)]
            feature_vector = np.array(features)

            # Scale
            if scaler_scale.any():
                scaled_vector = (feature_vector - scaler_mean) / scaler_scale
            else:
                scaled_vector = feature_vector

            # Calculate Euclidean distances
            distances = np.linalg.norm(centroids - scaled_vector, axis=1)
            nearest_idx = np.argmin(distances)

            # Confidence could be inverse of distance or probability if using GMM.
            # Here, simple distance-based confidence is tricky, but we can return 1/(1+dist)
            confidence = 1.0 / (1.0 + distances[nearest_idx])

            label = labels[nearest_idx] if nearest_idx < len(labels) else f"CLUSTER_{nearest_idx}"

            metrics = RegimeMetrics(
                trend_score=float(state.get('sma_slope', 0)),
                volatility=float(state['volatility']),
                additional_metrics={"rsi": float(state.get('rsi', 0))}
            )

            return RegimeResult(
                symbol=last_candle.symbol,
                regime_label=label,
                regime_id=int(nearest_idx),
                confidence=float(confidence),
                metrics=metrics,
                updated_at=datetime.utcnow()
            )

        except Exception as e:
            print(f"Error in ML classification: {e}")
            return self._classify_rule_based(state, last_candle)
