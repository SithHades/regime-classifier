import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sqlalchemy import select, update
from common.database import get_db, engine
from common.models import RawCandle, ModelRegistry
from common.features import calculate_features
import json

def train_model():
    print("Starting training job...")

    # 1. Fetch data
    # Fetch last 2 years (or configurable)
    # Ideally use pandas read_sql
    query = select(RawCandle).where(RawCandle.time >= datetime.utcnow() - timedelta(days=730))

    # Use pandas to read sql
    print("Fetching data from DB...")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        print("No data found. Aborting.")
        return

    print(f"Fetched {len(df)} rows.")

    # 2. Feature Engineering
    # We need to process per symbol if there are multiple, but let's assume one symbol or we group.
    # The prompt says "Calculates technicals... writes result per symbol".
    # But for clustering, we want to cluster market states.
    # Usually we train on BTC or aggregated market data.
    # Let's assume we train on BTC-USD or whatever is in DB.
    # If multiple symbols, we might want to stack them or just pick BTC.
    # Let's group by symbol and apply features, then stack.

    df_features = df.groupby('symbol', group_keys=False).apply(calculate_features)

    # Drop NaNs created by rolling windows
    df_clean = df_features.dropna()

    if df_clean.empty:
        print("Not enough data after feature engineering.")
        return

    # Select features for clustering
    # Prompt mentions: Volatility, Returns (implied by "highest negative return")
    # We calculated 'log_return', 'volatility_24h', 'rsi_14'.
    # Let's use Volatility and Returns as primary, maybe RSI.
    feature_cols = ['log_return', 'volatility_24h', 'rsi_14']
    X = df_clean[feature_cols].values

    # 3. Scale
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 4. KMeans
    k = 4
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    kmeans.fit(X_scaled)

    labels = kmeans.labels_
    centroids = kmeans.cluster_centers_

    # 5. Auto-Labeling
    # "The cluster with highest negative return + highest vol is automatically tagged 'CRASH/PANIC'"
    # We need to interpret the centroids.
    # Centroids are in scaled space.

    # Let's analyze each cluster
    cluster_stats = []
    for i in range(k):
        # logical_indices = labels == i
        # original_values = X[logical_indices]
        # mean_vals = original_values.mean(axis=0)
        # Actually, centroids represent the center in scaled space.
        # We can map back to original space using inverse_transform
        center_scaled = centroids[i].reshape(1, -1)
        center_original = scaler.inverse_transform(center_scaled)[0]

        # cols: log_return, volatility_24h, rsi_14
        avg_ret = center_original[0]
        avg_vol = center_original[1]

        cluster_stats.append({
            'cluster_id': i,
            'avg_ret': avg_ret,
            'avg_vol': avg_vol,
            'center_scaled': centroids[i].tolist()
        })

    # Heuristic: Score = Volatility - Return (since negative return is panic)
    # Or just "Highest Vol AND Highest Negative Return"
    # Let's sort by Volatility descending.
    cluster_stats.sort(key=lambda x: x['avg_vol'], reverse=True)

    # The one with highest vol is a candidate. Check if it has negative return.
    # The prompt says: "highest negative return + highest vol".
    # Let's create a score: vol - return (because return is negative for crash)
    # High Vol (positive) - Large Negative Return (negative) = Large Positive Score

    best_panic_cluster = None
    max_score = -float('inf')

    for stats in cluster_stats:
        # Standardize comparables?
        # Actually, since we have scaled centroids, we can use them directly.
        # Feature 0: log_return, Feature 1: volatility
        # We want Low Return (negative), High Volatility (positive)
        # In scaled space:
        c = stats['center_scaled']
        scaled_ret = c[0]
        scaled_vol = c[1]

        # specific logic: highest vol + highest negative return
        # Let's say score = scaled_vol - scaled_ret
        score = scaled_vol - scaled_ret
        if score > max_score:
            max_score = score
            best_panic_cluster = stats['cluster_id']

    # Assign labels
    # We only have a strict rule for PANIC.
    # Others: "BULL", "BEAR", "CALM"?
    # For now, let's just label the Panic one "PANIC" and others "REGIME_X"
    # Or we can try to be smarter.
    # High Return = BULL
    # Low Vol = CALM

    regime_labels = {}
    remaining_clusters = set(range(k))

    if best_panic_cluster is not None:
        regime_labels[best_panic_cluster] = "PANIC"
        remaining_clusters.remove(best_panic_cluster)

    # Try to find BULL (Highest Return)
    best_bull = None
    max_ret = -float('inf')
    for cid in remaining_clusters:
        stats = next(s for s in cluster_stats if s['cluster_id'] == cid)
        if stats['avg_ret'] > max_ret:
            max_ret = stats['avg_ret']
            best_bull = cid

    if best_bull is not None:
        regime_labels[best_bull] = "BULL"
        remaining_clusters.remove(best_bull)

    # Remaining 2: Low Vol?
    # Let's just call them REGIME_{ID} for now to be safe, or map remaining.
    for cid in remaining_clusters:
        regime_labels[cid] = f"REGIME_{cid}"

    # Construct Model Parameters
    model_params = {
        "scaler_mean": scaler.mean_.tolist(),
        "scaler_scale": scaler.scale_.tolist(),
        "centroids": centroids.tolist(),
        "labels": regime_labels,
        "feature_cols": feature_cols
    }

    print("Identified Regimes:", regime_labels)

    # 6. Save to DB
    db = next(get_db())
    try:
        # Deactivate old models
        # prompt: "Insert new row and set Is_Active = TRUE (and set others to FALSE)"
        db.execute(
            update(ModelRegistry).where(ModelRegistry.is_active == True).values(is_active=False)
        )

        new_model = ModelRegistry(
            created_at=datetime.utcnow(),
            algorithm="KMeans",
            parameters=model_params,
            is_active=True
        )
        db.add(new_model)
        db.commit()
        print("Model saved to registry.")

    except Exception as e:
        db.rollback()
        print(f"Error saving model: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    train_model()
