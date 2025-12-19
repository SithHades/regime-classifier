import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sqlalchemy import select, update
from common.database import get_db, engine
from common.models import RawCandle, ModelRegistry
from common.features import calculate_features
import logging

# Configure logging
logger = logging.getLogger(__name__)

def train_model():
    logger.info("Starting training job...")

    # 1. Fetch data
    # Fetch last 2 years (or configurable)
    query = select(RawCandle).where(RawCandle.time >= datetime.utcnow() - timedelta(days=730))

    # Use pandas to read sql
    logger.info("Fetching data from DB...")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        logger.warning("No data found. Aborting.")
        return

    logger.info(f"Fetched {len(df)} rows.")

    # 2. Feature Engineering
    df_features = df.groupby('symbol', group_keys=False).apply(calculate_features)

    # Drop NaNs created by rolling windows
    df_clean = df_features.dropna()

    if df_clean.empty:
        logger.warning("Not enough data after feature engineering.")
        return

    # Select features for clustering
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
    cluster_stats = []
    for i in range(k):
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

    cluster_stats.sort(key=lambda x: x['avg_vol'], reverse=True)

    best_panic_cluster = None
    max_score = -float('inf')

    for stats in cluster_stats:
        c = stats['center_scaled']
        scaled_ret = c[0]
        scaled_vol = c[1]

        # specific logic: highest vol + highest negative return
        score = scaled_vol - scaled_ret
        if score > max_score:
            max_score = score
            best_panic_cluster = stats['cluster_id']

    regime_labels = {}
    remaining_clusters = set(range(k))

    if best_panic_cluster is not None:
        regime_labels[best_panic_cluster] = "PANIC"
        remaining_clusters.remove(best_panic_cluster)

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

    logger.info(f"Identified Regimes: {regime_labels}")

    # 6. Save to DB
    db = next(get_db())
    try:
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
        logger.info("Model saved to registry.")

    except Exception as e:
        db.rollback()
        logger.error(f"Error saving model: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    train_model()
