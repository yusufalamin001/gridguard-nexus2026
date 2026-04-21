"""
model/predict.py

Live inference engine for GridGuard.
Loads the trained XGBoost artifact and scores the latest available
SCADA telemetry to generate failure probabilities per corridor.
Writes results to the risk_scores table in DuckDB for the dashboard.

Usage:
    python model/predict.py
"""

import os
import pandas as pd
import xgboost as xgb
import duckdb

from config import DUCKDB_PATH, FEATURE_COLS
from logger import get_logger

logger = get_logger("predict")

MODEL_PATH = "model/artifacts/xgboost_gridguard.json"


def get_latest_telemetry() -> pd.DataFrame:
    """
    Fetches the most recent feature row per corridor from model_features.
    Returns a DataFrame with one row per corridor ready for inference.

    Returns:
        pd.DataFrame with corridor_id, corridor_name, timestamp,
        and all FEATURE_COLS populated.
    """
    con = duckdb.connect(DUCKDB_PATH)
    try:
        query = """
            WITH ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY corridor_id
                        ORDER BY timestamp DESC
                    ) AS rn
                FROM model_features
            )
            SELECT * FROM ranked WHERE rn = 1
        """
        df = con.execute(query).df()
        logger.info(
            "Fetched latest telemetry — %d corridors",
            len(df)
        )
        return df
    except Exception as e:
        logger.error("Failed to fetch latest telemetry: %s", e)
        raise
    finally:
        con.close()


def write_risk_scores(results_df: pd.DataFrame) -> None:
    """
    Writes failure probability predictions to the risk_scores table in DuckDB.
    Dashboard reads from this table for live display.

    Args:
        results_df: DataFrame with corridor_id, corridor_name,
                    timestamp, failure_probability_pct
    """
    con = duckdb.connect(DUCKDB_PATH)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS risk_scores (
                corridor_id             INTEGER NOT NULL,
                corridor_name           VARCHAR,
                timestamp               TIMESTAMP,
                failure_probability_pct DOUBLE,
                scored_at               TIMESTAMP DEFAULT current_timestamp,
                PRIMARY KEY (corridor_id)
            )
        """)

        con.execute("""
            INSERT INTO risk_scores (
                corridor_id, corridor_name, timestamp, failure_probability_pct
            )
            SELECT
                corridor_id, corridor_name, timestamp, failure_probability_pct
            FROM results_df
            ON CONFLICT (corridor_id)
            DO UPDATE SET
                corridor_name           = EXCLUDED.corridor_name,
                timestamp               = EXCLUDED.timestamp,
                failure_probability_pct = EXCLUDED.failure_probability_pct,
                scored_at               = current_timestamp
        """)

        logger.info(
            "Risk scores written to DuckDB — %d corridors",
            len(results_df)
        )
    except Exception as e:
        logger.error("Failed to write risk scores: %s", e)
        raise
    finally:
        con.close()


def run_inference() -> pd.DataFrame:
    """
    Loads latest telemetry, runs XGBoost inference, writes results to DuckDB.

    Returns:
        pd.DataFrame with corridor_id, corridor_name, timestamp,
        and failure_probability_pct sorted by risk descending.
    """
    logger.info("Starting live prediction engine...")

    # 1. Load latest telemetry
    live_df = get_latest_telemetry()
    if live_df.empty:
        logger.error("No telemetry data found in model_features.")
        raise ValueError("Empty telemetry — run the pipeline first.")

    # 2. Load trained model artifact
    if not os.path.exists(MODEL_PATH):
        logger.error("Model artifact not found at %s — run model/train.py first.", MODEL_PATH)
        raise FileNotFoundError(f"Model not found: {MODEL_PATH}")

    model = xgb.XGBClassifier()
    model.load_model(MODEL_PATH)
    logger.info("Model artifact loaded from %s", MODEL_PATH)

    # 3. Run inference
    # predict_proba returns [[P(0), P(1)], ...] — we take column 1 (failure)
    X_live = live_df[FEATURE_COLS]
    probabilities = model.predict_proba(X_live)[:, 1]

    # 4. Build results DataFrame
    results_df = live_df[['corridor_id', 'corridor_name', 'timestamp']].copy()
    results_df['failure_probability_pct'] = (probabilities * 100).round(2)
    results_df = results_df.sort_values(
        by='failure_probability_pct', ascending=False
    ).reset_index(drop=True)

    # 5. Write to DuckDB for dashboard consumption
    write_risk_scores(results_df)

    print("\n--- LIVE GRID RISK ASSESSMENTS ---")
    print(results_df.to_string(index=False))

    logger.info("Inference complete — top risk: %s at %.1f%%",
                results_df.iloc[0]['corridor_name'],
                results_df.iloc[0]['failure_probability_pct'])

    return results_df


def main() -> None:
    """
    Main orchestration for live inference pipeline.
    """
    logger.info("=== Starting GridGuard Inference Engine ===")
    try:
        run_inference()
        logger.info("=== Inference Engine Complete ===")
    except Exception as e:
        logger.error("=== Inference Engine Failed: %s ===", e)
        raise


if __name__ == "__main__":
    main()
