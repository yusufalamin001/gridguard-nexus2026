"""
model/predict.py

Live inference engine for GridGuard.
Loads the trained XGBoost artifact and scores the latest available
SCADA telemetry to generate failure probabilities per corridor.
Writes results to the risk_scores table in DuckDB for the dashboard.

Usage:
    python model/predict.py
    python model/predict.py --demo
"""

import argparse
import json
import os
import numpy as np
import pandas as pd
import xgboost as xgb
import duckdb

from config import DUCKDB_PATH, FEATURE_COLS
from logger import get_logger

logger = get_logger("predict")

MODEL_PATH     = "model/artifacts/xgboost_gridguard.json"
THRESHOLD_PATH = "model/artifacts/threshold.json"

# Default threshold if artifact is missing — conservative for grid safety
DEFAULT_THRESHOLD = 0.30


def load_threshold() -> float:
    """
    Loads the optimal decision threshold saved by train.py.
    Falls back to DEFAULT_THRESHOLD if the file doesn't exist so
    predict.py always produces binary labels even before retraining.
    """
    if os.path.exists(THRESHOLD_PATH):
        with open(THRESHOLD_PATH) as f:
            data = json.load(f)
        threshold = float(data.get("threshold", DEFAULT_THRESHOLD))
        logger.info("Threshold loaded from artifact: %.4f", threshold)
        return threshold

    logger.warning(
        "threshold.json not found — using default %.2f. "
        "Run model/train.py to generate the optimised threshold.",
        DEFAULT_THRESHOLD
    )
    return DEFAULT_THRESHOLD


def get_latest_telemetry() -> pd.DataFrame:
    """
    Fetches the most recent feature row per corridor from model_features.
    Returns a DataFrame with one row per corridor ready for inference.
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
        logger.info("Fetched latest telemetry — %d corridors", len(df))
        return df
    except Exception as e:
        logger.error("Failed to fetch latest telemetry: %s", e)
        raise
    finally:
        con.close()


def get_demo_telemetry() -> pd.DataFrame:
    """
    Demo mode — fetches each corridor at a DIFFERENT offset from its injected
    collapse event, producing a realistic spread of risk probabilities across
    all 8 corridors simultaneously for dashboard demonstration.

    Injected collapse events (from simulate.py):
        Corridor 1 (Benin-Onitsha) -> Dec 29 14:00  -> fetched AT collapse    (~99%)
        Corridor 2 (Ikeja-Ota)     -> Oct 15 15:00  -> fetched 1hr before     (~97%)
        Corridor 3 (Kano-Kaduna)   -> Nov 10 19:00  -> fetched 4hrs before    (~40-70%)
        Corridor 4 (Egbin-Lagos)   -> Nov 25 14:00  -> fetched 4hrs before    (~40-70%)
        Corridors 5-8              -> own events     -> fetched at peak stress (~20-40%)
    """
    DEMO_TARGETS = {
        1: '2025-12-29 14:00:00',  # AT collapse hour        -> ~99.97%
        2: '2025-10-15 14:00:00',  # 1hr before collapse     -> ~97%
        3: '2025-11-10 15:00:00',  # 4hrs before collapse    -> ~40-70%
        4: '2025-11-25 10:00:00',  # 4hrs before collapse    -> ~40-70%
    }

    con = duckdb.connect(DUCKDB_PATH)
    try:
        frames = []

        # Corridors with targeted timestamps
        for corridor_id, ts in DEMO_TARGETS.items():
            row = con.execute("""
                SELECT * FROM model_features
                WHERE corridor_id = ?
                  AND timestamp = ?
                LIMIT 1
            """, [corridor_id, ts]).df()

            if row.empty:
                row = con.execute("""
                    WITH closest AS (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                ORDER BY ABS(EPOCH(timestamp) - EPOCH(?::TIMESTAMP))
                            ) AS rn
                        FROM model_features
                        WHERE corridor_id = ?
                    )
                    SELECT * FROM closest WHERE rn = 1
                """, [ts, corridor_id]).df()
                logger.warning(
                    "Corridor %d: exact timestamp %s not found — using closest",
                    corridor_id, ts
                )

            frames.append(row)

        # Corridors 5-8 — fetch 8 hours BEFORE their own collapse event
        # so they show ambient stress scores (20-40%), not collapse scores (99%).
        # These timestamps are pre-ramp, showing realistic chronic stress levels.
        OTHER_TARGETS = {
            5: '2025-10-22 08:00:00',   # 8hrs before Shiroro collapse ramp
            6: '2025-11-03 12:00:00',   # 8hrs before Jebba collapse ramp
            7: '2025-10-28 05:00:00',   # 8hrs before Afam collapse ramp
            8: '2025-11-18 07:00:00',   # 8hrs before Kainji collapse ramp
        }
        for corridor_id, ts in OTHER_TARGETS.items():
            row = con.execute("""
                SELECT * FROM model_features
                WHERE corridor_id = ?
                  AND timestamp = ?
                LIMIT 1
            """, [corridor_id, ts]).df()

            if row.empty:
                # Fallback: any non-failure hour for this corridor
                row = con.execute("""
                    SELECT * FROM model_features
                    WHERE corridor_id = ?
                      AND failure_event < 0.5
                    ORDER BY timestamp
                    LIMIT 1
                """, [corridor_id]).df()
                logger.warning(
                    "Corridor %d: pre-ramp timestamp %s not found — using fallback",
                    corridor_id, ts
                )
            frames.append(row)

        df = pd.concat(frames, ignore_index=True)

        logger.info(
            "Demo mode — fetched varied buildup snapshot — %d corridors",
            len(df)
        )
        for _, row in df.iterrows():
            logger.info(
                "  Corridor %d (%s): %s — failure_event=%g",
                int(row['corridor_id']),
                row['corridor_name'],
                row['timestamp'],
                row['failure_event']
            )
        return df

    except Exception as e:
        logger.error("Failed to fetch demo telemetry: %s", e)
        raise
    finally:
        con.close()


def write_risk_scores(results_df: pd.DataFrame) -> None:
    """
    Writes failure probability predictions to the risk_scores table in DuckDB.
    Dashboard reads from this table for live display.
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
                scored_at               = now()
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


def run_inference(demo: bool = False) -> pd.DataFrame:
    """
    Loads telemetry, runs XGBoost inference, writes results to DuckDB.

    Args:
        demo: If True, uses the Dec 29 2025 collapse window instead of
              the latest timestamp. Pass --demo for presentations.

    Returns:
        pd.DataFrame sorted by failure_probability_pct descending.
    """
    logger.info("Starting live prediction engine%s...",
                " [DEMO MODE — Dec 29 2025]" if demo else "")

    # 1. Load telemetry
    if demo:
        live_df = get_demo_telemetry()
        logger.info(
            "Demo mode active — replaying Dec 29 2025 Benin-Onitsha collapse event"
        )
    else:
        live_df = get_latest_telemetry()

    if live_df.empty:
        logger.error("No telemetry data found in model_features.")
        raise ValueError("Empty telemetry — run the pipeline first.")

    # 2. Load trained model artifact
    # Load via xgb.Booster (not XGBClassifier) because train.py saves via
    # get_booster().save_model() to work around the imbalanced-learn/XGBoost
    # version conflict where SMOTE leaves _estimator_type undefined.
    # Raw Booster output is a margin score — sigmoid converts to [0,1],
    # mathematically identical to what predict_proba() does internally.
    if not os.path.exists(MODEL_PATH):
        logger.error(
            "Model artifact not found at %s — run model/train.py first.",
            MODEL_PATH
        )
        raise FileNotFoundError(f"Model not found: {MODEL_PATH}")

    booster = xgb.Booster()
    booster.load_model(MODEL_PATH)
    logger.info("Model artifact loaded from %s", MODEL_PATH)

    # 3. Load optimal decision threshold
    threshold = load_threshold()

    # 4. Run inference
    # Booster.predict() returns raw margin scores — sigmoid converts to [0,1]
    X_live        = live_df[FEATURE_COLS]
    dmatrix       = xgb.DMatrix(X_live)
    margins       = booster.predict(dmatrix, output_margin=True)
    probabilities = 1.0 / (1.0 + np.exp(-margins))   # sigmoid -> [0,1]

    # 5. Build results DataFrame
    results_df = live_df[['corridor_id', 'corridor_name', 'timestamp']].copy()
    results_df['failure_probability_pct'] = (probabilities * 100).round(2)
    results_df['at_risk'] = (probabilities >= threshold).astype(int)

    results_df = results_df.sort_values(
        by='failure_probability_pct', ascending=False
    ).reset_index(drop=True)

    # 6. Write to DuckDB
    write_risk_scores(results_df)

    print("\n--- LIVE GRID RISK ASSESSMENTS ---")
    print(f"Decision threshold: {threshold:.4f}  "
          f"({results_df['at_risk'].sum()} corridor(s) flagged AT RISK)\n")
    print(results_df[
        ['corridor_name', 'failure_probability_pct', 'at_risk', 'timestamp']
    ].to_string(index=False))

    logger.info(
        "Inference complete — top risk: %s at %.2f%% (threshold: %.4f)",
        results_df.iloc[0]['corridor_name'],
        results_df.iloc[0]['failure_probability_pct'],
        threshold,
    )

    return results_df


def main() -> None:
    """
    Main orchestration for live inference pipeline.

    Usage:
        python model/predict.py           # live mode — latest telemetry
        python model/predict.py --demo    # demo mode — Dec 29 2025 collapse
    """
    parser = argparse.ArgumentParser(description="GridGuard Inference Engine")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Replay the Dec 29 2025 Benin-Onitsha collapse event "
             "instead of using the latest telemetry. Use for presentations."
    )
    args = parser.parse_args()

    logger.info("=== Starting GridGuard Inference Engine%s ===",
                " [DEMO MODE]" if args.demo else "")
    try:
        run_inference(demo=args.demo)
        logger.info("=== Inference Engine Complete ===")
    except Exception as e:
        logger.error("=== Inference Engine Failed: %s ===", e)
        raise


if __name__ == "__main__":
    main()