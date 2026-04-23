"""
model/predict.py

Live inference engine for GridGuard.
Loads the trained XGBoost artifact and scores the latest available
SCADA telemetry to generate failure probabilities per corridor.
Writes results to the risk_scores table in DuckDB for the dashboard.

Usage:
    python model/predict.py
"""

import argparse
import json
import os
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
        Corridor 1 (Benin-Onitsha) → Dec 29 14:00  → fetched AT collapse    (~99%)
        Corridor 2 (Ikeja-Ota)     → Oct 15 15:00  → fetched 5hr before     (~97%)
        Corridor 3 (Kano-Kaduna)   → Nov 10 19:00  → fetched 9hr before     (~40-60%)
        Corridor 4 (Egbin-Lagos)   → Nov 25 14:00  → fetched 4hr before     (~50-70%)
        Corridors 5-8              → no collapse    → fetched at peak stress (~5-30%)

    The varying offsets expose the model's probability buildup curve rather than
    just the binary collapse moment — making the dashboard far more informative.

    Usage:
        python model/predict.py --demo
    """
    # (corridor_id, target_timestamp) — chosen to produce probability variety
    # Corridor 1: AT the Dec 29 collapse hour                → ~99.97%
    # Corridor 2: 1hr before Oct 15 collapse (14:00 → 15:00)→ ~97%
    # Corridor 3: 4hrs before Nov 10 collapse (15:00 → 19:00)→ intermediate
    # Corridor 4: 4hrs before Nov 25 collapse (10:00 → 14:00)→ intermediate
    DEMO_TARGETS = {
        1: '2025-12-29 14:00:00',  # AT collapse hour        → ~99.97%
        2: '2025-10-15 14:00:00',  # 1hr before collapse     → ~97%
        3: '2025-11-10 15:00:00',  # 4hrs before collapse    → ~40-70%
        4: '2025-11-25 10:00:00',  # 4hrs before collapse    → ~40-70%
    }

    con = duckdb.connect(DUCKDB_PATH)
    try:
        frames = []

        # For corridors with targeted timestamps — specific buildup offsets
        for corridor_id, ts in DEMO_TARGETS.items():
            row = con.execute("""
                SELECT * FROM model_features
                WHERE corridor_id = ?
                  AND timestamp = ?
                LIMIT 1
            """, [corridor_id, ts]).df()

            if row.empty:
                # Fallback: grab the closest available timestamp
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

        # For corridors without targeted timestamps — use their peak stress hour
        other_ids = [i for i in range(1, 9) if i not in DEMO_TARGETS]
        if other_ids:
            placeholders = ', '.join(['?' for _ in other_ids])
            peak_rows = con.execute(f"""
                WITH ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY corridor_id
                            ORDER BY
                                ABS(freq_deviation) DESC,
                                ABS(voltage_deviation) DESC
                        ) AS rn
                    FROM model_features
                    WHERE corridor_id IN ({placeholders})
                )
                SELECT * FROM ranked WHERE rn = 1
            """, other_ids).df()
            frames.append(peak_rows)

        df = pd.concat(frames, ignore_index=True)

        logger.info(
            "Demo mode — fetched varied buildup snapshot — %d corridors",
            len(df)
        )
        for _, row in df.iterrows():
            logger.info(
                "  Corridor %d (%s): %s — failure_event=%d",
                int(row['corridor_id']),
                row['corridor_name'],
                row['timestamp'],
                int(row['failure_event'])
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
        demo: If True, uses the Dec 29 2025 collapse window instead of the
              latest timestamp. Pass --demo at the command line for presentations.

    Returns:
        pd.DataFrame with corridor_id, corridor_name, timestamp,
        failure_probability_pct, and at_risk flag sorted by risk descending.
    """
    logger.info("Starting live prediction engine%s...",
                " [DEMO MODE — Dec 29 2025]" if demo else "")

    # 1. Load telemetry — live or demo window
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
    if not os.path.exists(MODEL_PATH):
        logger.error(
            "Model artifact not found at %s — run model/train.py first.",
            MODEL_PATH
        )
        raise FileNotFoundError(f"Model not found: {MODEL_PATH}")

    model = xgb.XGBClassifier()
    model.load_model(MODEL_PATH)
    logger.info("Model artifact loaded from %s", MODEL_PATH)

    # 3. Load optimal decision threshold
    threshold = load_threshold()

    # 4. Run inference
    # predict_proba returns [[P(0), P(1)], ...] — we take column 1 (failure)
    X_live        = live_df[FEATURE_COLS]
    probabilities = model.predict_proba(X_live)[:, 1]

    # 5. Build results DataFrame
    results_df = live_df[['corridor_id', 'corridor_name', 'timestamp']].copy()
    results_df['failure_probability_pct'] = (probabilities * 100).round(2)

    # at_risk flag uses the optimised threshold — not the raw 50% default
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
        python model/predict.py --demo    # demo mode — Dec 29 2025 collapse event
    """
    parser = argparse.ArgumentParser(description="GridGuard Inference Engine")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run in demo mode — replays the Dec 29 2025 Benin-Onitsha collapse event "
             "instead of using the latest telemetry timestamp. Use this for presentations."
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