"""
model/train.py

Trains an XGBoost classifier using a rigorous chronological split.
Trains on Oct/Nov data (3 failures) and evaluates on completely
unseen Dec data (containing the Dec 29 Benin-Onitsha collapse).

Usage:
    python model/train.py
"""

import os
import pandas as pd
import xgboost as xgb
from sklearn.metrics import classification_report

from config import DUCKDB_PATH, FEATURE_COLS, TARGET_COL
from logger import get_logger

logger = get_logger("train")


def train_xgboost() -> None:
    """
    Loads engineered features, performs chronological train/test split,
    trains XGBoost classifier, evaluates on unseen December data,
    and saves the model artifact to model/artifacts/xgboost_gridguard.json.
    """
    logger.info("Loading engineered features from processed data...")

    try:
        df = pd.read_csv(
            "data/processed/model_features.csv",
            parse_dates=['timestamp']
        )
    except FileNotFoundError:
        logger.error("model_features.csv not found. Run pipeline/features.py first.")
        raise

    logger.info("Loaded %d rows, %d failure events total",
                len(df), int(df[TARGET_COL].sum()))

    # ── 1. Chronological Train/Test Split ─────────────────────────────
    # Train on October + November (in-sample failures)
    # Test on December (unseen — contains the Dec 29 collapse)
    train_df = df[df['timestamp'] <  '2025-12-01']
    test_df  = df[df['timestamp'] >= '2025-12-01']

    logger.info(
        "Chronological split — Train: %d rows | Test: %d rows",
        len(train_df), len(test_df)
    )

    X_train = train_df[FEATURE_COLS]
    y_train = train_df[TARGET_COL]
    X_test  = test_df[FEATURE_COLS]
    y_test  = test_df[TARGET_COL]

    # ── 2. Handle Class Imbalance ──────────────────────────────────────
    # Grid collapses are rare — this weight tells XGBoost to treat
    # failure events as proportionally more important during training
    num_normal   = int((y_train == 0).sum())
    num_failures = int((y_train == 1).sum())
    imbalance_ratio = num_normal / num_failures if num_failures > 0 else 1

    logger.info(
        "Training class balance — Normal: %d | Failures: %d | Ratio: %.1f",
        num_normal, num_failures, imbalance_ratio
    )

    # ── 3. Train the Model ─────────────────────────────────────────────
    logger.info("Training XGBoost classifier...")
    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1,
        scale_pos_weight=imbalance_ratio,
        random_state=42,
        eval_metric='logloss',
        use_label_encoder=False,
    )

    model.fit(X_train, y_train)
    logger.info("Model training complete")

    # ── 4. Out-of-Time Validation on December Data ────────────────────
    logger.info("Evaluating model on UNSEEN December data...")
    predictions = model.predict(X_test)

    report = classification_report(
        y_test, predictions,
        target_names=["Normal", "Failure"]
    )
    print("\n--- OUT-OF-TIME VALIDATION REPORT (DECEMBER UNSEEN DATA) ---")
    print(report)
    logger.info("Validation report:\n%s", report)

    # ── 5. Save Model Artifact ─────────────────────────────────────────
    os.makedirs("model/artifacts", exist_ok=True)
    model_path = "model/artifacts/xgboost_gridguard.json"
    model.save_model(model_path)
    logger.info("Model artifact saved to %s", model_path)


def main() -> None:
    """
    Main orchestration for model training pipeline.
    """
    logger.info("=== Starting XGBoost Training Pipeline ===")
    try:
        train_xgboost()
        logger.info("=== Training Pipeline Complete ===")
    except Exception as e:
        logger.error("=== Training Pipeline Failed: %s ===", e)
        raise


if __name__ == "__main__":
    main()
