"""
model/train.py

Trains an XGBoost classifier using a rigorous chronological split.
Trains on Oct/Nov data and evaluates on completely unseen Dec data
(containing the Dec 29 Benin-Onitsha collapse).

KEY DESIGN:
1. Soft labels — failure_event contains graduated values (0.20-1.0) for
   ramp hours and fractional values (0.25-0.28) for ambient stress corridors.
   XGBoost binary:logistic handles fractional targets natively via its loss
   function: loss = -y*log(p) - (1-y)*log(1-p). This produces a graduated
   probability boundary rather than a cliff edge.

2. No SMOTE — soft labels give XGBoost the gradient it needs directly.
   scale_pos_weight compensates for class imbalance instead.

3. Threshold tuning — precision-recall curve finds optimal cutoff and
   saves to model/artifacts/threshold.json.
"""

import json
import os
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import classification_report, precision_recall_curve

from config import DUCKDB_PATH, FEATURE_COLS, TARGET_COL
from logger import get_logger

logger = get_logger("train")

THRESHOLD_PATH = "model/artifacts/threshold.json"


def find_optimal_threshold(y_true, y_proba) -> float:
    """
    Finds the probability threshold that maximises F1 on the training set.
    Uses precision-recall curve to evaluate every meaningful cutoff.
    """
    precisions, recalls, thresholds = precision_recall_curve(y_true, y_proba)
    f1_scores = []
    for p, r in zip(precisions[:-1], recalls[:-1]):
        f1 = (2 * p * r / (p + r)) if (p + r) > 0 else 0.0
        f1_scores.append(f1)

    best_idx       = int(np.argmax(f1_scores))
    best_threshold = float(thresholds[best_idx])
    best_f1        = f1_scores[best_idx]

    logger.info("Optimal threshold: %.4f  (F1 = %.4f)", best_threshold, best_f1)
    return best_threshold


def train_xgboost() -> None:
    """
    Loads engineered features, performs chronological train/test split,
    applies SMOTE oversampling, trains XGBoost, tunes threshold, and
    saves both model artifact and threshold to model/artifacts/.
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

    logger.info("Loaded %d rows", len(df))

    # ── 1. Chronological split ────────────────────────────────────────────────
    # Train: Oct + Nov | Test: December (unseen)
    train_df = df[df['timestamp'] <  '2025-12-01']
    test_df  = df[df['timestamp'] >= '2025-12-01']

    X_train_raw = train_df[FEATURE_COLS]
    X_test      = test_df[FEATURE_COLS]
    y_test      = test_df[TARGET_COL]

    # ── 2. Use soft labels directly — no SMOTE needed ─────────────────────
    # XGBoost binary:logistic handles fractional targets natively.
    # y=0.82 pulls the prediction toward 82%, y=0.30 toward 30%.
    # This gives XGBoost the gradient to learn a graduated boundary.
    y_train = train_df[TARGET_COL]   # fractional 0.0–1.0

    # Binary version for scale_pos_weight and threshold evaluation
    y_train_binary = (y_train >= 0.5).astype(int)
    num_normal   = int((y_train_binary == 0).sum())
    num_failures = int((y_train_binary == 1).sum())

    logger.info(
        "Training set — Normal: %d | Stressed/Failure (>=0.5): %d | Ratio: %.1f",
        num_normal, num_failures, num_normal / num_failures
    )

    # ── 3. Train XGBoost on soft labels via raw Booster API ───────────────
    # XGBClassifier (sklearn API) rejects fractional labels — it expects
    # integer class indices. xgb.train() with DMatrix accepts fractional
    # targets directly, treating them as expected probabilities in the
    # binary:logistic loss: loss = -y*log(p) - (1-y)*log(1-p).
    logger.info("Training XGBoost booster with soft labels...")
    dtrain = xgb.DMatrix(X_train_raw, label=y_train)

    params = {
        "objective":        "binary:logistic",
        "eval_metric":      "aucpr",
        "max_depth":        4,
        "learning_rate":    0.05,
        "scale_pos_weight": num_normal / num_failures,
        "subsample":        0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 1,
        "seed":             42,
    }
    booster = xgb.train(params, dtrain, num_boost_round=300)
    logger.info("Model training complete")

    # ── 4. Threshold tuning on training set ──────────────────────────────────
    logger.info("Tuning decision threshold...")
    # Raw booster outputs probabilities directly (binary:logistic applies
    # sigmoid internally) — no need for predict_proba()
    train_probas      = booster.predict(dtrain)
    optimal_threshold = find_optimal_threshold(y_train_binary, train_probas)

    os.makedirs("model/artifacts", exist_ok=True)
    with open(THRESHOLD_PATH, 'w') as f:
        json.dump({"threshold": optimal_threshold}, f, indent=2)
    logger.info("Threshold saved to %s", THRESHOLD_PATH)

    # ── 5. Out-of-time validation on December data ────────────────────────────
    y_test_binary = (y_test >= 0.5).astype(int)

    logger.info("Evaluating on UNSEEN December data...")
    dtest       = xgb.DMatrix(X_test)
    test_probas = booster.predict(dtest)
    preds_default = (test_probas >= 0.5).astype(int)
    preds_optimal = (test_probas >= optimal_threshold).astype(int)

    print("\n--- OUT-OF-TIME VALIDATION — DEFAULT THRESHOLD (0.50) ---")
    print(classification_report(y_test_binary, preds_default,
                                 target_names=["Normal", "Failure"]))

    print(f"\n--- OUT-OF-TIME VALIDATION — OPTIMAL THRESHOLD ({optimal_threshold:.4f}) ---")
    print(classification_report(y_test_binary, preds_optimal,
                                 target_names=["Normal", "Failure"],
                                 zero_division=0))

    # Raw probabilities on known failure hours
    failure_mask = y_test_binary == 1
    if failure_mask.any():
        print("\n--- RAW PROBABILITIES ON KNOWN FAILURE HOURS ---")
        failure_proba_df = test_df[failure_mask.values][
            ['corridor_id', 'corridor_name', 'timestamp']
        ].copy()
        failure_proba_df['failure_proba'] = test_probas[failure_mask.values]
        print(failure_proba_df.to_string(index=False))

    # ── 6. Save model ─────────────────────────────────────────────────────────
    model_path = "model/artifacts/xgboost_gridguard.json"
    booster.save_model(model_path)
    logger.info("Model artifact saved to %s", model_path)


def main() -> None:
    logger.info("=== Starting XGBoost Training Pipeline ===")
    try:
        train_xgboost()
        logger.info("=== Training Pipeline Complete ===")
    except Exception as e:
        logger.error("=== Training Pipeline Failed: %s ===", e)
        raise


if __name__ == "__main__":
    main()