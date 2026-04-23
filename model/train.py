"""
model/train.py

Trains an XGBoost classifier using a rigorous chronological split.
Trains on Oct/Nov data and evaluates on completely unseen Dec data
(containing the Dec 29 Benin-Onitsha collapse).

KEY FIX: Adds probability threshold tuning after training.
With only ~21 positive training examples (3 collapses × 7 hours),
the default 0.5 threshold is too high — the model outputs meaningful
probabilities well below 0.5 for risky corridors. We find the optimal
threshold using F1 score on the training set and save it alongside
the model artifact so predict.py uses the same cutoff.

Usage:
    python model/train.py
"""

import json
import os
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import classification_report, f1_score, precision_recall_curve

from config import DUCKDB_PATH, FEATURE_COLS, TARGET_COL
from logger import get_logger

logger = get_logger("train")

THRESHOLD_PATH = "model/artifacts/threshold.json"


def find_optimal_threshold(y_true, y_proba) -> float:
    """
    Finds the probability threshold that maximises F1 score.
    Uses precision-recall curve so we evaluate every meaningful cutoff.

    Returns:
        float: optimal threshold in [0, 1]
    """
    precisions, recalls, thresholds = precision_recall_curve(y_true, y_proba)

    # F1 at each threshold (thresholds has one fewer element than P/R arrays)
    f1_scores = []
    for p, r in zip(precisions[:-1], recalls[:-1]):
        f1 = (2 * p * r / (p + r)) if (p + r) > 0 else 0.0
        f1_scores.append(f1)

    best_idx       = int(np.argmax(f1_scores))
    best_threshold = float(thresholds[best_idx])
    best_f1        = f1_scores[best_idx]

    logger.info(
        "Optimal threshold: %.4f  (F1 = %.4f at index %d)",
        best_threshold, best_f1, best_idx
    )
    return best_threshold


def train_xgboost() -> None:
    """
    Loads engineered features, performs chronological train/test split,
    trains XGBoost classifier, tunes the decision threshold, evaluates
    on unseen December data, and saves both the model artifact and the
    optimal threshold to model/artifacts/.
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

    logger.info(
        "Loaded %d rows, %d failure-labelled hours total",
        len(df), int(df[TARGET_COL].sum())
    )

    # ── 1. Chronological Train/Test Split ────────────────────────────────────
    # Train: October + November (contains 3 failures × 7 hours = 21 positive rows)
    # Test:  December (unseen — contains the Dec 29 Benin-Onitsha collapse × 7 hours)
    train_df = df[df['timestamp'] <  '2025-12-01']
    test_df  = df[df['timestamp'] >= '2025-12-01']

    logger.info(
        "Chronological split — Train: %d rows (%d failures) | Test: %d rows (%d failures)",
        len(train_df), int(train_df[TARGET_COL].sum()),
        len(test_df),  int(test_df[TARGET_COL].sum())
    )

    X_train = train_df[FEATURE_COLS]
    y_train = train_df[TARGET_COL]
    X_test  = test_df[FEATURE_COLS]
    y_test  = test_df[TARGET_COL]

    # ── 2. Class Imbalance Weight ─────────────────────────────────────────────
    num_normal   = int((y_train == 0).sum())
    num_failures = int((y_train == 1).sum())
    imbalance_ratio = num_normal / num_failures if num_failures > 0 else 1.0

    logger.info(
        "Training class balance — Normal: %d | Failures: %d | scale_pos_weight: %.1f",
        num_normal, num_failures, imbalance_ratio
    )

    # ── 3. Train XGBoost ──────────────────────────────────────────────────────
    logger.info("Training XGBoost classifier...")
    model = xgb.XGBClassifier(
        n_estimators=200,          # more trees helps with small positive set
        max_depth=4,
        learning_rate=0.05,        # lower LR + more trees = better generalisation
        scale_pos_weight=imbalance_ratio,
        subsample=0.8,             # row sampling — reduces overfitting
        colsample_bytree=0.8,      # feature sampling per tree
        min_child_weight=1,        # allow splits on small positive class
        random_state=42,
        eval_metric='aucpr',       # area under PR curve — better than logloss
                                   # for imbalanced data
        use_label_encoder=False,
    )

    model.fit(X_train, y_train)
    logger.info("Model training complete")

    # ── 4. Threshold Tuning on Training Set ──────────────────────────────────
    # We tune on train (not test) to avoid leakage.
    # The threshold is then applied at inference time in predict.py.
    logger.info("Tuning decision threshold on training set probabilities...")
    train_probas   = model.predict_proba(X_train)[:, 1]
    optimal_threshold = find_optimal_threshold(y_train, train_probas)

    # Save threshold alongside model artifact
    os.makedirs("model/artifacts", exist_ok=True)
    with open(THRESHOLD_PATH, 'w') as f:
        json.dump({"threshold": optimal_threshold}, f, indent=2)
    logger.info("Threshold saved to %s", THRESHOLD_PATH)

    # ── 5. Out-of-Time Validation on December Data ────────────────────────────
    logger.info("Evaluating model on UNSEEN December data...")
    test_probas  = model.predict_proba(X_test)[:, 1]

    # Evaluate at BOTH default (0.5) and optimal threshold
    preds_default = (test_probas >= 0.5).astype(int)
    preds_optimal = (test_probas >= optimal_threshold).astype(int)

    print("\n--- OUT-OF-TIME VALIDATION — DEFAULT THRESHOLD (0.50) ---")
    print(classification_report(y_test, preds_default,
                                 target_names=["Normal", "Failure"]))

    print(f"\n--- OUT-OF-TIME VALIDATION — OPTIMAL THRESHOLD ({optimal_threshold:.4f}) ---")
    print(classification_report(y_test, preds_optimal,
                                 target_names=["Normal", "Failure"]))

    # Show the raw probabilities for the failure hours so we can verify signal
    failure_mask = y_test == 1
    if failure_mask.any():
        print("\n--- RAW PROBABILITIES ON KNOWN FAILURE HOURS ---")
        failure_proba_df = test_df[failure_mask][
            ['corridor_id', 'corridor_name', 'timestamp']
        ].copy()
        failure_proba_df['failure_proba'] = test_probas[failure_mask.values]
        print(failure_proba_df.to_string(index=False))

    logger.info(
        "Validation complete — optimal threshold: %.4f",
        optimal_threshold
    )

    # ── 6. Save Model Artifact ────────────────────────────────────────────────
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