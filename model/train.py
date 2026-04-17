"""
model/train.py

Trains an XGBoost classifier using a rigorous chronological split.
Trains on Oct/Nov data (3 failures) and evaluates on completely 
unseen Dec data (containing the Dec 29 Benin-Onitsha collapse).
"""

import pandas as pd
import xgboost as xgb
from sklearn.metrics import classification_report, confusion_matrix
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logger import get_logger

logger = get_logger("train")

def train_xgboost():
    logger.info("Loading engineered features from processed data...")
    
    try:
        df = pd.read_csv("data/processed/model_features.csv", parse_dates=['timestamp'])
    except FileNotFoundError:
        logger.error("model_features.csv not found.")
        return

    # 1. Chronological Train/Test Split
    # Train on October & November. Test on December.
    train_df = df[df['timestamp'] < '2025-12-01']
    test_df = df[df['timestamp'] >= '2025-12-01']
    
    logger.info("Chronological Split -> Train: %d rows, Test: %d rows", len(train_df), len(test_df))

 # Define features (Must match the CSV columns exactly)
    feature_cols = [
        'voltage_kv', 'volt_lag_1h', 'volt_lag_2h', 'volt_roll_avg_3h',
        'temperature', 'temp_roll_avg_3h', 'humidity', 'hour_of_day'
    ]
    target_col = 'failure_event'
    
    X_train, y_train = train_df[feature_cols], train_df[target_col]
    X_test, y_test = test_df[feature_cols], test_df[target_col]

    # 2. Handle Class Imbalance (on Training Data ONLY)
    num_normal = len(y_train[y_train == 0])
    num_failures = len(y_train[y_train == 1])
    imbalance_ratio = num_normal / num_failures if num_failures > 0 else 1
    
    logger.info("Training balance -> Normal: %d, Failures: %d", num_normal, num_failures)

    # 3. Train the Model
    logger.info("Training XGBoost Classifier...")
    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1,
        scale_pos_weight=imbalance_ratio,
        random_state=42,
        eval_metric='logloss'
    )
    
    model.fit(X_train, y_train)
    logger.info("Model training complete.")
    
    # 4. The True Evaluation
    logger.info("Evaluating model on UNSEEN December data...")
    predictions = model.predict(X_test)
    
    print("\n--- OUT-OF-TIME VALIDATION REPORT (DECEMBER UNSEEN DATA) ---")
    print(classification_report(y_test, predictions, target_names=["Normal", "Failure"]))
    
    # 5. Save the Model
    os.makedirs("model/artifacts", exist_ok=True)
    model_path = "model/artifacts/xgboost_gridguard.json"
    model.save_model(model_path)
    logger.info("Saved trained model artifact to %s", model_path)

if __name__ == "__main__":
    train_xgboost()