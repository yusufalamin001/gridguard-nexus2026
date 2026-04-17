"""
model/predict.py

The Live Inference Engine.
Loads the trained XGBoost artifact and scores the latest available 
SCADA telemetry in the DuckDB database to generate live failure probabilities.
"""

import pandas as pd
import xgboost as xgb
import duckdb
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import DUCKDB_PATH
from logger import get_logger

logger = get_logger("predict")

def get_latest_telemetry() -> pd.DataFrame:
    con = duckdb.connect(DUCKDB_PATH)
    try:
        # Removed the 'AS' aliases so it pulls the exact names from DuckDB
        query = """
            WITH ranked_telemetry AS (
                SELECT 
                    corridor_id,
                    corridor_name,
                    timestamp,
                    voltage_kv,
                    volt_lag_1h,
                    volt_lag_2h,
                    volt_roll_avg_3h,
                    temperature,
                    temp_roll_avg_3h,
                    humidity,
                    hour_of_day,
                    ROW_NUMBER() OVER(PARTITION BY corridor_id ORDER BY timestamp DESC) as rn
                FROM model_features
            )
            SELECT * FROM ranked_telemetry WHERE rn = 1
        """
        df = con.execute(query).df()
        return df
    except Exception as e:
        logger.error("Failed to fetch latest telemetry: %s", e)
        raise
    finally:
        con.close()
        
def run_inference():
    logger.info("Starting live prediction engine...")
    
    # 1. Load the latest data
    live_df = get_latest_telemetry()
    if live_df.empty:
        logger.error("No telemetry data found.")
        return
        
    logger.info("Pulled latest telemetry for %d transmission corridors.", len(live_df))

    # 2. Load the trained model artifact
    model_path = "model/artifacts/xgboost_gridguard.json"
    if not os.path.exists(model_path):
        logger.error("Trained model artifact not found. Run model/train.py first.")
        return
        
    model = xgb.XGBClassifier()
    model.load_model(model_path)
    
    # 3. Prepare the feature columns (must match training exactly)
    feature_cols = [
        'voltage_kv', 'volt_lag_1h', 'volt_lag_2h', 'volt_roll_avg_3h',
        'temperature', 'temp_roll_avg_3h', 'humidity', 'hour_of_day'
    ]
    
    X_live = live_df[feature_cols]

    # 4. Generate Probabilities
    # predict_proba returns a 2D array: [Probability of 0, Probability of 1]
    # We slice [:, 1] to only keep the probability of failure.
    probabilities = model.predict_proba(X_live)[:, 1]
    
    # 5. Format the Output
    # We round the probability and convert it to a percentage for the dashboard
    live_df['failure_probability_pct'] = (probabilities * 100).round(2)
    
    # Create a clean output dataframe for the scoring layer
    results_df = live_df[['corridor_id', 'corridor_name', 'timestamp', 'failure_probability_pct']]
    
    print("\n--- LIVE GRID RISK ASSESSMENTS ---")
    print(results_df.sort_values(by='failure_probability_pct', ascending=False).to_string(index=False))
    
    logger.info("Inference complete.")
    return results_df

if __name__ == "__main__":
    run_inference()