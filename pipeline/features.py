"""
pipeline/features.py

Engineers temporal and statistical features for the XGBoost model.
Joins weather and SCADA telemetry, then calculates lag variables 
and rolling averages to give the model historical context.
"""

import duckdb
from config import DUCKDB_PATH
from logger import get_logger

logger = get_logger("features")

def engineer_features() -> None:
    """
    Executes a complex SQL transformation to engineer ML features.
    Saves the output directly into a new 'model_features' table.
    """
    logger.info("Starting feature engineering using DuckDB window functions...")
    con = duckdb.connect(DUCKDB_PATH)
    
    try:
        # Create a clean slate for our feature table
        con.execute("DROP TABLE IF EXISTS model_features")
        
        # We use a CTAS (Create Table As Select) statement for blazing fast ETL
        feature_query = """
            CREATE TABLE model_features AS
            WITH joined_data AS (
                SELECT 
                    s.corridor_id,
                    c.name as corridor_name,
                    c.economic_loss_per_hr,
                    c.critical_infra_score,
                    s.timestamp,
                    s.voltage_kv,
                    s.frequency_hz,
                    w.temperature,
                    w.humidity,
                    s.failure_event,
                    -- Extract basic temporal cycles
                    EXTRACT(HOUR FROM s.timestamp) as hour_of_day,
                    EXTRACT(ISODOW FROM s.timestamp) as day_of_week
                FROM scada_telemetry s
                JOIN weather_readings w 
                    ON s.corridor_id = w.corridor_id AND s.timestamp = w.timestamp
                JOIN corridors c 
                    ON s.corridor_id = c.id
            )
            SELECT 
                *,
                -- 1-Hour and 2-Hour Lags (What happened recently?)
                LAG(voltage_kv, 1) OVER (PARTITION BY corridor_id ORDER BY timestamp) as volt_lag_1h,
                LAG(frequency_hz, 1) OVER (PARTITION BY corridor_id ORDER BY timestamp) as freq_lag_1h,
                LAG(voltage_kv, 2) OVER (PARTITION BY corridor_id ORDER BY timestamp) as volt_lag_2h,
                
                -- 3-Hour Rolling Averages (Is the system degrading over time?)
                AVG(temperature) OVER (
                    PARTITION BY corridor_id 
                    ORDER BY timestamp 
                    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
                ) as temp_roll_avg_3h,
                
                AVG(voltage_kv) OVER (
                    PARTITION BY corridor_id 
                    ORDER BY timestamp 
                    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
                ) as volt_roll_avg_3h
                
            FROM joined_data
            -- Exclude the first few rows per corridor where lag features are NULL
            QUALIFY volt_lag_2h IS NOT NULL 
        """
        
        con.execute(feature_query)
        
        # Verify row count
        count = con.execute("SELECT COUNT(*) FROM model_features").fetchone()[0]
        logger.info("Successfully engineered features. Created %d rows.", count)
        
    except Exception as e:
        logger.error("Feature engineering failed: %s", e)
        raise
    finally:
        con.close()

def main():
    logger.info("=== Starting Feature Engineering Pipeline ===")
    engineer_features()
    logger.info("=== Feature Engineering Completed Successfully ===")

if __name__ == "__main__":
    main()