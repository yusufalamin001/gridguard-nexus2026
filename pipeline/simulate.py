"""
pipeline/simulate.py

Generates synthetic, weather-correlated SCADA telemetry for GridGuard.
Reads the temporal backbone from weather_readings and injects physical 
grid behaviors (voltage sag, frequency instability) and targeted failure events.
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime

from config import DUCKDB_PATH
from logger import get_logger

logger = get_logger("simulate")

def extract_baseline_data() -> pd.DataFrame:
    """
    Pulls the weather readings and corridor metadata from DuckDB.
    """
    logger.info("Extracting baseline weather and corridor data from DuckDB...")
    
    query = """
        SELECT 
            w.corridor_id,
            c.name AS corridor_name,
            c.disco_name,
            w.timestamp,
            w.temperature,
            w.humidity
        FROM weather_readings w
        JOIN corridors c ON w.corridor_id = c.id
        ORDER BY w.corridor_id, w.timestamp
    """
    
    con = duckdb.connect(DUCKDB_PATH)
    try:
        df = con.execute(query).df()
        logger.info("Successfully extracted %d baseline records.", len(df))
        return df
    except Exception as e:
        logger.error("Failed to extract baseline data: %s", e)
        raise
    finally:
        con.close()

def generate_telemetry(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies physical grid logic to generate voltage and frequency data.
    Injects thermal penalties, peak load sag, and specific failure events.
    """
    logger.info("Generating synthetic SCADA telemetry based on thermal physics...")
    
    # Initialize arrays for new columns
    voltages = []
    frequencies = []
    failure_events = []
    
    # Use a fixed random seed so our pipeline generates the exact same data every time
    np.random.seed(42)
    
    for index, row in df.iterrows():
        hour = row['timestamp'].hour
        temp = row['temperature'] or 28.0  # Guard against None — Nigerian average fallback
        humidity = row.get('humidity') or 70.0  # Guard against None — Nigerian average fallback
        
        # 1. Base Physics — 330kV corridors: 1,3,5,6,8 | 132kV corridors: 2,4,7
        if row['corridor_id'] in [1, 3, 5, 6, 8]:
            base_voltage = 330.0
        else:
            base_voltage = 132.0
            
        # 2. Thermal Derating (Resistance increases with heat)
        current_voltage = base_voltage
        if temp > 35:
            current_voltage = base_voltage * 0.95  # 5% sag
        elif temp > 30:
            current_voltage = base_voltage * 0.98  # 2% sag

        # 2b. Humidity modifier — hot + humid compounds insulation stress
        if humidity > 80 and temp > 30:
            current_voltage *= 0.97  # additional 3% sag under hot + humid conditions

        # 3. Base Frequency using numpy RNG for reproducibility
        current_freq = 50.0 + np.random.uniform(-0.2, 0.2)

        # 3b. Frequency instability under high humidity + heat
        if humidity > 80 and temp > 30:
            current_freq -= 0.1
        
        # 4. Target Injections (Historical Failures)
        anomalies = [
            (2, 10, 15, 15),  # Ikeja-Ota: Oct 15 at 3 PM (Heat stress)
            (3, 11, 10, 19),  # Kano-Kaduna: Nov 10 at 7 PM (Peak load failure)
            (4, 11, 25, 14),  # Egbin-Lagos Island: Nov 25 at 2 PM (Heat stress)
            (1, 12, 29, 14)   # Benin-Onitsha: Dec 29 at 2 PM (The ultimate test case)
        ]
        
        is_collapse_hour = (
            row['corridor_id'], 
            row['timestamp'].month, 
            row['timestamp'].day, 
            hour
        ) in anomalies
        
        if is_collapse_hour:
            current_voltage = base_voltage * 0.70  # Massive 30% voltage drop
            current_freq = 48.2  # Severe decay
            failure_events.append(1)
        else:
            failure_events.append(0)
            
        # 5. THE MISSING APPENDS - Saving the calculations to the lists
        voltages.append(round(current_voltage, 2))
        frequencies.append(round(current_freq, 2))
        
    # Bind the generated data back into the Pandas DataFrame
    df['voltage_kv'] = voltages
    df['frequency_hz'] = frequencies
    df['failure_event'] = failure_events
    
    logger.info("Successfully generated telemetry. Injected %d failure events.", sum(failure_events))
    return df

def load_scada_data(df: pd.DataFrame) -> None:
    """
    Loads the simulated SCADA telemetry back into DuckDB idempotently.
    """
    logger.info("Loading SCADA telemetry into DuckDB...")
    con = duckdb.connect(DUCKDB_PATH)
    try:
        # Create the schema
        con.execute("""
            CREATE TABLE IF NOT EXISTS scada_telemetry (
                corridor_id INTEGER NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                voltage_kv DOUBLE,
                frequency_hz DOUBLE,
                failure_event INTEGER,
                created_at TIMESTAMP DEFAULT current_timestamp,
                PRIMARY KEY (corridor_id, timestamp),
                FOREIGN KEY (corridor_id) REFERENCES corridors(id)
            )
        """)
        
        # Explicit Upsert (Idempotent loading)
        con.execute("""
            INSERT INTO scada_telemetry (corridor_id, timestamp, voltage_kv, frequency_hz, failure_event)
            SELECT corridor_id, timestamp, voltage_kv, frequency_hz, failure_event
            FROM df
            ON CONFLICT (corridor_id, timestamp) 
            DO UPDATE SET 
                voltage_kv = EXCLUDED.voltage_kv,
                frequency_hz = EXCLUDED.frequency_hz,
                failure_event = EXCLUDED.failure_event
        """)
        logger.info("Successfully loaded scada_telemetry table.")
    except Exception as e:
        logger.error("Failed to load SCADA data: %s", e)
        raise
    finally:
        con.close()

def main():
    """
    Main orchestration function for the SCADA simulation pipeline.
    """
    logger.info("=== Starting SCADA Simulation Pipeline ===")
    try:
        df_baseline = extract_baseline_data()
        df_simulated = generate_telemetry(df_baseline)
        load_scada_data(df_simulated)
        logger.info(
            "=== SCADA Simulation Complete — %d records, %d failure events ===",
            len(df_simulated),
            int(df_simulated['failure_event'].sum())
        )
    except Exception as e:
        logger.error("=== SCADA Simulation Failed: %s ===", e)
        raise

if __name__ == "__main__":
    main()