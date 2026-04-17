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
    
    for idx, row in df.iterrows():
        # 1. Base values based on line type (330kV vs 132kV)
        base_voltage = 330.0 if "330" in row['corridor_name'] else 132.0
        base_freq = 50.0
        temp = row['temperature'] if pd.notnull(row['temperature']) else 25.0
        
        # 2. Daily Peak Load Penalty (6 PM to 10 PM)
        # As everyone goes home and turns on appliances, load spikes and voltage drops
        hour = row['timestamp'].hour
        is_peak = 18 <= hour <= 22
        peak_multiplier = 0.95 if is_peak else 1.0 
        
        # 3. Thermal Penalty (Resistance & De-rating)
        if temp >= 35.0:
            thermal_penalty = 0.88  # 12% drop due to severe heat/resistance
        elif temp >= 30.0:
            thermal_penalty = 0.96  # 4% drop
        else:
            thermal_penalty = 1.0
            
        # Add slight statistical noise to simulate real sensor variance
        v_noise = np.random.normal(0, base_voltage * 0.01)
        f_noise = np.random.normal(0, 0.15) 
        
        # Calculate normal operational telemetry
        current_voltage = (base_voltage * peak_multiplier * thermal_penalty) + v_noise
        current_freq = base_freq + f_noise
        
        # 4. The December 29th Collapse Injection (Benin-Onitsha, ID: 1)
        # We simulate the cascading breaker failure triggering at 2 PM (14:00) during peak heat
        is_collapse_day = (
            row['corridor_id'] == 1 and 
            row['timestamp'].month == 12 and 
            row['timestamp'].day == 29 and
            hour == 14  
        )
        
        if is_collapse_day:
            current_voltage = base_voltage * 0.70  # Massive 30% voltage drop
            current_freq = 48.2  # Severe decay, well outside NERC 49.75Hz safe band
            failure_events.append(1)
        else:
            failure_events.append(0)
            
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
    
    df_baseline = extract_baseline_data()
    df_simulated = generate_telemetry(df_baseline)
    load_scada_data(df_simulated)
    
    logger.info("=== SCADA Simulation Completed Successfully ===")

if __name__ == "__main__":
    main()