"""
pipeline/simulate.py

Generates synthetic, weather-correlated SCADA telemetry for GridGuard.
Reads the temporal backbone from weather_readings and injects physical
grid behaviors (voltage sag, frequency instability) and targeted failure events.

KEY FIXES:
1. Expanded FAILURE_EVENTS — corridors 5-8 now each have one collapse event
   in the training window (Oct/Nov), giving the model failure history for
   every corridor. Positive training examples: 21 → 56.

2. Soft failure labels — ambient stress corridors (5-8) now carry fractional
   failure_event values (0.25-0.28) on every normal hour. XGBoost handles
   soft labels natively. This teaches the model that chronic stress = elevated
   risk rather than zero risk, producing a graduated probability spread.

3. Degradation ramp — unchanged. Each collapse is preceded by 6 hours of
   progressive voltage/frequency degradation.
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from config import DUCKDB_PATH
from logger import get_logger

logger = get_logger("simulate")


# ── Failure definitions ───────────────────────────────────────────────────────
FAILURE_EVENTS = [
    # Original NERC-validated events
    (2, 10, 15, 15),   # Ikeja-Ota:           Oct 15 at 3 PM  (heat stress)
    (3, 11, 10, 19),   # Kano-Kaduna:         Nov 10 at 7 PM  (peak load)
    (4, 11, 25, 14),   # Egbin-Lagos:         Nov 25 at 2 PM  (heat stress)
    (1, 12, 29, 14),   # Benin-Onitsha:       Dec 29 at 2 PM  (NERC confirmed)
    # New synthetic events — corridors 5-8 in training window (Oct/Nov)
    (5, 10, 22, 16),   # Shiroro-Gwagwalada:  Oct 22 at 4 PM  (aging infrastructure)
    (6, 11,  3, 20),   # Jebba-Olorunsogo:   Nov  3 at 8 PM  (peak load + line loss)
    (7, 10, 28, 13),   # Afam-Port Harcourt: Oct 28 at 1 PM  (coastal humidity)
    (8, 11, 18, 15),   # Kainji-Birnin Kebbi:Nov 18 at 3 PM  (hydro shortfall)
]

DEGRADATION_RAMP = {
    -6: (0.98, -0.05),
    -5: (0.97, -0.08),
    -4: (0.96, -0.12),
    -3: (0.94, -0.18),
    -2: (0.90, -0.25),
    -1: (0.82, -0.40),
}

# Graduated soft labels for ramp hours — teaches XGBoost that risk builds
# progressively toward collapse rather than jumping from 0 to 1.
# Hours -3 to -1 (≥0.50) are treated as class 1 by the SMOTE threshold.
# Hours -4 to -6 (<0.50) are class 0 but carry degraded features,
# giving the model a gradient to learn moderate risk from.
RAMP_SOFT_LABELS = {
    -6: 0.20,
    -5: 0.28,
    -4: 0.38,
    -3: 0.52,
    -2: 0.68,
    -1: 0.82,
}

# Format: corridor_id → (voltage_mult, freq_delta, soft_label)
# soft_label is fractional failure_event — teaches XGBoost elevated baseline risk
AMBIENT_STRESS = {
    5: (0.965, -0.08, 0.28),
    6: (0.972, -0.06, 0.25),
    7: (0.978, -0.10, 0.27),
    8: (0.968, -0.07, 0.26),
}


def _build_failure_lookup() -> dict:
    lookup = {}
    for corridor_id, month, day, collapse_hour in FAILURE_EVENTS:
        # Collapse hour: hard label 1.0
        lookup[(corridor_id, month, day, collapse_hour)] = (0.70, -1.80, 1.0)
        collapse_dt = datetime(2025, month, day, collapse_hour)
        for hours_before, (volt_mult, freq_delta) in DEGRADATION_RAMP.items():
            ramp_dt = collapse_dt + timedelta(hours=hours_before)
            key = (corridor_id, ramp_dt.month, ramp_dt.day, ramp_dt.hour)
            if key not in lookup:
                # Graduated soft label — risk grows as collapse approaches
                lookup[key] = (volt_mult, freq_delta, RAMP_SOFT_LABELS[hours_before])
    return lookup


def extract_baseline_data() -> pd.DataFrame:
    logger.info("Extracting baseline weather and corridor data from DuckDB...")
    query = """
        SELECT
            w.corridor_id,
            c.name  AS corridor_name,
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
    logger.info("Generating synthetic SCADA telemetry based on thermal physics...")

    failure_lookup = _build_failure_lookup()
    total_expected = len(FAILURE_EVENTS) * (1 + len(DEGRADATION_RAMP))
    logger.info("Failure lookup built — %d anomaly hours across %d events",
                total_expected, len(FAILURE_EVENTS))

    voltages       = []
    frequencies    = []
    failure_events = []

    np.random.seed(42)

    for _, row in df.iterrows():
        hour     = row['timestamp'].hour
        month    = row['timestamp'].month
        day      = row['timestamp'].day
        temp     = row['temperature'] or 28.0
        humidity = row.get('humidity') or 70.0

        base_voltage = 330.0 if row['corridor_id'] in [1, 3, 5, 6, 8] else 132.0

        current_voltage = base_voltage
        if temp > 35:
            current_voltage = base_voltage * 0.95
        elif temp > 30:
            current_voltage = base_voltage * 0.98
        if humidity > 80 and temp > 30:
            current_voltage *= 0.97

        current_freq = 50.0 + np.random.uniform(-0.2, 0.2)
        if humidity > 80 and temp > 30:
            current_freq -= 0.1

        lookup_key = (row['corridor_id'], month, day, hour)

        if lookup_key in failure_lookup:
            volt_mult, freq_delta, soft_label = failure_lookup[lookup_key]
            is_collapse = soft_label == 1.0
            if is_collapse:
                current_voltage = base_voltage * volt_mult
                current_freq    = 48.2
            else:
                current_voltage = current_voltage * volt_mult
                current_freq    = current_freq + freq_delta
            failure_events.append(soft_label)  # graduated: 0.20 → 0.82 → 1.0

        elif row['corridor_id'] in AMBIENT_STRESS:
            volt_mult, freq_delta, soft_label = AMBIENT_STRESS[row['corridor_id']]
            current_voltage = current_voltage * volt_mult
            current_freq    = current_freq + freq_delta
            failure_events.append(soft_label)

        else:
            failure_events.append(0.0)

        voltages.append(round(current_voltage, 2))
        frequencies.append(round(current_freq, 2))

    df['voltage_kv']    = voltages
    df['frequency_hz']  = frequencies
    df['failure_event'] = failure_events

    hard_failures = sum(1 for x in failure_events if x == 1.0)
    soft_stress   = sum(1 for x in failure_events if 0.0 < x < 1.0)
    logger.info("Telemetry generated — %d hard failure hours, %d soft stress hours",
                hard_failures, soft_stress)
    return df


def load_scada_data(df: pd.DataFrame) -> None:
    logger.info("Loading SCADA telemetry into DuckDB...")
    con = duckdb.connect(DUCKDB_PATH)
    try:
        # failure_event is DOUBLE (not INTEGER) to support soft labels
        con.execute("""
            CREATE TABLE IF NOT EXISTS scada_telemetry (
                corridor_id   INTEGER   NOT NULL,
                timestamp     TIMESTAMP NOT NULL,
                voltage_kv    DOUBLE,
                frequency_hz  DOUBLE,
                failure_event DOUBLE,
                created_at    TIMESTAMP DEFAULT current_timestamp,
                PRIMARY KEY (corridor_id, timestamp),
                FOREIGN KEY (corridor_id) REFERENCES corridors(id)
            )
        """)
        con.execute("""
            INSERT INTO scada_telemetry
                (corridor_id, timestamp, voltage_kv, frequency_hz, failure_event)
            SELECT corridor_id, timestamp, voltage_kv, frequency_hz, failure_event
            FROM df
            ON CONFLICT (corridor_id, timestamp)
            DO UPDATE SET
                voltage_kv    = EXCLUDED.voltage_kv,
                frequency_hz  = EXCLUDED.frequency_hz,
                failure_event = EXCLUDED.failure_event
        """)
        logger.info("Successfully loaded scada_telemetry table.")
    except Exception as e:
        logger.error("Failed to load SCADA data: %s", e)
        raise
    finally:
        con.close()


def main():
    logger.info("=== Starting SCADA Simulation Pipeline ===")
    try:
        df_baseline  = extract_baseline_data()
        df_simulated = generate_telemetry(df_baseline)
        load_scada_data(df_simulated)
        logger.info("=== SCADA Simulation Complete — %d records ===",
                    len(df_simulated))
    except Exception as e:
        logger.error("=== SCADA Simulation Failed: %s ===", e)
        raise


if __name__ == "__main__":
    main()