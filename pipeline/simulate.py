"""
pipeline/simulate.py

Generates synthetic, weather-correlated SCADA telemetry for GridGuard.
Reads the temporal backbone from weather_readings and injects physical
grid behaviors (voltage sag, frequency instability) and targeted failure events.

KEY FIX: Each failure event is now preceded by a 6-hour degradation ramp
so the model has genuine pre-failure signal to learn from — not just a single
point-in-time collapse surrounded by normal readings.

Degradation schedule per failure (hours before collapse → severity):
    -6h : voltage 98%, freq -0.05 Hz  (early thermal stress)
    -5h : voltage 97%, freq -0.08 Hz
    -4h : voltage 96%, freq -0.12 Hz
    -3h : voltage 94%, freq -0.18 Hz
    -2h : voltage 90%, freq -0.25 Hz  (visible instability)
    -1h : voltage 82%, freq -0.40 Hz  (pre-collapse)
     0h : voltage 70%, freq  48.20 Hz (collapse — failure_event = 1)
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from config import DUCKDB_PATH
from logger import get_logger

logger = get_logger("simulate")


# ── Failure definitions ───────────────────────────────────────────────────────
# Each tuple: (corridor_id, month, day, collapse_hour)
FAILURE_EVENTS = [
    (2, 10, 15, 15),   # Ikeja-Ota:        Oct 15 at 3 PM  (heat stress)
    (3, 11, 10, 19),   # Kano-Kaduna:      Nov 10 at 7 PM  (peak load)
    (4, 11, 25, 14),   # Egbin-Lagos:      Nov 25 at 2 PM  (heat stress)
    (1, 12, 29, 14),   # Benin-Onitsha:    Dec 29 at 2 PM  (ultimate test case)
]

# Degradation ramp: hours_before_collapse → (voltage_multiplier, freq_delta)
# Hour 0 (the collapse itself) is handled separately below
DEGRADATION_RAMP = {
    -6: (0.98, -0.05),
    -5: (0.97, -0.08),
    -4: (0.96, -0.12),
    -3: (0.94, -0.18),
    -2: (0.90, -0.25),
    -1: (0.82, -0.40),
}

# ── Ambient stress signatures for corridors 5–8 ───────────────────────────────
# These corridors don't collapse but carry persistent mild thermal stress
# that produces meaningful non-zero probability scores at inference time.
# Format: corridor_id → (voltage_mult, freq_delta)
# Applied continuously to ALL hours for these corridors so the model
# sees a realistic spread rather than perfectly clean readings.
#
# Physical rationale:
#   Corridor 5 (Shiroro-Gwagwalada 330kV) — aging infrastructure, frequent
#     partial outages in NERC data, mild persistent sag
#   Corridor 6 (Jebba-Olorunsogo 330kV)  — long transmission distance,
#     line losses create chronic low-voltage signature
#   Corridor 7 (Afam-Port Harcourt 132kV) — coastal humidity, salt
#     corrosion on insulators, elevated frequency instability
#   Corridor 8 (Kainji-Birnin Kebbi 330kV) — hydro-dependent, seasonal
#     water level variations cause generation shortfalls
AMBIENT_STRESS = {
    5: (0.965, -0.08),   # 3.5% voltage sag, slight freq drag
    6: (0.972, -0.06),   # 2.8% sag, minor instability
    7: (0.978, -0.10),   # 2.2% sag, higher freq noise (coastal)
    8: (0.968, -0.07),   # 3.2% sag, hydro variability
}

# ── Demo targets for corridors 3 & 4 ─────────────────────────────────────────
# Shifted to the pre-buildup window — 3 hours BEFORE the degradation ramp
# begins, so the model shows rising probability rather than already-high scores.
# This gives a more realistic and visually compelling dashboard spread.
# Format: (corridor_id, month, day, hour)
DEMO_TARGETS = {
    3: datetime(2025, 11, 10, 13),   # 6h before Kano-Kaduna ramp starts at 13:00
    4: datetime(2025, 11, 25, 8),    # 6h before Egbin-Lagos ramp starts at 8:00
}


def _build_failure_lookup() -> dict:
    """
    Pre-computes a lookup dict mapping (corridor_id, month, day, hour) to
    a (voltage_multiplier, freq_delta, is_collapse) tuple for fast O(1)
    access inside the row-level loop.
    """
    lookup = {}

    for corridor_id, month, day, collapse_hour in FAILURE_EVENTS:
        # Collapse hour itself
        lookup[(corridor_id, month, day, collapse_hour)] = (0.70, -1.80, True)

        # Degradation ramp hours leading up to the collapse
        collapse_dt = datetime(2025, month, day, collapse_hour)
        for hours_before, (volt_mult, freq_delta) in DEGRADATION_RAMP.items():
            ramp_dt = collapse_dt + timedelta(hours=hours_before)
            key = (corridor_id, ramp_dt.month, ramp_dt.day, ramp_dt.hour)
            # Don't overwrite the collapse hour if ramp somehow lands on it
            if key not in lookup:
                lookup[key] = (volt_mult, freq_delta, False)

    return lookup


def extract_baseline_data() -> pd.DataFrame:
    """
    Pulls the weather readings and corridor metadata from DuckDB.
    """
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
    """
    Applies physical grid logic to generate voltage and frequency data.

    Injects:
      - Thermal derating (voltage sag under heat / humidity)
      - Frequency instability under hot + humid conditions
      - 6-hour progressive degradation ramp before each failure event
      - Point-in-time collapse at the failure hour (failure_event = 1)

    Ramp hours are labelled failure_event = 1 as well so the model
    learns pre-failure patterns, not just the instant of collapse.
    """
    logger.info("Generating synthetic SCADA telemetry based on thermal physics...")

    # Pre-build failure lookup for O(1) access per row
    failure_lookup = _build_failure_lookup()
    total_expected = len(FAILURE_EVENTS) * (1 + len(DEGRADATION_RAMP))
    logger.info(
        "Failure lookup built — %d anomaly hours across %d events "
        "(1 collapse + %d ramp hours each)",
        total_expected, len(FAILURE_EVENTS), len(DEGRADATION_RAMP)
    )

    voltages       = []
    frequencies    = []
    failure_events = []

    # Fixed seed — pipeline generates identical data on every run
    np.random.seed(42)

    for _, row in df.iterrows():
        hour     = row['timestamp'].hour
        month    = row['timestamp'].month
        day      = row['timestamp'].day
        temp     = row['temperature'] or 28.0   # Nigerian avg fallback
        humidity = row.get('humidity') or 70.0  # Nigerian avg fallback

        # ── Base voltage by corridor type ─────────────────────────────
        base_voltage = 330.0 if row['corridor_id'] in [1, 3, 5, 6, 8] else 132.0

        # ── Thermal derating ──────────────────────────────────────────
        current_voltage = base_voltage
        if temp > 35:
            current_voltage = base_voltage * 0.95   # 5% sag
        elif temp > 30:
            current_voltage = base_voltage * 0.98   # 2% sag

        # Hot + humid compounds insulation stress
        if humidity > 80 and temp > 30:
            current_voltage *= 0.97                 # additional 3% sag

        # ── Base frequency (reproducible RNG) ────────────────────────
        current_freq = 50.0 + np.random.uniform(-0.2, 0.2)

        if humidity > 80 and temp > 30:
            current_freq -= 0.1                     # instability under stress

        # ── Failure injection (collapse + ramp) ──────────────────────
        lookup_key = (row['corridor_id'], month, day, hour)

        if lookup_key in failure_lookup:
            volt_mult, freq_delta, is_collapse = failure_lookup[lookup_key]

            if is_collapse:
                # Hard collapse — override thermal physics entirely
                current_voltage = base_voltage * volt_mult   # 70% of nominal
                current_freq    = 48.2                        # severe decay
            else:
                # Degradation ramp — apply on top of existing thermal physics
                # so hot days degrade faster (physically realistic)
                current_voltage = current_voltage * volt_mult
                current_freq    = current_freq + freq_delta

            failure_events.append(1)

        elif row['corridor_id'] in AMBIENT_STRESS:
            # ── Persistent mild stress for corridors 5–8 ─────────────
            # Applied to every normal hour so the model sees a realistic
            # probability spread at inference time rather than flat near-zero.
            # Degradation ramp and collapse hours override this if they match.
            volt_mult, freq_delta = AMBIENT_STRESS[row['corridor_id']]
            current_voltage = current_voltage * volt_mult
            current_freq    = current_freq + freq_delta
            failure_events.append(0)   # stressed but not failed

        else:
            failure_events.append(0)

        voltages.append(round(current_voltage, 2))
        frequencies.append(round(current_freq, 2))

    df['voltage_kv']    = voltages
    df['frequency_hz']  = frequencies
    df['failure_event'] = failure_events

    total_failures = sum(failure_events)
    logger.info(
        "Telemetry generated — %d failure-labelled hours "
        "(expected ~%d: %d collapses + %d ramp hours)",
        total_failures,
        total_expected,
        len(FAILURE_EVENTS),
        len(FAILURE_EVENTS) * len(DEGRADATION_RAMP),
    )
    return df


def load_scada_data(df: pd.DataFrame) -> None:
    """
    Loads the simulated SCADA telemetry back into DuckDB idempotently.
    """
    logger.info("Loading SCADA telemetry into DuckDB...")
    con = duckdb.connect(DUCKDB_PATH)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS scada_telemetry (
                corridor_id   INTEGER   NOT NULL,
                timestamp     TIMESTAMP NOT NULL,
                voltage_kv    DOUBLE,
                frequency_hz  DOUBLE,
                failure_event INTEGER,
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
    """
    Main orchestration function for the SCADA simulation pipeline.
    """
    logger.info("=== Starting SCADA Simulation Pipeline ===")
    try:
        df_baseline  = extract_baseline_data()
        df_simulated = generate_telemetry(df_baseline)
        load_scada_data(df_simulated)
        logger.info(
            "=== SCADA Simulation Complete — %d records, %d failure-labelled hours ===",
            len(df_simulated),
            int(df_simulated['failure_event'].sum())
        )
    except Exception as e:
        logger.error("=== SCADA Simulation Failed: %s ===", e)
        raise


if __name__ == "__main__":
    main()