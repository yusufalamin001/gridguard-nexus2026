"""
pipeline/features.py

Engineers temporal and statistical features for the XGBoost model.
Joins weather, SCADA telemetry, corridors, and disco_performance,
calculates lag variables and rolling statistics, and exports the
final dataset for model training.
"""

import duckdb
import os
from config import DUCKDB_PATH
from logger import get_logger

logger = get_logger("features")


def engineer_features() -> None:
    """
    Executes a complex SQL transformation to engineer ML features.
    Joins scada_telemetry with weather_readings, corridors, and
    disco_performance to produce a rich feature set for XGBoost.

    Features engineered:
        - Voltage lag (1h, 2h) and rolling average (3h)
        - Frequency deviation from nominal, lag (1h, 3h),
          rolling average (6h), and volatility (6h)
        - Voltage deviation from grid code lower limit
        - Temperature rolling average (3h)
        - Hour of day, day of week (cyclical demand patterns)
        - economic_loss_per_hr from disco_performance (most recent year)
        - critical_infra_score from corridors

    Saves output to model_features table in DuckDB.
    """
    logger.info("Starting feature engineering using DuckDB window functions...")
    con = duckdb.connect(DUCKDB_PATH)

    try:
        # Fresh feature set every run — always reflects latest pipeline data
        logger.info("Dropping and recreating model_features table for fresh feature set")
        con.execute("DROP TABLE IF EXISTS model_features")

        feature_query = """
            CREATE TABLE model_features AS
            WITH joined_data AS (
                SELECT
                    s.corridor_id,
                    c.name                              AS corridor_name,
                    c.disco_name,
                    c.critical_infra_score,
                    -- economic_loss_per_hr from disco_performance
                    -- using most recent year available (2023)
                    d.economic_loss_per_hr,
                    s.timestamp,
                    s.voltage_kv,
                    s.frequency_hz,
                    -- Frequency deviation from nominal 50Hz
                    s.frequency_hz - 50.0               AS freq_deviation,
                    ABS(s.frequency_hz - 50.0)          AS freq_deviation_abs,
                    -- Voltage deviation from grid code lower limit
                    -- 330kV lines: lower limit 313.5kV
                    -- 132kV lines: lower limit 125.4kV (330 * 0.95 scaled)
                    CASE
                        WHEN c.name LIKE '%330kV%'
                            THEN s.voltage_kv - 313.5
                        ELSE
                            s.voltage_kv - 125.4
                    END                                  AS voltage_deviation,
                    w.temperature,
                    w.humidity,
                    s.failure_event,
                    EXTRACT(HOUR   FROM s.timestamp)    AS hour_of_day,
                    EXTRACT(ISODOW FROM s.timestamp)    AS day_of_week
                FROM scada_telemetry s
                JOIN weather_readings w
                    ON  s.corridor_id = w.corridor_id
                    AND s.timestamp   = w.timestamp
                JOIN corridors c
                    ON s.corridor_id = c.id
                -- Join disco_performance on short_name, most recent year only
                JOIN disco_performance d
                    ON  c.disco_name  = d.short_name
                    AND d.year        = (
                        SELECT MAX(year)
                        FROM disco_performance dp
                        WHERE dp.short_name = c.disco_name
                    )
            )
            SELECT
                *,
                -- ── Voltage lag features ──────────────────────────────
                LAG(voltage_kv, 1) OVER (
                    PARTITION BY corridor_id ORDER BY timestamp
                )                                           AS volt_lag_1h,
                LAG(voltage_kv, 2) OVER (
                    PARTITION BY corridor_id ORDER BY timestamp
                )                                           AS volt_lag_2h,
                AVG(voltage_kv) OVER (
                    PARTITION BY corridor_id
                    ORDER BY timestamp
                    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
                )                                           AS volt_roll_avg_3h,

                -- ── Frequency lag and rolling features ───────────────
                LAG(frequency_hz, 1) OVER (
                    PARTITION BY corridor_id ORDER BY timestamp
                )                                           AS freq_lag_1h,
                LAG(frequency_hz, 3) OVER (
                    PARTITION BY corridor_id ORDER BY timestamp
                )                                           AS freq_lag_3h,
                AVG(frequency_hz) OVER (
                    PARTITION BY corridor_id
                    ORDER BY timestamp
                    ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                )                                           AS freq_roll_avg_6h,
                STDDEV(frequency_hz) OVER (
                    PARTITION BY corridor_id
                    ORDER BY timestamp
                    ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                )                                           AS freq_volatility_6h,

                -- ── Temperature rolling average ───────────────────────
                AVG(temperature) OVER (
                    PARTITION BY corridor_id
                    ORDER BY timestamp
                    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
                )                                           AS temp_roll_avg_3h

            FROM joined_data
            -- Remove rows where lag features are NULL
            -- (first few rows per corridor before window fills)
            QUALIFY volt_lag_2h IS NOT NULL
                AND freq_lag_3h  IS NOT NULL
        """

        con.execute(feature_query)

        count = con.execute(
            "SELECT COUNT(*) FROM model_features"
        ).fetchone()[0]

        failures = con.execute(
            "SELECT SUM(failure_event) FROM model_features"
        ).fetchone()[0]

        logger.info(
            "Feature engineering complete — %d rows, %d failure events",
            count,
            int(failures or 0)
        )

    except Exception as e:
        logger.error("Feature engineering failed: %s", e)
        raise
    finally:
        con.close()


def export_features() -> None:
    """
    Exports the engineered features to a static CSV file for the ML model.
    Output: data/processed/model_features.csv
    """
    logger.info("Exporting features to data/processed/model_features.csv...")

    os.makedirs("data/processed", exist_ok=True)

    con = duckdb.connect(DUCKDB_PATH)
    try:
        con.execute("""
            COPY (
                SELECT * FROM model_features
                ORDER BY corridor_id, timestamp
            )
            TO 'data/processed/model_features.csv'
            (HEADER, DELIMITER ',')
        """)
        logger.info("Successfully exported features to CSV.")
    except Exception as e:
        logger.error("Failed to export features: %s", e)
        raise
    finally:
        con.close()


def main() -> None:
    """
    Main orchestration for feature engineering pipeline.
    """
    logger.info("=== Starting Feature Engineering Pipeline ===")
    try:
        engineer_features()
        export_features()
        logger.info("=== Feature Engineering Complete ===")
    except Exception as e:
        logger.error("=== Feature Engineering Failed: %s ===", e)
        raise


if __name__ == "__main__":
    main()