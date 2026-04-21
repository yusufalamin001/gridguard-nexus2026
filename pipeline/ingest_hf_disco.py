"""
pipeline/ingest_hf_disco.py

Pulls historical Nigerian DisCo performance data from Hugging Face
and loads it into DuckDB for dynamic economic consequence scoring.

Calculates economic_loss_per_hr per DisCo using:
    - energy_received_mwh from the dataset
    - technical and commercial loss percentages
    - NERC Band A average tariff of ₦225/kWh (2025)

Run after ingest.py.
Usage:
    python pipeline/ingest_hf_disco.py
"""

import pandas as pd
import duckdb
from config import DUCKDB_PATH
from logger import get_logger

logger = get_logger("ingest_hf_disco")

HF_DATASET_URL = (
    "hf://datasets/electricsheepafrica/"
    "nigerian_electricity_disco_performance/"
    "nigerian_electricity_disco_performance.parquet"
)

# NERC Band A average tariff 2025 — source: MYTO 2025
TARIFF_NAIRA_PER_KWH = 225.0

# Maps full HuggingFace DisCo names to short names used in corridors table
# Must match disco_name values in CORRIDORS in ingest.py exactly
DISCO_NAME_MAP = {
    "Abuja Electricity Distribution Company":        "Abuja",
    "Benin Electricity Distribution Company":        "Benin",
    "Eko Electricity Distribution Company":          "Eko",
    "Enugu Electricity Distribution Company":        "Enugu",
    "Ibadan Electricity Distribution Company":       "Ibadan",
    "Ikeja Electricity Distribution Company":        "Ikeja",
    "Jos Electricity Distribution Company":          "Jos",
    "Kaduna Electricity Distribution Company":       "Kaduna",
    "Kano Electricity Distribution Company":         "Kano",
    "Port Harcourt Electricity Distribution Company":"Port Harcourt",
    "Yola Electricity Distribution Company":         "Yola",
}


def calculate_economic_loss_per_hr(
    energy_received_mwh: float,
    technical_losses_pct: float,
    commercial_losses_pct: float,
) -> float:
    """
    Calculate hourly economic loss in ₦M for a DisCo zone.

    Formula:
        hourly_energy  = energy_received_mwh / (365 * 24)
        loss_rate      = (technical_losses_pct + commercial_losses_pct) / 100
        hourly_loss_₦  = hourly_energy * 1000 * loss_rate * tariff
        result         = hourly_loss_₦ / 1_000_000  (convert to ₦M)

    Args:
        energy_received_mwh:    Annual energy received by DisCo in MWh
        technical_losses_pct:   Technical loss percentage
        commercial_losses_pct:  Commercial loss percentage

    Returns:
        Hourly economic loss in ₦ Millions, rounded to 2 decimal places
    """
    hourly_energy_mwh = energy_received_mwh / (365 * 24)
    loss_rate = (technical_losses_pct + commercial_losses_pct) / 100
    hourly_loss_naira = hourly_energy_mwh * 1000 * loss_rate * TARIFF_NAIRA_PER_KWH
    return round(hourly_loss_naira / 1_000_000, 2)


def load_disco_performance() -> None:
    """
    Fetch DisCo performance dataset from HuggingFace, calculate
    economic_loss_per_hr per DisCo, and store in DuckDB.

    Table created:
        disco_performance — DisCo operational and economic metrics
                            with calculated hourly loss values
    """
    logger.info("Fetching DisCo performance dataset from HuggingFace...")

    df = pd.read_parquet(HF_DATASET_URL)
    logger.info(
        "Dataset loaded — %d rows across %d DisCos (%d-%d)",
        len(df),
        df['disco_name'].nunique(),
        df['year'].min(),
        df['year'].max()
    )

    # Calculate economic_loss_per_hr for every DisCo-year combination
    df['economic_loss_per_hr'] = df.apply(
        lambda row: calculate_economic_loss_per_hr(
            row['energy_received_mwh'],
            row['technical_losses_pct'],
            row['commercial_losses_pct'],
        ),
        axis=1
    )

    logger.info("economic_loss_per_hr calculated for all DisCo-year rows")

    # Add short_name column for joining with corridors table
    df['short_name'] = df['disco_name'].map(DISCO_NAME_MAP)

    unmapped = df[df['short_name'].isna()]['disco_name'].unique()
    if len(unmapped) > 0:
        logger.warning(
            "Unmapped DisCo names — will not join correctly: %s",
            unmapped.tolist()
        )
    else:
        logger.info("All DisCo names mapped to short names successfully")

    con = duckdb.connect(DUCKDB_PATH)
    try:
        # Fresh load — dataset is versioned and stable
        logger.info("Dropping and recreating disco_performance table")
        con.execute("DROP TABLE IF EXISTS disco_performance")
        con.execute("""
            CREATE TABLE disco_performance AS
            SELECT
                disco_name,
                short_name,
                year,
                customers_total,
                customers_metered,
                collection_efficiency_pct,
                technical_losses_pct,
                commercial_losses_pct,
                energy_received_mwh,
                energy_billed_mwh,
                economic_loss_per_hr
            FROM df
        """)

        # Verify what we stored
        row_count = con.execute(
            "SELECT COUNT(*) FROM disco_performance"
        ).fetchone()[0]
        
        logger.info(
            "disco_performance table loaded — %d rows",
            row_count
        )

        # Show a sample so we can verify the calculation looks sensible
        sample = con.execute("""
            SELECT disco_name, year, economic_loss_per_hr
            FROM disco_performance
            WHERE year = (SELECT MAX(year) FROM disco_performance)
            ORDER BY economic_loss_per_hr DESC
            LIMIT 5
        """).fetchall()

        logger.info("Top 5 DisCos by economic_loss_per_hr (most recent year):")
        for row in sample:
            logger.info("  %s (%d): NGN %.2fM/hr", row[0], row[1], row[2])

    except Exception as e:
        logger.error("Failed to load disco_performance: %s", e)
        raise
    finally:
        con.close()


def main() -> None:
    """
    Main orchestration for HuggingFace DisCo data ingestion.
    """
    logger.info("=== Starting HuggingFace DisCo Ingestion ===")
    try:
        load_disco_performance()
        logger.info("=== HuggingFace DisCo Ingestion Complete ===")
    except Exception as e:
        logger.error("=== HuggingFace DisCo Ingestion Failed: %s ===", e)
        raise


if __name__ == "__main__":
    main()