"""
scoring/consequence.py

The Business Logic Engine.
Takes raw AI failure probabilities and multiplies them by physical
infrastructure and economic constraints to generate ranked risk scores.

Run after model/predict.py.
Usage:
    python scoring/consequence.py
"""

import os
import pandas as pd
import duckdb
from config import DUCKDB_PATH
from logger import get_logger
from model.predict import run_inference

logger = get_logger("scoring")


def get_corridor_metadata() -> pd.DataFrame:
    """
    Fetches corridor physical and economic metadata from DuckDB.
    Joins corridors with disco_performance on short_name to get
    pre-calculated economic_loss_per_hr from the HuggingFace dataset.

    Returns:
        DataFrame with corridor_id, critical_infra_score,
        corridor_name, disco_name, economic_loss_per_hr
    """
    con = duckdb.connect(DUCKDB_PATH)
    try:
        query = """
            SELECT
                c.id                    AS corridor_id,
                c.name                  AS corridor_name,
                c.disco_name,
                c.critical_infra_score,
                d.economic_loss_per_hr
            FROM corridors c
            JOIN disco_performance d
                ON  c.disco_name = d.short_name
                AND d.year = (
                    SELECT MAX(year)
                    FROM disco_performance dp
                    WHERE dp.short_name = c.disco_name
                )
        """
        df = con.execute(query).df()
        logger.info(
            "Corridor metadata loaded — %d corridors with economic data",
            len(df)
        )
        return df
    except Exception as e:
        logger.error("Failed to fetch corridor metadata: %s", e)
        raise
    finally:
        con.close()


def calculate_consequences() -> None:
    """
    Merges live risk predictions with corridor metadata and computes
    a composite risk score for each corridor.

    Risk Score Formula:
        risk_exposure  = (failure_probability / 100) * economic_loss_per_hr
        infra_mult     = 1 + (critical_infra_score * 0.20)
        risk_score     = (risk_exposure * infra_mult) / scaling_factor

    Output saved to data/processed/raw_consequence_scores.csv
    """
    logger.info("Calculating economic consequences...")

    live_risks = run_inference()
    if live_risks is None or live_risks.empty:
        logger.error("No predictions returned from the model.")
        return

    metadata = get_corridor_metadata()
    df = pd.merge(live_risks, metadata, on='corridor_id')

    if df.empty:
        logger.error("Merge produced empty DataFrame — check disco_name mapping")
        return

    # Composite risk score — probability × economic loss × infra weight
    df['risk_exposure_naira'] = (
        df['failure_probability_pct'] / 100
    ) * df['economic_loss_per_hr']

    df['infra_multiplier'] = 1 + (df['critical_infra_score'] * 0.20)

    scaling_factor = 500_000
    df['risk_score'] = (
        (df['risk_exposure_naira'] * df['infra_multiplier']) / scaling_factor
    ).round(1)

    os.makedirs("data/processed", exist_ok=True)
    export_path = "data/processed/raw_consequence_scores.csv"
    df.to_csv(export_path, index=False)

    logger.info(
        "Raw consequence scores saved — top risk: %s (score: %.1f)",
        df.sort_values('risk_score', ascending=False).iloc[0]['disco_name'],
        df['risk_score'].max()
    )


def main() -> None:
    """
    Main orchestration for consequence scoring pipeline.
    """
    logger.info("=== Starting Consequence Scoring Pipeline ===")
    try:
        calculate_consequences()
        logger.info("=== Consequence Scoring Complete ===")
    except Exception as e:
        logger.error("=== Consequence Scoring Failed: %s ===", e)
        raise


if __name__ == "__main__":
    main()