"""
scoring/consequence.py

The Business Logic Engine.
Takes raw AI failure probabilities and multiplies them by physical
infrastructure and economic constraints to generate ranked risk scores.

Run after model/predict.py.
Usage:
    python scoring/consequence.py
    python scoring/consequence.py --demo    # replay Dec 29 collapse
"""

import argparse
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


def calculate_consequences(demo: bool = False) -> None:
    """
    Merges live risk predictions with corridor metadata and computes
    a composite risk score for each corridor.

    Args:
        demo: If True, passes demo=True to run_inference() so the Dec 29
              collapse event is replayed instead of the latest timestamp.

    Risk Score Formula:
        risk_exposure  = (failure_probability_pct / 100) * economic_loss_per_hr
        infra_mult     = 1 + (critical_infra_score * 0.05)
        risk_score     = risk_exposure * infra_mult

    Key unit note:
        economic_loss_per_hr is already in ₦ Millions from the HuggingFace
        DisCo dataset (e.g. 44.44 = ₦44.44M/hr). No further unit conversion
        is needed. Scores therefore sit naturally in the 0–50 range for normal
        corridors, spiking toward 100 for high-probability + high-loss cases
        (e.g. Benin-Onitsha at 97% × ₦44.44M/hr × 1.05 infra ≈ 45.3).

    Output saved to data/processed/raw_consequence_scores.csv
    """
    logger.info("Calculating economic consequences%s...",
                " [DEMO MODE]" if demo else "")

    live_risks = run_inference(demo=demo)
    if live_risks is None or live_risks.empty:
        logger.error("No predictions returned from the model.")
        return

    metadata = get_corridor_metadata()
    df = pd.merge(live_risks, metadata, on='corridor_id')

    if df.empty:
        logger.error("Merge produced empty DataFrame — check disco_name mapping")
        return

    # ── Composite risk score ──────────────────────────────────────────────────
    # economic_loss_per_hr is already in ₦ Millions (e.g. 44.44 = ₦44.44M/hr)
    # from the HuggingFace DisCo dataset — no unit conversion needed.
    #
    # Formula:
    #   risk_exposure  = (failure_probability_pct / 100) * economic_loss_per_hr
    #   infra_mult     = 1 + (critical_infra_score * 0.05)
    #   risk_score     = risk_exposure * infra_mult
    #
    # This gives scores in the range 0–~50 for normal corridors,
    # spiking toward 100 for high-probability + high-loss corridors
    # (e.g. Benin-Onitsha at 97% probability × 44.44 ₦M/hr ≈ 43).
    # No additional scaling factor — the ₦M unit is already the right scale.
    df['risk_exposure_naira'] = (
        df['failure_probability_pct'] / 100
    ) * df['economic_loss_per_hr']

    df['infra_multiplier'] = 1 + (df['critical_infra_score'] * 0.05)

    df['risk_score'] = (
        df['risk_exposure_naira'] * df['infra_multiplier']
    ).round(1)

    os.makedirs("data/processed", exist_ok=True)
    export_path = "data/processed/raw_consequence_scores.csv"
    df.to_csv(export_path, index=False)

    top = df.sort_values('risk_score', ascending=False).iloc[0]
    logger.info(
        "Raw consequence scores saved — top risk: %s "
        "(score: %.1f, prob: %.1f%%, loss: ₦%.2fM/hr)",
        top['corridor_name'],
        top['risk_score'],
        top['failure_probability_pct'],
        top['economic_loss_per_hr'],
    )

    # Console preview
    print("\n" + "=" * 70)
    print("         CONSEQUENCE SCORES (₦M/hr × Probability × Infra)")
    print("=" * 70)
    cols = ['corridor_name', 'failure_probability_pct',
            'economic_loss_per_hr', 'risk_score']
    print(df.sort_values('risk_score', ascending=False)[cols].to_string(index=False))
    print("=" * 70 + "\n")


def main() -> None:
    """
    Main orchestration for consequence scoring pipeline.

    Usage:
        python scoring/consequence.py           # live mode
        python scoring/consequence.py --demo    # replay Dec 29 collapse event
    """
    parser = argparse.ArgumentParser(description="GridGuard Consequence Scoring")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Replay the Dec 29 2025 Benin-Onitsha collapse event for presentations."
    )
    args = parser.parse_args()

    logger.info("=== Starting Consequence Scoring Pipeline%s ===",
                " [DEMO MODE]" if args.demo else "")
    try:
        calculate_consequences(demo=args.demo)
        logger.info("=== Consequence Scoring Complete ===")
    except Exception as e:
        logger.error("=== Consequence Scoring Failed: %s ===", e)
        raise


if __name__ == "__main__":
    main()