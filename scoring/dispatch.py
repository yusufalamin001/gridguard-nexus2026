"""
scoring/dispatch.py

The UI Formatting Engine.
Reads raw consequence scores, applies priority rankings, and formats
the final dispatch queue for the Streamlit dashboard.

Run after scoring/consequence.py.
Usage:
    python scoring/dispatch.py
"""

import pandas as pd
import os
from logger import get_logger

logger = get_logger("dispatch")

INPUT_PATH  = "data/processed/raw_consequence_scores.csv"
OUTPUT_PATH = "data/processed/dispatch_queue.csv"


def generate_dispatch_queue() -> None:
    """
    Reads raw consequence scores, ranks corridors by risk score,
    and exports a formatted dispatch queue for the dashboard.

    Output columns:
        Priority, Corridor, AI Probability (%), Risk Score,
        NGN Loss/hr, Critical Infra

    Output saved to data/processed/dispatch_queue.csv
    """
    logger.info("Generating final ranked dispatch queue...")

    if not os.path.exists(INPUT_PATH):
        logger.error(
            "%s not found — run scoring/consequence.py first",
            INPUT_PATH
        )
        raise FileNotFoundError(f"Missing: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH)

    # Sort highest risk first, assign priority rank starting at 1
    dispatch_df = df.sort_values(
        by='risk_score', ascending=False
    ).reset_index(drop=True)

    dispatch_df.insert(0, 'Priority', range(1, len(dispatch_df) + 1))

    # Select and rename columns for dashboard display
    final_output = dispatch_df[[
        'Priority',
        'corridor_name',
        'failure_probability_pct',
        'risk_score',
        'economic_loss_per_hr',
        'critical_infra_score',
    ]].rename(columns={
        'corridor_name':           'Corridor',
        'failure_probability_pct': 'AI Probability (%)',
        'risk_score':              'Risk Score',
        'economic_loss_per_hr':    'NGN Loss/hr',
        'critical_infra_score':    'Critical Infra',
    })

    # Console preview
    print("\n" + "=" * 75)
    print("            LIVE DISPATCH QUEUE (PRIORITY RANKED)")
    print("=" * 75)
    print(final_output.to_string(index=False))
    print("=" * 75 + "\n")

    os.makedirs("data/processed", exist_ok=True)
    final_output.to_csv(OUTPUT_PATH, index=False)

    logger.info(
        "Dispatch queue exported — %d corridors ranked, saved to %s",
        len(final_output),
        OUTPUT_PATH
    )


def main() -> None:
    """
    Main orchestration for dispatch queue generation.
    """
    logger.info("=== Starting Dispatch Queue Generation ===")
    try:
        generate_dispatch_queue()
        logger.info("=== Dispatch Queue Complete ===")
    except Exception as e:
        logger.error("=== Dispatch Queue Failed: %s ===", e)
        raise


if __name__ == "__main__":
    main()