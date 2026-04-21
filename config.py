# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Paths
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/gridguard.duckdb")

# NASA POWER API
NASA_POWER_BASE_URL = os.getenv(
    "NASA_POWER_BASE_URL",
    "https://power.larc.nasa.gov/api/temporal/hourly/point"
)

# Grid code thresholds (from NERC / Nigerian Grid Code)
FREQ_NOMINAL_HZ       = 50.0
FREQ_LOWER_NORMAL     = 49.75
FREQ_UPPER_NORMAL     = 50.25
FREQ_LOWER_STRESS     = 48.75
FREQ_UPPER_STRESS     = 51.25
VOLTAGE_NOMINAL_KV    = 330.0
VOLTAGE_LOWER_LIMIT   = 313.5
VOLTAGE_UPPER_LIMIT   = 346.5

# Model settings
PREDICTION_WINDOW_HRS = 6
RANDOM_SEED           = 42
TEST_SIZE             = 0.2

# Feature columns for XGBoost — must match column names in model_features table
# Order matters: keep consistent between train.py and predict.py
FEATURE_COLS = [
    'voltage_kv',
    'frequency_hz',
    'freq_deviation',
    'freq_deviation_abs',
    'voltage_deviation',
    'temperature',
    'humidity',
    'hour_of_day',
    'day_of_week',
    'economic_loss_per_hr',
    'critical_infra_score',
    'volt_lag_1h',
    'volt_lag_2h',
    'volt_roll_avg_3h',
    'freq_lag_1h',
    'freq_lag_3h',
    'freq_roll_avg_6h',
    'freq_volatility_6h',
    'temp_roll_avg_3h',
]

# Target variable — the column XGBoost learns to predict
TARGET_COL = 'failure_event'