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

# Nigeria bounding box for API calls
NIGERIA_LAT_MIN = 4.0
NIGERIA_LAT_MAX = 14.0
NIGERIA_LON_MIN = 2.7
NIGERIA_LON_MAX = 15.0

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

# DisCo ATC&C loss rates from NERC Q4 2025 (Table 9)
DISCO_ATCC = {
    "Abuja":        30.99,
    "Benin":        44.50,
    "Eko":          14.20,
    "Enugu":        38.62,
    "Ibadan":       42.51,
    "Ikeja":        16.33,
    "Jos":          64.84,
    "Kaduna":       69.45,
    "Kano":         44.12,
    "Port Harcourt":33.01,
    "Yola":         52.77,
}