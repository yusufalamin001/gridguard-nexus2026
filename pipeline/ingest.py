"""
pipeline/ingest.py

Fetches and stores all external data GridGuard needs:
  - Transmission corridor reference data
  - Critical infrastructure counts (OpenStreetMap Overpass API)
  - Historical weather data (NASA POWER API)

Run this script once before any other pipeline script.
Usage:
    python pipeline/ingest.py
"""

import duckdb
import pandas as pd
import requests
from datetime import datetime, timedelta
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential

from config import DUCKDB_PATH, NASA_POWER_BASE_URL
from logger import get_logger

# Initialise logger for this module
logger = get_logger("ingest")


CORRIDORS = [
    {
        "id": 1,
        "name": "Benin-Onitsha 330KV",
        "disco_name": "Benin",
        "latitude": 6.3350,
        "longitude": 5.6270,
        "economic_loss_per_hr": 4.2,
    },
    {
        "id": 2,
        "name": "Ikeja-Ota 132kV",
        "disco_name": "Ikeja",
        "latitude": 6.6018,
        "longitude": 3.3515,
        "economic_loss_per_hr": 2.8,
    },
    {
        "id": 3,
        "name": "Kano-Kaduna 330kV",
        "disco_name": "Kano",
        "latitude": 11.9964,
        "longitude": 8.5167,
        "economic_loss_per_hr": 1.9,
    },
    {
        "id": 4,
        "name": "Egbin-Lagos Island 132kV",
        "disco_name": "Eko",
        "latitude": 6.5244,
        "longitude": 3.3792,
        "economic_loss_per_hr": 3.1,
    },
    {
        "id": 5,
        "name": "Shiroro-Gwagwalada 330kV",
        "disco_name": "Abuja",
        "latitude": 9.0765,
        "longitude": 7.3986,
        "economic_loss_per_hr": 2.5,
    },
    {
        "id": 6,
        "name": "Jebba-Olorunsogo 330kV",
        "disco_name": "Ibadan",
        "latitude": 8.9167,
        "longitude": 4.8333,
        "economic_loss_per_hr": 1.4,
    },
    {
        "id": 7,
        "name": "Afam-Port Harcourt 132kV",
        "disco_name": "Port Harcourt",
        "latitude": 4.8156,
        "longitude": 7.0498,
        "economic_loss_per_hr": 1.8,
    },
    {
        "id": 8,
        "name": "Kainji-Birnin Kebbi 330kV",
        "disco_name": "Yola",
        "latitude": 11.5890,
        "longitude": 4.2000,
        "economic_loss_per_hr": 0.9,
    },
]


def init_database() -> None:
    """
    Initialise DuckDB and create all tables GridGuard needs.
    Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.

    Tables created:
        corridors        — transmission line reference data
        weather_readings — hourly NASA POWER data per corridor

    Returns:
        None
    """
    logger.info("Initialising DuckDB at %s", DUCKDB_PATH)

    # Create data/ directory if it doesn't exist
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DUCKDB_PATH)

    try:
        # ── TABLE 1: corridors ─────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS corridors (
                id                   INTEGER PRIMARY KEY,
                name                 VARCHAR NOT NULL,
                disco_name           VARCHAR NOT NULL,
                latitude             DOUBLE  NOT NULL,
                longitude            DOUBLE  NOT NULL,
                economic_loss_per_hr DOUBLE  NOT NULL,
                hospital_count       INTEGER DEFAULT 0,
                school_count         INTEGER DEFAULT 0,
                market_count         INTEGER DEFAULT 0,
                critical_infra_score INTEGER DEFAULT 0,
                created_at           TIMESTAMP DEFAULT current_timestamp,
                updated_at           TIMESTAMP DEFAULT current_timestamp
            )
        """)
        logger.info("Corridors table ready")

        # ── TABLE 2: weather_readings ──────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS weather_readings (
                corridor_id  INTEGER  NOT NULL,
                timestamp    TIMESTAMP NOT NULL,
                temperature  DOUBLE,
                humidity     DOUBLE,
                created_at   TIMESTAMP DEFAULT current_timestamp,
                PRIMARY KEY (corridor_id, timestamp),
                FOREIGN KEY (corridor_id) REFERENCES corridors(id)
            )
        """)
        logger.info("Weather readings table ready")

        # ── UNIQUE INDEX — duplicate prevention ────────────
        try:
            con.execute("""
                CREATE UNIQUE INDEX idx_weather_unique
                ON weather_readings (corridor_id, timestamp)
            """)
            logger.info("Unique index on weather_readings created")
        except Exception:
            # Index already exists — safe to continue
            logger.debug("Unique index already exists — skipping")

    except Exception as e:
        logger.error("Database initialisation failed: %s", e)
        raise

    finally:
        con.close()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=15), reraise=True)
def _fetch_osm_counts(lat: float, lon: float, radius: int = 20000) -> dict:
    """
    Helper function to ping Overpass API for a specific coordinate.
    Wrapped in a retry decorator to handle API rate limiting.
    """
    overpass_url = "http://overpass-api.de/api/interpreter"
    
    # Overpass QL: 'nwr' means Nodes, Ways, and Relations. 
    # We only request 'tags' to keep the JSON payload extremely lightweight.
    overpass_query = f"""
    [out:json][timeout:50];
    (
      nwr["amenity"="hospital"](around:{radius},{lat},{lon});
      nwr["amenity"="school"](around:{radius},{lat},{lon});
      nwr["amenity"="marketplace"](around:{radius},{lat},{lon});
    );
    out tags;
    """
    
    # Overpass strictly requires a custom User-Agent to avoid being blocked
    headers = {
        "User-Agent": "GridGuard-Predictive-System/1.0 (Academic/Hackathon Project)"
    }
    
    response = requests.post(overpass_url, data={'data': overpass_query}, headers=headers, timeout=60)
    response.raise_for_status()  # Trigger retry on HTTP errors
    
    elements = response.json().get("elements", [])
    
    # Initialize counts
    counts = {"hospital": 0, "school": 0, "marketplace": 0}
    
    for element in elements:
        amenity = element.get("tags", {}).get("amenity")
        if amenity in counts:
            counts[amenity] += 1
            
    return counts

def fetch_critical_infra(corridors_list: list) -> list:
    """
    Iterate through the corridors and append critical infrastructure counts.
    Updates the dictionaries in-memory for later database insertion.
    """
    logger.info("Starting OpenStreetMap Overpass API extraction (20km radius)...")
    
    updated_corridors = []
    
    for corridor in corridors_list:
        logger.info("Fetching OSM data for %s corridor...", corridor['name'])
        
        try:
            counts = _fetch_osm_counts(corridor['latitude'], corridor['longitude'], radius=20000)
            
            # Mutate a copy of the dictionary to append our new features
            updated_corridor = corridor.copy()
            updated_corridor['hospital_count'] = counts['hospital']
            updated_corridor['school_count'] = counts['school']
            updated_corridor['market_count'] = counts['marketplace']
            
            # Simple baseline calculation for consequence scoring
            updated_corridor['critical_infra_score'] = (
                (counts['hospital'] * 5) + 
                (counts['school'] * 2) + 
                (counts['marketplace'] * 3)
            )
            
            updated_corridors.append(updated_corridor)
            logger.info("Success: %s -> %s", corridor['name'], counts)
            
        except requests.exceptions.RequestException as e:
            logger.error("Failed to fetch OSM data for %s after retries: %s", corridor['name'], e)
            # Assign zeros if the API completely fails so the pipeline doesn't break
            updated_corridor = corridor.copy()
            updated_corridor.update({'hospital_count': 0, 'school_count': 0, 'market_count': 0, 'critical_infra_score': 0})
            updated_corridors.append(updated_corridor)
            
    return updated_corridors

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=15), reraise=True)
def _fetch_nasa_power_hourly(lat: float, lon: float, start_date: str, end_date: str) -> dict:
    """
    Helper function to ping NASA POWER API for a specific coordinate.
    Wrapped in a retry decorator to handle network blips.
    """
    url = "https://power.larc.nasa.gov/api/temporal/hourly/point"
    params = {
        "parameters": "T2M,RH2M", 
        "community": "RE",        # Renewable Energy community 
        "longitude": lon,
        "latitude": lat,
        "start": start_date,
        "end": end_date,
        "format": "JSON"
    }
    
    # NASA POWER doesn't require an API key or custom User-Agent, but setting a timeout is critical
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    return response.json()

def fetch_nasa_power(corridors_list: list, start_date: str = "20251001", end_date: str = "20251231") -> list:
    """
    Iterate through the corridors and fetch hourly weather data for Q4 2025.
    Returns a flat list of dictionaries ready for database insertion.
    """
    logger.info("Starting NASA POWER API extraction (%s to %s)...", start_date, end_date)
    
    weather_records = []
    
    for corridor in corridors_list:
        logger.info("Fetching weather data for %s...", corridor['name'])
        
        try:
            data = _fetch_nasa_power_hourly(
                lat=corridor['latitude'], 
                lon=corridor['longitude'], 
                start_date=start_date, 
                end_date=end_date
            )
            
            # NASA POWER nests the time series data deeply
            t2m_data = data.get("properties", {}).get("parameter", {}).get("T2M", {})
            rh2m_data = data.get("properties", {}).get("parameter", {}).get("RH2M", {})
            
            # The keys are string timestamps like "2025100100" (YYYYMMDDHH)
            for ts_str in t2m_data.keys():
                temp = t2m_data[ts_str]
                humidity = rh2m_data.get(ts_str)
                
                # Clean up NASA's missing value flags
                if temp == -999.0: temp = None
                if humidity == -999.0: humidity = None
                    
                # Parse string into an actual Python datetime object
                dt_obj = datetime.strptime(ts_str, "%Y%m%d%H")
                
                weather_records.append({
                    "corridor_id": corridor["id"],
                    "timestamp": dt_obj,
                    "temperature": temp,
                    "humidity": humidity
                })
            
            logger.info("Success: Downloaded %d hourly records for %s", len(t2m_data), corridor['name'])
            
        except requests.exceptions.RequestException as e:
            logger.error("Failed to fetch weather for %s: %s", corridor['name'], e)
            
    return weather_records


def load_corridors(corridors_data: list) -> None:
    """
    Loads the enriched corridor data into DuckDB.
    Uses INSERT OR REPLACE to update existing records gracefully.
    """
    logger.info("Loading %d corridors into DuckDB...", len(corridors_data))
    
    # Convert list of dictionaries to a Pandas DataFrame
    df = pd.DataFrame(corridors_data)
    
    con = duckdb.connect(DUCKDB_PATH)
    try:
        con.execute("""
            INSERT OR REPLACE INTO corridors (
                id, name, disco_name, latitude, longitude, 
                economic_loss_per_hr, hospital_count, school_count, 
                market_count, critical_infra_score
            )
            SELECT 
                id, name, disco_name, latitude, longitude, 
                economic_loss_per_hr, hospital_count, school_count, 
                market_count, critical_infra_score
            FROM df
        """)
        logger.info("Successfully loaded corridors table.")
    except Exception as e:
        logger.error("Failed to load corridors: %s", e)
        raise
    finally:
        con.close()

def load_weather(weather_data: list) -> None:
    """
    Loads the hourly weather readings into DuckDB.
    Uses explicit ON CONFLICT syntax for robust idempotency.
    """
    if not weather_data:
        logger.warning("No weather data provided to load.")
        return
        
    logger.info("Loading %d weather records into DuckDB...", len(weather_data))
    
    df = pd.DataFrame(weather_data)
    
    con = duckdb.connect(DUCKDB_PATH)
    try:
        con.execute("""
            INSERT INTO weather_readings (corridor_id, timestamp, temperature, humidity)
            SELECT corridor_id, timestamp, temperature, humidity
            FROM df
            ON CONFLICT (corridor_id, timestamp) 
            DO UPDATE SET 
                temperature = EXCLUDED.temperature,
                humidity = EXCLUDED.humidity
        """)
        logger.info("Successfully loaded weather readings table.")
    except Exception as e:
        logger.error("Failed to load weather data: %s", e)
        raise
    finally:
        con.close()
        
def main():
    """
    Main orchestration function for the ingestion pipeline.
    """
    logger.info("=== Starting GridGuard Ingestion Pipeline ===")
    
    # Step 1: Ensure database schema exists
    init_database()
    
    # Step 2: Fetch and enrich corridors with OSM infrastructure counts
    updated_corridors = fetch_critical_infra(CORRIDORS)
    load_corridors(updated_corridors)
    
    # Step 3: Fetch NASA POWER weather data for Q4 2025
    # The December 29 partial collapse occurred in this window
    weather_data = fetch_nasa_power(updated_corridors, start_date="20251001", end_date="20251231")
    load_weather(weather_data)
    
    logger.info("=== Ingestion Pipeline Completed Successfully ===")

if __name__ == "__main__":
    main()