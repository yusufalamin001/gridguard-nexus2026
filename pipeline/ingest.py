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
import time
from datetime import datetime
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential

from config import DUCKDB_PATH, NASA_POWER_BASE_URL
from logger import get_logger

# Initialise logger for this module
logger = get_logger("ingest")


CORRIDORS = [
    {
        "id": 1,
        "name": "Benin-Onitsha 330kV",
        "disco_name": "Benin",
        "latitude": 6.3350,
        "longitude": 5.6270,
    },
    {
        "id": 2,
        "name": "Ikeja-Ota 132kV",
        "disco_name": "Ikeja",
        "latitude": 6.6018,
        "longitude": 3.3515,
        "osm_radius": 50000,
    },
    {
        "id": 3,
        "name": "Kano-Kaduna 330kV",
        "disco_name": "Kano",
        "latitude": 11.9964,
        "longitude": 8.5167,
    },
    {
        "id": 4,
        "name": "Egbin-Lagos Island 132kV",
        "disco_name": "Eko",
        "latitude": 6.5244,
        "longitude": 3.3792,
        "osm_radius": 50000,
    },
    {
        "id": 5,
        "name": "Shiroro-Gwagwalada 330kV",
        "disco_name": "Abuja",
        "latitude": 9.0765,
        "longitude": 7.3986,
        "osm_radius": 50000,
    },
    {
        "id": 6,
        "name": "Jebba-Olorunsogo 330kV",
        "disco_name": "Ibadan",
        "latitude": 8.9167,
        "longitude": 4.8333,
    },
    {
        "id": 7,
        "name": "Afam-Port Harcourt 132kV",
        "disco_name": "Port Harcourt",
        "latitude": 4.8156,
        "longitude": 7.0498,
        "osm_radius": 50000,
    },
    {
        "id": 8,
        "name": "Kainji-Birnin Kebbi 330kV",
        "disco_name": "Abuja",
        "latitude": 11.5890,
        "longitude": 4.2000,
    },
]

# Minimum floor values by DisCo — prevents obviously wrong zeros
DISCO_INFRA_FLOORS = {
    "Ikeja":         {"hospital": 3, "school": 5, "market": 8},
    "Eko":           {"hospital": 4, "school": 4, "market": 10},
    "Abuja":         {"hospital": 2, "school": 3, "market": 4},
    "Port Harcourt": {"hospital": 2, "school": 3, "market": 5},
    "Benin":         {"hospital": 1, "school": 2, "market": 3},
    "Ibadan":        {"hospital": 2, "school": 3, "market": 6},
    "Kano":          {"hospital": 2, "school": 3, "market": 8},
    "Yola":          {"hospital": 1, "school": 1, "market": 2},
}

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
  nwr["shop"="market"](around:{radius},{lat},{lon});
  nwr["landuse"="retail"](around:{radius},{lat},{lon});
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
    counts = {"hospital": 0, "school": 0, "market": 0}

    for element in elements:
        tags = element.get("tags", {})
        amenity = tags.get("amenity", "")
        shop = tags.get("shop", "")
        landuse = tags.get("landuse", "")
    
        if amenity == "hospital":
            counts["hospital"] += 1
        elif amenity == "school":
             counts["school"] += 1
        elif ( amenity == "marketplace" or shop == "market" or landuse == "retail" ):
            counts["market"] += 1
            
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
            counts = _fetch_osm_counts(corridor['latitude'], corridor['longitude'], radius=corridor.get('osm_radius', 20000))
            
            # Apply floor values if OSM returned all zeros
            if sum(counts.values()) == 0:
                floor = DISCO_INFRA_FLOORS.get(corridor['disco_name'], {})
                counts['hospital'] = counts['hospital'] or floor.get('hospital', 0)
                counts['school']   = counts['school']   or floor.get('school',   0)
                counts['market']   = counts['market']   or floor.get('market',   0)
                logger.warning(
                    "OSM returned zeros for %s — applying DisCo floor values",
                    corridor['name']
                )

            # Mutate a copy of the dictionary to append our new features
            updated_corridor = corridor.copy()
            updated_corridor['hospital_count'] = counts['hospital']
            updated_corridor['school_count'] = counts['school']
            updated_corridor['market_count'] = counts['market']
            
            # Weights: hospitals=5 (life-critical), schools=2, markets=3
            # Markets weighted higher than baseline due to economic significance
            # of informal market sector in Nigerian DisCo zones
            updated_corridor['critical_infra_score'] = (
                (counts['hospital'] * 5) + 
                (counts['school'] * 2) + 
                (counts['market'] * 3)
            )
            
            updated_corridors.append(updated_corridor)
            logger.info("Success: %s -> %s", corridor['name'], counts)

            # Pause between requests to respect rate limits
            time.sleep(5)  # 5 seconds between each corridor
            
        except requests.exceptions.RequestException as e:
            logger.error(
                "Failed to fetch OSM data for %s after retries: %s",
                corridor['name'], e
            )
            # Apply floor values on complete API failure
            floor = DISCO_INFRA_FLOORS.get(corridor['disco_name'], {})
            updated_corridor = corridor.copy()
            updated_corridor['hospital_count']    = floor.get('hospital', 0)
            updated_corridor['school_count']      = floor.get('school', 0)
            updated_corridor['market_count']      = floor.get('market', 0)
            updated_corridor['critical_infra_score'] = (
                (floor.get('hospital', 0) * 5) +
                (floor.get('school',   0) * 2) +
                (floor.get('market',   0) * 3)
            )
            updated_corridors.append(updated_corridor)
            logger.warning(
                "Applied DisCo floor values for %s: %s",
                corridor['name'],
                floor
            )
            time.sleep(10)
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
                try:
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
                except (ValueError, KeyError) as e:
                    logger.warning(
                        "Skipping malformed record %s for %s: %s",
                         ts_str, corridor['name'], e
                    )
                    continue
            
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
                hospital_count, school_count, 
                market_count, critical_infra_score
            )
            SELECT 
                id, name, disco_name, latitude, longitude,
                hospital_count, school_count, 
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
    try:
        init_database()
    
    # Step 2: Fetch and enrich corridors with OSM infrastructure counts
        updated_corridors = fetch_critical_infra(CORRIDORS)
        load_corridors(updated_corridors)
    
    # Step 3: Fetch NASA POWER weather data for Q4 2025
    # The December 29 partial collapse occurred in this window
        weather_data = fetch_nasa_power(
            updated_corridors, 
            start_date="20251001", 
            end_date="20251231"
        )
    
        load_weather(weather_data)
    
        logger.info(
            "=== Ingestion Pipeline Complete — %d corridors, %d weather records ===",
            len(updated_corridors),       
            len(weather_data)
        )
    except Exception as e:
        logger.error("=== Ingestion Pipeline Failed: %s ===", e)
        raise

if __name__ == "__main__":
    main()