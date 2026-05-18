"""
RDL Wind Ingestion â€” Wind Speed & Energy Intelligence Pipeline

Source: Open-Meteo API (completely free, no API key)
Ingests multi-height wind data (10m, 80m, 120m) for NW England.
Used for: aviation safety (TR-001 jets), renewable energy analysis.

Schedule: Daily at 08:30 UTC (offset from weather to avoid overlap)
RDL Output: s3://bellosdata-bronze-raw/rdl/wind/
Asset: Triggers downstream ODL fact_wind_measurement builder

Author: Awujoo (AWUJOO-041) | Genesis: 2026-05-17
Trade Route: TR-001 (Private Jets â€” wind corridor analysis)
"""

from datetime import datetime, timedelta
import logging
import urllib.parse

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

# â”€â”€ Assets â”€â”€
RDL_WIND_ASSET = Asset("s3://bellosdata-bronze-raw/rdl/wind")

# â”€â”€ NW England Wind Measurement Points â”€â”€
# Includes aviation sites + elevated/coastal sites for wind intelligence
WIND_POINTS = {
    "manchester_airport":  {"lat": 53.36, "lon": -2.27, "label": "Manchester Airport (EGCC)"},
    "liverpool_airport":   {"lat": 53.33, "lon": -2.85, "label": "Liverpool JLA (EGGP)"},
    "barton_aerodrome":    {"lat": 53.47, "lon": -2.38, "label": "Barton Aerodrome (EGCB)"},
    "blackpool_airport":   {"lat": 53.77, "lon": -3.03, "label": "Blackpool Airport (EGNH)"},
    "hawarden_airport":    {"lat": 53.18, "lon": -2.98, "label": "Hawarden (EGNR)"},
    "morecambe_bay":       {"lat": 54.07, "lon": -2.85, "label": "Morecambe Bay (Coastal)"},
    "winter_hill":         {"lat": 53.63, "lon": -2.49, "label": "Winter Hill (Elevated)"},
    "great_orme":          {"lat": 53.33, "lon": -3.85, "label": "Great Orme (Headland)"},
    "carlisle_airport":    {"lat": 54.94, "lon": -2.81, "label": "Carlisle (EGNC)"},
    "walney_island":       {"lat": 54.12, "lon": -3.26, "label": "Walney Island (Offshore Wind)"},
}

# Multi-height wind variables
WIND_VARS = [
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
    "wind_speed_80m", "wind_direction_80m",
    "wind_speed_120m", "wind_direction_120m",
    "temperature_2m", "surface_pressure",
]

OPEN_METEO_BASE = "https://api.open-meteo.com/v1/forecast"


@dag(
    dag_id="rdl_wind_ingestion",
    description="Ingest multi-height wind data for NW England (Open-Meteo â†’ RDL)",
    schedule="30 8 * * *",
    start_date=datetime(2026, 5, 17),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "awujoo",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["wind", "rdl", "ingestion", "enrichment", "energy", "TR-001", "sovereign"],
)
def rdl_wind_ingestion():

    @task()
    def ingest_wind_grid() -> dict:
        """Fetch multi-height wind data for all measurement points."""
        from bellosdata_common import http_get

        all_measurements = []

        for point_id, point in WIND_POINTS.items():
            params = urllib.parse.urlencode({
                "latitude": point["lat"],
                "longitude": point["lon"],
                "hourly": ",".join(WIND_VARS),
                "past_days": 3,
                "forecast_days": 1,
                "timezone": "Europe/London",
                "wind_speed_unit": "ms",
            })
            url = f"{OPEN_METEO_BASE}?{params}"

            try:
                data = http_get(url, timeout=30)
                hourly = data.get("hourly", {})
                times = hourly.get("time", [])

                for i, ts in enumerate(times):
                    record = {
                        "point_id": point_id,
                        "label": point["label"],
                        "latitude": data.get("latitude"),
                        "longitude": data.get("longitude"),
                        "elevation": data.get("elevation"),
                        "timestamp": ts,
                    }
                    for var in WIND_VARS:
                        values = hourly.get(var, [])
                        record[var] = values[i] if i < len(values) else None

                    # Derived: estimate wind power density (W/mÂ²)
                    # P = 0.5 * Ï * vÂ³ where Ï â‰ˆ 1.225 kg/mÂ³
                    ws80 = record.get("wind_speed_80m")
                    if ws80 is not None and ws80 > 0:
                        record["wind_power_density_80m"] = round(
                            0.5 * 1.225 * (ws80 ** 3), 2
                        )
                    else:
                        record["wind_power_density_80m"] = None

                    record["source"] = "OPEN_METEO"
                    record["ingested_at"] = datetime.utcnow().isoformat() + "Z"
                    all_measurements.append(record)

                logger.info(f"Wind: {point['label']} â†’ {len(times)} hourly records")

            except Exception as e:
                logger.warning(f"Wind failed for {point['label']}: {e}")

        logger.info(f"Total wind measurements: {len(all_measurements)}")
        return {
            "status": "OK" if all_measurements else "FAILED",
            "records": len(all_measurements),
            "measurement_points": len(WIND_POINTS),
            "data": all_measurements,
        }

    @task(outlets=[RDL_WIND_ASSET])
    def write_to_rdl(wind_result: dict) -> dict:
        """Write wind measurements to RDL."""
        from bellosdata_common import rdl_write, write_manifest

        rdl_result = rdl_write("wind", wind_result.get("data", []))
        return write_manifest(
            dag_id="rdl_wind_ingestion",
            source_name="wind",
            rdl_result=rdl_result,
            record_counts={
                "hourly_measurements": wind_result.get("records", 0),
                "measurement_points": wind_result.get("measurement_points", 0),
            },
            extra={
                "api_source": "open-meteo.com",
                "height_levels": ["10m", "80m", "120m"],
                "points": list(WIND_POINTS.keys()),
            },
        )

    # â”€â”€ DAG Flow â”€â”€
    wind = ingest_wind_grid()
    write_to_rdl(wind)


rdl_wind_ingestion()
