"""
RDL Weather Ingestion â€” Open-Meteo Weather Pipeline

Source: Open-Meteo API (completely free, no API key, unlimited requests)
Ingests hourly weather for 8 NW England grid points.
Captures: temp, precipitation, humidity, wind, cloud cover, visibility, UV.

Schedule: Daily at 08:00 UTC
RDL Output: s3://bellosdata-bronze-raw/rdl/weather/
Asset: Triggers downstream ODL fact_weather_observation builder

Author: Awujoo (AWUJOO-041) | Genesis: 2026-05-17
Trade Route: Cross-cutting enrichment (TR-001 jets, TR-004 birds)
"""

from datetime import datetime, timedelta
import json
import logging

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

# â”€â”€ Assets â”€â”€
RDL_WEATHER_ASSET = Asset("s3://bellosdata-bronze-raw/rdl/weather")

# â”€â”€ NW England Grid Points â”€â”€
NW_GRID_POINTS = {
    "manchester_city":    {"lat": 53.48, "lon": -2.24, "label": "Manchester City Centre"},
    "liverpool":          {"lat": 53.41, "lon": -2.98, "label": "Liverpool"},
    "blackpool":          {"lat": 53.82, "lon": -3.05, "label": "Blackpool (Coastal)"},
    "lancaster":          {"lat": 54.05, "lon": -2.80, "label": "Lancaster"},
    "chester":            {"lat": 53.19, "lon": -2.89, "label": "Chester"},
    "kendal":             {"lat": 54.33, "lon": -2.74, "label": "Kendal (Lakes Gateway)"},
    "barton_aerodrome":   {"lat": 53.47, "lon": -2.38, "label": "Barton Aerodrome (EGCB)"},
    "hawarden_airport":   {"lat": 53.18, "lon": -2.98, "label": "Hawarden Airport (EGNR)"},
}

# Open-Meteo hourly variables we want
HOURLY_VARS = [
    "temperature_2m", "relative_humidity_2m", "dew_point_2m",
    "apparent_temperature", "precipitation", "rain", "snowfall",
    "cloud_cover", "visibility", "wind_speed_10m", "wind_direction_10m",
    "wind_gusts_10m", "surface_pressure", "uv_index",
    "weather_code",
]

OPEN_METEO_BASE = "https://api.open-meteo.com/v1/forecast"


@dag(
    dag_id="rdl_weather_ingestion",
    description="Ingest hourly weather for NW England grid (Open-Meteo â†’ RDL)",
    schedule="0 8 * * *",
    start_date=datetime(2026, 5, 17),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "awujoo",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["weather", "rdl", "ingestion", "enrichment", "open-meteo", "sovereign"],
)
def rdl_weather_ingestion():

    @task()
    def ingest_weather_grid() -> dict:
        """
        Fetch hourly weather data for all NW grid points.
        Open-Meteo returns past 3 days + 7 day forecast in one call.
        """
        from bellosdata_common import http_get
        import urllib.parse

        all_observations = []

        for point_id, point in NW_GRID_POINTS.items():
            params = urllib.parse.urlencode({
                "latitude": point["lat"],
                "longitude": point["lon"],
                "hourly": ",".join(HOURLY_VARS),
                "past_days": 3,
                "forecast_days": 1,
                "timezone": "Europe/London",
            })
            url = f"{OPEN_METEO_BASE}?{params}"

            try:
                data = http_get(url, timeout=30)

                hourly = data.get("hourly", {})
                times = hourly.get("time", [])

                # Flatten: one record per hour per location
                for i, ts in enumerate(times):
                    obs = {
                        "point_id": point_id,
                        "label": point["label"],
                        "latitude": data.get("latitude"),
                        "longitude": data.get("longitude"),
                        "elevation": data.get("elevation"),
                        "timezone": data.get("timezone"),
                        "timestamp": ts,
                    }
                    for var in HOURLY_VARS:
                        values = hourly.get(var, [])
                        obs[var] = values[i] if i < len(values) else None

                    obs["source"] = "OPEN_METEO"
                    obs["ingested_at"] = datetime.utcnow().isoformat() + "Z"
                    all_observations.append(obs)

                logger.info(
                    f"Weather: {point['label']} â†’ {len(times)} hourly records"
                )

            except Exception as e:
                logger.warning(f"Weather failed for {point['label']}: {e}")
                continue

        logger.info(f"Total weather observations: {len(all_observations)}")
        return {
            "status": "OK" if all_observations else "FAILED",
            "records": len(all_observations),
            "grid_points": len(NW_GRID_POINTS),
            "data": all_observations,
        }

    @task(outlets=[RDL_WEATHER_ASSET])
    def write_to_rdl(weather_result: dict) -> dict:
        """Write weather observations to RDL as raw JSON + ingest_date."""
        from bellosdata_common import rdl_write, write_manifest

        rdl_result = rdl_write("weather", weather_result.get("data", []))

        manifest = write_manifest(
            dag_id="rdl_weather_ingestion",
            source_name="weather",
            rdl_result=rdl_result,
            record_counts={
                "hourly_observations": weather_result.get("records", 0),
                "grid_points": weather_result.get("grid_points", 0),
            },
            extra={
                "api_source": "open-meteo.com",
                "variables": HOURLY_VARS,
                "grid_points": list(NW_GRID_POINTS.keys()),
            },
        )
        return manifest

    # â”€â”€ DAG Flow â”€â”€
    weather = ingest_weather_grid()
    write_to_rdl(weather)


# Instantiate
rdl_weather_ingestion()
