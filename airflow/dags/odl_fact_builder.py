"""
ODL Fact Builder — YAML-Driven Fact Table Materialiser

Reads fact definitions from config/facts.yml and materialises
typed fact tables into Silver S3 from RDL raw data.

Triggered by upstream ODL dim Assets (dims must exist before facts).
Each fact is built independently based on its YAML spec.

Architecture:
  config/facts.yml       → This DAG reads specs
  rdl/{source}/          → This DAG reads raw data
  odl/dim/{dim}/         → This DAG reads dims for SK lookups
  odl/fact/{fact_name}/  → This DAG writes typed Delta tables

Author: Awujoo (AWUJOO-041 Phase 2) | Genesis: 2026-05-17
"""

from datetime import datetime, timedelta
import json
import logging
import os

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

# Input Assets — ODL dims must be built first
ODL_DIM_DATE = Asset("s3://playdarch-silver-curated/odl/dim/dim_date")
ODL_DIM_LOCATION = Asset("s3://playdarch-silver-curated/odl/dim/dim_location")
ODL_DIM_WEATHER_STATION = Asset("s3://playdarch-silver-curated/odl/dim/dim_weather_station")
ODL_DIM_AIRPORT = Asset("s3://playdarch-silver-curated/odl/dim/dim_airport")

# Output Assets
ODL_FACT_WEATHER = Asset("s3://playdarch-silver-curated/odl/fact/fact_weather_observation")
ODL_FACT_WIND = Asset("s3://playdarch-silver-curated/odl/fact/fact_wind_measurement")
ODL_FACT_FLIGHT = Asset("s3://playdarch-silver-curated/odl/fact/fact_flight_movement")
ODL_FACT_COMPANY = Asset("s3://playdarch-silver-curated/odl/fact/fact_company_filing")

CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config")


def _load_yaml(filename):
    import yaml
    with open(os.path.join(CONFIG_DIR, filename), "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _get_fact_spec(fact_name):
    config = _load_yaml("facts.yml")
    for fact in config.get("facts", []):
        if fact["name"] == fact_name:
            return fact
    raise ValueError(f"Fact '{fact_name}' not found in facts.yml")


def _read_rdl(source_path):
    """Read Delta table from RDL Bronze S3, return parsed JSON records."""
    from deltalake import DeltaTable
    from aws_session import get_aws_session, AWS_REGION
    session = get_aws_session()
    creds = session.get_credentials().get_frozen_credentials()
    opts = {"AWS_ACCESS_KEY_ID": creds.access_key,
            "AWS_SECRET_ACCESS_KEY": creds.secret_key, "AWS_REGION": AWS_REGION}
    if creds.token:
        opts["AWS_SESSION_TOKEN"] = creds.token
    try:
        dt = DeltaTable(f"s3://playdarch-bronze-raw/{source_path}", storage_options=opts)
        df = dt.to_pandas()
        records = []
        for _, row in df.iterrows():
            try:
                records.append(json.loads(row["json"]))
            except (json.JSONDecodeError, KeyError):
                continue
        return records
    except Exception as e:
        logger.warning(f"No RDL data at {source_path}: {e}")
        return []


@dag(
    dag_id="odl_fact_builder",
    description="Build ODL fact tables from YAML specs + RDL data + dim lookups",
    schedule=[ODL_DIM_DATE, ODL_DIM_LOCATION, ODL_DIM_WEATHER_STATION, ODL_DIM_AIRPORT],
    start_date=datetime(2026, 5, 17),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "awujoo", "retries": 1, "retry_delay": timedelta(minutes=3)},
    tags=["odl", "facts", "builder", "yaml-driven", "sovereign"],
)
def odl_fact_builder():

    @task(outlets=[ODL_FACT_WEATHER])
    def build_fact_weather_observation():
        """Build fact_weather_observation — hourly weather per station."""
        from bellosdata_common import odl_write
        spec = _get_fact_spec("fact_weather_observation")
        logger.info(f"Building {spec['name']} (grain: {spec['grain']})")

        records = _read_rdl(spec["source_rdl"])
        if not records:
            return odl_write("fact", "fact_weather_observation", [])

        rows = []
        for rec in records:
            pid = rec.get("point_id", "")
            hourly = rec.get("hourly_data", [])
            if not hourly:
                # Single-record format
                hourly = [rec]

            for hour in hourly:
                ts = hour.get("timestamp", hour.get("time", ""))
                if not ts:
                    continue
                date_str = ts[:10]
                try:
                    date_key = int(date_str.replace("-", ""))
                except (ValueError, TypeError):
                    continue

                rows.append({
                    "station_point_id": pid,
                    "observation_timestamp": ts,
                    "observation_date_key": date_key,
                    "temperature_2m_c": hour.get("temperature_2m"),
                    "apparent_temperature_c": hour.get("apparent_temperature"),
                    "relative_humidity_pct": hour.get("relative_humidity_2m"),
                    "precipitation_mm": hour.get("precipitation"),
                    "rain_mm": hour.get("rain"),
                    "snowfall_cm": hour.get("snowfall"),
                    "cloud_cover_pct": hour.get("cloud_cover"),
                    "visibility_m": hour.get("visibility"),
                    "wind_speed_10m_ms": hour.get("wind_speed_10m"),
                    "wind_direction_10m_deg": hour.get("wind_direction_10m"),
                    "wind_gusts_10m_ms": hour.get("wind_gusts_10m"),
                    "surface_pressure_hpa": hour.get("surface_pressure"),
                    "uv_index": hour.get("uv_index"),
                    "weather_code": hour.get("weather_code"),
                })

        result = odl_write("fact", "fact_weather_observation", rows)
        logger.info(f"fact_weather_observation: {result['record_count']} rows")
        return result

    @task(outlets=[ODL_FACT_WIND])
    def build_fact_wind_measurement():
        """Build fact_wind_measurement — multi-height wind per point."""
        from bellosdata_common import odl_write
        spec = _get_fact_spec("fact_wind_measurement")
        logger.info(f"Building {spec['name']} (grain: {spec['grain']})")

        records = _read_rdl(spec["source_rdl"])
        if not records:
            return odl_write("fact", "fact_wind_measurement", [])

        rows = []
        for rec in records:
            pid = rec.get("point_id", "")
            hourly = rec.get("hourly_data", [])
            if not hourly:
                hourly = [rec]

            for hour in hourly:
                ts = hour.get("timestamp", hour.get("time", ""))
                if not ts:
                    continue
                date_str = ts[:10]
                try:
                    date_key = int(date_str.replace("-", ""))
                except (ValueError, TypeError):
                    continue

                rows.append({
                    "point_id": pid,
                    "measurement_timestamp": ts,
                    "measurement_date_key": date_key,
                    "wind_speed_10m_ms": hour.get("wind_speed_10m"),
                    "wind_speed_80m_ms": hour.get("wind_speed_80m"),
                    "wind_speed_120m_ms": hour.get("wind_speed_120m"),
                    "wind_direction_10m_deg": hour.get("wind_direction_10m"),
                    "wind_direction_80m_deg": hour.get("wind_direction_80m"),
                    "wind_direction_120m_deg": hour.get("wind_direction_120m"),
                    "wind_gusts_10m_ms": hour.get("wind_gusts_10m"),
                    "wind_power_density_80m_wm2": hour.get("wind_power_density_80m"),
                })

        result = odl_write("fact", "fact_wind_measurement", rows)
        logger.info(f"fact_wind_measurement: {result['record_count']} rows")
        return result

    @task(outlets=[ODL_FACT_FLIGHT])
    def build_fact_flight_movement():
        """Build fact_flight_movement — aircraft position snapshots."""
        from bellosdata_common import odl_write
        spec = _get_fact_spec("fact_flight_movement")
        logger.info(f"Building {spec['name']} (grain: {spec['grain']})")

        records = _read_rdl(spec["source_rdl"])
        if not records:
            return odl_write("fact", "fact_flight_movement", [])

        rows = []
        for rec in records:
            ts = rec.get("snapshot_time", rec.get("time", ""))
            if not ts:
                continue
            date_str = str(ts)[:10]
            try:
                date_key = int(date_str.replace("-", ""))
            except (ValueError, TypeError):
                continue

            rows.append({
                "icao24": rec.get("icao24", ""),
                "snapshot_timestamp": ts,
                "snapshot_date_key": date_key,
                "nearest_airport_icao": rec.get("nearest_airport", ""),
                "altitude_m": rec.get("baro_altitude", rec.get("geo_altitude")),
                "velocity_ms": rec.get("velocity"),
                "vertical_rate_ms": rec.get("vertical_rate"),
                "heading_deg": rec.get("true_track"),
                "on_ground": rec.get("on_ground", False),
                "latitude": rec.get("latitude"),
                "longitude": rec.get("longitude"),
                "callsign": rec.get("callsign", "").strip(),
                "origin_country": rec.get("origin_country", ""),
            })

        result = odl_write("fact", "fact_flight_movement", rows)
        logger.info(f"fact_flight_movement: {result['record_count']} rows")
        return result

    @task(outlets=[ODL_FACT_COMPANY])
    def build_fact_company_filing():
        """Build fact_company_filing — CH filing events for NW companies."""
        from bellosdata_common import odl_write
        spec = _get_fact_spec("fact_company_filing")
        logger.info(f"Building {spec['name']} (grain: {spec['grain']})")

        records = _read_rdl(spec["source_rdl"])
        if not records:
            return odl_write("fact", "fact_company_filing", [])

        rows = []
        for rec in records:
            cn = rec.get("company_number", "")
            if not cn:
                continue

            inc_date = rec.get("incorporation_date", "")
            try:
                date_key = int(datetime.strptime(inc_date, "%d/%m/%Y").strftime("%Y%m%d"))
            except (ValueError, TypeError):
                date_key = None

            postcode = rec.get("postcode", "")
            sics = rec.get("sic_codes", [])

            rows.append({
                "company_number": cn,
                "company_name": rec.get("company_name", ""),
                "filing_date_key": date_key,
                "registered_postcode": postcode,
                "company_status": rec.get("company_status", ""),
                "company_category": rec.get("company_category", ""),
                "sic_primary": sics[0] if sics else "",
                "account_category": rec.get("account_category", ""),
                "filing_count": 1,
                "filing_type": "REGISTRATION",
                "filing_category": "incorporation",
            })

        result = odl_write("fact", "fact_company_filing", rows)
        logger.info(f"fact_company_filing: {result['record_count']} rows")
        return result

    # All facts build in parallel
    build_fact_weather_observation()
    build_fact_wind_measurement()
    build_fact_flight_movement()
    build_fact_company_filing()

odl_fact_builder()
