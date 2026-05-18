"""
Private Jet Fleet Intelligence â€” CAA & ADS-B Ingestion Pipeline

Ingests UK private jet data from:
1. CAA G-INFO Aircraft Register (public CSV)
2. OpenSky Network ADS-B data (free API)
3. Companies House cross-reference (existing Bronze layer)

Focus: North West England (Manchester-Liverpool corridor)
Schedule: Weekly (data doesn't change daily)
Author: Awujoo (AWUJOO-028)
Genesis: 2026-05-16

Trade Route: TR-001 â€” Private Jet Fleet Intelligence
"""

from datetime import datetime, timedelta
import json
import csv
import io
import os
import logging

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

# â”€â”€ Assets â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
PRIVATE_JET_BRONZE = Asset("s3://bellosdata-bronze-raw/private-jets")

# â”€â”€ Configuration â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
S3_BUCKET = "bellosdata-bronze-raw"
S3_PREFIX = "private-jets"
from aws_session import get_aws_session, AWS_REGION
REGION = AWS_REGION

# North West airports (ICAO codes)
NW_AIRPORTS = {
    "EGCC": "Manchester",
    "EGGP": "Liverpool John Lennon",
    "EGNR": "Hawarden",
    "EGCB": "City of Manchester (Barton)",
    "EGNH": "Blackpool",
    "EGNC": "Carlisle Lake District",
}

# Private jet categories (CAA aircraft types)
JET_CATEGORIES = [
    "Cessna Citation",
    "Bombardier",
    "Gulfstream",
    "Dassault Falcon",
    "Embraer",
    "Learjet",
    "Hawker",
    "Beechcraft King Air",
    "Pilatus PC-12",
    "Pilatus PC-24",
    "HondaJet",
    "Eclipse",
    "Raytheon",
]

# Age threshold for "old" jets
OLD_JET_YEARS = 15


@dag(
    dag_id="private_jet_ingestion",
    description="Ingest UK private jet fleet data from CAA register and ADS-B sources",
    schedule="0 6 * * 1",  # Every Monday at 06:00 UTC
    start_date=datetime(2026, 5, 16),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "awujoo",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["private-jets", "ingestion", "TR-001", "bronze", "sovereign"],
)
def private_jet_ingestion():

    @task()
    def ingest_caa_register() -> dict:
        """
        Download the CAA G-INFO aircraft register.
        Source: https://siteapps.caa.co.uk/g-info/
        The register is available as a downloadable CSV.
        """
        import urllib.request
        import ssl

        # CAA G-INFO download endpoint (public data)
        CAA_URL = "https://siteapps.caa.co.uk/g-info/download/aircraft-register.csv"

        logger.info("Downloading CAA G-INFO aircraft register...")

        try:
            ctx = ssl.create_default_context()
            req = urllib.request.Request(CAA_URL, headers={
                "User-Agent": "BellosData-PrivateJetIntel/1.0 (awujoo@bellosdata.com)"
            })
            response = urllib.request.urlopen(req, context=ctx, timeout=120)
            raw_data = response.read().decode("utf-8", errors="replace")
            logger.info(f"Downloaded {len(raw_data)} bytes from CAA G-INFO")
        except Exception as e:
            logger.warning(f"CAA download failed: {e}. Using fallback scrape method.")
            # Fallback: try the alternative endpoint
            try:
                alt_url = "https://publicapps.caa.co.uk/modalapplication.aspx?appid=1&mode=summary&type=aircraft"
                req = urllib.request.Request(alt_url, headers={
                    "User-Agent": "BellosData-PrivateJetIntel/1.0"
                })
                response = urllib.request.urlopen(req, context=ctx, timeout=120)
                raw_data = response.read().decode("utf-8", errors="replace")
                logger.info(f"Fallback downloaded {len(raw_data)} bytes")
            except Exception as e2:
                logger.error(f"Both CAA endpoints failed: {e2}")
                raw_data = None

        if not raw_data:
            return {"status": "FAILED", "records": 0, "source": "CAA G-INFO"}

        # Parse CSV
        aircraft = []
        reader = csv.DictReader(io.StringIO(raw_data))
        current_year = datetime.now().year

        for row in reader:
            # Normalize field names (CAA uses varying formats)
            reg = row.get("Registration", row.get("registration", "")).strip()
            ac_type = row.get("Type", row.get("type", row.get("Aircraft Type", ""))).strip()
            manufacturer = row.get("Manufacturer", row.get("manufacturer", "")).strip()
            year_str = row.get("Year of Manufacture", row.get("year_of_manufacture", "")).strip()
            owner = row.get("Registered Owner", row.get("registered_owner", "")).strip()
            status = row.get("Status", row.get("status", "")).strip()
            base = row.get("Base", row.get("base", "")).strip()

            # Filter: only jets/turboprops that match our categories
            full_type = f"{manufacturer} {ac_type}".upper()
            is_jet = any(cat.upper() in full_type for cat in JET_CATEGORIES)

            if not is_jet:
                continue

            # Parse year
            try:
                year = int(year_str) if year_str else None
            except ValueError:
                year = None

            age = (current_year - year) if year else None
            is_old = age is not None and age >= OLD_JET_YEARS

            aircraft.append({
                "registration": reg,
                "manufacturer": manufacturer,
                "type": ac_type,
                "year_of_manufacture": year,
                "age_years": age,
                "is_old_jet": is_old,
                "registered_owner": owner,
                "status": status,
                "base": base,
                "source": "CAA_GINFO",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
            })

        logger.info(f"Parsed {len(aircraft)} private jets from CAA register")

        return {
            "status": "OK",
            "records": len(aircraft),
            "source": "CAA G-INFO",
            "data": aircraft,
        }

    @task()
    def ingest_opensky_nw_traffic() -> dict:
        """
        Query OpenSky Network for recent private jet traffic
        in the North West England bounding box.

        Bounding box: ~52.5N to 54.5N, ~3.5W to ~1.5W
        (covers Manchester, Liverpool, Hawarden, Barton, Blackpool)
        """
        import urllib.request
        import ssl

        # NW England bounding box
        LAMIN = 52.5
        LAMAX = 54.5
        LOMIN = -3.5
        LOMAX = -1.5

        # OpenSky REST API (free, no auth for state vectors)
        OPENSKY_URL = (
            f"https://opensky-network.org/api/states/all"
            f"?lamin={LAMIN}&lomin={LOMIN}&lamax={LAMAX}&lomax={LOMAX}"
        )

        logger.info(f"Querying OpenSky for NW England traffic...")

        try:
            ctx = ssl.create_default_context()
            req = urllib.request.Request(OPENSKY_URL, headers={
                "User-Agent": "BellosData-PrivateJetIntel/1.0"
            })
            response = urllib.request.urlopen(req, context=ctx, timeout=60)
            data = json.loads(response.read().decode("utf-8"))
        except Exception as e:
            logger.warning(f"OpenSky query failed: {e}")
            return {"status": "FAILED", "records": 0, "source": "OpenSky"}

        states = data.get("states", [])
        timestamp = data.get("time", 0)

        # OpenSky state vector fields:
        # 0:icao24, 1:callsign, 2:origin_country, 3:time_position,
        # 4:last_contact, 5:longitude, 6:latitude, 7:baro_altitude,
        # 8:on_ground, 9:velocity, 10:true_track, 11:vertical_rate,
        # 12:sensors, 13:geo_altitude, 14:squawk, 15:spi, 16:position_source

        flights = []
        for s in (states or []):
            callsign = (s[1] or "").strip()
            icao24 = s[0]
            origin = s[2]
            lon = s[5]
            lat = s[6]
            alt = s[7]
            on_ground = s[8]
            velocity = s[9]

            flights.append({
                "icao24": icao24,
                "callsign": callsign,
                "origin_country": origin,
                "longitude": lon,
                "latitude": lat,
                "altitude_m": alt,
                "on_ground": on_ground,
                "velocity_ms": velocity,
                "snapshot_time": timestamp,
                "region": "NW_ENGLAND",
                "source": "OPENSKY",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
            })

        logger.info(f"Captured {len(flights)} aircraft in NW England airspace")

        return {
            "status": "OK",
            "records": len(flights),
            "source": "OpenSky",
            "snapshot_time": timestamp,
            "data": flights,
        }

    @task(outlets=[PRIVATE_JET_BRONZE])
    def write_to_bronze(caa_result: dict, opensky_result: dict) -> dict:
        """Write ingested data to S3 Bronze layer as Delta Lake tables."""
        import pyarrow as pa
        from deltalake import write_deltalake
        import boto3

        # Configure S3 storage options for Delta Lake writer
        session = get_aws_session()
        credentials = session.get_credentials().get_frozen_credentials()
        storage_options = {
            "AWS_ACCESS_KEY_ID": credentials.access_key,
            "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
            "AWS_REGION": REGION,
        }
        if credentials.token:
            storage_options["AWS_SESSION_TOKEN"] = credentials.token

        s3 = session.client("s3", region_name=REGION)

        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        ts_str = now.strftime("%Y%m%dT%H%M%SZ")

        uploaded = {}

        # Write CAA register data as Delta table
        if caa_result.get("status") == "OK" and caa_result.get("data"):
            caa_table = pa.Table.from_pylist(caa_result["data"])
            caa_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/caa-register"
            write_deltalake(
                caa_path,
                caa_table,
                mode="append",
                storage_options=storage_options,
            )
            uploaded["caa_register"] = {
                "delta_table": caa_path,
                "records": caa_result["records"],
            }
            logger.info(f"Wrote {caa_result['records']} CAA records â†’ {caa_path} (Delta)")

        # Write OpenSky traffic data as Delta table
        if opensky_result.get("status") == "OK" and opensky_result.get("data"):
            osky_table = pa.Table.from_pylist(opensky_result["data"])
            osky_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/opensky-traffic"
            write_deltalake(
                osky_path,
                osky_table,
                mode="append",
                storage_options=storage_options,
            )
            uploaded["opensky_traffic"] = {
                "delta_table": osky_path,
                "records": opensky_result["records"],
            }
            logger.info(f"Wrote {opensky_result['records']} OpenSky records â†’ {osky_path} (Delta)")

        # Write ingestion manifest (JSON â€” metadata, not governed data)
        manifest = {
            "ingestion_id": f"PJ-{ts_str}",
            "timestamp": now.isoformat() + "Z",
            "trade_route": "TR-001",
            "format": "DELTA",
            "sources": {
                "caa": caa_result.get("status"),
                "opensky": opensky_result.get("status"),
            },
            "total_records": sum(v.get("records", 0) for v in uploaded.values()),
            "delta_tables": {k: v["delta_table"] for k, v in uploaded.items()},
        }
        manifest_key = f"{S3_PREFIX}/manifests/{date_str}/manifest_{ts_str}.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info(f"Ingestion manifest: {json.dumps(manifest, indent=2)}")

        return manifest

    # â”€â”€ DAG Flow â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    caa_data = ingest_caa_register()
    opensky_data = ingest_opensky_nw_traffic()
    write_to_bronze(caa_data, opensky_data)


# Instantiate
private_jet_ingestion()
