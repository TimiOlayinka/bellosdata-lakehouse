"""
RDL Airports Ingestion — UK Airport & Runway Pipeline

Source: OurAirports (ourairports.com/data/) — Free CSV, Public Domain
Ingests all UK airports with ICAO/IATA codes, coordinates, elevation,
runways, frequencies, and navaids. Focus on NW England for TR-001 jets.

Schedule: Monthly (1st of month at 04:00 UTC)
RDL Output: s3://playdarch-bronze-raw/rdl/airports/
Asset: Triggers downstream ODL dim_airport builder

Author: Awujoo (AWUJOO-041) | Genesis: 2026-05-17
Trade Route: TR-001 (Private Jet Fleet Intelligence)
"""

from datetime import datetime, timedelta
import json
import logging

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

# ── Assets ──
RDL_AIRPORTS_ASSET = Asset("s3://playdarch-bronze-raw/rdl/airports")

# ── OurAirports Data URLs (Public Domain CSV) ──
OURAIRPORTS_BASE = "https://davidmegginson.github.io/ourairports-data"
AIRPORTS_CSV = f"{OURAIRPORTS_BASE}/airports.csv"
RUNWAYS_CSV = f"{OURAIRPORTS_BASE}/runways.csv"
FREQUENCIES_CSV = f"{OURAIRPORTS_BASE}/airport-frequencies.csv"
NAVAIDS_CSV = f"{OURAIRPORTS_BASE}/navaids.csv"

# UK ISO country code
UK_COUNTRY = "GB"

# NW England bounding box for filtering navaids
NW_BOUNDS = {
    "lat_min": 52.5, "lat_max": 55.0,
    "lon_min": -4.0, "lon_max": -1.5,
}

# Key NW airports (ICAO) for enhanced data collection
NW_KEY_AIRPORTS = [
    "EGCC",  # Manchester
    "EGGP",  # Liverpool John Lennon
    "EGNR",  # Hawarden
    "EGCB",  # Barton (City of Manchester)
    "EGNH",  # Blackpool
    "EGNC",  # Carlisle Lake District
    "EGNM",  # Leeds Bradford (close to NW)
    "EGTC",  # Cranfield (biz aviation)
]


@dag(
    dag_id="rdl_airports_ingestion",
    description="Ingest UK airports, runways, frequencies from OurAirports → RDL",
    schedule="0 4 1 * *",
    start_date=datetime(2026, 5, 17),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "awujoo",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["airports", "rdl", "ingestion", "enrichment", "TR-001", "sovereign"],
)
def rdl_airports_ingestion():

    @task()
    def ingest_airports() -> dict:
        """Download and filter UK airports from OurAirports CSV."""
        from bellosdata_common import http_get_csv

        logger.info("Downloading OurAirports airports.csv...")
        all_airports = http_get_csv(AIRPORTS_CSV, timeout=120)

        # Filter to UK only
        uk_airports = []
        for row in all_airports:
            if row.get("iso_country") != UK_COUNTRY:
                continue

            try:
                lat = float(row.get("latitude_deg", 0))
                lon = float(row.get("longitude_deg", 0))
            except (ValueError, TypeError):
                lat, lon = None, None

            # Determine if in NW England bounding box
            in_nw = (lat and lon and
                     NW_BOUNDS["lat_min"] <= lat <= NW_BOUNDS["lat_max"] and
                     NW_BOUNDS["lon_min"] <= lon <= NW_BOUNDS["lon_max"])

            uk_airports.append({
                "ident": row.get("ident", ""),
                "type": row.get("type", ""),
                "name": row.get("name", ""),
                "latitude_deg": lat,
                "longitude_deg": lon,
                "elevation_ft": row.get("elevation_ft"),
                "continent": row.get("continent"),
                "iso_country": row.get("iso_country"),
                "iso_region": row.get("iso_region"),
                "municipality": row.get("municipality"),
                "scheduled_service": row.get("scheduled_service"),
                "gps_code": row.get("gps_code"),
                "iata_code": row.get("iata_code"),
                "local_code": row.get("local_code"),
                "home_link": row.get("home_link"),
                "keywords": row.get("keywords"),
                "in_nw_england": in_nw,
                "is_key_nw": row.get("ident", "") in NW_KEY_AIRPORTS,
                "record_type": "airport",
                "source": "OURAIRPORTS",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
            })

        nw_count = sum(1 for a in uk_airports if a["in_nw_england"])
        logger.info(f"UK airports: {len(uk_airports)} total, {nw_count} in NW England")

        return {
            "status": "OK",
            "records": len(uk_airports),
            "nw_count": nw_count,
            "data": uk_airports,
        }

    @task()
    def ingest_runways() -> dict:
        """Download runway data and match to UK airports."""
        from bellosdata_common import http_get_csv

        logger.info("Downloading OurAirports runways.csv...")
        all_runways = http_get_csv(RUNWAYS_CSV, timeout=120)

        # We'll filter to UK in the join, but grab all for now
        # (airport_ident links to airports.ident)
        uk_runways = []
        for row in all_runways:
            ref = row.get("airport_ref", "")
            ident = row.get("airport_ident", "")

            # We'll include all — the ODL layer will join/filter
            uk_runways.append({
                "id": row.get("id"),
                "airport_ref": ref,
                "airport_ident": ident,
                "length_ft": row.get("length_ft"),
                "width_ft": row.get("width_ft"),
                "surface": row.get("surface"),
                "lighted": row.get("lighted"),
                "closed": row.get("closed"),
                "le_ident": row.get("le_ident"),
                "le_latitude_deg": row.get("le_latitude_deg"),
                "le_longitude_deg": row.get("le_longitude_deg"),
                "le_elevation_ft": row.get("le_elevation_ft"),
                "le_heading_degT": row.get("le_heading_degT"),
                "he_ident": row.get("he_ident"),
                "he_latitude_deg": row.get("he_latitude_deg"),
                "he_longitude_deg": row.get("he_longitude_deg"),
                "he_elevation_ft": row.get("he_elevation_ft"),
                "he_heading_degT": row.get("he_heading_degT"),
                "record_type": "runway",
                "source": "OURAIRPORTS",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
            })

        logger.info(f"Total runways downloaded: {len(uk_runways)}")
        return {
            "status": "OK",
            "records": len(uk_runways),
            "data": uk_runways,
        }

    @task()
    def ingest_frequencies() -> dict:
        """Download airport radio frequencies."""
        from bellosdata_common import http_get_csv

        logger.info("Downloading OurAirports frequencies.csv...")
        all_freqs = http_get_csv(FREQUENCIES_CSV, timeout=120)

        freqs = []
        for row in all_freqs:
            freqs.append({
                "id": row.get("id"),
                "airport_ref": row.get("airport_ref"),
                "airport_ident": row.get("airport_ident"),
                "type": row.get("type"),
                "description": row.get("description"),
                "frequency_mhz": row.get("frequency_mhz"),
                "record_type": "frequency",
                "source": "OURAIRPORTS",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
            })

        logger.info(f"Total frequencies: {len(freqs)}")
        return {"status": "OK", "records": len(freqs), "data": freqs}

    @task(outlets=[RDL_AIRPORTS_ASSET])
    def write_to_rdl(airports: dict, runways: dict, freqs: dict) -> dict:
        """Write all airport data to RDL as raw JSON + ingest_date."""
        from bellosdata_common import rdl_write, write_manifest

        all_records = []
        for dataset in [airports, runways, freqs]:
            all_records.extend(dataset.get("data", []))

        rdl_result = rdl_write("airports", all_records)

        manifest = write_manifest(
            dag_id="rdl_airports_ingestion",
            source_name="airports",
            rdl_result=rdl_result,
            record_counts={
                "airports": airports.get("records", 0),
                "runways": runways.get("records", 0),
                "frequencies": freqs.get("records", 0),
            },
            extra={
                "api_source": "ourairports.com",
                "nw_airports": airports.get("nw_count", 0),
                "key_nw_icao": NW_KEY_AIRPORTS,
            },
        )
        return manifest

    # ── DAG Flow (parallel ingestion → single RDL write) ──
    ap = ingest_airports()
    rw = ingest_runways()
    fq = ingest_frequencies()
    write_to_rdl(ap, rw, fq)


# Instantiate
rdl_airports_ingestion()
