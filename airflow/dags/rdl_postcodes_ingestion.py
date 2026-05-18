"""
RDL Postcodes Ingestion — UK Postcode Geography Pipeline

Source: postcodes.io (100% free, open-source, no API key)
Ingests NW England postcodes with lat/lon, LSOA, MSOA, ward, constituency.
This is the FOUNDATIONAL enrichment — everything links to postcodes.

Schedule: Monthly (1st of month at 03:00 UTC)
RDL Output: s3://playdarch-bronze-raw/rdl/postcodes/
Asset: Triggers downstream ODL dim_location builder

Author: Awujoo (AWUJOO-041) | Genesis: 2026-05-17
Trade Route: Cross-cutting enrichment (TR-001, TR-002, TR-004)
"""

from datetime import datetime, timedelta
import json
import logging
import time

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

# ── Assets ──
RDL_POSTCODES_ASSET = Asset("s3://playdarch-bronze-raw/rdl/postcodes")

# ── NW England Postcode Districts ──
# These are the outcode prefixes covering the North West
NW_OUTCODE_PREFIXES = [
    # Greater Manchester
    "M", "BL", "OL", "SK", "WN", "WA",
    # Merseyside
    "L", "CH",
    # Lancashire
    "BB", "PR", "FY", "LA",
    # Cumbria
    "CA",
    # Cheshire
    "CW",
]

# postcodes.io bulk lookup — 100 postcodes per request
POSTCODES_IO_BASE = "https://api.postcodes.io"
OUTCODES_ENDPOINT = f"{POSTCODES_IO_BASE}/outcodes"


@dag(
    dag_id="rdl_postcodes_ingestion",
    description="Ingest UK postcode geography for NW England (postcodes.io → RDL)",
    schedule="0 3 1 * *",
    start_date=datetime(2026, 5, 17),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "awujoo",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["postcodes", "rdl", "ingestion", "enrichment", "geography", "sovereign"],
)
def rdl_postcodes_ingestion():

    @task()
    def ingest_outcodes() -> dict:
        """
        Fetch all NW England outcodes (postcode districts) with metadata.

        Outcodes are the first half of a postcode (e.g. "M1", "L1", "PR1").
        postcodes.io returns centroid lat/lon, admin/parish for each.
        We enumerate outcode numbers for each prefix.
        """
        from bellosdata_common import http_get

        all_outcodes = []

        for prefix in NW_OUTCODE_PREFIXES:
            # Try outcode numbers 1-99 for each prefix
            for num in range(1, 100):
                outcode = f"{prefix}{num}"
                url = f"{OUTCODES_ENDPOINT}/{outcode}"

                try:
                    data = http_get(url, retries=1, timeout=10)
                    result = data.get("result")

                    if result:
                        all_outcodes.append({
                            "outcode": outcode,
                            "prefix": prefix,
                            "latitude": result.get("latitude"),
                            "longitude": result.get("longitude"),
                            "admin_district": result.get("admin_district", []),
                            "admin_county": result.get("admin_county", []),
                            "admin_ward": result.get("admin_ward", []),
                            "parish": result.get("parish", []),
                            "parliamentary_constituency": result.get(
                                "parliamentary_constituency", []
                            ),
                            "country": result.get("country", []),
                            "source": "POSTCODES_IO",
                            "ingested_at": datetime.utcnow().isoformat() + "Z",
                        })
                except Exception:
                    # Outcode doesn't exist — skip silently
                    continue

                # Be respectful — small delay between requests
                time.sleep(0.05)

            logger.info(f"Prefix {prefix}: scanned, found outcodes so far: {len(all_outcodes)}")

        logger.info(f"Total NW outcodes ingested: {len(all_outcodes)}")
        return {
            "status": "OK" if all_outcodes else "EMPTY",
            "records": len(all_outcodes),
            "prefixes_scanned": len(NW_OUTCODE_PREFIXES),
            "data": all_outcodes,
        }

    @task()
    def ingest_random_postcodes() -> dict:
        """
        Fetch detailed postcode records using postcodes.io random endpoint
        filtered by outcode. Gets full LSOA/MSOA/ward detail.

        We sample postcodes from each NW outcode to build a rich geography table.
        """
        from bellosdata_common import http_get

        all_postcodes = []

        for prefix in NW_OUTCODE_PREFIXES:
            # Get a sample of postcodes for this prefix area
            for num in range(1, 50):
                outcode = f"{prefix}{num}"
                url = f"{POSTCODES_IO_BASE}/random/postcodes?outcode={outcode}"

                try:
                    data = http_get(url, retries=1, timeout=10)
                    result = data.get("result")

                    if result:
                        all_postcodes.append({
                            "postcode": result.get("postcode"),
                            "outcode": result.get("outcode"),
                            "incode": result.get("incode"),
                            "quality": result.get("quality"),
                            "latitude": result.get("latitude"),
                            "longitude": result.get("longitude"),
                            "eastings": result.get("eastings"),
                            "northings": result.get("northings"),
                            "country": result.get("country"),
                            "region": result.get("region"),
                            "admin_district": result.get("admin_district"),
                            "admin_county": result.get("admin_county"),
                            "admin_ward": result.get("admin_ward"),
                            "parish": result.get("parish"),
                            "parliamentary_constituency": result.get(
                                "parliamentary_constituency"
                            ),
                            "lsoa": result.get("lsoa"),
                            "msoa": result.get("msoa"),
                            "ced": result.get("ced"),
                            "ccg": result.get("ccg"),
                            "nuts": result.get("nuts"),
                            "pfa": result.get("pfa"),
                            "source": "POSTCODES_IO",
                            "ingested_at": datetime.utcnow().isoformat() + "Z",
                        })
                except Exception:
                    continue

                time.sleep(0.05)

            logger.info(f"Prefix {prefix}: sampled postcodes, total: {len(all_postcodes)}")

        logger.info(f"Total detailed postcodes: {len(all_postcodes)}")
        return {
            "status": "OK" if all_postcodes else "EMPTY",
            "records": len(all_postcodes),
            "data": all_postcodes,
        }

    @task(outlets=[RDL_POSTCODES_ASSET])
    def write_to_rdl(outcodes_result: dict, postcodes_result: dict) -> dict:
        """Write all postcode data to RDL as raw JSON + ingest_date."""
        from bellosdata_common import rdl_write, write_manifest

        # Combine both datasets into a single RDL write
        all_records = []

        if outcodes_result.get("data"):
            for record in outcodes_result["data"]:
                record["record_type"] = "outcode"
                all_records.append(record)

        if postcodes_result.get("data"):
            for record in postcodes_result["data"]:
                record["record_type"] = "postcode"
                all_records.append(record)

        # Write to RDL
        rdl_result = rdl_write("postcodes", all_records)

        # Write manifest
        manifest = write_manifest(
            dag_id="rdl_postcodes_ingestion",
            source_name="postcodes",
            rdl_result=rdl_result,
            record_counts={
                "outcodes": outcodes_result.get("records", 0),
                "postcodes": postcodes_result.get("records", 0),
            },
            extra={
                "api_source": "postcodes.io",
                "nw_prefixes": NW_OUTCODE_PREFIXES,
            },
        )

        return manifest

    # ── DAG Flow ──
    outcodes = ingest_outcodes()
    postcodes = ingest_random_postcodes()
    write_to_rdl(outcodes, postcodes)


# Instantiate
rdl_postcodes_ingestion()
