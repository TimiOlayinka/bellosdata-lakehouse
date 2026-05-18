"""
BellosData Unity Catalog — Table Registration Script

Registers the full medallion lakehouse schema in Unity Catalog OSS
via the REST API. Organised by trade route with layer prefixes:

    rdl_ = Raw Data Layer (Bronze)
    odl_ = Operational Data Layer (Silver)
    adl_ = Analytical Data Layer (Gold)

Usage:
    $env:PYTHONIOENCODING="utf-8"
    python scripts/register_tables.py

Author: Antigravity (for Awujoo)
Genesis: 2026-05-16
"""

import json
import urllib.request
import urllib.error
import sys

UC_BASE = "http://localhost:8070/api/2.1/unity-catalog"

# ── Catalog & Schema Definitions ──────────────────────────────

CATALOG = "bellosdata"
CATALOG_COMMENT = "BellosData Sovereign Lakehouse — Trade Route Architecture on S3"

SCHEMAS = {
    "private_jets": "TR-001 — UK Private Jet Fleet Intelligence (CAA, OpenSky)",
    "nw_birds": "TR-004 — North West Bird Watch Intelligence (eBird, iNaturalist, Wikimedia)",
    "core": "Operational Lakehouse — Sessions, Merchants, Trade Routes",
}

# ── Table Definitions ─────────────────────────────────────────
#
# Naming convention:
#   rdl_ = Raw Data Layer (Bronze)
#   odl_ = Operational Data Layer (Silver)
#   adl_ = Analytical Data Layer (Gold)

TABLES = [
    # ══════════════════════════════════════════════════════════
    # private_jets (TR-001)
    # ══════════════════════════════════════════════════════════
    {
        "schema": "private_jets",
        "name": "rdl_caa_register",
        "comment": "CAA G-INFO Aircraft Register — UK private jet registrations (Raw Data Layer)",
        "storage_location": "s3://playdarch-bronze-raw/private-jets/caa-register/",
        "columns": [
            {"name": "registration", "type_text": "string", "type_name": "STRING"},
            {"name": "manufacturer", "type_text": "string", "type_name": "STRING"},
            {"name": "type", "type_text": "string", "type_name": "STRING"},
            {"name": "year_of_manufacture", "type_text": "int", "type_name": "INT"},
            {"name": "age_years", "type_text": "int", "type_name": "INT"},
            {"name": "is_old_jet", "type_text": "boolean", "type_name": "BOOLEAN"},
            {"name": "registered_owner", "type_text": "string", "type_name": "STRING"},
            {"name": "status", "type_text": "string", "type_name": "STRING"},
            {"name": "base", "type_text": "string", "type_name": "STRING"},
            {"name": "source", "type_text": "string", "type_name": "STRING"},
            {"name": "ingested_at", "type_text": "string", "type_name": "STRING"},
        ],
    },
    {
        "schema": "private_jets",
        "name": "rdl_opensky_traffic",
        "comment": "OpenSky ADS-B traffic snapshots — NW England airspace (Raw Data Layer)",
        "storage_location": "s3://playdarch-bronze-raw/private-jets/opensky-traffic/",
        "columns": [
            {"name": "icao24", "type_text": "string", "type_name": "STRING"},
            {"name": "callsign", "type_text": "string", "type_name": "STRING"},
            {"name": "origin_country", "type_text": "string", "type_name": "STRING"},
            {"name": "longitude", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "latitude", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "altitude_m", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "on_ground", "type_text": "boolean", "type_name": "BOOLEAN"},
            {"name": "velocity_ms", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "snapshot_time", "type_text": "long", "type_name": "LONG"},
            {"name": "region", "type_text": "string", "type_name": "STRING"},
            {"name": "source", "type_text": "string", "type_name": "STRING"},
            {"name": "ingested_at", "type_text": "string", "type_name": "STRING"},
        ],
    },
    {
        "schema": "private_jets",
        "name": "odl_fleet_enriched",
        "comment": "Enriched fleet data — CAA + OpenSky joined, deduplicated (Operational Data Layer)",
        "storage_location": "s3://playdarch-silver-curated/private-jets/fleet-enriched/",
        "columns": [
            {"name": "registration", "type_text": "string", "type_name": "STRING"},
            {"name": "icao24", "type_text": "string", "type_name": "STRING"},
            {"name": "manufacturer", "type_text": "string", "type_name": "STRING"},
            {"name": "type", "type_text": "string", "type_name": "STRING"},
            {"name": "age_years", "type_text": "int", "type_name": "INT"},
            {"name": "is_old_jet", "type_text": "boolean", "type_name": "BOOLEAN"},
            {"name": "registered_owner", "type_text": "string", "type_name": "STRING"},
            {"name": "last_seen_lat", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "last_seen_lon", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "flight_count", "type_text": "int", "type_name": "INT"},
            {"name": "updated_at", "type_text": "string", "type_name": "STRING"},
        ],
    },
    {
        "schema": "private_jets",
        "name": "adl_aging_fleet_report",
        "comment": "Business-ready aging fleet intelligence product (Analytical Data Layer)",
        "storage_location": "s3://playdarch-gold-products/private-jets/aging-fleet-report/",
        "columns": [
            {"name": "registration", "type_text": "string", "type_name": "STRING"},
            {"name": "manufacturer", "type_text": "string", "type_name": "STRING"},
            {"name": "type", "type_text": "string", "type_name": "STRING"},
            {"name": "age_years", "type_text": "int", "type_name": "INT"},
            {"name": "registered_owner", "type_text": "string", "type_name": "STRING"},
            {"name": "base", "type_text": "string", "type_name": "STRING"},
            {"name": "flight_count_30d", "type_text": "int", "type_name": "INT"},
            {"name": "risk_score", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "report_date", "type_text": "string", "type_name": "STRING"},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # nw_birds (TR-004)
    # ══════════════════════════════════════════════════════════
    {
        "schema": "nw_birds",
        "name": "rdl_ebird_sightings",
        "comment": "eBird sightings — NW England bird observations (Raw Data Layer)",
        "storage_location": "s3://playdarch-bronze-raw/nw-birds/ebird-sightings/",
        "columns": [
            {"name": "species_code", "type_text": "string", "type_name": "STRING"},
            {"name": "common_name", "type_text": "string", "type_name": "STRING"},
            {"name": "scientific_name", "type_text": "string", "type_name": "STRING"},
            {"name": "location_name", "type_text": "string", "type_name": "STRING"},
            {"name": "latitude", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "longitude", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "observation_date", "type_text": "string", "type_name": "STRING"},
            {"name": "how_many", "type_text": "int", "type_name": "INT"},
            {"name": "county", "type_text": "string", "type_name": "STRING"},
            {"name": "region_code", "type_text": "string", "type_name": "STRING"},
            {"name": "source", "type_text": "string", "type_name": "STRING"},
            {"name": "ingested_at", "type_text": "string", "type_name": "STRING"},
        ],
    },
    {
        "schema": "nw_birds",
        "name": "rdl_inaturalist",
        "comment": "iNaturalist observations with CC-licensed photos — NW England (Raw Data Layer)",
        "storage_location": "s3://playdarch-bronze-raw/nw-birds/inaturalist/",
        "columns": [
            {"name": "inat_id", "type_text": "int", "type_name": "INT"},
            {"name": "species_common", "type_text": "string", "type_name": "STRING"},
            {"name": "species_scientific", "type_text": "string", "type_name": "STRING"},
            {"name": "taxon_id", "type_text": "int", "type_name": "INT"},
            {"name": "observed_on", "type_text": "string", "type_name": "STRING"},
            {"name": "place_guess", "type_text": "string", "type_name": "STRING"},
            {"name": "latitude", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "longitude", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "quality_grade", "type_text": "string", "type_name": "STRING"},
            {"name": "photos_count", "type_text": "int", "type_name": "INT"},
            {"name": "cc_photos_count", "type_text": "int", "type_name": "INT"},
            {"name": "source", "type_text": "string", "type_name": "STRING"},
            {"name": "ingested_at", "type_text": "string", "type_name": "STRING"},
        ],
    },
    {
        "schema": "nw_birds",
        "name": "rdl_wikimedia_photos",
        "comment": "Wikimedia Commons CC-licensed bird photos — NW signature species (Raw Data Layer)",
        "storage_location": "s3://playdarch-bronze-raw/nw-birds/wikimedia-photos/",
        "columns": [
            {"name": "species_name", "type_text": "string", "type_name": "STRING"},
            {"name": "title", "type_text": "string", "type_name": "STRING"},
            {"name": "url_original", "type_text": "string", "type_name": "STRING"},
            {"name": "url_thumbnail", "type_text": "string", "type_name": "STRING"},
            {"name": "width", "type_text": "int", "type_name": "INT"},
            {"name": "height", "type_text": "int", "type_name": "INT"},
            {"name": "mime_type", "type_text": "string", "type_name": "STRING"},
            {"name": "license", "type_text": "string", "type_name": "STRING"},
            {"name": "artist", "type_text": "string", "type_name": "STRING"},
            {"name": "source", "type_text": "string", "type_name": "STRING"},
            {"name": "ingested_at", "type_text": "string", "type_name": "STRING"},
        ],
    },
    {
        "schema": "nw_birds",
        "name": "odl_species_unified",
        "comment": "Unified species data — eBird + iNaturalist cross-referenced (Operational Data Layer)",
        "storage_location": "s3://playdarch-silver-curated/nw-birds/species-unified/",
        "columns": [
            {"name": "species_code", "type_text": "string", "type_name": "STRING"},
            {"name": "common_name", "type_text": "string", "type_name": "STRING"},
            {"name": "scientific_name", "type_text": "string", "type_name": "STRING"},
            {"name": "total_sightings", "type_text": "int", "type_name": "INT"},
            {"name": "total_photos", "type_text": "int", "type_name": "INT"},
            {"name": "cc_photos_available", "type_text": "int", "type_name": "INT"},
            {"name": "counties_observed", "type_text": "string", "type_name": "STRING"},
            {"name": "last_observed", "type_text": "string", "type_name": "STRING"},
            {"name": "updated_at", "type_text": "string", "type_name": "STRING"},
        ],
    },
    {
        "schema": "nw_birds",
        "name": "adl_bird_intelligence",
        "comment": "Business-ready NW bird intelligence product with imagery (Analytical Data Layer)",
        "storage_location": "s3://playdarch-gold-products/nw-birds/bird-intelligence/",
        "columns": [
            {"name": "species_code", "type_text": "string", "type_name": "STRING"},
            {"name": "common_name", "type_text": "string", "type_name": "STRING"},
            {"name": "scientific_name", "type_text": "string", "type_name": "STRING"},
            {"name": "total_sightings", "type_text": "int", "type_name": "INT"},
            {"name": "cc_photo_urls", "type_text": "string", "type_name": "STRING"},
            {"name": "hotspot_locations", "type_text": "string", "type_name": "STRING"},
            {"name": "seasonality_index", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "conservation_status", "type_text": "string", "type_name": "STRING"},
            {"name": "report_date", "type_text": "string", "type_name": "STRING"},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # core (Operational Lakehouse)
    # ══════════════════════════════════════════════════════════
    {
        "schema": "core",
        "name": "rdl_sessions",
        "comment": "Awujoo session records — compute token tracking (Raw Data Layer)",
        "storage_location": "s3://playdarch-bronze-raw/lakehouse/sessions/",
        "columns": [
            {"name": "session_id", "type_text": "string", "type_name": "STRING"},
            {"name": "date", "type_text": "string", "type_name": "STRING"},
            {"name": "tokens_spent", "type_text": "int", "type_name": "INT"},
            {"name": "tokens_returned", "type_text": "int", "type_name": "INT"},
            {"name": "value_generated", "type_text": "int", "type_name": "INT"},
            {"name": "status", "type_text": "string", "type_name": "STRING"},
            {"name": "filed_by", "type_text": "string", "type_name": "STRING"},
        ],
    },
    {
        "schema": "core",
        "name": "rdl_merchants",
        "comment": "Merchant profile data — active ecosystem members (Raw Data Layer)",
        "storage_location": "s3://playdarch-bronze-raw/lakehouse/merchants/",
        "columns": [
            {"name": "merchant_id", "type_text": "string", "type_name": "STRING"},
            {"name": "name", "type_text": "string", "type_name": "STRING"},
            {"name": "role", "type_text": "string", "type_name": "STRING"},
            {"name": "trust_score", "type_text": "int", "type_name": "INT"},
            {"name": "is_active", "type_text": "boolean", "type_name": "BOOLEAN"},
        ],
    },
    {
        "schema": "core",
        "name": "rdl_trade_routes",
        "comment": "Trade route registry — BellosData revenue streams (Raw Data Layer)",
        "storage_location": "s3://playdarch-bronze-raw/lakehouse/trade_routes/",
        "columns": [
            {"name": "route_id", "type_text": "string", "type_name": "STRING"},
            {"name": "name", "type_text": "string", "type_name": "STRING"},
            {"name": "status", "type_text": "string", "type_name": "STRING"},
            {"name": "target_revenue_gbp", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "actual_revenue_gbp", "type_text": "double", "type_name": "DOUBLE"},
        ],
    },
    {
        "schema": "core",
        "name": "odl_session_analytics",
        "comment": "Cleaned session metrics — token efficiency, value ratios (Operational Data Layer)",
        "storage_location": "s3://playdarch-silver-curated/lakehouse/session_analytics/",
        "columns": [
            {"name": "session_id", "type_text": "string", "type_name": "STRING"},
            {"name": "date", "type_text": "string", "type_name": "STRING"},
            {"name": "tokens_spent", "type_text": "int", "type_name": "INT"},
            {"name": "value_generated", "type_text": "int", "type_name": "INT"},
            {"name": "efficiency_ratio", "type_text": "double", "type_name": "DOUBLE"},
        ],
    },
    {
        "schema": "core",
        "name": "adl_merchant_pulse",
        "comment": "Real-time merchant health dashboard (Analytical Data Layer)",
        "storage_location": "s3://playdarch-gold-products/lakehouse/merchant_pulse/",
        "columns": [
            {"name": "merchant_id", "type_text": "string", "type_name": "STRING"},
            {"name": "name", "type_text": "string", "type_name": "STRING"},
            {"name": "trust_score", "type_text": "int", "type_name": "INT"},
            {"name": "total_sessions", "type_text": "int", "type_name": "INT"},
            {"name": "total_revenue_gbp", "type_text": "double", "type_name": "DOUBLE"},
        ],
    },
    {
        "schema": "core",
        "name": "adl_trade_flow",
        "comment": "Revenue and delivery tracking across all trade routes (Analytical Data Layer)",
        "storage_location": "s3://playdarch-gold-products/lakehouse/trade_flow/",
        "columns": [
            {"name": "route_id", "type_text": "string", "type_name": "STRING"},
            {"name": "name", "type_text": "string", "type_name": "STRING"},
            {"name": "status", "type_text": "string", "type_name": "STRING"},
            {"name": "target_revenue_gbp", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "actual_revenue_gbp", "type_text": "double", "type_name": "DOUBLE"},
            {"name": "delivery_count", "type_text": "int", "type_name": "INT"},
        ],
    },
]


def api_call(method: str, path: str, body: dict | None = None) -> dict | None:
    """Make a REST call to Unity Catalog server."""
    url = f"{UC_BASE}/{path}"
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        if e.code == 409:
            print(f"  Already exists: {path}")
            return None
        print(f"  HTTP {e.code} on {method} {path}: {error_body}")
        return None
    except Exception as e:
        print(f"  Error on {method} {path}: {e}")
        return None


def main():
    print("=" * 60)
    print("BellosData Unity Catalog - Table Registration")
    print("Trade Route Architecture with rdl/odl/adl layers")
    print("=" * 60)

    # 1. Ensure catalog exists
    print(f"\n[CATALOG] {CATALOG}")
    result = api_call("POST", "catalogs", {
        "name": CATALOG,
        "comment": CATALOG_COMMENT,
    })
    if result:
        print(f"  + Created")
    else:
        print(f"  = Already exists")

    # 2. Create schemas
    for schema_name, schema_comment in SCHEMAS.items():
        print(f"\n[SCHEMA] {CATALOG}.{schema_name}")
        result = api_call("POST", "schemas", {
            "name": schema_name,
            "catalog_name": CATALOG,
            "comment": schema_comment,
        })
        if result:
            print(f"  + Created: {schema_comment}")

    # 3. Register tables
    for table in TABLES:
        full_name = f"{CATALOG}.{table['schema']}.{table['name']}"
        layer = table["name"][:3].upper()
        print(f"\n[{layer}] {full_name}")

        columns = []
        for i, col in enumerate(table["columns"]):
            columns.append({
                "name": col["name"],
                "type_text": col["type_text"],
                "type_json": json.dumps({"type": col["type_name"].lower()}),
                "type_name": col["type_name"],
                "type_precision": 0,
                "type_scale": 0,
                "position": i,
                "nullable": True,
            })

        result = api_call("POST", "tables", {
            "name": table["name"],
            "catalog_name": CATALOG,
            "schema_name": table["schema"],
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": table["storage_location"],
            "comment": table["comment"],
            "columns": columns,
        })
        if result:
            print(f"  + Registered -> {table['storage_location']}")

    # Summary
    print("\n" + "=" * 60)
    print("Registration complete!")
    print(f"  Browse: http://localhost:3000")
    print(f"  API:    {UC_BASE}/tables?catalog_name={CATALOG}")
    print("")
    print("  Structure:")
    print(f"    {CATALOG}")
    for schema in SCHEMAS:
        schema_tables = [t for t in TABLES if t["schema"] == schema]
        print(f"      {schema}/")
        for t in schema_tables:
            layer = {"rdl": "Raw", "odl": "Ops", "adl": "Ana"}[t["name"][:3]]
            print(f"        {t['name']:30s} [{layer}]")
    print("=" * 60)


if __name__ == "__main__":
    main()
