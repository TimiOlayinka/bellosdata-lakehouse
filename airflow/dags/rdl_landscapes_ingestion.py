"""
RDL Landscapes Ingestion — NW England Habitat & Landscape Pipeline

Sources (all free, no API key):
  - Natural England Open Data Portal (SSSIs, NNRs, AONBs)
  - National Parks England boundary data
  - NW England RSPB reserves (curated reference)

Schedule: Quarterly (1st Jan/Apr/Jul/Oct at 05:00 UTC)
RDL Output: s3://playdarch-bronze-raw/rdl/landscapes/
Asset: Triggers downstream ODL dim_habitat builder

Author: Awujoo (AWUJOO-041 Phase 2) | Genesis: 2026-05-17
Trade Route: TR-004 NW Bird Watch Intelligence (habitat context)
"""

from datetime import datetime, timedelta
import json
import logging

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

RDL_LANDSCAPES_ASSET = Asset("s3://playdarch-bronze-raw/rdl/landscapes")

# NW England bounding box (same as used in bird/weather DAGs)
NW_BBOX = {"lat_min": 53.0, "lat_max": 55.8, "lon_min": -3.6, "lon_max": -1.8}

# ── NW England Protected Landscapes (curated reference data) ──
# These are authoritative — manually compiled from Natural England / JNCC
NW_PROTECTED_SITES = [
    # National Parks
    {"name": "Lake District National Park", "designation": "National Park",
     "biome": "Temperate", "landscape_type": "National Park",
     "habitat_category": "Upland", "area_hectares": 229200,
     "lat": 54.45, "lon": -3.05, "managing_org": "Lake District NPA",
     "public_access": True, "key_species": "Red Squirrel,Osprey,Golden Eagle,Vendace"},
    {"name": "Yorkshire Dales National Park (West)", "designation": "National Park",
     "biome": "Temperate", "landscape_type": "National Park",
     "habitat_category": "Upland", "area_hectares": 228400,
     "lat": 54.25, "lon": -2.15, "managing_org": "Yorkshire Dales NPA",
     "public_access": True, "key_species": "Curlew,Black Grouse,Peregrine"},
    # AONBs (now National Landscapes)
    {"name": "Arnside & Silverdale AONB", "designation": "AONB",
     "biome": "Temperate", "landscape_type": "AONB",
     "habitat_category": "Limestone Pavement", "area_hectares": 7500,
     "lat": 54.17, "lon": -2.82, "managing_org": "Arnside & Silverdale AONB Partnership",
     "public_access": True, "key_species": "Bittern,Marsh Harrier,Scoters"},
    {"name": "Forest of Bowland AONB", "designation": "AONB",
     "biome": "Temperate", "landscape_type": "AONB",
     "habitat_category": "Moorland", "area_hectares": 80300,
     "lat": 53.95, "lon": -2.55, "managing_org": "Forest of Bowland AONB",
     "public_access": True, "key_species": "Hen Harrier,Merlin,Ring Ouzel"},
    {"name": "Solway Coast AONB", "designation": "AONB",
     "biome": "Temperate", "landscape_type": "AONB",
     "habitat_category": "Coastal", "area_hectares": 11500,
     "lat": 54.85, "lon": -3.35, "managing_org": "Solway Coast AONB",
     "public_access": True, "key_species": "Barnacle Goose,Natterjack Toad"},
    # NNRs (National Nature Reserves)
    {"name": "Ainsdale Sand Dunes NNR", "designation": "NNR",
     "biome": "Temperate", "landscape_type": "NNR",
     "habitat_category": "Coastal/Dune", "area_hectares": 508,
     "lat": 53.60, "lon": -3.07, "managing_org": "Natural England",
     "public_access": True, "key_species": "Natterjack Toad,Sand Lizard"},
    {"name": "Ribble Estuary NNR", "designation": "NNR",
     "biome": "Temperate", "landscape_type": "NNR",
     "habitat_category": "Wetland/Estuary", "area_hectares": 4520,
     "lat": 53.72, "lon": -2.85, "managing_org": "Natural England",
     "public_access": True, "key_species": "Pink-footed Goose,Knot,Bar-tailed Godwit"},
    {"name": "Roudsea Wood & Mosses NNR", "designation": "NNR",
     "biome": "Temperate", "landscape_type": "NNR",
     "habitat_category": "Woodland/Bog", "area_hectares": 200,
     "lat": 54.22, "lon": -3.05, "managing_org": "Natural England",
     "public_access": True, "key_species": "Roe Deer,Lesser Horseshoe Bat"},
    # RSPB Reserves
    {"name": "Leighton Moss RSPB", "designation": "RSPB Reserve",
     "biome": "Temperate", "landscape_type": "Nature Reserve",
     "habitat_category": "Wetland/Reedbed", "area_hectares": 320,
     "lat": 54.17, "lon": -2.80, "managing_org": "RSPB",
     "public_access": True, "key_species": "Bittern,Marsh Harrier,Bearded Tit,Otter"},
    {"name": "Marshside RSPB", "designation": "RSPB Reserve",
     "biome": "Temperate", "landscape_type": "Nature Reserve",
     "habitat_category": "Wetland/Saltmarsh", "area_hectares": 145,
     "lat": 53.65, "lon": -3.02, "managing_org": "RSPB",
     "public_access": True, "key_species": "Pink-footed Goose,Avocet,Pintail"},
    {"name": "Martin Mere WWT", "designation": "WWT Reserve",
     "biome": "Temperate", "landscape_type": "Nature Reserve",
     "habitat_category": "Wetland", "area_hectares": 160,
     "lat": 53.62, "lon": -2.86, "managing_org": "WWT",
     "public_access": True, "key_species": "Whooper Swan,Pink-footed Goose,Ruff"},
    {"name": "Morecambe Bay RSPB", "designation": "RSPB Reserve",
     "biome": "Temperate", "landscape_type": "Nature Reserve",
     "habitat_category": "Coastal/Mudflat", "area_hectares": 34000,
     "lat": 54.10, "lon": -2.85, "managing_org": "RSPB/Natural England",
     "public_access": True, "key_species": "Knot,Oystercatcher,Eider"},
    # SSSIs (key NW sites)
    {"name": "Sefton Coast SSSI", "designation": "SSSI",
     "biome": "Temperate", "landscape_type": "SSSI",
     "habitat_category": "Coastal/Dune", "area_hectares": 2100,
     "lat": 53.55, "lon": -3.05, "managing_org": "Natural England",
     "public_access": True, "key_species": "Red Squirrel,Sand Lizard,Great Crested Newt"},
    {"name": "Bowland Fells SSSI", "designation": "SSSI",
     "biome": "Temperate", "landscape_type": "SSSI",
     "habitat_category": "Moorland/Blanket Bog", "area_hectares": 16000,
     "lat": 54.00, "lon": -2.55, "managing_org": "Natural England",
     "public_access": True, "key_species": "Hen Harrier,Merlin,Short-eared Owl"},
    {"name": "Dee Estuary SSSI", "designation": "SSSI",
     "biome": "Temperate", "landscape_type": "SSSI",
     "habitat_category": "Wetland/Estuary", "area_hectares": 14300,
     "lat": 53.33, "lon": -3.15, "managing_org": "Natural England",
     "public_access": True, "key_species": "Dunlin,Redshank,Shelduck,Pintail"},
    {"name": "Geltsdale RSPB & SSSI", "designation": "SSSI",
     "biome": "Temperate", "landscape_type": "Nature Reserve",
     "habitat_category": "Upland/Moorland", "area_hectares": 4900,
     "lat": 54.80, "lon": -2.60, "managing_org": "RSPB",
     "public_access": True, "key_species": "Black Grouse,Ring Ouzel,Merlin"},
    # Urban green spaces
    {"name": "Chat Moss", "designation": "Local Nature Reserve",
     "biome": "Temperate", "landscape_type": "Local Nature Reserve",
     "habitat_category": "Lowland Bog", "area_hectares": 2500,
     "lat": 53.47, "lon": -2.45, "managing_org": "Lancashire Wildlife Trust",
     "public_access": True, "key_species": "Water Vole,Willow Tit"},
    {"name": "Mere Sands Wood", "designation": "LWT Reserve",
     "biome": "Temperate", "landscape_type": "Nature Reserve",
     "habitat_category": "Woodland/Lake", "area_hectares": 42,
     "lat": 53.63, "lon": -2.82, "managing_org": "Lancashire Wildlife Trust",
     "public_access": True, "key_species": "Kingfisher,Great Crested Grebe"},
]


@dag(
    dag_id="rdl_landscapes_ingestion",
    description="Ingest NW England protected landscapes & habitats → RDL",
    schedule="0 5 1 1,4,7,10 *",  # Quarterly
    start_date=datetime(2026, 5, 17),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "awujoo", "retries": 1, "retry_delay": timedelta(minutes=3)},
    tags=["landscapes", "rdl", "ingestion", "habitat", "TR-004", "sovereign"],
)
def rdl_landscapes_ingestion():

    @task()
    def ingest_protected_sites():
        """Ingest curated NW England protected landscape sites."""
        records = []
        for site in NW_PROTECTED_SITES:
            record = {
                **site,
                "habitat_id": site["name"].lower().replace(" ", "_").replace("&", "and"),
                "region": "North West England",
                "country": "England",
                "biome_code": "TEMPERATE",
                "landscape_code": site["designation"].upper().replace(" ", "_"),
                "habitat_cat_code": site["habitat_category"].upper().replace("/", "_").replace(" ", "_"),
                "centroid_lat": site["lat"],
                "centroid_lon": site["lon"],
                "in_nw_bbox": (NW_BBOX["lat_min"] <= site["lat"] <= NW_BBOX["lat_max"]
                               and NW_BBOX["lon_min"] <= site["lon"] <= NW_BBOX["lon_max"]),
                "source": "NATURAL_ENGLAND_CURATED",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
            }
            records.append(record)

        logger.info(f"Curated sites: {len(records)} NW protected landscapes")
        return {"status": "OK", "records": len(records), "data": records}

    @task()
    def enrich_from_ne_api(curated_result):
        """Try to enrich with Natural England's SSSI API (free, no key)."""
        from bellosdata_common import http_get

        ne_base = "https://designatedsites.naturalengland.org.uk/SearchResults.aspx"
        enriched = curated_result.get("data", [])
        api_enrichments = 0

        # Natural England SSSI API — try the GeoJSON endpoint
        sssi_geojson_url = (
            "https://services.arcgis.com/JJzESW51TqeY9uj9/arcgis/rest/services/"
            "Sites_of_Special_Scientific_Interest_England/FeatureServer/0/query"
            "?where=1%3D1&outFields=SSSI_NAME,STATUS,HECTARES,CITATION"
            "&geometry=-3.6,53.0,-1.8,55.8&geometryType=esriGeometryEnvelope"
            "&inSR=4326&spatialRel=esriSpatialRelContains"
            "&returnGeometry=false&f=json&resultRecordCount=200"
        )

        try:
            data = http_get(sssi_geojson_url, retries=2, timeout=30)
            features = data.get("features", [])
            logger.info(f"NE SSSI API: {len(features)} SSSIs in NW bbox")

            for feat in features:
                attrs = feat.get("attributes", {})
                name = attrs.get("SSSI_NAME", "").strip()
                if not name:
                    continue
                # Check if we already have this site
                existing = [s for s in enriched if s["name"].lower() == name.lower()]
                if existing:
                    existing[0]["ne_status"] = attrs.get("STATUS", "")
                    existing[0]["ne_hectares"] = attrs.get("HECTARES")
                    api_enrichments += 1
                else:
                    enriched.append({
                        "name": name,
                        "designation": "SSSI",
                        "biome": "Temperate",
                        "biome_code": "TEMPERATE",
                        "landscape_type": "SSSI",
                        "landscape_code": "SSSI",
                        "habitat_category": "Mixed",
                        "habitat_cat_code": "MIXED",
                        "habitat_id": name.lower().replace(" ", "_"),
                        "area_hectares": attrs.get("HECTARES"),
                        "ne_status": attrs.get("STATUS", ""),
                        "region": "North West England",
                        "country": "England",
                        "managing_org": "Natural England",
                        "public_access": True,
                        "source": "NATURAL_ENGLAND_API",
                        "ingested_at": datetime.utcnow().isoformat() + "Z",
                    })
        except Exception as e:
            logger.warning(f"NE API enrichment skipped: {e}")

        logger.info(f"Total landscape records: {len(enriched)} (enriched: {api_enrichments})")
        return {"status": "OK", "records": len(enriched), "api_enrichments": api_enrichments, "data": enriched}

    @task(outlets=[RDL_LANDSCAPES_ASSET])
    def write_to_rdl(landscape_result):
        """Write landscape data to RDL as raw JSON + ingest_date."""
        from bellosdata_common import rdl_write, write_manifest

        rdl = rdl_write("landscapes", landscape_result.get("data", []))
        return write_manifest("rdl_landscapes_ingestion", "landscapes", rdl,
            record_counts={"total_sites": landscape_result.get("records", 0),
                          "api_enrichments": landscape_result.get("api_enrichments", 0)},
            extra={"sources": ["Natural England", "RSPB", "WWT", "NPA"]})

    curated = ingest_protected_sites()
    enriched = enrich_from_ne_api(curated)
    write_to_rdl(enriched)

rdl_landscapes_ingestion()
