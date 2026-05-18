"""
NW Bird Watch Intelligence â€” Multi-Source Ingestion Pipeline

Ingests North West England bird data from:
1. eBird API (Cornell Lab) â€” Recent bird sightings by county
2. iNaturalist API â€” Bird observations with CC-licensed photos
3. Wikimedia Commons API â€” CC-BY bird species photos for product enrichment

Focus: North West England (Lancashire, Cumbria, Greater Manchester, Merseyside, Cheshire)
Schedule: Weekly (Wednesdays 07:00 UTC â€” midweek for freshest sighting data)
Author: Awujoo (AWUJOO-029)
Genesis: 2026-05-16

Trade Route: TR-004 â€” NW Bird Watch Intelligence
"""

from datetime import datetime, timedelta
import json
import os
import logging

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

# â”€â”€ Assets â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
NW_BIRD_BRONZE = Asset("s3://bellosdata-bronze-raw/nw-birds")

# â”€â”€ Configuration â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
S3_BUCKET = "bellosdata-bronze-raw"
S3_PREFIX = "nw-birds"
from aws_session import get_aws_session, AWS_REGION
REGION = AWS_REGION

# eBird region codes for NW England counties
# Format: GB-ENG-XXX (subnational2 codes)
NW_EBIRD_REGIONS = {
    "GB-ENG-LAN": "Lancashire",
    "GB-ENG-CMA": "Cumbria",
    "GB-ENG-GTM": "Greater Manchester",
    "GB-ENG-MSY": "Merseyside",
    "GB-ENG-CHE": "Cheshire",
}

# NW England bounding box for iNaturalist
NW_BOUNDS = {
    "swlat": 52.8,
    "swlng": -3.6,
    "nelat": 54.8,
    "nelng": -1.8,
}

# Key NW bird species to enrich with Wikimedia photos
NW_SIGNATURE_SPECIES = [
    "Eurasian Bittern",
    "Marsh Harrier",
    "Bearded Tit",
    "Avocet",
    "Pink-footed Goose",
    "Whooper Swan",
    "Hen Harrier",
    "Peregrine Falcon",
    "Red Kite",
    "Osprey",
    "Barn Owl",
    "Little Egret",
    "Kingfisher",
    "Nuthatch",
    "Great Crested Grebe",
    "Curlew",
    "Lapwing",
    "Oystercatcher",
    "Dunlin",
    "Sanderling",
]


@dag(
    dag_id="nw_bird_ingestion",
    description="Ingest NW England bird sightings, observations, and species photos",
    schedule="0 7 * * 3",  # Every Wednesday at 07:00 UTC
    start_date=datetime(2026, 5, 16),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "awujoo",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["birds", "ingestion", "TR-004", "bronze", "sovereign", "nw-england"],
)
def nw_bird_ingestion():

    @task()
    def ingest_ebird_sightings() -> dict:
        """
        Fetch recent bird observations from eBird API for each NW England county.
        
        eBird API docs: https://documenter.getpostman.com/view/664302/S1ENwy59
        Endpoint: GET /v2/data/obs/{regionCode}/recent
        Rate limit: Reasonable use (no hard limit published, be respectful)
        License: Non-commercial research use
        """
        import urllib.request
        import ssl

        # eBird API key â€” stored as env var, falls back to public limited access
        EBIRD_API_KEY = os.environ.get("EBIRD_API_KEY", "")

        if not EBIRD_API_KEY:
            logger.warning(
                "No EBIRD_API_KEY set. Register at https://ebird.org/api/keygen. "
                "Proceeding with limited access."
            )

        ctx = ssl.create_default_context()
        all_sightings = []

        for region_code, county_name in NW_EBIRD_REGIONS.items():
            url = (
                f"https://api.ebird.org/v2/data/obs/{region_code}/recent"
                f"?back=14&maxResults=200&includeProvisional=true"
            )

            headers = {
                "User-Agent": "BellosData-BirdIntel/1.0 (awujoo@bellosdata.com)",
            }
            if EBIRD_API_KEY:
                headers["X-eBirdApiToken"] = EBIRD_API_KEY

            try:
                req = urllib.request.Request(url, headers=headers)
                response = urllib.request.urlopen(req, context=ctx, timeout=30)
                data = json.loads(response.read().decode("utf-8"))

                for obs in data:
                    all_sightings.append({
                        "species_code": obs.get("speciesCode", ""),
                        "common_name": obs.get("comName", ""),
                        "scientific_name": obs.get("sciName", ""),
                        "location_name": obs.get("locName", ""),
                        "latitude": obs.get("lat"),
                        "longitude": obs.get("lng"),
                        "observation_date": obs.get("obsDt", ""),
                        "how_many": obs.get("howMany"),
                        "is_reviewed": obs.get("obsReviewed", False),
                        "is_valid": obs.get("obsValid", False),
                        "county": county_name,
                        "region_code": region_code,
                        "source": "EBIRD",
                        "ingested_at": datetime.utcnow().isoformat() + "Z",
                    })

                logger.info(f"eBird: {len(data)} sightings from {county_name}")

            except Exception as e:
                logger.warning(f"eBird failed for {county_name} ({region_code}): {e}")
                continue

        logger.info(f"eBird total: {len(all_sightings)} sightings across NW England")

        return {
            "status": "OK" if all_sightings else "PARTIAL",
            "records": len(all_sightings),
            "source": "eBird",
            "counties_queried": len(NW_EBIRD_REGIONS),
            "data": all_sightings,
        }

    @task()
    def ingest_inaturalist_observations() -> dict:
        """
        Fetch recent bird observations from iNaturalist API for NW England.
        
        iNaturalist API docs: https://api.inaturalist.org/v1/docs/
        Endpoint: GET /v1/observations
        Rate limit: 100 requests/minute
        License: Varies per observation (filter for CC-licensed photos)
        """
        import urllib.request
        import ssl
        import urllib.parse

        ctx = ssl.create_default_context()

        # Taxon ID 3 = Aves (birds) in iNaturalist
        AVES_TAXON_ID = 3

        params = urllib.parse.urlencode({
            "taxon_id": AVES_TAXON_ID,
            "swlat": NW_BOUNDS["swlat"],
            "swlng": NW_BOUNDS["swlng"],
            "nelat": NW_BOUNDS["nelat"],
            "nelng": NW_BOUNDS["nelng"],
            "quality_grade": "research",
            "photos": "true",  # Only observations with photos
            "per_page": 200,
            "order": "desc",
            "order_by": "observed_on",
            "d1": (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d"),
        })

        url = f"https://api.inaturalist.org/v1/observations?{params}"

        logger.info("Querying iNaturalist for NW England bird observations with photos...")

        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "BellosData-BirdIntel/1.0 (awujoo@bellosdata.com)",
                "Accept": "application/json",
            })
            response = urllib.request.urlopen(req, context=ctx, timeout=60)
            data = json.loads(response.read().decode("utf-8"))
        except Exception as e:
            logger.warning(f"iNaturalist query failed: {e}")
            return {"status": "FAILED", "records": 0, "source": "iNaturalist"}

        results = data.get("results", [])
        observations = []

        for obs in results:
            taxon = obs.get("taxon", {})
            photos = obs.get("photos", [])

            # Extract CC-licensed photo URLs only
            cc_photos = []
            for photo in photos:
                license_code = photo.get("license_code", "")
                if license_code and license_code.startswith("cc"):
                    cc_photos.append({
                        "url": photo.get("url", "").replace("square", "medium"),
                        "license": license_code,
                        "attribution": photo.get("attribution", ""),
                    })

            observations.append({
                "inat_id": obs.get("id"),
                "species_common": taxon.get("preferred_common_name", ""),
                "species_scientific": taxon.get("name", ""),
                "taxon_id": taxon.get("id"),
                "observed_on": obs.get("observed_on", ""),
                "place_guess": obs.get("place_guess", ""),
                "latitude": obs.get("geojson", {}).get("coordinates", [None, None])[1],
                "longitude": obs.get("geojson", {}).get("coordinates", [None, None])[0],
                "quality_grade": obs.get("quality_grade", ""),
                "photos_count": len(photos),
                "cc_photos": cc_photos,
                "cc_photos_count": len(cc_photos),
                "source": "INATURALIST",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
            })

        logger.info(
            f"iNaturalist: {len(observations)} bird observations with photos. "
            f"CC-licensed photos: {sum(o['cc_photos_count'] for o in observations)}"
        )

        return {
            "status": "OK" if observations else "EMPTY",
            "records": len(observations),
            "total_cc_photos": sum(o["cc_photos_count"] for o in observations),
            "source": "iNaturalist",
            "data": observations,
        }

    @task()
    def ingest_wikimedia_bird_photos() -> dict:
        """
        Fetch CC-licensed bird photos from Wikimedia Commons for NW signature species.
        
        MediaWiki API docs: https://www.mediawiki.org/wiki/API:Main_page
        Endpoint: https://commons.wikimedia.org/w/api.php
        License: CC-BY / CC-BY-SA (commercial use allowed with attribution)
        """
        import urllib.request
        import urllib.parse
        import ssl

        ctx = ssl.create_default_context()
        API_URL = "https://commons.wikimedia.org/w/api.php"

        all_photos = []

        for species_name in NW_SIGNATURE_SPECIES:
            params = urllib.parse.urlencode({
                "action": "query",
                "generator": "search",
                "gsrsearch": f"{species_name} bird",
                "gsrnamespace": "6",  # File namespace
                "gsrlimit": "5",  # Top 5 images per species
                "prop": "imageinfo",
                "iiprop": "url|extmetadata|size|mime",
                "iiurlwidth": "800",  # Request 800px thumbnail
                "format": "json",
            })

            url = f"{API_URL}?{params}"

            try:
                req = urllib.request.Request(url, headers={
                    "User-Agent": "BellosData-BirdIntel/1.0 (awujoo@bellosdata.com)",
                })
                response = urllib.request.urlopen(req, context=ctx, timeout=30)
                data = json.loads(response.read().decode("utf-8"))
            except Exception as e:
                logger.warning(f"Wikimedia failed for {species_name}: {e}")
                continue

            pages = data.get("query", {}).get("pages", {})

            for page_id, page in pages.items():
                imageinfo = page.get("imageinfo", [{}])[0]
                extmeta = imageinfo.get("extmetadata", {})

                # Extract license info
                license_short = extmeta.get("LicenseShortName", {}).get("value", "unknown")
                artist = extmeta.get("Artist", {}).get("value", "unknown")
                description = extmeta.get("ImageDescription", {}).get("value", "")

                # Only keep CC-licensed images
                license_lower = license_short.lower()
                if "cc" not in license_lower and "public domain" not in license_lower:
                    continue

                all_photos.append({
                    "species_name": species_name,
                    "title": page.get("title", ""),
                    "url_original": imageinfo.get("url", ""),
                    "url_thumbnail": imageinfo.get("thumburl", ""),
                    "width": imageinfo.get("width"),
                    "height": imageinfo.get("height"),
                    "mime_type": imageinfo.get("mime", ""),
                    "license": license_short,
                    "artist": artist,
                    "description": description[:500] if description else "",
                    "source": "WIKIMEDIA_COMMONS",
                    "ingested_at": datetime.utcnow().isoformat() + "Z",
                })

            logger.info(f"Wikimedia: {species_name} â†’ {len(pages)} images found")

        logger.info(f"Wikimedia total: {len(all_photos)} CC-licensed bird photos for {len(NW_SIGNATURE_SPECIES)} species")

        return {
            "status": "OK" if all_photos else "EMPTY",
            "records": len(all_photos),
            "species_covered": len(set(p["species_name"] for p in all_photos)),
            "source": "Wikimedia Commons",
            "data": all_photos,
        }

    @task(outlets=[NW_BIRD_BRONZE])
    def write_to_bronze(
        ebird_result: dict,
        inat_result: dict,
        wikimedia_result: dict,
    ) -> dict:
        """Write all ingested bird data to S3 Bronze layer as Delta Lake tables."""
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

        # â”€â”€ eBird sightings â†’ Delta â”€â”€
        if ebird_result.get("status") in ("OK", "PARTIAL") and ebird_result.get("data"):
            table = pa.Table.from_pylist(ebird_result["data"])
            delta_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/ebird-sightings"
            write_deltalake(
                delta_path,
                table,
                mode="append",
                storage_options=storage_options,
            )
            uploaded["ebird_sightings"] = {
                "delta_table": delta_path,
                "records": ebird_result["records"],
            }
            logger.info(f"Wrote {ebird_result['records']} eBird records â†’ {delta_path} (Delta)")

        # â”€â”€ iNaturalist observations â†’ Delta â”€â”€
        # Flatten cc_photos list to JSON string for Delta compatibility
        if inat_result.get("status") == "OK" and inat_result.get("data"):
            # Serialize nested cc_photos as JSON strings for flat Delta table
            flat_data = []
            for obs in inat_result["data"]:
                flat_obs = {k: v for k, v in obs.items() if k != "cc_photos"}
                flat_obs["cc_photos_json"] = json.dumps(obs.get("cc_photos", []))
                flat_data.append(flat_obs)

            table = pa.Table.from_pylist(flat_data)
            delta_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/inaturalist"
            write_deltalake(
                delta_path,
                table,
                mode="append",
                storage_options=storage_options,
            )
            uploaded["inaturalist"] = {
                "delta_table": delta_path,
                "records": inat_result["records"],
                "cc_photos": inat_result.get("total_cc_photos", 0),
            }
            logger.info(f"Wrote {inat_result['records']} iNaturalist records â†’ {delta_path} (Delta)")

        # â”€â”€ Wikimedia Commons photos â†’ Delta â”€â”€
        if wikimedia_result.get("status") == "OK" and wikimedia_result.get("data"):
            table = pa.Table.from_pylist(wikimedia_result["data"])
            delta_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/wikimedia-photos"
            write_deltalake(
                delta_path,
                table,
                mode="append",
                storage_options=storage_options,
            )
            uploaded["wikimedia_photos"] = {
                "delta_table": delta_path,
                "records": wikimedia_result["records"],
                "species_covered": wikimedia_result.get("species_covered", 0),
            }
            logger.info(f"Wrote {wikimedia_result['records']} Wikimedia photos â†’ {delta_path} (Delta)")

        # â”€â”€ Ingestion manifest (JSON â€” metadata, not governed data) â”€â”€
        total_records = sum(v.get("records", 0) for v in uploaded.values())
        manifest = {
            "ingestion_id": f"BIRD-{ts_str}",
            "timestamp": now.isoformat() + "Z",
            "trade_route": "TR-004",
            "product": "NW Bird Watch Intelligence",
            "format": "DELTA",
            "sources": {
                "ebird": ebird_result.get("status"),
                "inaturalist": inat_result.get("status"),
                "wikimedia": wikimedia_result.get("status"),
            },
            "total_records": total_records,
            "delta_tables": {k: v["delta_table"] for k, v in uploaded.items()},
        }
        manifest_key = f"{S3_PREFIX}/manifests/{date_str}/manifest_{ts_str}.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info(f"Bird ingestion manifest: {json.dumps(manifest, indent=2)}")

        return manifest

    # â”€â”€ DAG Flow â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # All three sources ingest in parallel, then write to Bronze
    ebird = ingest_ebird_sightings()
    inat = ingest_inaturalist_observations()
    wikimedia = ingest_wikimedia_bird_photos()
    write_to_bronze(ebird, inat, wikimedia)


# Instantiate
nw_bird_ingestion()
