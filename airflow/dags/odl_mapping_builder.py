"""
ODL Mapping Builder — YAML-Driven Cross-Dimension Bridge Materialiser

Reads mapping definitions from config/mappings.yml and materialises
bridge/link tables that connect dimensions in the ODL.

Triggered by upstream ODL dim Assets (dims must exist before mappings).
Each mapping is built independently based on its YAML spec.

Architecture:
  config/mappings.yml    → This DAG reads specs
  odl/dim/{dim}/         → This DAG reads dims for FK resolution
  rdl/{source}/          → This DAG reads raw data for entity matching
  odl/map/{map_name}/    → This DAG writes typed Delta tables

Author: Awujoo (AWUJOO-041 Phase 2) | Genesis: 2026-05-17
"""

from datetime import datetime, timedelta
import json
import logging
import math
import os

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

# Input Assets — ODL dims
ODL_DIM_LOCATION = Asset("s3://playdarch-silver-curated/odl/dim/dim_location")
ODL_DIM_WEATHER_STATION = Asset("s3://playdarch-silver-curated/odl/dim/dim_weather_station")
ODL_DIM_AIRPORT = Asset("s3://playdarch-silver-curated/odl/dim/dim_airport")

# Output Assets
ODL_MAP_POSTCODE = Asset("s3://playdarch-silver-curated/odl/map/map_postcode_to_location")
ODL_MAP_COMPANY = Asset("s3://playdarch-silver-curated/odl/map/map_company_to_postcode")
ODL_MAP_AIRPORT = Asset("s3://playdarch-silver-curated/odl/map/map_airport_to_location")
ODL_MAP_SPECIES = Asset("s3://playdarch-silver-curated/odl/map/map_species_to_habitat")
ODL_MAP_OWNER = Asset("s3://playdarch-silver-curated/odl/map/map_owner_to_company")
ODL_MAP_STATION = Asset("s3://playdarch-silver-curated/odl/map/map_station_to_location")

CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config")


def _load_yaml(filename):
    import yaml
    with open(os.path.join(CONFIG_DIR, filename), "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _read_rdl(source_path):
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
        logger.warning(f"No RDL at {source_path}: {e}")
        return []


def _read_odl_dim(dim_name):
    from deltalake import DeltaTable
    from aws_session import get_aws_session, AWS_REGION
    session = get_aws_session()
    creds = session.get_credentials().get_frozen_credentials()
    opts = {"AWS_ACCESS_KEY_ID": creds.access_key,
            "AWS_SECRET_ACCESS_KEY": creds.secret_key, "AWS_REGION": AWS_REGION}
    if creds.token:
        opts["AWS_SESSION_TOKEN"] = creds.token
    try:
        path = f"s3://playdarch-silver-curated/odl/dim/{dim_name}"
        dt = DeltaTable(path, storage_options=opts)
        return dt.to_pandas().to_dict("records")
    except Exception as e:
        logger.warning(f"No ODL dim {dim_name}: {e}")
        return []


def _haversine_km(lat1, lon1, lat2, lon2):
    """Calculate haversine distance in km between two points."""
    R = 6371
    try:
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2) ** 2 +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
             math.sin(dlon / 2) ** 2)
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    except (TypeError, ValueError):
        return None


@dag(
    dag_id="odl_mapping_builder",
    description="Build ODL mapping/bridge tables from YAML specs + dim lookups",
    schedule=[ODL_DIM_LOCATION, ODL_DIM_WEATHER_STATION, ODL_DIM_AIRPORT],
    start_date=datetime(2026, 5, 17),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "awujoo", "retries": 1, "retry_delay": timedelta(minutes=3)},
    tags=["odl", "mappings", "builder", "yaml-driven", "sovereign"],
)
def odl_mapping_builder():

    @task(outlets=[ODL_MAP_POSTCODE])
    def build_map_postcode_to_location():
        """Build postcode → location SK mapping from dim_location."""
        from bellosdata_common import odl_write
        dim_data = _read_odl_dim("dim_location")
        if not dim_data:
            return odl_write("map", "map_postcode_to_location", [])

        rows = []
        for loc in dim_data:
            pc = loc.get("postcode", "")
            if not pc:
                continue
            rows.append({
                "postcode": pc,
                "location_sk": loc.get("location_sk"),
                "outcode": loc.get("outcode", ""),
                "latitude": loc.get("latitude"),
                "longitude": loc.get("longitude"),
            })

        result = odl_write("map", "map_postcode_to_location", rows)
        logger.info(f"map_postcode_to_location: {result['record_count']} mappings")
        return result

    @task(outlets=[ODL_MAP_COMPANY])
    def build_map_company_to_postcode():
        """Build company → postcode mapping from RDL companies."""
        from bellosdata_common import odl_write
        records = _read_rdl("rdl/companies")
        if not records:
            return odl_write("map", "map_company_to_postcode", [])

        rows = []
        seen = set()
        for rec in records:
            cn = rec.get("company_number", "")
            if not cn or cn in seen:
                continue
            seen.add(cn)
            sics = rec.get("sic_codes", [])
            rows.append({
                "company_number": cn,
                "company_name": rec.get("company_name", ""),
                "registered_postcode": rec.get("postcode", ""),
                "sic_codes": ",".join(sics) if isinstance(sics, list) else str(sics),
                "company_status": rec.get("company_status", ""),
            })

        result = odl_write("map", "map_company_to_postcode", rows)
        logger.info(f"map_company_to_postcode: {result['record_count']} mappings")
        return result

    @task(outlets=[ODL_MAP_AIRPORT])
    def build_map_airport_to_location():
        """Build airport → nearest postcode mapping using haversine."""
        from bellosdata_common import odl_write
        airports = _read_odl_dim("dim_airport")
        locations = _read_odl_dim("dim_location")
        if not airports or not locations:
            return odl_write("map", "map_airport_to_location", [])

        # Build postcode lat/lon index
        pc_index = []
        for loc in locations:
            lat, lon = loc.get("latitude"), loc.get("longitude")
            if lat and lon:
                pc_index.append((loc.get("postcode", ""), lat, lon))

        rows = []
        for apt in airports:
            alat = apt.get("latitude_deg")
            alon = apt.get("longitude_deg")
            if not alat or not alon:
                continue

            # Find nearest postcode by haversine
            best_pc, best_dist = "", float("inf")
            for pc, plat, plon in pc_index:
                dist = _haversine_km(alat, alon, plat, plon)
                if dist is not None and dist < best_dist:
                    best_dist = dist
                    best_pc = pc

            rows.append({
                "icao_code": apt.get("icao_code", ""),
                "airport_name": apt.get("airport_name", ""),
                "nearest_postcode": best_pc,
                "latitude_deg": alat,
                "longitude_deg": alon,
                "distance_km": round(best_dist, 2) if best_dist < float("inf") else None,
            })

        result = odl_write("map", "map_airport_to_location", rows)
        logger.info(f"map_airport_to_location: {result['record_count']} mappings")
        return result

    @task(outlets=[ODL_MAP_SPECIES])
    def build_map_species_to_habitat():
        """Build species → habitat mapping from RDL birds + landscapes."""
        from bellosdata_common import odl_write

        birds = _read_rdl("rdl/nw-birds")
        landscapes = _read_rdl("rdl/landscapes")

        if not birds:
            return odl_write("map", "map_species_to_habitat", [])

        # Build habitat keyword index from landscape data
        habitat_index = {}
        for site in landscapes:
            cat = site.get("habitat_category", "")
            species_str = site.get("key_species", "")
            if species_str:
                for sp in species_str.split(","):
                    sp_clean = sp.strip().lower()
                    if sp_clean:
                        habitat_index.setdefault(sp_clean, []).append({
                            "habitat_id": site.get("habitat_id", ""),
                            "habitat_name": site.get("name", ""),
                            "category": cat,
                        })

        rows = []
        seen = set()
        for bird in birds:
            code = bird.get("species_code", bird.get("speciesCode", ""))
            name = bird.get("common_name", bird.get("comName", "")).strip()
            if not code or code in seen:
                continue
            seen.add(code)

            # Match species to habitats
            name_lower = name.lower()
            habitats = habitat_index.get(name_lower, [])

            if habitats:
                for hab in habitats:
                    rows.append({
                        "species_code": code,
                        "common_name": name,
                        "habitat_id": hab["habitat_id"],
                        "habitat_name": hab["habitat_name"],
                        "affinity": "primary",
                        "season": "all_year",
                    })
            else:
                # Default generic habitat
                rows.append({
                    "species_code": code,
                    "common_name": name,
                    "habitat_id": "general_nw",
                    "habitat_name": "General NW England",
                    "affinity": "secondary",
                    "season": "all_year",
                })

        result = odl_write("map", "map_species_to_habitat", rows)
        logger.info(f"map_species_to_habitat: {result['record_count']} mappings")
        return result

    @task(outlets=[ODL_MAP_OWNER])
    def build_map_owner_to_company():
        """Build jet owner → company mapping using fuzzy string matching."""
        from bellosdata_common import odl_write

        jets = _read_rdl("rdl/private-jets")
        companies = _read_rdl("rdl/companies")

        if not jets or not companies:
            return odl_write("map", "map_owner_to_company", [])

        # Build company name index (lowered)
        co_index = {}
        for co in companies:
            name = co.get("company_name", "").strip().upper()
            if name:
                co_index[name] = co.get("company_number", "")

        rows = []
        seen_owners = set()
        for jet in jets:
            owner = jet.get("registered_owner", jet.get("RegisteredOwner", "")).strip()
            if not owner or owner in seen_owners:
                continue
            seen_owners.add(owner)

            owner_upper = owner.upper()

            # Exact match first
            if owner_upper in co_index:
                rows.append({
                    "registered_owner": owner,
                    "company_number": co_index[owner_upper],
                    "company_name": owner_upper,
                    "match_confidence": 1.0,
                    "match_method": "exact",
                })
                continue

            # Simple substring match
            best_match, best_score = "", 0.0
            for co_name, co_num in co_index.items():
                if owner_upper in co_name or co_name in owner_upper:
                    score = min(len(owner_upper), len(co_name)) / max(len(owner_upper), len(co_name))
                    if score > best_score:
                        best_score = score
                        best_match = co_name

            if best_score > 0.6:
                rows.append({
                    "registered_owner": owner,
                    "company_number": co_index.get(best_match, ""),
                    "company_name": best_match,
                    "match_confidence": round(best_score, 3),
                    "match_method": "fuzzy",
                })

        result = odl_write("map", "map_owner_to_company", rows)
        logger.info(f"map_owner_to_company: {result['record_count']} mappings")
        return result

    @task(outlets=[ODL_MAP_STATION])
    def build_map_station_to_location():
        """Build weather station → nearest postcode mapping."""
        from bellosdata_common import odl_write
        stations = _read_odl_dim("dim_weather_station")
        locations = _read_odl_dim("dim_location")
        if not stations or not locations:
            return odl_write("map", "map_station_to_location", [])

        pc_index = [(l.get("postcode", ""), l.get("latitude"), l.get("longitude"))
                     for l in locations if l.get("latitude") and l.get("longitude")]

        rows = []
        for stn in stations:
            slat, slon = stn.get("latitude"), stn.get("longitude")
            if not slat or not slon:
                continue
            best_pc, best_dist = "", float("inf")
            for pc, plat, plon in pc_index:
                dist = _haversine_km(slat, slon, plat, plon)
                if dist is not None and dist < best_dist:
                    best_dist = dist
                    best_pc = pc

            rows.append({
                "point_id": stn.get("point_id", ""),
                "label": stn.get("label", ""),
                "nearest_postcode": best_pc,
                "distance_km": round(best_dist, 2) if best_dist < float("inf") else None,
            })

        result = odl_write("map", "map_station_to_location", rows)
        logger.info(f"map_station_to_location: {result['record_count']} mappings")
        return result

    # All mappings build in parallel
    build_map_postcode_to_location()
    build_map_company_to_postcode()
    build_map_airport_to_location()
    build_map_species_to_habitat()
    build_map_owner_to_company()
    build_map_station_to_location()

odl_mapping_builder()
