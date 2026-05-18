"""
ODL Dimension Builder â€” YAML-Driven Dimension Materialiser

Reads dimension definitions from config/dimensions.yml and
materialises both FLAT and HIERARCHY dims into Silver S3.

Triggered by upstream RDL Assets (postcodes, weather, airports, etc).
Each dimension is built independently based on its YAML spec.

Architecture:
  config/dimensions.yml  â†’  This DAG reads specs
  rdl/{source}/          â†’  This DAG reads raw data
  odl/dim/{dim_name}/    â†’  This DAG writes typed Delta tables

Author: Awujoo (AWUJOO-041) | Genesis: 2026-05-17
Constitutional basis: Cotrugli's Three Books â€” The Ledger must be clean.
"""

from datetime import datetime, timedelta
import json
import logging
import os

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

# â”€â”€ Assets (inputs â€” RDL triggers) â”€â”€
RDL_POSTCODES = Asset("s3://bellosdata-bronze-raw/rdl/postcodes")
RDL_WEATHER = Asset("s3://bellosdata-bronze-raw/rdl/weather")
RDL_AIRPORTS = Asset("s3://bellosdata-bronze-raw/rdl/airports")

# â”€â”€ Assets (outputs â€” ODL dims) â”€â”€
ODL_DIM_LOCATION = Asset("s3://bellosdata-silver-curated/odl/dim/dim_location")
ODL_DIM_WEATHER_STATION = Asset("s3://bellosdata-silver-curated/odl/dim/dim_weather_station")
ODL_DIM_AIRPORT = Asset("s3://bellosdata-silver-curated/odl/dim/dim_airport")
ODL_DIM_DATE = Asset("s3://bellosdata-silver-curated/odl/dim/dim_date")

# Config path
CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config")


def _load_yaml(filename: str) -> dict:
    """Load a YAML config file."""
    import yaml
    filepath = os.path.join(CONFIG_DIR, filename)
    with open(filepath, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _get_dim_spec(dim_name: str) -> dict:
    """Get a specific dimension's spec from dimensions.yml."""
    config = _load_yaml("dimensions.yml")
    for dim in config.get("dimensions", []):
        if dim["name"] == dim_name:
            return dim
    raise ValueError(f"Dimension '{dim_name}' not found in dimensions.yml")


@dag(
    dag_id="odl_dim_builder",
    description="Build ODL dimension tables from YAML specs + RDL data",
    schedule=[RDL_POSTCODES, RDL_WEATHER, RDL_AIRPORTS],
    start_date=datetime(2026, 5, 17),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "awujoo",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["odl", "dimensions", "builder", "yaml-driven", "sovereign"],
)
def odl_dim_builder():

    @task(outlets=[ODL_DIM_DATE])
    def build_dim_date() -> dict:
        """
        Generate dim_date â€” a pure calendar dimension.
        No RDL source needed â€” generated from date range.
        Follows spec from dimensions.yml.
        """
        from bellosdata_common import odl_write
        import calendar

        spec = _get_dim_spec("dim_date")
        logger.info(f"Building {spec['name']} (type={spec['type']}, scd={spec['scd_type']})")

        # Generate 3 years: 2025-01-01 to 2027-12-31
        rows = []
        start_year, end_year = 2025, 2027

        # UK bank holidays (simplified â€” major ones)
        uk_bank_holidays_2026 = {
            "2026-01-01", "2026-04-03", "2026-04-06",
            "2026-05-04", "2026-05-25", "2026-08-31",
            "2026-12-25", "2026-12-28",
        }

        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                for day in range(1, calendar.monthrange(year, month)[1] + 1):
                    dt = datetime(year, month, day)
                    iso_cal = dt.isocalendar()
                    date_str = dt.strftime("%Y-%m-%d")

                    # Season (meteorological)
                    if month in (3, 4, 5):
                        season = "Spring"
                    elif month in (6, 7, 8):
                        season = "Summer"
                    elif month in (9, 10, 11):
                        season = "Autumn"
                    else:
                        season = "Winter"

                    # UK fiscal year (April-March)
                    fy_start = year if month >= 4 else year - 1
                    fiscal_year = f"FY{fy_start}-{str(fy_start + 1)[-2:]}"

                    quarter = (month - 1) // 3 + 1

                    rows.append({
                        "date_key": int(dt.strftime("%Y%m%d")),
                        "full_date": date_str,
                        "day_of_week": dt.strftime("%A"),
                        "day_of_week_num": iso_cal[2],
                        "day_of_month": day,
                        "day_of_year": dt.timetuple().tm_yday,
                        "week_of_year": iso_cal[1],
                        "month_num": month,
                        "month_name": dt.strftime("%B"),
                        "quarter": quarter,
                        "quarter_name": f"Q{quarter}",
                        "year": year,
                        "is_weekend": iso_cal[2] >= 6,
                        "is_bank_holiday": date_str in uk_bank_holidays_2026,
                        "season": season,
                        "fiscal_year": fiscal_year,
                    })

        result = odl_write("dim", "dim_date", rows)
        logger.info(f"dim_date: {result['record_count']} date records generated")
        return result

    @task(outlets=[ODL_DIM_LOCATION])
    def build_dim_location() -> dict:
        """
        Build dim_location â€” HIERARCHY dimension.
        Reads RDL postcodes, denormalises the 6-level UK geography hierarchy.
        """
        from bellosdata_common import odl_write
        from deltalake import DeltaTable
        from aws_session import get_aws_session, AWS_REGION

        spec = _get_dim_spec("dim_location")
        logger.info(f"Building {spec['name']} (type={spec['type']}, hierarchy={spec['hierarchy']['name']})")

        # Read latest RDL postcodes
        session = get_aws_session()
        creds = session.get_credentials().get_frozen_credentials()
        storage_options = {
            "AWS_ACCESS_KEY_ID": creds.access_key,
            "AWS_SECRET_ACCESS_KEY": creds.secret_key,
            "AWS_REGION": AWS_REGION,
        }
        if creds.token:
            storage_options["AWS_SESSION_TOKEN"] = creds.token

        try:
            dt = DeltaTable(
                "s3://bellosdata-bronze-raw/rdl/postcodes",
                storage_options=storage_options,
            )
            df = dt.to_pandas()
        except Exception as e:
            logger.warning(f"No RDL postcodes found: {e}. Generating stub dim.")
            return odl_write("dim", "dim_location", [])

        # Parse the raw JSON column
        rows = []
        sk = 1
        seen_postcodes = set()

        for _, row in df.iterrows():
            try:
                record = json.loads(row["json"])
            except (json.JSONDecodeError, KeyError):
                continue

            # Only process full postcode records (not outcodes)
            if record.get("record_type") != "postcode":
                continue

            postcode = record.get("postcode", "")
            if not postcode or postcode in seen_postcodes:
                continue
            seen_postcodes.add(postcode)

            # Denormalise the hierarchy: leaf node with all ancestor fields
            rows.append({
                "location_sk": sk,
                "postcode": postcode,
                "outcode": record.get("outcode", ""),
                "incode": record.get("incode", ""),
                "latitude": record.get("latitude"),
                "longitude": record.get("longitude"),
                "eastings": record.get("eastings"),
                "northings": record.get("northings"),
                # Hierarchy levels (denormalised)
                "country_name": record.get("country", ""),
                "region_name": record.get("region", ""),
                "admin_county": record.get("admin_county", ""),
                "admin_district": record.get("admin_district", ""),
                "admin_ward": record.get("admin_ward", ""),
                "parish": record.get("parish", ""),
                "parliamentary_constituency": record.get("parliamentary_constituency", ""),
                # Statistical areas
                "lsoa": record.get("lsoa", ""),
                "msoa": record.get("msoa", ""),
                "ccg": record.get("ccg", ""),
                "nuts": record.get("nuts", ""),
                "pfa": record.get("pfa", ""),
                # SCD2 fields
                "valid_from": datetime.utcnow().isoformat() + "Z",
                "valid_to": "9999-12-31T00:00:00+00:00",
                "is_current": True,
            })
            sk += 1

        result = odl_write("dim", "dim_location", rows)
        logger.info(f"dim_location: {result['record_count']} postcodes (HIERARCHY)")
        return result

    @task(outlets=[ODL_DIM_WEATHER_STATION])
    def build_dim_weather_station() -> dict:
        """
        Build dim_weather_station â€” FLAT dimension.
        Reads RDL weather to extract unique grid points.
        """
        from bellosdata_common import odl_write
        from deltalake import DeltaTable
        from aws_session import get_aws_session, AWS_REGION

        spec = _get_dim_spec("dim_weather_station")
        logger.info(f"Building {spec['name']} (type={spec['type']})")

        session = get_aws_session()
        creds = session.get_credentials().get_frozen_credentials()
        storage_options = {
            "AWS_ACCESS_KEY_ID": creds.access_key,
            "AWS_SECRET_ACCESS_KEY": creds.secret_key,
            "AWS_REGION": AWS_REGION,
        }
        if creds.token:
            storage_options["AWS_SESSION_TOKEN"] = creds.token

        try:
            dt = DeltaTable(
                "s3://bellosdata-bronze-raw/rdl/weather",
                storage_options=storage_options,
            )
            df = dt.to_pandas()
        except Exception as e:
            logger.warning(f"No RDL weather found: {e}. Generating stub dim.")
            return odl_write("dim", "dim_weather_station", [])

        # Extract unique stations from weather data
        seen = set()
        rows = []
        sk = 1

        for _, row in df.iterrows():
            try:
                record = json.loads(row["json"])
            except (json.JSONDecodeError, KeyError):
                continue

            pid = record.get("point_id", "")
            if not pid or pid in seen:
                continue
            seen.add(pid)

            # Classify station type
            label = record.get("label", "").lower()
            if "airport" in label or "aerodrome" in label:
                station_type = "aviation"
            elif "coastal" in label or "bay" in label:
                station_type = "coastal"
            elif "hill" in label or "orme" in label:
                station_type = "elevated"
            else:
                station_type = "urban"

            rows.append({
                "station_sk": sk,
                "point_id": pid,
                "label": record.get("label", ""),
                "latitude": record.get("latitude"),
                "longitude": record.get("longitude"),
                "elevation_m": record.get("elevation"),
                "station_type": station_type,
                "nearest_postcode": None,  # Populated by mapping table
            })
            sk += 1

        result = odl_write("dim", "dim_weather_station", rows)
        logger.info(f"dim_weather_station: {result['record_count']} stations (FLAT)")
        return result

    @task(outlets=[ODL_DIM_AIRPORT])
    def build_dim_airport() -> dict:
        """
        Build dim_airport â€” HIERARCHY dimension.
        Reads RDL airports, denormalises countryâ†’regionâ†’airportâ†’runway.
        """
        from bellosdata_common import odl_write
        from deltalake import DeltaTable
        from aws_session import get_aws_session, AWS_REGION

        spec = _get_dim_spec("dim_airport")
        logger.info(f"Building {spec['name']} (type={spec['type']}, hierarchy={spec['hierarchy']['name']})")

        session = get_aws_session()
        creds = session.get_credentials().get_frozen_credentials()
        storage_options = {
            "AWS_ACCESS_KEY_ID": creds.access_key,
            "AWS_SECRET_ACCESS_KEY": creds.secret_key,
            "AWS_REGION": AWS_REGION,
        }
        if creds.token:
            storage_options["AWS_SESSION_TOKEN"] = creds.token

        try:
            dt = DeltaTable(
                "s3://bellosdata-bronze-raw/rdl/airports",
                storage_options=storage_options,
            )
            df = dt.to_pandas()
        except Exception as e:
            logger.warning(f"No RDL airports found: {e}. Generating stub dim.")
            return odl_write("dim", "dim_airport", [])

        # Parse records â€” airports and runways
        airports_raw = {}
        runways_by_airport = {}

        for _, row in df.iterrows():
            try:
                record = json.loads(row["json"])
            except (json.JSONDecodeError, KeyError):
                continue

            rtype = record.get("record_type")
            if rtype == "airport":
                ident = record.get("ident", "")
                if ident:
                    airports_raw[ident] = record
            elif rtype == "runway":
                apt = record.get("airport_ident", "")
                if apt:
                    runways_by_airport.setdefault(apt, []).append(record)

        # Build dim rows â€” one per airport with denormalised hierarchy
        rows = []
        sk = 1

        for icao, apt in airports_raw.items():
            runways = runways_by_airport.get(icao, [])

            # Runway aggregates
            num_runways = len(runways)
            longest = 0
            surface = ""
            for rw in runways:
                try:
                    length = int(rw.get("length_ft", 0) or 0)
                    if length > longest:
                        longest = length
                        surface = rw.get("surface", "")
                except (ValueError, TypeError):
                    pass

            try:
                elev = int(apt.get("elevation_ft", 0) or 0)
            except (ValueError, TypeError):
                elev = None

            rows.append({
                "airport_sk": sk,
                "icao_code": icao,
                "iata_code": apt.get("iata_code", ""),
                "airport_name": apt.get("name", ""),
                "airport_type": apt.get("type", ""),
                "latitude_deg": apt.get("latitude_deg"),
                "longitude_deg": apt.get("longitude_deg"),
                "elevation_ft": elev,
                "municipality": apt.get("municipality", ""),
                "scheduled_service": apt.get("scheduled_service") == "yes",
                # Hierarchy fields
                "iso_country": apt.get("iso_country", ""),
                "iso_region": apt.get("iso_region", ""),
                # NW flags
                "in_nw_england": apt.get("in_nw_england", False),
                "is_key_nw": apt.get("is_key_nw", False),
                # Runway aggregates
                "num_runways": num_runways,
                "longest_runway_ft": longest if longest > 0 else None,
                "runway_surface": surface,
                "nearest_postcode": None,
            })
            sk += 1

        result = odl_write("dim", "dim_airport", rows)
        logger.info(f"dim_airport: {result['record_count']} airports (HIERARCHY)")
        return result

    # â”€â”€ DAG Flow (all dims build in parallel) â”€â”€
    build_dim_date()
    build_dim_location()
    build_dim_weather_station()
    build_dim_airport()


odl_dim_builder()
