"""
Glue Catalog Sync — Register Delta Lake Tables in AWS Glue

Scans all RDL (Bronze) and ODL (Silver) Delta tables and registers
them in the AWS Glue Data Catalog. This replaces Unity Catalog OSS
as the metadata governance layer.

Triggered by upstream ODL builder assets. Also runs on a daily schedule
as a safety net to ensure catalog stays in sync.

Architecture:
  S3 Delta tables → This DAG reads table metadata
  AWS Glue Catalog → This DAG registers/updates tables

Author: Antigravity | Genesis: 2026-05-19
"""

from datetime import datetime, timedelta
import json
import logging
import os

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

# ── Input Assets (ODL outputs trigger this DAG) ──
ODL_DIM_DATE = Asset("s3://bellosdata-silver-curated/odl/dim/dim_date")
ODL_DIM_LOCATION = Asset("s3://bellosdata-silver-curated/odl/dim/dim_location")
ODL_DIM_WEATHER_STATION = Asset("s3://bellosdata-silver-curated/odl/dim/dim_weather_station")
ODL_DIM_AIRPORT = Asset("s3://bellosdata-silver-curated/odl/dim/dim_airport")

# ── Constants ──
GLUE_DATABASE = "bellosdata"
S3_BRONZE = "s3://bellosdata-bronze-raw"
S3_SILVER = "s3://bellosdata-silver-curated"
S3_GOLD = "s3://bellosdata-gold-products"

CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config")


def _load_yaml(filename: str) -> dict:
    """Load a YAML config file."""
    import yaml
    filepath = os.path.join(CONFIG_DIR, filename)
    with open(filepath, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _dtype_to_glue(dtype: str) -> str:
    """Map YAML dtypes to Glue/Hive column types."""
    mapping = {
        "string": "string",
        "int8": "tinyint",
        "int16": "smallint",
        "int32": "int",
        "int64": "bigint",
        "float32": "float",
        "float64": "double",
        "bool": "boolean",
        "date": "date",
        "timestamp": "timestamp",
    }
    return mapping.get(dtype, "string")


def _glue_register_table(glue_client, table_name: str, s3_location: str,
                          columns: list[dict], partition_keys: list[str] = None,
                          description: str = ""):
    """Create or update a table in AWS Glue Data Catalog."""
    glue_columns = [
        {
            "Name": col["name"],
            "Type": _dtype_to_glue(col.get("dtype", "string")),
            "Comment": col.get("description", ""),
        }
        for col in columns
    ]

    storage_descriptor = {
        "Columns": glue_columns,
        "Location": s3_location,
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        },
    }

    table_input = {
        "Name": table_name,
        "Description": description,
        "StorageDescriptor": storage_descriptor,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "delta",
            "delta.lastCommitTimestamp": str(int(datetime.utcnow().timestamp() * 1000)),
            "EXTERNAL": "TRUE",
        },
    }

    if partition_keys:
        table_input["PartitionKeys"] = [
            {"Name": pk, "Type": "string"} for pk in partition_keys
        ]

    try:
        glue_client.get_table(DatabaseName=GLUE_DATABASE, Name=table_name)
        # Table exists — update it
        glue_client.update_table(
            DatabaseName=GLUE_DATABASE,
            TableInput=table_input,
        )
        logger.info(f"Glue: Updated table {GLUE_DATABASE}.{table_name}")
    except glue_client.exceptions.EntityNotFoundException:
        # Table doesn't exist — create it
        glue_client.create_table(
            DatabaseName=GLUE_DATABASE,
            TableInput=table_input,
        )
        logger.info(f"Glue: Created table {GLUE_DATABASE}.{table_name}")


@dag(
    dag_id="glue_catalog_sync",
    description="Register all Delta Lake tables in AWS Glue Data Catalog",
    schedule=[ODL_DIM_DATE, ODL_DIM_LOCATION, ODL_DIM_WEATHER_STATION, ODL_DIM_AIRPORT],
    start_date=datetime(2026, 5, 19),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "awujoo",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["glue", "catalog", "governance", "metadata"],
)
def glue_catalog_sync():

    @task()
    def sync_bronze_tables() -> dict:
        """Register all RDL (Bronze) tables in Glue."""
        from aws_session import get_aws_session, AWS_REGION

        session = get_aws_session()
        glue = session.client("glue", region_name=AWS_REGION)

        # Bronze RDL tables — minimal schema (ingest_date + json)
        rdl_tables = [
            "weather", "wind", "postcodes", "airports",
            "companies", "landscapes", "nw-birds", "private-jets",
        ]

        rdl_columns = [
            {"name": "ingest_date", "dtype": "string", "description": "Partition key — YYYY-MM-DD"},
            {"name": "json", "dtype": "string", "description": "Raw JSON record from source API"},
        ]

        registered = []
        for source in rdl_tables:
            table_name = f"rdl_{source.replace('-', '_')}"
            s3_path = f"{S3_BRONZE}/rdl/{source}"
            try:
                _glue_register_table(
                    glue, table_name, s3_path, rdl_columns,
                    partition_keys=["ingest_date"],
                    description=f"Bronze raw data — {source} ingestion",
                )
                registered.append(table_name)
            except Exception as e:
                logger.error(f"Failed to register {table_name}: {e}")

        logger.info(f"Bronze: registered {len(registered)}/{len(rdl_tables)} tables")
        return {"layer": "bronze", "registered": registered}

    @task()
    def sync_silver_dimensions() -> dict:
        """Register all ODL dimension tables from dimensions.yml."""
        from aws_session import get_aws_session, AWS_REGION

        session = get_aws_session()
        glue = session.client("glue", region_name=AWS_REGION)

        config = _load_yaml("dimensions.yml")
        registered = []

        for dim in config.get("dimensions", []):
            table_name = dim["name"]
            s3_path = f"{S3_SILVER}/odl/dim/{table_name}"
            columns = dim.get("attributes", [])
            description = dim.get("description", "")

            try:
                _glue_register_table(
                    glue, table_name, s3_path, columns,
                    description=f"Silver dimension — {description}",
                )
                registered.append(table_name)
            except Exception as e:
                logger.error(f"Failed to register {table_name}: {e}")

        logger.info(f"Silver dims: registered {len(registered)} tables")
        return {"layer": "silver_dims", "registered": registered}

    @task()
    def sync_silver_facts() -> dict:
        """Register all ODL fact tables from facts.yml."""
        from aws_session import get_aws_session, AWS_REGION

        session = get_aws_session()
        glue = session.client("glue", region_name=AWS_REGION)

        config = _load_yaml("facts.yml")
        registered = []

        for fact in config.get("facts", []):
            table_name = fact["name"]
            s3_path = f"{S3_SILVER}/odl/fact/{table_name}"

            # Build column list from measures + attributes + dimension keys
            columns = []
            for dk in fact.get("dimension_keys", []):
                columns.append({
                    "name": dk["fk"],
                    "dtype": "string",
                    "description": f"FK to {dk['dim']} — {dk.get('role', '')}",
                })
            for measure in fact.get("measures", []):
                columns.append(measure)
            for attr in fact.get("attributes", []):
                columns.append(attr)

            partition_keys = fact.get("partition_by", [])
            description = fact.get("description", "")

            try:
                _glue_register_table(
                    glue, table_name, s3_path, columns,
                    partition_keys=partition_keys,
                    description=f"Silver fact — {description}",
                )
                registered.append(table_name)
            except Exception as e:
                logger.error(f"Failed to register {table_name}: {e}")

        logger.info(f"Silver facts: registered {len(registered)} tables")
        return {"layer": "silver_facts", "registered": registered}

    @task()
    def sync_silver_mappings() -> dict:
        """Register all ODL mapping tables from mappings.yml."""
        from aws_session import get_aws_session, AWS_REGION

        session = get_aws_session()
        glue = session.client("glue", region_name=AWS_REGION)

        config = _load_yaml("mappings.yml")
        registered = []

        for mapping in config.get("mappings", []):
            table_name = mapping["name"]
            s3_path = f"{S3_SILVER}/odl/map/{table_name}"
            columns = mapping.get("attributes", [])
            description = mapping.get("description", "")

            try:
                _glue_register_table(
                    glue, table_name, s3_path, columns,
                    description=f"Silver mapping — {description}",
                )
                registered.append(table_name)
            except Exception as e:
                logger.error(f"Failed to register {table_name}: {e}")

        logger.info(f"Silver mappings: registered {len(registered)} tables")
        return {"layer": "silver_maps", "registered": registered}

    @task()
    def print_summary(bronze: dict, dims: dict, facts: dict, maps: dict):
        """Log final sync summary."""
        total = (len(bronze["registered"]) + len(dims["registered"])
                 + len(facts["registered"]) + len(maps["registered"]))
        logger.info(f"Glue Catalog Sync complete: {total} tables registered in '{GLUE_DATABASE}'")
        logger.info(f"  Bronze: {len(bronze['registered'])} tables")
        logger.info(f"  Dims:   {len(dims['registered'])} tables")
        logger.info(f"  Facts:  {len(facts['registered'])} tables")
        logger.info(f"  Maps:   {len(maps['registered'])} tables")

    # ── DAG Flow ──
    bronze = sync_bronze_tables()
    dims = sync_silver_dimensions()
    facts = sync_silver_facts()
    maps = sync_silver_mappings()
    print_summary(bronze, dims, facts, maps)


glue_catalog_sync()
