"""
BellosData Common Pipeline Infrastructure â€” Shared Module

Provides:
  1. rdl_write()    â€” Write raw JSON + ingest_date to Bronze S3 (Delta)
  2. odl_write()    â€” Write typed tables to Silver S3 (Delta)
  3. http_get()     â€” Resilient HTTP GET with retry + user-agent
  4. http_get_csv() â€” Download and parse CSV from URL
  5. write_manifest() â€” Audit manifest for every ingestion run

Architecture:
  RDL â†’ s3://bellosdata-bronze-raw/rdl/{source}/
  ODL â†’ s3://bellosdata-silver-curated/odl/{type}/{table}/

Author: Awujoo (AWUJOO-041) | Genesis: 2026-05-17
"""

import json
import logging
import ssl
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

# â”€â”€ S3 Configuration â”€â”€
S3_BRONZE_BUCKET = "bellosdata-bronze-raw"
S3_SILVER_BUCKET = "bellosdata-silver-curated"
RDL_PREFIX = "rdl"
ODL_PREFIX = "odl"

DEFAULT_USER_AGENT = "BellosData-Pipeline/1.0 (awujoo@bellosdata.com)"
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3
RETRY_BACKOFF = 2


def _get_aws_resources():
    """Lazy-load AWS session and credentials for Delta writes."""
    from aws_session import get_aws_session, AWS_REGION
    session = get_aws_session()
    creds = session.get_credentials().get_frozen_credentials()
    opts = {
        "AWS_ACCESS_KEY_ID": creds.access_key,
        "AWS_SECRET_ACCESS_KEY": creds.secret_key,
        "AWS_REGION": AWS_REGION,
    }
    if creds.token:
        opts["AWS_SESSION_TOKEN"] = creds.token
    return session.client("s3", region_name=AWS_REGION), opts


# â”€â”€ RDL Writer â”€â”€

def rdl_write(source_name: str, records: list[dict],
              ingest_date: Optional[str] = None) -> dict:
    """Write raw data to RDL. Schema: {ingest_date: STRING, json: STRING}."""
    import pyarrow as pa
    from deltalake import write_deltalake

    if not ingest_date:
        ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    rdl_rows = [
        {"ingest_date": ingest_date,
         "json": json.dumps(r, ensure_ascii=False, default=str)}
        for r in records
    ]

    if not rdl_rows:
        logger.warning(f"RDL '{source_name}': 0 records, skipping.")
        return {"delta_path": None, "record_count": 0,
                "ingest_date": ingest_date, "source": source_name, "status": "EMPTY"}

    table = pa.table({
        "ingest_date": [r["ingest_date"] for r in rdl_rows],
        "json": pa.array([r["json"] for r in rdl_rows], type=pa.large_string()),
    })

    _, storage_options = _get_aws_resources()
    delta_path = f"s3://{S3_BRONZE_BUCKET}/{RDL_PREFIX}/{source_name}"

    write_deltalake(delta_path, table, mode="append",
                    storage_options=storage_options,
                    partition_by=["ingest_date"])

    logger.info(f"RDL: {len(rdl_rows)} records â†’ {delta_path} ({ingest_date})")
    return {"delta_path": delta_path, "record_count": len(rdl_rows),
            "ingest_date": ingest_date, "source": source_name, "status": "OK"}


# â”€â”€ ODL Writer â”€â”€

def odl_write(table_type: str, table_name: str,
              data: list[dict], schema=None) -> dict:
    """Write typed data to ODL Silver S3 as Delta."""
    import pyarrow as pa
    from deltalake import write_deltalake

    if not data:
        logger.warning(f"ODL '{table_name}': 0 records, skipping.")
        return {"delta_path": None, "record_count": 0,
                "table_name": table_name, "status": "EMPTY"}

    table = pa.Table.from_pylist(data) if not schema else pa.table(
        {col: [r.get(col) for r in data] for col in schema.names}, schema=schema)

    _, storage_options = _get_aws_resources()
    delta_path = f"s3://{S3_SILVER_BUCKET}/{ODL_PREFIX}/{table_type}/{table_name}"

    write_deltalake(delta_path, table, mode="overwrite",
                    storage_options=storage_options)

    logger.info(f"ODL: {len(data)} records â†’ {delta_path}")
    return {"delta_path": delta_path, "record_count": len(data),
            "table_name": table_name, "table_type": table_type, "status": "OK"}


# â”€â”€ HTTP Helper â”€â”€

def http_get(url: str, headers: Optional[dict] = None,
             retries: int = DEFAULT_RETRIES, timeout: int = DEFAULT_TIMEOUT,
             parse_json: bool = True) -> Any:
    """Resilient HTTP GET with exponential backoff retry."""
    ctx = ssl.create_default_context()
    req_headers = {"User-Agent": DEFAULT_USER_AGENT}
    if headers:
        req_headers.update(headers)

    last_error = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=req_headers)
            response = urllib.request.urlopen(req, context=ctx, timeout=timeout)
            raw = response.read()
            return json.loads(raw.decode("utf-8")) if parse_json else raw
        except urllib.error.HTTPError as e:
            last_error = e
            if e.code == 429 or e.code >= 500:
                wait = RETRY_BACKOFF * (2 ** attempt) * (2 if e.code == 429 else 1)
                logger.warning(f"HTTP {e.code} on {url}. Retry in {wait}s...")
                time.sleep(wait)
            else:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last_error = e
            wait = RETRY_BACKOFF * (2 ** attempt)
            logger.warning(f"Conn error {url} ({attempt+1}/{retries}): {e}. {wait}s...")
            time.sleep(wait)

    raise last_error or RuntimeError(f"Failed to fetch {url}")


def http_get_csv(url: str, headers: Optional[dict] = None,
                 timeout: int = 120) -> list[dict]:
    """Download and parse a CSV file from a URL."""
    import csv, io
    raw = http_get(url, headers=headers, timeout=timeout, parse_json=False)
    return list(csv.DictReader(io.StringIO(raw.decode("utf-8", errors="replace"))))


# â”€â”€ Manifest Writer â”€â”€

def write_manifest(dag_id: str, source_name: str, rdl_result: dict,
                   record_counts: Optional[dict] = None,
                   extra: Optional[dict] = None) -> dict:
    """Write ingestion audit manifest to S3."""
    now = datetime.now(timezone.utc)
    ts_str = now.strftime("%Y%m%dT%H%M%SZ")

    manifest = {
        "manifest_id": f"{source_name.upper()}-{ts_str}",
        "timestamp": now.isoformat(), "dag_id": dag_id,
        "source": source_name, "layer": "RDL", "format": "DELTA",
        "rdl_path": rdl_result.get("delta_path"),
        "ingest_date": rdl_result.get("ingest_date"),
        "total_records": rdl_result.get("record_count", 0),
        "status": rdl_result.get("status", "UNKNOWN"),
    }
    if record_counts:
        manifest["record_breakdown"] = record_counts
    if extra:
        manifest.update(extra)

    s3, _ = _get_aws_resources()
    key = f"{RDL_PREFIX}/{source_name}/manifests/{now:%Y-%m-%d}/manifest_{ts_str}.json"
    s3.put_object(Bucket=S3_BRONZE_BUCKET, Key=key,
                  Body=json.dumps(manifest, indent=2).encode("utf-8"),
                  ContentType="application/json")
    logger.info(f"Manifest: s3://{S3_BRONZE_BUCKET}/{key}")
    return manifest
