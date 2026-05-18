"""
RDL Companies House Enhanced â€” NW England Company Intelligence Pipeline

Source: Companies House Bulk Download (BasicCompanyDataAsOneFile)
OGL v3.0 licensed. Schedule: Monthly (1st, 04:00 UTC).
RDL Output: s3://bellosdata-bronze-raw/rdl/companies/

Author: Awujoo (AWUJOO-041 Phase 2) | Genesis: 2026-05-17
Trade Route: TR-002 Company Intelligence Reports
"""

from datetime import datetime, timedelta
import csv
import io
import json
import logging
import os

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

RDL_COMPANIES_ASSET = Asset("s3://bellosdata-bronze-raw/rdl/companies")

NW_OUTCODE_PREFIXES = (
    "M", "BL", "OL", "SK", "WN", "WA", "L", "CH",
    "BB", "PR", "FY", "LA", "CA", "CW",
)

OGL_ATTRIBUTION = (
    "Contains public sector information licensed under the "
    "Open Government Licence v3.0. Source: Companies House."
)


def _is_nw_postcode(postcode):
    if not postcode:
        return False
    pc = postcode.strip().upper()
    for prefix in NW_OUTCODE_PREFIXES:
        if pc.startswith(prefix) and len(pc) > len(prefix):
            if pc[len(prefix)].isdigit():
                return True
    return False


def _compute_company_age(inc_date):
    if not inc_date:
        return None
    try:
        dt = datetime.strptime(inc_date.strip(), "%d/%m/%Y")
        return (datetime.utcnow() - dt).days // 365
    except (ValueError, TypeError):
        return None


@dag(
    dag_id="rdl_companies_enhanced",
    description="Ingest NW England companies from CH bulk data (OGL v3.0 â†’ RDL)",
    schedule="0 4 1 * *",
    start_date=datetime(2026, 5, 17),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "awujoo", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["companies", "rdl", "ingestion", "enrichment", "TR-002", "sovereign"],
)
def rdl_companies_enhanced():

    @task()
    def fetch_bulk_csv():
        """Get CH bulk CSV â€” try S3 cache, then download, then local."""
        try:
            from aws_session import get_aws_session, AWS_REGION
            session = get_aws_session()
            s3 = session.client("s3", region_name=AWS_REGION)
            s3_key = "rdl/companies/bulk/BasicCompanyDataAsOneFile-latest.csv"
            resp = s3.head_object(Bucket="bellosdata-bronze-raw", Key=s3_key)
            logger.info(f"S3 cache found: {resp['ContentLength']/1e6:.0f} MB")
            return {"source": "s3_cache", "path": f"s3://bellosdata-bronze-raw/{s3_key}"}
        except Exception:
            pass

        try:
            import urllib.request, zipfile
            url = "http://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2026-05-01.zip"
            dest_zip, dest_csv = "/tmp/ch_bulk.zip", "/tmp/ch_bulk.csv"
            if not os.path.exists(dest_csv):
                urllib.request.urlretrieve(url, dest_zip)
                with zipfile.ZipFile(dest_zip, 'r') as zf:
                    name = [n for n in zf.namelist() if n.endswith('.csv')][0]
                    with zf.open(name) as s, open(dest_csv, 'wb') as d:
                        d.write(s.read())
                os.remove(dest_zip)
            return {"source": "ch_download", "path": dest_csv}
        except Exception as e:
            logger.warning(f"CH download failed: {e}")

        return {"source": "unavailable", "path": None}

    @task()
    def filter_nw_companies(source_info):
        """Filter 5.7M records to NW England, enrich with age/SIC/charges."""
        path = source_info.get("path")
        source = source_info.get("source")
        if not path:
            return {"status": "STUB", "records": 0, "data": []}

        nw = []
        total = 0

        if source == "s3_cache":
            from aws_session import get_aws_session, AWS_REGION
            s3 = get_aws_session().client("s3", region_name=AWS_REGION)
            bucket, key = path.split("/")[2], "/".join(path.split("/")[3:])
            body = s3.get_object(Bucket=bucket, Key=key)["Body"]
            reader = csv.DictReader(io.TextIOWrapper(body, encoding="utf-8", errors="replace"))
        else:
            reader = csv.DictReader(open(path, "r", encoding="utf-8", errors="replace"))

        for row in reader:
            total += 1
            postcode = row.get("RegAddress.PostCode", "").strip()
            if not _is_nw_postcode(postcode):
                continue
            cn = row.get(" CompanyNumber", "").strip()
            if not cn:
                continue

            sics = [row.get(f"SICCode.SicText_{i}", "").strip() for i in range(1, 5)]
            sics = [s for s in sics if s]
            charges = int(row.get("Mortgages.NumMortCharges", "0") or "0")

            nw.append({
                "company_number": cn,
                "company_name": row.get("CompanyName", "").strip(),
                "company_status": row.get("CompanyStatus", "").strip(),
                "company_category": row.get("CompanyCategory", "").strip(),
                "incorporation_date": row.get("IncorporationDate", "").strip(),
                "company_age_years": _compute_company_age(row.get("IncorporationDate", "")),
                "postcode": postcode,
                "post_town": row.get("RegAddress.PostTown", "").strip(),
                "county": row.get("RegAddress.County", "").strip(),
                "sic_codes": sics,
                "sic_primary": sics[0] if sics else "",
                "account_category": row.get("Accounts.AccountCategory", "").strip(),
                "num_charges": charges,
                "has_charges": charges > 0,
                "uri": row.get("URI", "").strip(),
                "source": "COMPANIES_HOUSE_BULK",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
            })
            if total % 500000 == 0:
                logger.info(f"Scanned {total:,} â€” NW: {len(nw):,}")

        logger.info(f"Done: {len(nw):,} NW companies from {total:,} total")
        return {"status": "OK", "records": len(nw), "total_scanned": total, "data": nw}

    @task(outlets=[RDL_COMPANIES_ASSET])
    def write_to_rdl(result):
        """Write NW companies to RDL as raw JSON + ingest_date."""
        from bellosdata_common import rdl_write, write_manifest
        rdl = rdl_write("companies", result.get("data", []))
        return write_manifest("rdl_companies_enhanced", "companies", rdl,
            record_counts={"nw": result.get("records", 0), "total": result.get("total_scanned", 0)},
            extra={"licence": "OGL v3.0", "trade_route": "TR-002"})

    source = fetch_bulk_csv()
    companies = filter_nw_companies(source)
    write_to_rdl(companies)

rdl_companies_enhanced()
