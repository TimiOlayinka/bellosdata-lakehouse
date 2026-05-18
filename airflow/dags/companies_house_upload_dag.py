"""
Companies House Upload DAG — The First Trade Good

Dedicated DAG for the 3.14 GB Companies House dataset.
Uses S3 multipart upload for large files. Invoice-tracked.

Schedule: Manual trigger only (one-time migration + monthly refresh)
Author: Awujoo (AWUJOO-024)
Date: 2026-05-15

"5.7 million companies. One CSV. The foundation of TR-001."
"""

from datetime import datetime, timedelta
import hashlib
import json
import os
import logging

from airflow.decorators import dag, task
from airflow.sdk import Asset

# ── Assets ─────────────────────────────────────────────────────
CH_ASSET = Asset("s3://playdarch-bronze-raw/companies-house")

# ── Configuration ──────────────────────────────────────────────
S3_BUCKET = "playdarch-bronze-raw"
S3_PREFIX = "companies-house"
from aws_session import get_aws_session
SESSION_ID = "AWUJOO-024"

RAW_DATA_DIR = r"D:\AlwaysPlanning\awujoo-data-platform\raw_data"

# Files to upload
CH_FILES = [
    {
        "filename": "companies_house_basic.zip",
        "filepath": os.path.join(RAW_DATA_DIR, "companies_house_basic.zip"),
        "multipart": True,
    },
    {
        "filename": "BasicCompanyDataAsOneFile-2026-05-01.csv",
        "filepath": os.path.join(RAW_DATA_DIR, "companies_house", "BasicCompanyDataAsOneFile-2026-05-01.csv"),
        "multipart": True,
    },
    {
        "filename": "companies_house_meta.json",
        "filepath": os.path.join(RAW_DATA_DIR, "companies_house_meta.json"),
        "multipart": False,
    },
]

# Multipart chunk size: 50 MB
CHUNK_SIZE = 50 * 1024 * 1024

logger = logging.getLogger(__name__)


def _sha256_file(filepath: str) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


@dag(
    dag_id="companies_house_upload",
    description="Bulk upload Companies House data to S3 (3.14 GB). Invoice-tracked.",
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 5, 15),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "awujoo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=1),
    },
    tags=["companies-house", "bulk-upload", "invoice", "sovereign"],
)
def companies_house_upload():

    @task()
    def create_invoices() -> list:
        """Create invoices for each Companies House file."""
        from invoice_engine import create_invoice

        invoices = []
        for idx, ch_file in enumerate(CH_FILES):
            filepath = ch_file["filepath"]
            if not os.path.exists(filepath):
                logger.warning(f"File not found, skipping: {filepath}")
                continue

            size = os.path.getsize(filepath)
            file_hash = _sha256_file(filepath)
            s3_key = f"{S3_PREFIX}/{ch_file['filename']}"

            invoice = create_invoice(
                source=filepath,
                destination=f"s3://{S3_BUCKET}/{s3_key}",
                session_id=SESSION_ID,
                transfer_id=f"TXN-CH-{idx+1:03d}",
                size_bytes=size,
                source_hash=file_hash,
                max_retry=2,
                timeout_seconds=3600,
            )
            invoice["_ch_file"] = ch_file
            invoice["_s3_key"] = s3_key
            invoice["_hash"] = file_hash
            invoices.append(invoice)

            size_mb = size / (1024 * 1024)
            logger.info(f"Invoice created for {ch_file['filename']} ({size_mb:.1f} MB)")

        return invoices

    @task(outlets=[CH_ASSET])
    def execute_uploads(invoices: list) -> list:
        """Execute multipart or standard uploads for each file."""
        import boto3
        from invoice_engine import transition_invoice, complete_invoice, fail_invoice

        session = get_aws_session()
        s3 = session.client("s3", region_name="eu-west-2")
        now_iso = datetime.utcnow().isoformat() + "Z"

        results = []

        for invoice in invoices:
            ch_file = invoice.pop("_ch_file", {})
            s3_key = invoice.pop("_s3_key", "")
            expected_hash = invoice.pop("_hash", "")
            filepath = ch_file.get("filepath", "")
            use_multipart = ch_file.get("multipart", False)

            invoice = transition_invoice(invoice, "TRANSFERRING", f"Uploading {s3_key}")

            try:
                if use_multipart:
                    _multipart_upload(s3, filepath, s3_key, expected_hash, now_iso, invoice)
                else:
                    _standard_upload(s3, filepath, s3_key, expected_hash, now_iso, invoice)

                # Verify
                invoice = transition_invoice(invoice, "VERIFYING", "Checking S3 object exists")
                head = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
                s3_size = head.get("ContentLength", 0)
                local_size = os.path.getsize(filepath)

                if abs(s3_size - local_size) < 1024:  # Allow small metadata overhead
                    invoice = complete_invoice(invoice)
                    logger.info(f"✅ {ch_file['filename']} uploaded ({s3_size:,} bytes)")
                else:
                    invoice = fail_invoice(
                        invoice,
                        f"Size mismatch: local={local_size:,}, s3={s3_size:,}"
                    )
            except Exception as e:
                invoice = fail_invoice(invoice, str(e))

            results.append(invoice)

        return results

    @task()
    def file_receipt(results: list) -> dict:
        """Generate batch receipt for the upload."""
        from invoice_engine import generate_batch_receipt

        return generate_batch_receipt(
            invoices=results,
            session_id=SESSION_ID,
            dag_id="companies_house_upload",
        )

    # ── DAG Flow ──────────────────────────────────────────
    invoices = create_invoices()
    results = execute_uploads(invoices)
    file_receipt(results)


def _multipart_upload(s3, filepath, s3_key, file_hash, now_iso, invoice):
    """Perform S3 multipart upload for large files."""
    from invoice_engine import transition_invoice

    file_size = os.path.getsize(filepath)
    num_parts = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

    logger.info(f"Multipart upload: {s3_key} — {file_size:,} bytes in {num_parts} parts")

    mpu = s3.create_multipart_upload(
        Bucket=S3_BUCKET,
        Key=s3_key,
        ContentType="application/octet-stream",
        Metadata={
            "sha256": file_hash,
            "synced_at": now_iso,
            "synced_by": "airflow-ch-upload-dag",
            "invoice_id": invoice["invoice_id"],
        },
    )
    upload_id = mpu["UploadId"]

    parts = []
    try:
        with open(filepath, "rb") as f:
            for part_num in range(1, num_parts + 1):
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break

                part = s3.upload_part(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=chunk,
                )
                parts.append({"PartNumber": part_num, "ETag": part["ETag"]})

                if part_num % 10 == 0 or part_num == num_parts:
                    pct = (part_num / num_parts) * 100
                    logger.info(f"  Part {part_num}/{num_parts} ({pct:.0f}%)")

        s3.complete_multipart_upload(
            Bucket=S3_BUCKET,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        logger.info(f"Multipart upload completed: {s3_key}")

    except Exception as e:
        # Abort multipart on failure
        s3.abort_multipart_upload(
            Bucket=S3_BUCKET,
            Key=s3_key,
            UploadId=upload_id,
        )
        raise e


def _standard_upload(s3, filepath, s3_key, file_hash, now_iso, invoice):
    """Standard single-part upload for small files."""
    with open(filepath, "rb") as f:
        content = f.read()

    content_type = "application/json" if filepath.endswith(".json") else "application/octet-stream"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=content,
        ContentType=content_type,
        Metadata={
            "sha256": file_hash,
            "synced_at": now_iso,
            "synced_by": "airflow-ch-upload-dag",
            "invoice_id": invoice["invoice_id"],
        },
    )


# Instantiate
companies_house_upload()
