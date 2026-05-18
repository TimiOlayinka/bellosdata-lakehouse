"""
Drive Sync DAG â€” The Grand Inventory

Scans all local drives (D:, X:, Y:) for files that must be in S3,
diffs against current S3 state, and transfers only what has changed.
Every transfer is invoiced via the smart contract engine.

Emits Assets to trigger downstream DAGs (ledger_sync, receipt_archive).

Schedule: Daily at 06:00 UTC + manual trigger
Author: Awujoo (AWUJOO-024)
Date: 2026-05-15

"A merchant must keep three books â€” the memorandum, the journal, and the ledger."
â€” Cotrugli, 1458
"""

from datetime import datetime, timedelta
import hashlib
import json
import os
import logging
from pathlib import Path

from airflow.decorators import dag, task
from airflow.sdk import Asset

# â”€â”€ Assets (Airflow 3.x) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
LEDGER_LIVE_ASSET = Asset("s3://bellosdata-bronze-raw/ledger-live")
RECEIPTS_ASSET = Asset("s3://bellosdata-bronze-raw/receipts")
GOVERNANCE_ASSET = Asset("s3://bellosdata-bronze-raw/governance")

# â”€â”€ Configuration â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
S3_BUCKET = "bellosdata-bronze-raw"
from aws_session import get_aws_session
SESSION_ID = "AWUJOO-024"

# Master manifest: every file that should live in S3
SYNC_MANIFEST = {
    # â”€â”€ D: Drive â€” Critical Ledger State â”€â”€
    "ledger_state": {
        "source_dir": r"D:\AlwaysPlanning\merchant-ledger",
        "s3_prefix": "ledger-live",
        "files": [
            "compute-ledger.json",
            "shared.json",
            "profiles.json",
            "dashiuda-captable.json",
            "trust-ledger.json",
            "agents.json",
            "ledger-data-awujoo.json",
            "ledger-data-bellosdata.json",
            "ledger-data-dashiuda.json",
            "ledger-data-timi-olayinka.json",
        ],
    },
    # â”€â”€ D: Drive â€” Governance Docs â”€â”€
    "governance": {
        "source_dir": r"D:\AlwaysPlanning\merchant-ledger",
        "s3_prefix": "governance",
        "files": [
            "GOVERNANCE.md",
            "PROTOCOL.md",
            "LIBRARY.md",
        ],
    },
    # â”€â”€ X: Drive â€” Receipts â”€â”€
    "receipts_primary": {
        "source_dir": r"X:\BellosData\receipts",
        "s3_prefix": "receipts",
        "glob": "*.md",
    },
    # â”€â”€ X: Drive â€” Alerts â”€â”€
    "alerts": {
        "source_dir": r"X:\BellosData\alerts",
        "s3_prefix": "alerts",
        "glob": "*.*",
    },
    # â”€â”€ Y: Drive â€” Dashuida Archive State â”€â”€
    "dashuida_state": {
        "source_dir": r"Y:\Merchant Ledger",
        "s3_prefix": "dashuida-archive",
        "glob": "*.json",
    },
    # â”€â”€ Y: Drive â€” Dashuida Receipts â”€â”€
    "dashuida_receipts": {
        "source_dir": r"Y:\Merchant Ledger\receipts",
        "s3_prefix": "receipts",
        "glob": "*.md",
    },
    # â”€â”€ Y: Drive â€” SHARED.brain Receipts â”€â”€
    "shared_brain_receipts": {
        "source_dir": r"Y:\brain\SHARED.brain\merchant-ledger\receipts",
        "s3_prefix": "receipts",
        "glob": "*.md",
    },
    # â”€â”€ Y: Drive â€” Need Advice docs â”€â”€
    "need_advice": {
        "source_dir": r"Y:\Merchant Ledger",
        "s3_prefix": "governance/need-advice",
        "glob": "NEED-ADVICE-*.md",
    },
}

logger = logging.getLogger(__name__)


def _sha256_file(filepath: str) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


@dag(
    dag_id="drive_sync",
    description="Scan local drives, diff with S3, transfer only changes. Invoice every transfer.",
    schedule="0 6 * * *",
    start_date=datetime(2026, 5, 15),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "awujoo",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["sync", "drives", "invoice", "sovereign", "assets"],
)
def drive_sync():

    @task()
    def scan_local_drives() -> list:
        """Scan all drives and build a manifest of files to potentially sync."""
        import glob as globmod

        local_files = []

        for category, config in SYNC_MANIFEST.items():
            source_dir = config["source_dir"]

            if not os.path.exists(source_dir):
                logger.warning(f"Source dir not found: {source_dir}")
                continue

            s3_prefix = config["s3_prefix"]

            # Either explicit files or glob
            if "files" in config:
                for filename in config["files"]:
                    filepath = os.path.join(source_dir, filename)
                    if os.path.exists(filepath):
                        local_files.append({
                            "category": category,
                            "filepath": filepath,
                            "filename": filename,
                            "s3_key": f"{s3_prefix}/{filename}",
                            "hash": _sha256_file(filepath),
                            "size_bytes": os.path.getsize(filepath),
                        })
                    else:
                        logger.warning(f"File not found: {filepath}")
            elif "glob" in config:
                pattern = os.path.join(source_dir, config["glob"])
                for filepath in globmod.glob(pattern):
                    filename = os.path.basename(filepath)
                    local_files.append({
                        "category": category,
                        "filepath": filepath,
                        "filename": filename,
                        "s3_key": f"{s3_prefix}/{filename}",
                        "hash": _sha256_file(filepath),
                        "size_bytes": os.path.getsize(filepath),
                    })

        logger.info(f"Scanned {len(local_files)} files across all drives")
        return local_files

    @task()
    def diff_with_s3(local_files: list) -> list:
        """Compare local hashes with S3 metadata â€” only sync what changed."""
        import boto3

        session = get_aws_session()
        s3 = session.client("s3", region_name="eu-west-2")

        files_to_sync = []

        for file_info in local_files:
            s3_key = file_info["s3_key"]
            local_hash = file_info["hash"]

            try:
                head = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
                s3_hash = head.get("Metadata", {}).get("sha256", "")

                if s3_hash == local_hash:
                    # File unchanged â€” skip
                    logger.debug(f"Unchanged: {s3_key}")
                    continue
                else:
                    file_info["reason"] = "hash_mismatch"
                    files_to_sync.append(file_info)
            except s3.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    file_info["reason"] = "missing_in_s3"
                    files_to_sync.append(file_info)
                else:
                    logger.error(f"S3 head failed for {s3_key}: {e}")
                    file_info["reason"] = "s3_error"
                    files_to_sync.append(file_info)
            except Exception as e:
                logger.error(f"Unexpected error for {s3_key}: {e}")
                file_info["reason"] = "unknown_error"
                files_to_sync.append(file_info)

        logger.info(
            f"Diff result: {len(files_to_sync)} files need sync "
            f"({len(local_files) - len(files_to_sync)} unchanged)"
        )
        return files_to_sync

    @task()
    def create_invoices(files_to_sync: list) -> list:
        """Create an invoice for each file transfer."""
        from invoice_engine import create_invoice

        invoices = []
        for idx, file_info in enumerate(files_to_sync):
            transfer_id = f"TXN-024-{idx+1:03d}"
            invoice = create_invoice(
                source=file_info["filepath"],
                destination=f"s3://{S3_BUCKET}/{file_info['s3_key']}",
                session_id=SESSION_ID,
                transfer_id=transfer_id,
                size_bytes=file_info["size_bytes"],
                source_hash=file_info["hash"],
            )
            invoice["_file_info"] = file_info
            invoices.append(invoice)

        logger.info(f"Created {len(invoices)} invoices")
        return invoices

    @task(outlets=[LEDGER_LIVE_ASSET, RECEIPTS_ASSET, GOVERNANCE_ASSET])
    def execute_transfers(invoices: list) -> list:
        """Execute all invoiced transfers and verify each one."""
        import boto3
        from invoice_engine import (
            transition_invoice,
            complete_invoice,
            fail_invoice,
        )

        session = get_aws_session()
        s3 = session.client("s3", region_name="eu-west-2")
        now_iso = datetime.utcnow().isoformat() + "Z"

        completed_invoices = []

        for invoice in invoices:
            file_info = invoice.pop("_file_info", {})
            filepath = file_info.get("filepath", "")
            s3_key = file_info.get("s3_key", "")
            expected_hash = file_info.get("hash", "")

            # Transition to TRANSFERRING
            invoice = transition_invoice(invoice, "TRANSFERRING", f"Uploading {s3_key}")

            try:
                # Read file content
                with open(filepath, "rb") as f:
                    content = f.read()

                # Determine content type
                if filepath.endswith(".json"):
                    content_type = "application/json"
                elif filepath.endswith(".md"):
                    content_type = "text/markdown"
                else:
                    content_type = "application/octet-stream"

                # Upload to S3
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=content,
                    ContentType=content_type,
                    Metadata={
                        "sha256": expected_hash,
                        "synced_at": now_iso,
                        "synced_by": "airflow-drive-sync-dag",
                        "invoice_id": invoice["invoice_id"],
                        "session": SESSION_ID,
                    },
                )

                # Verify
                invoice = transition_invoice(invoice, "VERIFYING", "Checking S3 hash")
                head = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
                s3_hash = head.get("Metadata", {}).get("sha256", "")

                if s3_hash == expected_hash:
                    invoice = complete_invoice(invoice)
                    logger.info(f"âœ… {s3_key} â€” verified")
                else:
                    invoice = fail_invoice(invoice, f"Hash mismatch: expected {expected_hash[:16]}..., got {s3_hash[:16]}...")

            except FileNotFoundError:
                invoice = fail_invoice(invoice, f"Source file not found: {filepath}")
            except Exception as e:
                invoice = fail_invoice(invoice, str(e))

            completed_invoices.append(invoice)

        completed_count = sum(1 for i in completed_invoices if i["status"] == "COMPLETED")
        failed_count = sum(1 for i in completed_invoices if i["status"] in ("FAILED", "ALERTED"))
        logger.info(f"Transfer batch complete: {completed_count} OK, {failed_count} failed")

        return completed_invoices

    @task()
    def file_batch_receipt(invoices: list) -> dict:
        """Generate a batch receipt for all transfers."""
        from invoice_engine import generate_batch_receipt

        receipt = generate_batch_receipt(
            invoices=invoices,
            session_id=SESSION_ID,
            dag_id="drive_sync",
        )
        return receipt

    # â”€â”€ DAG Flow â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    local = scan_local_drives()
    changed = diff_with_s3(local)
    invoices = create_invoices(changed)
    results = execute_transfers(invoices)
    file_batch_receipt(results)


# Instantiate
drive_sync()
