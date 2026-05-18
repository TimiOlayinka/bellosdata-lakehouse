"""
Invoice Engine — Smart Contract Module for Data Transfers

Every data transfer in the sovereign ecosystem creates an invoice
BEFORE execution. The invoice transitions through states:
  INVOICED → TRANSFERRING → VERIFYING → COMPLETED → RECEIPTED
                                      → FAILED → RETRY/ALERTED

Author: Awujoo (AWUJOO-024)
Date: 2026-05-15

"A merchant who cannot account for his goods is no merchant at all."
"""

import hashlib
import json
import os
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────
INVOICES_DIR = r"X:\BellosData\receipts\invoices"
RECEIPTS_PRIMARY = r"X:\BellosData\receipts"
RECEIPTS_DASHUIDA = r"Y:\Merchant Ledger\receipts"
ALERTS_DIR = r"X:\BellosData\alerts"


def _sha256_file(filepath: str) -> str:
    """Compute SHA-256 hash of a file's contents."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(data: bytes) -> str:
    """Compute SHA-256 hash of raw bytes."""
    return hashlib.sha256(data).hexdigest()


def _now_iso() -> str:
    """Get current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def create_invoice(
    source: str,
    destination: str,
    session_id: str,
    transfer_id: str,
    size_bytes: int = 0,
    source_hash: str = "",
    max_retry: int = 3,
    timeout_seconds: int = 300,
) -> dict:
    """
    Create an invoice for a data transfer.

    The invoice is the smart contract — it defines pre/post conditions
    and must be satisfied before the transfer is considered complete.
    """
    invoice = {
        "invoice_id": transfer_id,
        "session": session_id,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "type": "data_transfer",
        "source": source,
        "destination": destination,
        "source_hash": source_hash,
        "size_bytes": size_bytes,
        "status": "INVOICED",
        "smart_contract": {
            "pre_conditions": [
                "file_exists",
                "s3_bucket_accessible",
                "aws_profile_valid",
            ],
            "post_conditions": [
                "s3_object_exists",
                "hash_matches",
                "receipt_filed",
            ],
            "max_retry": max_retry,
            "timeout_seconds": timeout_seconds,
        },
        "attempts": 0,
        "receipt_id": None,
        "error": None,
        "history": [
            {"timestamp": _now_iso(), "status": "INVOICED", "detail": "Invoice created"},
        ],
    }

    # Write invoice to disk
    os.makedirs(INVOICES_DIR, exist_ok=True)
    invoice_path = os.path.join(INVOICES_DIR, f"{transfer_id}.json")
    with open(invoice_path, "w", encoding="utf-8") as f:
        json.dump(invoice, f, indent=2, ensure_ascii=False)

    logger.info(f"Invoice created: {transfer_id} | {source} → {destination}")
    return invoice


def transition_invoice(invoice: dict, new_status: str, detail: str = "") -> dict:
    """Transition an invoice to a new status, appending to history."""
    invoice["status"] = new_status
    invoice["updated_at"] = _now_iso()
    invoice["history"].append({
        "timestamp": _now_iso(),
        "status": new_status,
        "detail": detail,
    })

    # Persist
    invoice_path = os.path.join(INVOICES_DIR, f"{invoice['invoice_id']}.json")
    os.makedirs(INVOICES_DIR, exist_ok=True)
    with open(invoice_path, "w", encoding="utf-8") as f:
        json.dump(invoice, f, indent=2, ensure_ascii=False)

    return invoice


def verify_s3_transfer(
    s3_client,
    bucket: str,
    key: str,
    expected_hash: str,
) -> bool:
    """
    Verify a file exists in S3 and its hash matches.
    Returns True if the post-condition is satisfied.
    """
    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)
        # Check metadata hash if available
        stored_hash = head.get("Metadata", {}).get("sha256", "")
        if stored_hash and stored_hash == expected_hash:
            return True
        # If no metadata hash, download and verify
        if not stored_hash:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read()
            actual_hash = _sha256_bytes(body)
            return actual_hash == expected_hash
        return False
    except Exception as e:
        logger.error(f"S3 verification failed for {key}: {e}")
        return False


def complete_invoice(invoice: dict, receipt_id: Optional[str] = None) -> dict:
    """
    Mark an invoice as COMPLETED and generate a receipt reference.
    """
    if receipt_id is None:
        receipt_id = f"RCPT-{invoice['invoice_id']}"

    invoice["receipt_id"] = receipt_id
    invoice = transition_invoice(
        invoice, "COMPLETED",
        f"Transfer verified. Receipt: {receipt_id}"
    )

    logger.info(f"Invoice {invoice['invoice_id']} COMPLETED → Receipt {receipt_id}")
    return invoice


def fail_invoice(invoice: dict, reason: str) -> dict:
    """
    Mark an invoice as FAILED. Increments attempt count.
    If max retries exceeded, transitions to ALERTED.
    """
    invoice["attempts"] += 1
    invoice["error"] = reason
    max_retry = invoice["smart_contract"]["max_retry"]

    if invoice["attempts"] >= max_retry:
        invoice = transition_invoice(
            invoice, "ALERTED",
            f"Max retries ({max_retry}) exhausted. Reason: {reason}"
        )
        # Write alert
        _write_alert(invoice, reason)
    else:
        invoice = transition_invoice(
            invoice, "FAILED",
            f"Attempt {invoice['attempts']}/{max_retry}. Reason: {reason}"
        )

    logger.warning(
        f"Invoice {invoice['invoice_id']} FAILED (attempt {invoice['attempts']}): {reason}"
    )
    return invoice


def _write_alert(invoice: dict, reason: str) -> None:
    """Write a transfer failure alert to the alerts directory."""
    os.makedirs(ALERTS_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
    alert_path = os.path.join(ALERTS_DIR, f"TRANSFER-ALERT-{ts}.md")
    with open(alert_path, "w", encoding="utf-8") as f:
        f.write(f"# ⚠️ TRANSFER FAILURE ALERT\n\n")
        f.write(f"| Field | Value |\n|---|---|\n")
        f.write(f"| Invoice | {invoice['invoice_id']} |\n")
        f.write(f"| Session | {invoice['session']} |\n")
        f.write(f"| Source | {invoice['source']} |\n")
        f.write(f"| Destination | {invoice['destination']} |\n")
        f.write(f"| Attempts | {invoice['attempts']} |\n")
        f.write(f"| Reason | {reason} |\n")
        f.write(f"| Timestamp | {_now_iso()} |\n\n")
        f.write("> ⛔ Max retries exhausted. File 'Need Advice' if critical.\n")


def generate_batch_receipt(
    invoices: list,
    session_id: str,
    dag_id: str,
) -> dict:
    """
    Generate a batch receipt summarising all invoices from a single DAG run.
    Returns the receipt dict and writes it to the receipts directories.
    """
    completed = [i for i in invoices if i["status"] == "COMPLETED"]
    failed = [i for i in invoices if i["status"] in ("FAILED", "ALERTED")]
    total_bytes = sum(i.get("size_bytes", 0) for i in completed)

    receipt = {
        "receipt_id": f"BATCH-{session_id}-{dag_id}",
        "session": session_id,
        "dag_id": dag_id,
        "timestamp": _now_iso(),
        "summary": {
            "total_invoices": len(invoices),
            "completed": len(completed),
            "failed": len(failed),
            "total_bytes_transferred": total_bytes,
        },
        "invoices": [
            {
                "id": i["invoice_id"],
                "source": i["source"],
                "destination": i["destination"],
                "status": i["status"],
                "size_bytes": i.get("size_bytes", 0),
            }
            for i in invoices
        ],
    }

    # Write receipt as JSON
    receipt_filename = f"BATCH-RECEIPT-{session_id}-{dag_id}-{datetime.now(timezone.utc):%Y-%m-%d}.json"
    for receipts_dir in [INVOICES_DIR, RECEIPTS_PRIMARY]:
        os.makedirs(receipts_dir, exist_ok=True)
        path = os.path.join(receipts_dir, receipt_filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(receipt, f, indent=2, ensure_ascii=False)

    logger.info(
        f"Batch receipt: {len(completed)} completed, "
        f"{len(failed)} failed, {total_bytes:,} bytes transferred"
    )
    return receipt
