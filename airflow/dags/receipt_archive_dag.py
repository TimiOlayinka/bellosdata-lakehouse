"""
Receipt Archive DAG â€” The Keeper of Records

Daily sync of all receipts to S3 and blockchain integrity verification.

Schedule: Asset-triggered (receipt://filed) + daily at midnight
Author: Awujoo (AWUJOO-018, refactored AWUJOO-024)
"""

from datetime import datetime, timedelta
import hashlib
import json
import os
import logging

from airflow.decorators import dag, task
from airflow.sdk import Asset
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from aws_session import get_aws_session, is_cloud

# â”€â”€ Assets â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
RECEIPTS_TRIGGER = Asset("s3://bellosdata-bronze-raw/receipts")
RECEIPTS_OUTLET = Asset("s3://bellosdata-bronze-raw/receipts-archived")

# Local receipt dirs (only accessible on local machine)
LOCAL_RECEIPT_DIRS = [
    r"X:\BellosData\receipts",
    r"Y:\Merchant Ledger\receipts",
    r"Y:\brain\SHARED.brain\merchant-ledger\receipts",
]
# Cloud: no local dirs â€” only verify S3 blockchain integrity
RECEIPT_DIRS = [] if is_cloud() else LOCAL_RECEIPT_DIRS
S3_BUCKET = "bellosdata-bronze-raw"
S3_RECEIPTS_PREFIX = "receipts"
S3_BLOCKCHAIN_PREFIX = "blockchain"
ALERTS_DIR = "/tmp/bellosdata/alerts" if is_cloud() else r"X:\BellosData\alerts"

logger = logging.getLogger(__name__)


@dag(
    dag_id="receipt_archive",
    description="Receipt sync to S3 + blockchain integrity check (Asset + daily)",
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
        assets=[RECEIPTS_TRIGGER],
    ),
    start_date=datetime(2026, 5, 15),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "awujoo", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["receipts", "archive", "blockchain", "sovereign", "assets"],
)
def receipt_archive():

    @task()
    def collect_receipts() -> list:
        receipts = []
        for receipt_dir in RECEIPT_DIRS:
            if not os.path.exists(receipt_dir):
                continue
            for fn in os.listdir(receipt_dir):
                if not fn.endswith(".md"):
                    continue
                fp = os.path.join(receipt_dir, fn)
                with open(fp, "r", encoding="utf-8") as f:
                    content = f.read()
                receipts.append({
                    "filename": fn, "source_dir": receipt_dir,
                    "hash": hashlib.sha256(content.encode()).hexdigest(),
                    "size_bytes": len(content.encode()), "content": content,
                })
        logger.info(f"Total receipts collected: {len(receipts)}")
        return receipts

    @task(outlets=[RECEIPTS_OUTLET])
    def sync_receipts_to_s3(receipts: list) -> dict:
        import boto3
        s3 = get_aws_session().client("s3", region_name="eu-west-2")
        uploaded, seen = 0, set()
        for r in receipts:
            if r["filename"] in seen:
                continue
            seen.add(r["filename"])
            key = f"{S3_RECEIPTS_PREFIX}/{r['filename']}"
            try:
                head = s3.head_object(Bucket=S3_BUCKET, Key=key)
                if head.get("Metadata", {}).get("sha256") == r["hash"]:
                    continue
            except Exception:
                pass
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=r["content"].encode(),
                          ContentType="text/markdown",
                          Metadata={"sha256": r["hash"], "synced_at": datetime.utcnow().isoformat()+"Z"})
            uploaded += 1
        return {"uploaded": uploaded, "unique": len(seen), "timestamp": datetime.utcnow().isoformat()+"Z"}

    @task()
    def verify_blockchain() -> dict:
        import boto3
        s3 = get_aws_session().client("s3", region_name="eu-west-2")
        try:
            resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{S3_BLOCKCHAIN_PREFIX}/BLOCK-")
            keys = sorted([o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json")])
        except Exception as e:
            return {"status": "ERROR", "error": str(e)}
        if not keys:
            return {"status": "EMPTY", "block_count": 0}
        prev_hash, valid, broken = "0"*64, 0, None
        for key in keys:
            block = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
            if block.get("previous_hash") != prev_hash:
                broken = block.get("block_number"); break
            h = {k: v for k, v in block.items() if k != "block_hash"}
            comp = hashlib.sha256(json.dumps(h, sort_keys=True).encode()).hexdigest()
            if block.get("block_hash") != comp:
                broken = block.get("block_number"); break
            prev_hash = block["block_hash"]; valid += 1
        if broken is not None:
            os.makedirs(ALERTS_DIR, exist_ok=True)
            with open(os.path.join(ALERTS_DIR, f"BLOCKCHAIN-ALERT-{datetime.utcnow():%Y%m%d}.md"), "w") as f:
                f.write(f"# BLOCKCHAIN INTEGRITY ALERT\\nBroken at block {broken}\\n")
        return {"status": "VALID" if broken is None else "BROKEN", "total": len(keys), "valid": valid}

    receipts = collect_receipts()
    sync_receipts_to_s3(receipts)
    verify_blockchain()

receipt_archive()
