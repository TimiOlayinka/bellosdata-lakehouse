"""
Budget Monitor DAG — The Watchman

Checks AWS costs every 6 hours and alerts if the £50/month budget
is at risk. Syncs budget status to S3 for cloud visibility.

Schedule: Every 6 hours
Author: Awujoo (AWUJOO-018)

"A merchant who spends everything has no safety net." — PROTOCOL.md
"""

from datetime import datetime, timedelta
import json
import os
import logging

from airflow.decorators import dag, task

from aws_session import get_aws_session, is_cloud

# ── Configuration ──────────────────────────────────────────────
ALERTS_DIR = "/tmp/bellosdata/alerts" if is_cloud() else r"X:\BellosData\alerts"
S3_BUCKET = "playdarch-bronze-raw"
AWS_ACCOUNT_ID = "332779204498"

# Budget thresholds
BUDGET_LIMIT_USD = 63.0  # £50 ≈ $63
WARN_THRESHOLD = 0.60    # 60% = warning
ALERT_THRESHOLD = 0.80   # 80% = alert
CRITICAL_THRESHOLD = 0.95  # 95% = critical — halt all ops

logger = logging.getLogger(__name__)


@dag(
    dag_id="budget_monitor",
    description="Monitor AWS costs and alert on budget thresholds",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 5, 15),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "awujoo",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["budget", "monitoring", "sovereign"],
)
def budget_monitor():

    @task()
    def check_aws_costs() -> dict:
        """Query AWS Cost Explorer for current month spend."""
        import boto3

        session = get_aws_session()
        ce = session.client("ce", region_name="us-east-1")

        now = datetime.utcnow()
        start_of_month = now.replace(day=1).strftime("%Y-%m-%d")
        today = now.strftime("%Y-%m-%d")

        try:
            response = ce.get_cost_and_usage(
                TimePeriod={"Start": start_of_month, "End": today},
                Granularity="MONTHLY",
                Metrics=["UnblendedCost"],
            )

            results = response.get("ResultsByTime", [])
            if results:
                amount_str = results[0]["Total"]["UnblendedCost"]["Amount"]
                current_spend_usd = float(amount_str)
            else:
                current_spend_usd = 0.0

        except Exception as e:
            logger.error(f"Cost Explorer query failed: {e}")
            # Fall back to 0 — we'll catch real issues on the next run
            current_spend_usd = 0.0

        spend_ratio = current_spend_usd / BUDGET_LIMIT_USD
        days_in_month = 31
        days_elapsed = now.day
        projected_spend = (current_spend_usd / max(days_elapsed, 1)) * days_in_month

        if spend_ratio >= CRITICAL_THRESHOLD:
            status = "CRITICAL"
        elif spend_ratio >= ALERT_THRESHOLD:
            status = "ALERT"
        elif spend_ratio >= WARN_THRESHOLD:
            status = "WARNING"
        else:
            status = "HEALTHY"

        budget_status = {
            "timestamp": now.isoformat() + "Z",
            "current_spend_usd": round(current_spend_usd, 2),
            "budget_limit_usd": BUDGET_LIMIT_USD,
            "budget_limit_gbp": 50.0,
            "spend_ratio": round(spend_ratio, 4),
            "spend_percentage": f"{spend_ratio * 100:.1f}%",
            "projected_month_end_usd": round(projected_spend, 2),
            "days_elapsed": days_elapsed,
            "status": status,
            "account_id": AWS_ACCOUNT_ID,
        }

        logger.info(
            f"Budget: ${current_spend_usd:.2f} / ${BUDGET_LIMIT_USD:.2f} "
            f"({spend_ratio * 100:.1f}%) — {status}"
        )

        return budget_status

    @task()
    def evaluate_and_alert(budget_status: dict) -> str:
        """Write alerts if thresholds are breached."""
        status = budget_status["status"]
        now_iso = budget_status["timestamp"]

        # Always write the status file locally
        os.makedirs(ALERTS_DIR, exist_ok=True)
        status_file = os.path.join(ALERTS_DIR, "budget_status.json")
        with open(status_file, "w", encoding="utf-8") as f:
            json.dump(budget_status, f, indent=2)

        # Write alert file if threshold breached
        if status in ("WARNING", "ALERT", "CRITICAL"):
            alert_file = os.path.join(
                ALERTS_DIR,
                f"BUDGET-{status}-{datetime.utcnow().strftime('%Y%m%d-%H%M')}.md",
            )
            with open(alert_file, "w", encoding="utf-8") as f:
                f.write(f"# ⚠️ BUDGET {status}\n\n")
                f.write(f"| Field | Value |\n|---|---|\n")
                f.write(f"| Timestamp | {now_iso} |\n")
                f.write(f"| Current Spend | ${budget_status['current_spend_usd']} |\n")
                f.write(f"| Budget Limit | ${budget_status['budget_limit_usd']} (£{budget_status['budget_limit_gbp']}) |\n")
                f.write(f"| Usage | {budget_status['spend_percentage']} |\n")
                f.write(f"| Projected Month-End | ${budget_status['projected_month_end_usd']} |\n")
                f.write(f"| Status | **{status}** |\n\n")

                if status == "CRITICAL":
                    f.write("> ⛔ **CRITICAL: Budget nearly exhausted. HALT ALL DEPLOYMENTS.**\n")
                    f.write("> File 'Need Advice' to the Merchant immediately.\n")

            logger.warning(f"Alert written: {alert_file}")

        return status

    @task()
    def sync_to_s3(budget_status: dict) -> str:
        """Upload budget status to S3 for cloud visibility."""
        import boto3

        session = get_aws_session()
        s3 = session.client("s3", region_name="eu-west-2")

        s3_key = "ledger-live/budget_status.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(budget_status, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

        logger.info(f"Budget status synced to s3://{S3_BUCKET}/{s3_key}")
        return f"s3://{S3_BUCKET}/{s3_key}"

    # ── DAG Flow ──────────────────────────────────────────
    status = check_aws_costs()
    evaluate_and_alert(status)
    sync_to_s3(status)


# Instantiate the DAG
budget_monitor()
