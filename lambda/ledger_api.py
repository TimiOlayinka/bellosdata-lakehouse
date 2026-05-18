"""
Ledger Cloud API â€” Lambda Function

A lightweight read-only API that serves Merchant Ledger data from S3.
This is the "cloud light" â€” when the local machine sleeps, this Lambda
keeps the books visible.

Endpoints:
  GET /ledger/status    â€” Latest sync block info
  GET /ledger/compute   â€” compute-ledger.json
  GET /ledger/shared    â€” shared.json
  GET /ledger/routes    â€” Trade routes from shared.json
  GET /ledger/blockchain â€” Full blockchain listing

Author: Awujoo (AWUJOO-018)
"""

import json
import boto3
import os

S3_BUCKET = os.environ.get("S3_BUCKET", "bellosdata-bronze-raw")
LEDGER_PREFIX = "ledger-live"
BLOCKCHAIN_PREFIX = "blockchain"

s3 = boto3.client("s3", region_name="eu-west-2")


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }


def _get_s3_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        return {"error": str(e)}


def handler(event, context):
    path = event.get("rawPath", event.get("path", "/"))
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")

    if method == "OPTIONS":
        return _response(200, {"status": "ok"})

    # Route handling
    if path == "/ledger/status":
        data = _get_s3_json(f"{BLOCKCHAIN_PREFIX}/_latest.json")
        if data is None:
            return _response(404, {"error": "No sync data. Genesis block not yet written."})
        manifest = _get_s3_json(f"{LEDGER_PREFIX}/_manifest.json")
        if manifest:
            data["manifest"] = manifest
        return _response(200, data)

    elif path == "/ledger/compute":
        data = _get_s3_json(f"{LEDGER_PREFIX}/compute-ledger.json")
        if data is None:
            return _response(404, {"error": "compute-ledger.json not synced yet"})
        return _response(200, data)

    elif path == "/ledger/shared":
        data = _get_s3_json(f"{LEDGER_PREFIX}/shared.json")
        if data is None:
            return _response(404, {"error": "shared.json not synced yet"})
        return _response(200, data)

    elif path == "/ledger/routes":
        shared = _get_s3_json(f"{LEDGER_PREFIX}/shared.json")
        if shared is None:
            return _response(404, {"error": "shared.json not synced yet"})
        routes = shared.get("trade_routes", [])
        balance = shared.get("balance_snapshot", {})
        return _response(200, {"trade_routes": routes, "balance": balance})

    elif path == "/ledger/blockchain":
        try:
            resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{BLOCKCHAIN_PREFIX}/BLOCK-")
            keys = sorted([o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json")])
            blocks = []
            for key in keys[-20:]:  # Last 20 blocks max
                block = _get_s3_json(key)
                if block:
                    blocks.append(block)
            return _response(200, {"blocks": blocks, "total": len(keys)})
        except Exception as e:
            return _response(500, {"error": str(e)})

    elif path == "/ledger/health":
        return _response(200, {
            "status": "alive",
            "service": "merchant-ledger-cloud-api",
            "version": "1.0.0",
            "message": "The light is on.",
        })

    else:
        return _response(404, {
            "error": "Not found",
            "available": ["/ledger/status", "/ledger/compute", "/ledger/shared",
                          "/ledger/routes", "/ledger/blockchain", "/ledger/health"],
        })
