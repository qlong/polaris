#!/usr/bin/env python3
"""
demo_write_orchestrator.py — Atomic multi-table write via PyIceberg orchestrator.

This demo implements Demo 3 from the Polaris multi-table transaction design:
Atomic Publish — two tables are updated atomically so readers see either both
old values or both new values, never a partial state.

Pattern:
  1. Orchestrator begins a write transaction (POST /v1/{catalog}/transactions)
  2. Orchestrator creates a subtransaction per table
  3. Spark (or any writer) writes data to the server-assigned staging paths
  4. Orchestrator commits each subtransaction (POST .../subtransactions/{id}/commit)
  5. Orchestrator commits the transaction (POST .../transactions/{id}/commit)
  6. Both tables become visible atomically

This demo simulates Spark writes using local file-based staging (no real Spark
required). Substitute spark.range(1000).write.parquet(staging_path) for real use.

Prerequisites:
    pip install requests

Environment variables:
    POLARIS_HOST     — Polaris base URL          (default: http://localhost:8181)
    POLARIS_CATALOG  — Catalog name              (default: quickstart_catalog)
    POLARIS_TOKEN    — Bearer token
    NAMESPACE        — Iceberg namespace         (default: demo_ns)
    TABLE_ORDERS     — Orders table name         (default: orders)
    TABLE_CUSTOMERS  — Customers table name      (default: customers)

Usage:
    python3 demo_write_orchestrator.py
"""

import os
import sys
import json
import time
import requests

POLARIS_HOST = os.environ.get("POLARIS_HOST", "http://localhost:8181")
POLARIS_CATALOG = os.environ.get("POLARIS_CATALOG", "quickstart_catalog")
TOKEN = os.environ.get("POLARIS_TOKEN", "")
NAMESPACE = os.environ.get("NAMESPACE", "demo_ns")
TABLE_ORDERS = os.environ.get("TABLE_ORDERS", "orders")
TABLE_CUSTOMERS = os.environ.get("TABLE_CUSTOMERS", "customers")

BASE = f"{POLARIS_HOST}/v1/{POLARIS_CATALOG}"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def api(method, path, **kwargs):
    url = f"{BASE}/{path}"
    resp = getattr(requests, method)(url, headers=HEADERS, **kwargs)
    if not resp.ok:
        print(f"  [ERROR] {method.upper()} {url} → {resp.status_code}: {resp.text}")
        resp.raise_for_status()
    return resp.json() if resp.content else {}


def separator(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def demo_atomic_write():
    separator("Step 1: Begin transaction")
    txn = api("post", "transactions", json={
        "isolation": "REPEATABLE_READ",
        "lease_seconds": 3600
    })
    txn_id = txn["txn_id"]
    txn_token = txn["txn_token"]
    print(f"  txn_id={txn_id}  expires={txn['expires_at']}")

    separator("Step 2: Create subtransactions (one per table)")
    sub_headers = {**HEADERS, "X-Txn-Token": txn_token}

    s_orders = api("post", f"transactions/{txn_id}/subtransactions", json={
        "table_identifier": f"{NAMESPACE}.{TABLE_ORDERS}",
        "operation": "APPEND"
    })
    s_customers = api("post", f"transactions/{txn_id}/subtransactions", json={
        "table_identifier": f"{NAMESPACE}.{TABLE_CUSTOMERS}",
        "operation": "APPEND"
    })
    print(f"  orders  subtxn_id={s_orders['subtxn_id']}")
    print(f"    staging_path={s_orders['assigned_staging_path']}")
    print(f"  customers subtxn_id={s_customers['subtxn_id']}")
    print(f"    staging_path={s_customers['assigned_staging_path']}")

    separator("Step 3: Write data to staging paths (simulated)")
    # In production: spark.range(N).write.parquet(s_orders["assigned_staging_path"])
    orders_staging = s_orders["assigned_staging_path"]
    customers_staging = s_customers["assigned_staging_path"]
    print(f"  [SIMULATED] Writing orders data to {orders_staging}")
    print(f"  [SIMULATED] Writing customers data to {customers_staging}")
    # Simulate a 1-second write
    time.sleep(1)

    # Construct the staged metadata locations (would be written by the engine)
    orders_metadata = f"{orders_staging}metadata.json"
    customers_metadata = f"{customers_staging}metadata.json"

    separator("Step 4: Commit subtransactions")
    api("post",
        f"transactions/{txn_id}/subtransactions/{s_orders['subtxn_id']}/commit",
        json={"staged_metadata_location": orders_metadata},
        headers={**HEADERS, "X-Subtxn-Token": s_orders["subtxn_token"]})
    print(f"  orders   subtxn committed → {orders_metadata}")

    api("post",
        f"transactions/{txn_id}/subtransactions/{s_customers['subtxn_id']}/commit",
        json={"staged_metadata_location": customers_metadata},
        headers={**HEADERS, "X-Subtxn-Token": s_customers["subtxn_token"]})
    print(f"  customers subtxn committed → {customers_metadata}")

    separator("Step 5: Commit transaction (atomic publish)")
    api("post", f"transactions/{txn_id}/commit",
        headers={**HEADERS, "X-Txn-Token": txn_token})
    print("  Transaction committed — both tables visible atomically ✓")

    separator("Verification: Load both tables and check they match")
    print("  Resolving table metadata locations after commit…")
    for table_name in [TABLE_ORDERS, TABLE_CUSTOMERS]:
        try:
            resp = api("get", f"namespaces/{NAMESPACE}/tables/{table_name}")
            loc = resp.get("metadata", {}).get("metadata-location", "(unknown)")
            print(f"  {table_name}: {loc}")
        except Exception as e:
            print(f"  {table_name}: could not load ({e})")


if __name__ == "__main__":
    if not TOKEN:
        print("WARNING: POLARIS_TOKEN not set — requests will likely fail with 401.")
    try:
        demo_atomic_write()
    except requests.HTTPError as e:
        print(f"\n[FATAL] HTTP error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[FATAL] {e}")
        sys.exit(1)
