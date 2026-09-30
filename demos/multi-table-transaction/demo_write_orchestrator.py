#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
demo_write_orchestrator.py — Atomic multi-table write via Polaris REST API.

Implements the atomic-publish pattern:
  1. Begin a write transaction  (POST .../transactions)
  2. Create one subtransaction per table to obtain assigned staging paths
  3. Writer produces new Iceberg metadata.json at each assigned_staging_path
  4. Commit each subtransaction  (POST .../subtransactions/{id}/commit)
  5. Commit the transaction      (POST .../transactions/{id}/commit)
  6. Both tables become visible atomically

This script exercises the REST API layer directly (step 3 is simulated).
For the Spark SQL path — where real Spark sessions use
  BEGIN WORKER SUBTRANSACTION / INSERT / COMMIT SUBTRANSACTION
see demo_uc2_spark.sh and demo_interactive.md Part 2.

Prerequisites:
    pip install requests

Environment variables:
    POLARIS_HOST     — Polaris base URL   (default: http://localhost:8181)
    POLARIS_CATALOG  — Catalog name       (default: demo_catalog)
    POLARIS_TOKEN    — Bearer token
    NAMESPACE        — Iceberg namespace  (default: demo_ns)
    TABLE_ORDERS     — Orders table name  (default: orders)
    TABLE_CUSTOMERS  — Customers table    (default: customers)

Usage:
    python3 demo_write_orchestrator.py
"""

import os
import sys
import time
import requests

POLARIS_HOST = os.environ.get("POLARIS_HOST", "http://localhost:8181")
POLARIS_CATALOG = os.environ.get("POLARIS_CATALOG", "demo_catalog")
TOKEN = os.environ.get("POLARIS_TOKEN", "")
NAMESPACE = os.environ.get("NAMESPACE", "demo_ns")
TABLE_ORDERS = os.environ.get("TABLE_ORDERS", "orders")
TABLE_CUSTOMERS = os.environ.get("TABLE_CUSTOMERS", "customers")

BASE = (
    f"{POLARIS_HOST}/api/catalog/polaris/v1/{POLARIS_CATALOG}"
)
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}


def api(method, path, **kwargs):
    url = f"{BASE}/{path}"
    resp = getattr(requests, method)(url, headers=HEADERS, **kwargs)
    if not resp.ok:
        print(
            f"  [ERROR] {method.upper()} {url}"
            f" → {resp.status_code}: {resp.text}"
        )
        resp.raise_for_status()
    return resp.json() if resp.content else {}


def separator(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def demo_atomic_write():
    separator("Step 1: Begin transaction")
    txn = api("post", "transactions", json={
        "isolation": "REPEATABLE_READ",
        "lease_seconds": 3600,
    })
    txn_id = txn["txn_id"]
    txn_token = txn["txn_token"]
    print(f"  txn_id={txn_id}  expires={txn['expires_at']}")

    separator("Step 2: Create subtransactions (one per table)")
    # X-Txn-Token is required on createSubtransaction.
    sub_headers = {**HEADERS, "X-Txn-Token": txn_token}

    r = requests.post(
        f"{BASE}/transactions/{txn_id}/subtransactions",
        headers=sub_headers,
        json={
            "table_identifier": f"{NAMESPACE}.{TABLE_ORDERS}",
            "operation": "APPEND",
        },
    )
    r.raise_for_status()
    s_orders = r.json()

    r = requests.post(
        f"{BASE}/transactions/{txn_id}/subtransactions",
        headers=sub_headers,
        json={
            "table_identifier": f"{NAMESPACE}.{TABLE_CUSTOMERS}",
            "operation": "APPEND",
        },
    )
    r.raise_for_status()
    s_customers = r.json()

    print(f"  orders    subtxn_id={s_orders['subtxn_id']}")
    print(f"    staging_path={s_orders['assigned_staging_path']}")
    print(f"  customers subtxn_id={s_customers['subtxn_id']}")
    print(f"    staging_path={s_customers['assigned_staging_path']}")

    # assigned_staging_path is the complete target .metadata.json file path
    # inside the table's own metadata directory.
    orders_staging = s_orders["assigned_staging_path"]
    customers_staging = s_customers["assigned_staging_path"]

    separator("Step 3: Write data to staging paths (simulated)")
    print(f"  [SIMULATED] Writing orders metadata to {orders_staging}")
    print(
        f"  [SIMULATED] Writing customers metadata"
        f" to {customers_staging}"
    )
    time.sleep(1)

    separator("Step 4: Commit subtransactions")
    # staged_metadata_location must equal the assigned_staging_path exactly.
    api(
        "post",
        f"transactions/{txn_id}"
        f"/subtransactions/{s_orders['subtxn_id']}/commit",
        json={"staged_metadata_location": orders_staging},
        headers={**HEADERS, "X-Subtxn-Token": s_orders["subtxn_token"]},
    )
    print(f"  orders    subtxn committed → {orders_staging}")

    api(
        "post",
        f"transactions/{txn_id}"
        f"/subtransactions/{s_customers['subtxn_id']}/commit",
        json={"staged_metadata_location": customers_staging},
        headers={
            **HEADERS,
            "X-Subtxn-Token": s_customers["subtxn_token"],
        },
    )
    print(f"  customers subtxn committed → {customers_staging}")

    separator("Step 5: Commit transaction (atomic publish)")
    api(
        "post",
        f"transactions/{txn_id}/commit",
        headers={**HEADERS, "X-Txn-Token": txn_token},
    )
    print("  Transaction committed — both tables visible atomically")

    separator("Verification: metadata-location after commit")
    for table_name in [TABLE_ORDERS, TABLE_CUSTOMERS]:
        try:
            resp = api(
                "get",
                f"namespaces/{NAMESPACE}/tables/{table_name}",
            )
            loc = resp.get("metadata-location", "(unknown)")
            print(f"  {table_name}: {loc}")
        except Exception as e:
            print(f"  {table_name}: could not load ({e})")


if __name__ == "__main__":
    if not TOKEN:
        print(
            "WARNING: POLARIS_TOKEN not set"
            " — requests will likely fail with 401."
        )
    try:
        demo_atomic_write()
    except requests.HTTPError as e:
        print(f"\n[FATAL] HTTP error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[FATAL] {e}")
        sys.exit(1)
