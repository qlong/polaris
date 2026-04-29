#!/usr/bin/env python3
"""
demo_spark_isolation.py — Transparent multi-table snapshot isolation via Iceberg time-travel.

This demo shows Demo 2 from the Polaris multi-table transaction design:
How an orchestrator can achieve consistent multi-table reads without any engine changes,
using server-side snapshot resolution + Iceberg's existing time-travel API.

Pattern (server-side resolution, no Spark plugin changes needed):
  1. Acquire a TxnSnapshot from Polaris (POST /v1/{catalog}/transaction-snapshots)
  2. For each table, call loadTable?txn_snapshot_id={id} to get the Iceberg metadata
     file at that consistent point in time
  3. Extract the Iceberg snapshot_id from the response
  4. Pass snapshot_id to Spark as option("snapshot-id", ...) for time-travel reads
  5. Both tables are read at the same consistent point — no torn reads

This approach needs zero changes to Iceberg, Spark, or any engine. The orchestrator
(this script / PyIceberg) drives the snapshot pinning.

Compare with the future Layer 2 (Spark plugin transparent isolation) where the catalog
plugin handles all this transparently — see the plan for details on why that requires
upstream Iceberg changes.

Prerequisites:
    pip install requests pyspark pyiceberg  # pyspark and pyiceberg only for the Spark section

Environment variables:
    POLARIS_HOST     — Polaris base URL          (default: http://localhost:8181)
    POLARIS_CATALOG  — Catalog name              (default: quickstart_catalog)
    POLARIS_TOKEN    — Bearer token
    NAMESPACE        — Iceberg namespace         (default: demo_ns)
    TABLE_ORDERS     — Orders table name         (default: orders)
    TABLE_CUSTOMERS  — Customers table name      (default: customers)
    USE_SPARK        — Set to "1" to run Spark reads (requires Spark installed)

Usage:
    python3 demo_spark_isolation.py
    USE_SPARK=1 python3 demo_spark_isolation.py
"""

import os
import sys
import time
import requests

POLARIS_HOST = os.environ.get("POLARIS_HOST", "http://localhost:8181")
POLARIS_CATALOG = os.environ.get("POLARIS_CATALOG", "quickstart_catalog")
TOKEN = os.environ.get("POLARIS_TOKEN", "")
NAMESPACE = os.environ.get("NAMESPACE", "demo_ns")
TABLE_ORDERS = os.environ.get("TABLE_ORDERS", "orders")
TABLE_CUSTOMERS = os.environ.get("TABLE_CUSTOMERS", "customers")
USE_SPARK = os.environ.get("USE_SPARK", "0") == "1"

BASE = f"{POLARIS_HOST}/v1/{POLARIS_CATALOG}"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def api(method, path, params=None, **kwargs):
    url = f"{BASE}/{path}"
    resp = getattr(requests, method)(url, headers=HEADERS, params=params, **kwargs)
    if not resp.ok:
        print(f"  [ERROR] {method.upper()} {url} → {resp.status_code}: {resp.text}")
        resp.raise_for_status()
    return resp.json() if resp.content else {}


def separator(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def resolve_table_at_snapshot(table_name, txn_snapshot_id):
    """
    Calls loadTable with txn_snapshot_id to get the resolved metadata location
    and current Iceberg snapshot_id at the consistent point in time.
    """
    resp = api("get",
               f"namespaces/{NAMESPACE}/tables/{table_name}",
               params={"txn_snapshot_id": txn_snapshot_id})
    metadata = resp.get("metadata", {})
    metadata_location = metadata.get("metadata-location")
    # The current-snapshot-id in the returned metadata is the Iceberg snapshot at this point
    iceberg_snapshot_id = metadata.get("current-snapshot-id")
    return metadata_location, iceberg_snapshot_id


def demo_consistent_reads():
    separator("Step 1: Acquire a consistent read snapshot")
    snap = api("post", "transaction-snapshots", json={"lease_seconds": 300})
    txn_snapshot_id = snap["txn_snapshot_id"]
    print(f"  txn_snapshot_id={txn_snapshot_id}  xmin={snap['xmin']}  xmax={snap['xmax']}")
    print(f"  expires={snap['expires_at']}")

    separator("Step 2: Resolve both tables at the snapshot")
    orders_location, orders_iceberg_snap = resolve_table_at_snapshot(TABLE_ORDERS, txn_snapshot_id)
    customers_location, customers_iceberg_snap = resolve_table_at_snapshot(TABLE_CUSTOMERS, txn_snapshot_id)

    print(f"  {TABLE_ORDERS}:")
    print(f"    metadata_location   = {orders_location}")
    print(f"    iceberg_snapshot_id = {orders_iceberg_snap}")
    print(f"  {TABLE_CUSTOMERS}:")
    print(f"    metadata_location   = {customers_location}")
    print(f"    iceberg_snapshot_id = {customers_iceberg_snap}")

    separator("Step 3: Read both tables at the consistent snapshot")
    if USE_SPARK:
        _read_with_spark(orders_iceberg_snap, customers_iceberg_snap)
    else:
        print("  [Spark reads skipped — set USE_SPARK=1 to enable]")
        print(f"  To read in Spark, use:")
        print(f'    spark.read.format("iceberg").option("snapshot-id", {orders_iceberg_snap})')
        print(f'         .load("polaris.{NAMESPACE}.{TABLE_ORDERS}")')
        print(f'    spark.read.format("iceberg").option("snapshot-id", {customers_iceberg_snap})')
        print(f'         .load("polaris.{NAMESPACE}.{TABLE_CUSTOMERS}")')
        print()
        print("  Both reads are at the SAME consistent point in time (txn_snapshot_id={txn_snapshot_id}).")
        print("  No torn reads possible even if a writer commits between the two read calls. ✓")

    separator("Step 4: Release snapshot (cleanup)")
    api("delete", f"transaction-snapshots/{txn_snapshot_id}")
    print(f"  txn_snapshot_id={txn_snapshot_id} released ✓")


def _read_with_spark(orders_iceberg_snap, customers_iceberg_snap):
    """Reads both tables using Spark time-travel at the resolved snapshot IDs."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        print("  [pyspark not installed — skipping Spark reads]")
        return

    spark = (
        SparkSession.builder.appName("polaris-snapshot-demo")
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", POLARIS_HOST)
        .config("spark.sql.catalog.polaris.token", TOKEN)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"  Reading {TABLE_ORDERS} at snapshot_id={orders_iceberg_snap}…")
    orders_df = (
        spark.read.format("iceberg")
        .option("snapshot-id", orders_iceberg_snap)
        .load(f"polaris.{NAMESPACE}.{TABLE_ORDERS}")
    )
    print(f"    Row count: {orders_df.count()}")

    print(f"  Reading {TABLE_CUSTOMERS} at snapshot_id={customers_iceberg_snap}…")
    customers_df = (
        spark.read.format("iceberg")
        .option("snapshot-id", customers_iceberg_snap)
        .load(f"polaris.{NAMESPACE}.{TABLE_CUSTOMERS}")
    )
    print(f"    Row count: {customers_df.count()}")
    print("  Both tables read at the same consistent point ✓")


if __name__ == "__main__":
    if not TOKEN:
        print("WARNING: POLARIS_TOKEN not set — requests will likely fail with 401.")
    try:
        demo_consistent_reads()
    except requests.HTTPError as e:
        print(f"\n[FATAL] HTTP error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[FATAL] {e}")
        sys.exit(1)
