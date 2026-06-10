#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# setup_spark_tables.sh — Create and populate demo_catalog tables via Spark SQL.
#
# Run this ONCE before the interactive demo to write real Iceberg metadata
# to disk (file:///tmp/polaris-warehouse/demo_catalog).
#
# Prerequisites:
#   1. Polaris running locally at http://localhost:8181 (start with ./start-polaris.sh).
#   2. spark-env.sh in the same directory.
#
# This script creates demo_catalog if it doesn't already exist.
#
# Usage:
#   cd demos/multi-table-transaction
#   ./setup_spark_tables.sh
# ---------------------------------------------------------------------------
set -euo pipefail

export SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/spark-env.sh"

# ---------------------------------------------------------------------------
# Ensure the catalog exists (idempotent — safe to re-run)
# ---------------------------------------------------------------------------
_mgmt() {
  local method="$1" path="$2"; shift 2
  curl -s -o /dev/null -w "%{http_code}" \
    -X "${method}" \
    -H "Authorization: Bearer ${POLARIS_TOKEN}" \
    -H 'Content-Type: application/json' \
    "${POLARIS_HOST}/api/management/v1${path}" \
    "$@"
}

_expect() {
  local sc="$1" label="$2"
  case "${sc}" in
    2*|409) ;;  # 2xx success or 409 already-exists are both fine
    *) echo "✗ ${label} failed (HTTP ${sc})"; exit 1 ;;
  esac
}

WAREHOUSE_DIR="file:///tmp/polaris-warehouse/${POLARIS_CATALOG}"
echo ""
echo "  Ensuring catalog '${POLARIS_CATALOG}' exists..."

SC=$(_mgmt POST /catalogs -d "{
  \"name\": \"${POLARIS_CATALOG}\",
  \"type\": \"INTERNAL\",
  \"readOnly\": false,
  \"properties\": {\"default-base-location\": \"${WAREHOUSE_DIR}\"},
  \"storageConfigInfo\": {
    \"storageType\": \"FILE\",
    \"allowedLocations\": [\"file:///tmp/polaris-warehouse\"]
  }
}")
_expect "${SC}" "catalog creation"
[ "${SC}" = "409" ] && echo "  ✓ Catalog already exists." || echo "  ✓ Catalog created (HTTP ${SC})."

SC=$(_mgmt PUT "/catalogs/${POLARIS_CATALOG}/catalog-roles/catalog_admin/grants" \
  -d '{"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}')
# Polaris returns 500 on duplicate grant instead of 409 — treat it as idempotent
case "${SC}" in
  2*|409|500) echo "  ✓ CATALOG_MANAGE_CONTENT granted to catalog_admin (or already was)." ;;
  *) echo "✗ catalog grant failed (HTTP ${SC})"; exit 1 ;;
esac

SC=$(_mgmt PUT "/principal-roles/service_admin/catalog-roles/${POLARIS_CATALOG}" \
  -d '{"name": "catalog_admin"}')
# Polaris returns 500 on duplicate assignment instead of 409 — treat it as idempotent
case "${SC}" in
  2*|409|500) echo "  ✓ catalog_admin assigned to service_admin (or already was)." ;;
  *) echo "✗ principal role assignment failed (HTTP ${SC})"; exit 1 ;;
esac

if [ ! -f "${ICEBERG_JAR}" ]; then
  echo "✗ Iceberg runtime jar not found: ${ICEBERG_JAR}"
  echo "  Build it with: cd ~/opensources/iceberg && ./gradlew :iceberg-spark-runtime-4.1:build"
  exit 1
fi

if [ ! -f "${SPARK_HOME}/bin/spark-sql" ]; then
  echo "✗ spark-sql not found at ${SPARK_HOME}/bin/spark-sql"
  exit 1
fi

SPARK_SQL() {
  "${SPARK_HOME}/bin/spark-sql" \
    --jars "${ICEBERG_JAR},${POLARIS_PLUGIN_JAR}" \
    --properties-file "${SCRIPT_DIR}/spark-defaults.conf" \
    --conf 'spark.sql.shuffle.partitions=2' \
    --conf 'spark.ui.enabled=false' \
    --master 'local[2]' \
    -e "$1"
}

# ---------------------------------------------------------------------------
# Phase 1: Drop catalog entries via Spark SQL + remove physical files.
# Two separate spark-sql invocations: drop (phase 1) then create+seed (phase 2).
# spark-sql -e does not exit non-zero on SQL errors, so splitting them prevents
# a silently-failed DROP from letting INSERTs accumulate rows across re-runs.
# ---------------------------------------------------------------------------
echo ""
echo "  Dropping existing tables for clean slate..."
SPARK_SQL "
DROP TABLE IF EXISTS polaris.${NAMESPACE}.${TABLE_ORDERS};
DROP TABLE IF EXISTS polaris.${NAMESPACE}.${TABLE_CUSTOMERS};
"
for tbl in "${TABLE_ORDERS}" "${TABLE_CUSTOMERS}"; do
  WDIR="/tmp/polaris-warehouse/${POLARIS_CATALOG}/${NAMESPACE}/${tbl}"
  rm -rf "${WDIR}"
  echo "  ✓ ${tbl}: catalog entry dropped + physical files removed."
done

echo ""
echo "================================================================="
echo "  Setting up demo tables in Polaris catalog: ${POLARIS_CATALOG}"
echo "================================================================="

# ---------------------------------------------------------------------------
# Phase 2: Create tables and seed data
# ---------------------------------------------------------------------------
SPARK_SQL "
-- Ensure namespace exists
CREATE NAMESPACE IF NOT EXISTS polaris.${NAMESPACE};

-- Create orders table
CREATE TABLE polaris.${NAMESPACE}.${TABLE_ORDERS} (
  order_id    BIGINT,
  customer_id BIGINT,
  amount      DECIMAL(10, 2),
  status      STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- Create customers table
CREATE TABLE polaris.${NAMESPACE}.${TABLE_CUSTOMERS} (
  customer_id BIGINT,
  name        STRING,
  email       STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- Seed orders (snapshot 1)
INSERT INTO polaris.${NAMESPACE}.${TABLE_ORDERS} VALUES
  (1001, 1, 99.99,  'shipped'),
  (1002, 2, 149.50, 'pending'),
  (1003, 1, 25.00,  'delivered'),
  (1004, 3, 500.00, 'processing'),
  (1005, 2, 75.25,  'shipped');

-- Seed customers (snapshot 1)
INSERT INTO polaris.${NAMESPACE}.${TABLE_CUSTOMERS} VALUES
  (1, 'Alice', 'alice@example.com'),
  (2, 'Bob',   'bob@example.com'),
  (3, 'Carol', 'carol@example.com');

-- Show current state
SELECT 'orders count: ' || count(*) FROM polaris.${NAMESPACE}.${TABLE_ORDERS};
SELECT 'customers count: ' || count(*) FROM polaris.${NAMESPACE}.${TABLE_CUSTOMERS};

-- Show snapshot IDs (useful for time-travel demo)
SELECT 'orders current snapshot_id: ' || snapshot_id
  FROM polaris.${NAMESPACE}.${TABLE_ORDERS}.snapshots
  WHERE committed_at = (SELECT max(committed_at) FROM polaris.${NAMESPACE}.${TABLE_ORDERS}.snapshots);

SELECT 'customers current snapshot_id: ' || snapshot_id
  FROM polaris.${NAMESPACE}.${TABLE_CUSTOMERS}.snapshots
  WHERE committed_at = (SELECT max(committed_at) FROM polaris.${NAMESPACE}.${TABLE_CUSTOMERS}.snapshots);
"

echo ""
echo "================================================================="
echo "  Tables created and seeded successfully."
echo "  Run the interactive demo: see demo_interactive.md"
echo "================================================================="
