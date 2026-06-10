#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# demo_api.sh — Multi-Table Transaction API Demo (Layer 0: curl)
#
# Demonstrates the full multi-table transaction lifecycle directly at the
# REST API level. No engine (Spark/Trino) required.
#
# Prerequisites:
#   1. Polaris running locally with schema-v5 applied.
#   2. Environment variables set (see CONFIGURATION below).
#
# Usage:
#   chmod +x demo_api.sh
#   ./demo_api.sh
# ---------------------------------------------------------------------------
set -euo pipefail

# ---------------------------------------------------------------------------
# CONFIGURATION — override with environment variables
# ---------------------------------------------------------------------------
POLARIS="${POLARIS_HOST:-http://localhost:8181}"
CATALOG="${POLARIS_CATALOG:-demo_catalog}"
TOKEN="${POLARIS_TOKEN:-}"         # Bearer token for OAuth2
NAMESPACE="${NAMESPACE:-demo_ns}"
TABLE_ORDERS="${TABLE_ORDERS:-orders}"
TABLE_CUSTOMERS="${TABLE_CUSTOMERS:-customers}"

# STAGING_BASE is no longer used — the server assigns a specific file path
# within each table's own metadata directory (e.g. s3://warehouse/ns/table/metadata/T-S-uuid.metadata.json).

POLARIS_BASE="${POLARIS}/api/catalog/polaris/v1/${CATALOG}"
ICEBERG_BASE="${POLARIS}/api/catalog/v1/${CATALOG}"

AUTH_HEADER="Authorization: Bearer ${TOKEN}"
CONTENT_TYPE="Content-Type: application/json"

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
info()  { echo -e "\n\033[1;34m▶ $*\033[0m"; }
ok()    { echo -e "\033[1;32m✓ $*\033[0m"; }
fail()  { echo -e "\033[1;31m✗ $*\033[0m" >&2; exit 1; }

polaris_post() {
  local path="$1"; shift
  curl -s -X POST \
    -H "${AUTH_HEADER}" \
    -H "${CONTENT_TYPE}" \
    "${POLARIS_BASE}${path}" \
    "$@"
}

polaris_delete() {
  local path="$1"
  curl -s -X DELETE \
    -H "${AUTH_HEADER}" \
    "${POLARIS_BASE}${path}"
}

polaris_get() {
  local path="$1"
  curl -s -X GET \
    -H "${AUTH_HEADER}" \
    "${POLARIS_BASE}${path}"
}

iceberg_get() {
  local path="$1"
  curl -s -X GET \
    -H "${AUTH_HEADER}" \
    "${ICEBERG_BASE}${path}"
}

# Minimal jq wrapper (falls back to python if jq not installed)
json_get() {
  local json="$1"; local key="$2"
  if command -v jq &>/dev/null; then
    echo "${json}" | jq -r ".[\"${key}\"]"
  else
    echo "${json}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('${key}',''))"
  fi
}

# ---------------------------------------------------------------------------
# DEMO STARTS HERE
# ---------------------------------------------------------------------------
echo ""
echo "================================================================="
echo "  Polaris Multi-Table Transaction Demo (Layer 0: REST API)"
echo "================================================================="

# ---------------------------------------------------------------------------
# PART 1: Consistent Read — Acquire a TxnSnapshot and read two tables at it
# ---------------------------------------------------------------------------
info "PART 1: Acquire a TxnSnapshot for consistent multi-table read"

SNAP_RESP=$(polaris_post "/transaction-snapshots" \
  -d '{"lease_seconds": 300}')
echo "Response: ${SNAP_RESP}"

SNAP_ID=$(json_get "${SNAP_RESP}" "txn_snapshot_id")
XMIN=$(json_get "${SNAP_RESP}" "xmin")
XMAX=$(json_get "${SNAP_RESP}" "xmax")
ok "Acquired TxnSnapshot: id=${SNAP_ID}  xmin=${XMIN}  xmax=${XMAX}"

info "Load orders table at txn_snapshot_id=${SNAP_ID}"
ORDERS_RESP=$(iceberg_get "/namespaces/${NAMESPACE}/tables/${TABLE_ORDERS}?txn_snapshot_id=${SNAP_ID}")
ORDERS_META=$(json_get "${ORDERS_RESP}" "metadata-location")
ok "orders metadata-location = ${ORDERS_META}"

info "Load customers table at the SAME txn_snapshot_id=${SNAP_ID}"
CUSTOMERS_RESP=$(iceberg_get "/namespaces/${NAMESPACE}/tables/${TABLE_CUSTOMERS}?txn_snapshot_id=${SNAP_ID}")
CUSTOMERS_META=$(json_get "${CUSTOMERS_RESP}" "metadata-location")
ok "customers metadata-location = ${CUSTOMERS_META}"

echo ""
echo "  Both tables resolved at the SAME logical timestamp (TxnSnapshot ${SNAP_ID})."
echo "  Reads are guaranteed consistent — no torn read possible."

info "Release the TxnSnapshot (explicit cleanup)"
polaris_delete "/transaction-snapshots/${SNAP_ID}"
ok "TxnSnapshot ${SNAP_ID} released"

# ---------------------------------------------------------------------------
# PART 2: Multi-Table Atomic Write
# ---------------------------------------------------------------------------
info "PART 2: Begin a multi-table write transaction"

TXN_RESP=$(polaris_post "/transactions" \
  -d '{"isolation": "REPEATABLE_READ", "lease_seconds": 3600}')
echo "Response: ${TXN_RESP}"

TXN_ID=$(json_get "${TXN_RESP}" "txn_id")
TXN_TOKEN=$(json_get "${TXN_RESP}" "txn_token")
TXN_SNAP_ID=$(json_get "${TXN_RESP}" "txn_snapshot_id")
ok "Transaction started: txn_id=${TXN_ID}  linked_txn_snapshot_id=${TXN_SNAP_ID}"

echo ""
echo "  The server pinned a TxnSnapshot at BEGIN time (txn_snapshot_id=${TXN_SNAP_ID})."
echo "  All reads within this transaction see a consistent view as of that moment."

info "Read both tables at the transaction's pinned snapshot (REPEATABLE READ)"
ORDERS_IN_TXN=$(iceberg_get "/namespaces/${NAMESPACE}/tables/${TABLE_ORDERS}?txn_id=${TXN_ID}")
CUSTOMERS_IN_TXN=$(iceberg_get "/namespaces/${NAMESPACE}/tables/${TABLE_CUSTOMERS}?txn_id=${TXN_ID}")
ok "orders   @ txn_id=${TXN_ID}: $(json_get "${ORDERS_IN_TXN}" "metadata-location")"
ok "customers@ txn_id=${TXN_ID}: $(json_get "${CUSTOMERS_IN_TXN}" "metadata-location")"

echo ""
echo "  Both reads use the same pinned snapshot — consistent across the transaction."
echo "  Once subtransactions are staged, ?txn_id reads will return the staged metadata (RYOW)."

info "Create subtransaction for orders (APPEND)"
SUBTXN1_RESP=$(polaris_post "/transactions/${TXN_ID}/subtransactions" \
  -H "X-Txn-Token: ${TXN_TOKEN}" \
  -d "{\"table_identifier\": \"${NAMESPACE}.${TABLE_ORDERS}\", \"operation\": \"APPEND\"}")
echo "Response: ${SUBTXN1_RESP}"

SUBTXN1_ID=$(json_get "${SUBTXN1_RESP}" "subtxn_id")
SUBTXN1_TOKEN=$(json_get "${SUBTXN1_RESP}" "subtxn_token")
STAGED_PATH1=$(json_get "${SUBTXN1_RESP}" "assigned_staging_path")
ok "Subtransaction 1 (orders): subtxn_id=${SUBTXN1_ID}  staged_path=${STAGED_PATH1}"

info "Create subtransaction for customers (APPEND)"
SUBTXN2_RESP=$(polaris_post "/transactions/${TXN_ID}/subtransactions" \
  -H "X-Txn-Token: ${TXN_TOKEN}" \
  -d "{\"table_identifier\": \"${NAMESPACE}.${TABLE_CUSTOMERS}\", \"operation\": \"APPEND\"}")
echo "Response: ${SUBTXN2_RESP}"

SUBTXN2_ID=$(json_get "${SUBTXN2_RESP}" "subtxn_id")
SUBTXN2_TOKEN=$(json_get "${SUBTXN2_RESP}" "subtxn_token")
STAGED_PATH2=$(json_get "${SUBTXN2_RESP}" "assigned_staging_path")
ok "Subtransaction 2 (customers): subtxn_id=${SUBTXN2_ID}  staged_path=${STAGED_PATH2}"

echo ""
echo "  In a real workload, Spark/PyIceberg apps would now write data files and"
echo "  the new Iceberg metadata JSON to the server-assigned path:"
echo "    orders:    ${STAGED_PATH1}"
echo "    customers: ${STAGED_PATH2}"
echo "  (Each path is a specific .metadata.json file inside the table's own"
echo "   metadata directory — invisible until commitTransaction swaps the pointer.)"
echo "  For this curl demo, we simulate the commit step directly."

info "Commit subtransaction 1 (orders) — pass the server-assigned path as staged_metadata_location"
polaris_post "/transactions/${TXN_ID}/subtransactions/${SUBTXN1_ID}/commit" \
  -H "X-Subtxn-Token: ${SUBTXN1_TOKEN}" \
  -d "{\"staged_metadata_location\": \"${STAGED_PATH1}\"}"
ok "Subtransaction 1 staged: ${STAGED_PATH1}"

info "Commit subtransaction 2 (customers)"
polaris_post "/transactions/${TXN_ID}/subtransactions/${SUBTXN2_ID}/commit" \
  -H "X-Subtxn-Token: ${SUBTXN2_TOKEN}" \
  -d "{\"staged_metadata_location\": \"${STAGED_PATH2}\"}"
ok "Subtransaction 2 staged: ${STAGED_PATH2}"

info "Commit the parent transaction (atomic publish)"
COMMIT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST \
  -H "${AUTH_HEADER}" \
  -H "${CONTENT_TYPE}" \
  -H "X-Txn-Token: ${TXN_TOKEN}" \
  "${POLARIS_BASE}/transactions/${TXN_ID}/commit")

if [ "${COMMIT_STATUS}" = "204" ]; then
  ok "Transaction ${TXN_ID} committed (HTTP 204)"
  echo ""
  echo "  Both tables (orders + customers) are now visible atomically."
  echo "  No reader ever saw one table updated without the other."
else
  fail "Commit failed with HTTP ${COMMIT_STATUS}"
fi

# ---------------------------------------------------------------------------
# PART 3: Transaction Status Check
# ---------------------------------------------------------------------------
info "PART 3: Verify transaction status"
STATUS_RESP=$(polaris_get "/transactions/${TXN_ID}")
STATUS=$(json_get "${STATUS_RESP}" "status")
ok "Transaction ${TXN_ID} status = ${STATUS}"

# ---------------------------------------------------------------------------
# DONE
# ---------------------------------------------------------------------------
echo ""
echo "================================================================="
echo "  Demo complete."
echo ""
echo "  What was demonstrated:"
echo "   1. TxnSnapshot: consistent multi-table reads at a pinned point"
echo "   2. Multi-table write transaction: BEGIN → subtransactions → COMMIT"
echo "   3. Atomic visibility: orders + customers published together"
echo "================================================================="
