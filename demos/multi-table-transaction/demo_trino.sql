-- demo_trino.sql — Consistent read snapshot demo using Trino session property
--
-- This demo shows Demo 2 from the Polaris multi-table transaction design:
-- how a Trino user can acquire a consistent multi-table read snapshot and
-- query two tables at the same logical point in time.
--
-- The Polaris Trino connector exposes a session-level property,
-- `polaris_read_snapshot`. When set to a txn_snapshot_id, the connector threads
-- that id on every loadTable call for the session, so all tables read at the
-- same logical point in time. Unset (the default) means "read latest".
--
-- Prerequisites:
--   - Trino connected to Polaris via the Iceberg connector
--   - Trino catalog named 'polaris' pointing to the Polaris REST catalog
--   - Tables demo_ns.orders and demo_ns.customers exist
--   - A Trino build that includes the Polaris `polaris_read_snapshot` session
--     property (the connector fork used by this repo's demos)
--
-- Usage:
--   trino --server http://localhost:8080 --file demo_trino.sql
--
-- The manual time-travel section at the bottom is a fallback for a stock Iceberg
-- connector that does NOT have the session property: resolve each table's Iceberg
-- snapshot_id at a pinned txn_snapshot_id, then use FOR VERSION AS OF <snapshot_id>.

-- =============================================================================
-- Step 1: Acquire a TxnSnapshot via the Polaris REST API (outside Trino).
-- =============================================================================
--   curl -X POST "${POLARIS_HOST}/api/catalog/polaris/v1/${CATALOG}/transaction-snapshots" \
--     -H "Authorization: Bearer ${TOKEN}" \
--     -H 'Content-Type: application/json' \
--     -d '{"lease_seconds": 300}'
--   → { "txn_snapshot_id": 1234, "read_point": 550, "expires_at": "..." }
--
-- Hand the txn_snapshot_id (1234 below) to this session. The same id can be
-- handed to a Spark session too (USE SNAPSHOT 1234) — both engines then share
-- one read_point. See demo_superpowers.md.

-- =============================================================================
-- Step 2: Pin this Trino session to the exported snapshot
-- =============================================================================
-- Replace 1234 with the txn_snapshot_id from Step 1.
SET SESSION polaris.polaris_read_snapshot = 1234;

-- Consistent read of orders at the pinned snapshot
SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    o.status
FROM polaris.demo_ns.orders AS o
LIMIT 20;

-- Consistent read of customers at the SAME pinned snapshot
SELECT
    c.customer_id,
    c.name,
    c.email
FROM polaris.demo_ns.customers AS c
LIMIT 20;

-- Consistent join — both tables read at the same logical point in time.
-- No torn reads even if a concurrent writer commits between the two scans,
-- because the connector threads the same txn_snapshot_id on both loadTable calls.
SELECT
    o.order_id,
    c.name          AS customer_name,
    o.amount,
    o.status
FROM polaris.demo_ns.orders    AS o
JOIN polaris.demo_ns.customers AS c
  ON o.customer_id = c.customer_id
WHERE o.status = 'PENDING'
ORDER BY o.amount DESC
LIMIT 10;

-- =============================================================================
-- Step 3: Unpin — return the session to reading the latest committed state
-- =============================================================================
-- RESET (not "= 0"): the property default is unset = latest. Setting it to 0
-- would pin to snapshot id 0, not clear the pin.
RESET SESSION polaris.polaris_read_snapshot;

-- =============================================================================
-- Step 4: Release the snapshot when done (outside Trino)
-- =============================================================================
-- curl -X DELETE "${POLARIS_HOST}/api/catalog/polaris/v1/${CATALOG}/transaction-snapshots/1234" \
--   -H "Authorization: Bearer ${TOKEN}"
-- → 204 No Content ✓

-- =============================================================================
-- FALLBACK: manual time-travel (stock Iceberg connector, no session property)
-- =============================================================================
-- If your Trino build lacks polaris_read_snapshot, resolve each table's Iceberg
-- snapshot_id at the pinned txn_snapshot_id, then time-travel per table:
--
--   curl "${POLARIS_HOST}/api/catalog/v1/${CATALOG}/namespaces/demo_ns/tables/orders?txn_snapshot_id=1234" \
--     -H "Authorization: Bearer ${TOKEN}"
--   → { "metadata": { "current-snapshot-id": 9001, ... } }
--   (repeat for customers → 8500)
--
-- SELECT * FROM polaris.demo_ns.orders    FOR VERSION AS OF 9001;
-- SELECT * FROM polaris.demo_ns.customers FOR VERSION AS OF 8500;
-- -- Replace 9001 / 8500 with the resolved current-snapshot-id values.
