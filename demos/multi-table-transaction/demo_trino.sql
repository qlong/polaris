-- demo_trino.sql — Consistent read snapshot demo using Trino session property
--
-- This demo shows Demo 2 from the Polaris multi-table transaction design:
-- how a Trino user can acquire a consistent multi-table read snapshot and
-- query two tables at the same logical point in time.
--
-- The Polaris Trino connector is extended (future work) to accept a session-level
-- isolation property. When set, the connector acquires a TxnSnapshot once per
-- session and threads txn_snapshot_id on every loadTable call automatically.
--
-- For now, this script demonstrates the manual equivalent using the REST API
-- to acquire a snapshot and then using Iceberg time-travel in Trino via
-- AS OF SNAPSHOT <id>, which maps to Iceberg's snapshot-id time-travel.
--
-- Prerequisites:
--   - Trino connected to Polaris via the Iceberg connector
--   - Trino catalog named 'polaris' pointing to the Polaris REST catalog
--   - Tables demo_ns.orders and demo_ns.customers exist
--
-- Usage:
--   trino --server http://localhost:8080 --file demo_trino.sql
--
-- NOTE: The session property approach below is the future target state.
--       Until the Polaris Trino connector extension lands, use the REST API
--       to get a txn_snapshot_id, resolve each table's Iceberg snapshot_id,
--       and then use FOR VERSION AS OF <snapshot_id> in Trino.

-- =============================================================================
-- FUTURE: transparent isolation via session property (requires connector work)
-- =============================================================================
-- SET SESSION polaris.txn_isolation = 'snapshot';
-- -- Connector automatically calls POST /v1/{catalog}/transaction-snapshots
-- -- and threads txn_snapshot_id on every subsequent loadTable within this session.
--
-- SELECT o.order_id, c.name, o.amount
-- FROM polaris.demo_ns.orders o
-- JOIN polaris.demo_ns.customers c ON o.customer_id = c.customer_id;
-- -- Both tables read at the same consistent snapshot. ✓

-- =============================================================================
-- CURRENT: manual time-travel using resolved Iceberg snapshot IDs
-- =============================================================================

-- Step 1: Acquire a TxnSnapshot via the Polaris REST API (outside Trino).
--   curl -X POST "${POLARIS_HOST}/v1/${CATALOG}/transaction-snapshots" \
--     -H "Authorization: Bearer ${TOKEN}" \
--     -d '{"lease_seconds": 300}'
--   → { "txn_snapshot_id": 1234, "xmin": 500, "xmax": 550, ... }
--
-- Step 2: Resolve each table's Iceberg snapshot at the consistent point:
--   curl "${POLARIS_HOST}/v1/${CATALOG}/namespaces/demo_ns/tables/orders?txn_snapshot_id=1234" \
--     -H "Authorization: Bearer ${TOKEN}"
--   → { "metadata": { "current-snapshot-id": 9001, ... } }
--
--   curl "${POLARIS_HOST}/v1/${CATALOG}/namespaces/demo_ns/tables/customers?txn_snapshot_id=1234" \
--     -H "Authorization: Bearer ${TOKEN}"
--   → { "metadata": { "current-snapshot-id": 8500, ... } }
--
-- Step 3: Use the resolved Iceberg snapshot IDs for time-travel reads in Trino.
--   Replace 9001 and 8500 below with the actual IDs from Step 2.

-- Consistent read of orders at the resolved snapshot
SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    o.status
FROM polaris.demo_ns.orders FOR VERSION AS OF 9001 AS o
LIMIT 20;

-- Consistent read of customers at the resolved snapshot
SELECT
    c.customer_id,
    c.name,
    c.email
FROM polaris.demo_ns.customers FOR VERSION AS OF 8500 AS c
LIMIT 20;

-- Consistent join — both tables at the same logical point in time
-- (no torn reads even if a concurrent writer committed between the two reads)
SELECT
    o.order_id,
    c.name          AS customer_name,
    o.amount,
    o.status
FROM polaris.demo_ns.orders    FOR VERSION AS OF 9001 AS o
JOIN polaris.demo_ns.customers FOR VERSION AS OF 8500 AS c
  ON o.customer_id = c.customer_id
WHERE o.status = 'PENDING'
ORDER BY o.amount DESC
LIMIT 10;

-- =============================================================================
-- Step 4: Release the snapshot when done (outside Trino)
-- =============================================================================
-- curl -X DELETE "${POLARIS_HOST}/v1/${CATALOG}/transaction-snapshots/1234" \
--   -H "Authorization: Bearer ${TOKEN}"
-- → 204 No Content ✓
