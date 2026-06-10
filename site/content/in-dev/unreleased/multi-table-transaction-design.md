# Multi-Table Transaction Support for Apache Iceberg

## Table of Contents

1. [Motivation](#motivation)
2. [Key Concepts](#key-concepts)
3. [Data Representation](#data-representation)
4. [Isolation Levels](#isolation-levels)
5. [Lock Hierarchy](#lock-hierarchy)
6. [Concurrent Operation Support](#concurrent-operation-support)
7. [Transaction Lifecycle](#transaction-lifecycle)
8. [Distributed Transaction Sharing](#distributed-transaction-sharing)
9. [Commit and Abort Protocol](#commit-and-abort-protocol)
10. [Behavioral Examples](#behavioral-examples)
11. [Failure Handling and Dead Transaction Detection](#failure-handling-and-dead-transaction-detection)
12. [Transaction Table Maintenance](#transaction-table-maintenance)
13. [REST API](#rest-api)
14. [Design Decisions and Trade-offs](#design-decisions-and-trade-offs)
15. [Security Considerations](#security-considerations)
    - [Why Pessimistic Locking for Multi-Table Writes?](#why-pessimistic-locking-for-multi-table-writes)
    - [Why 64-bit Transaction IDs?](#why-64-bit-transaction-ids)
    - [Why Not Distributed Catalog?](#why-not-distributed-catalog)
    - [The Server Process Stays Stateless Even Though Transactions Are Stateful](#the-server-process-stays-stateless-even-though-transactions-are-stateful)
    - [Why Transaction Coordination Cannot Be Pushed to the Engine or Client Side](#why-transaction-coordination-cannot-be-pushed-to-the-engine-or-client-side)

> **Implementation status (2026-06-09):** Phase 1 and Phase 2 are implemented. All known open items resolved: (1) `assigned_staging_path` now points to a specific file inside the table's own metadata directory (`{table.location}/metadata/{txn_id}-{subtxn_id}-{uuid}.metadata.json`), fixing the credential-vending location check failure on `loadTable?txn_id=T` (RYOW). (2) `IcebergCatalogHandler` RYOW lookup now uses `tableIdentifier.toString()` to correctly match `subtransactions.table_uuid`.

> **Key insight — two kinds of "snapshot"**: The word "snapshot" has two distinct meanings in this system. A **`TxnSnapshot`** `(xmin, xmax, xip_list)` is the MVCC visibility state that determines which transactions a reader can see — a PostgreSQL concept, analogous to `pg_snapshot`. An **Iceberg table snapshot** (`table_snapshot_id`) is the native Iceberg point-in-time state of a table's data files. The catalog server maintains a `committed_table_snapshots` mapping table that translates a `TxnSnapshot` into the specific Iceberg table snapshot to serve for each table. Pure readers acquire a `txn_snapshot_id` — lightweight, lock-free, no CLOG entry — and pass it on every table read; the server resolves the correct Iceberg table snapshot automatically. A query that reads multiple tables **must** use a shared `TxnSnapshot`; without one, successive reads can observe different committed states and produce a torn cross-table view.

---

## Motivation

Iceberg's current `Transaction` API is single-table only. A transaction can atomically commit a schema change alongside a data append to one table, but there is no mechanism to atomically commit changes across two or more tables, nor any mechanism for a reader to get a consistent view across multiple tables at a single point in time.

This is a real limitation for two distinct classes of workloads:

### Write-Side: Cross-Table Atomicity

- **Star schema ETL**: loading `fact_orders` and `dim_customer` must be atomic — a reader scanning between the two commits sees broken foreign key relationships.
- **Event + state consistency**: a streaming pipeline that writes to an `events` table and a `current_state` table must never have them diverge.
- **Compliance / audit**: a `patient_records` write and its corresponding `audit_log` write must be inseparable by regulation.
- **Table swap patterns**: atomically replacing `products_staging` into `products` while archiving the previous version.

### Read-Side: Consistent Multi-Table Snapshot

Even when writes are individually correct, a reader scanning multiple tables without a shared snapshot can see a torn view:

```
T1 commits to orders   at t=100ms  (txn_id=5010)
T2 commits to customers at t=200ms (txn_id=5011)

Reader without shared snapshot:
  GET /tables/orders   at t=150ms → sees T1's commit    ✓
  GET /tables/customers at t=150ms → does NOT see T2 yet ✓
  ... time passes ...
  GET /tables/orders   at t=250ms → sees T1's commit    ✓
  GET /tables/customers at t=250ms → sees T2's commit    ✓

The reader sees orders at state N+1 but customers at state M — internally
inconsistent for any query that joins across the two tables.
```

A reader must acquire a **shared snapshot** before reading multiple tables. All tables read under the same snapshot are guaranteed to reflect the same logical point in time, regardless of when the individual GET requests are issued.

The existing workaround — accept eventual consistency between tables, or use Nessie branches — is either semantically incorrect for the use case or operationally complex.

This document proposes a full multi-table transaction model for Iceberg's REST catalog, modeled closely after PostgreSQL's proven transaction architecture but adapted to Iceberg's OLAP workload characteristics and the catalog-server-as-coordinator topology.

---

## Key Concepts

### Transaction ID (TxnID)

Every transaction — single-table or multi-table — is assigned a **64-bit monotonically increasing integer** by the catalog server at `BEGIN` time. This ID is the global logical clock for the system.

```
txn_id: 1, 2, 3, ..., 9,223,372,036,854,775,807
```

Properties:
- **Monotone**: higher ID means "happened later". Total ordering of all transactions.
- **64-bit**: no wraparound for ~292,000 years at 1 billion transactions/second. Eliminates the XID wraparound problem that has plagued PostgreSQL for decades.
- **Single sequence**: top-level transactions, subtransactions, and snapshots all share the same sequence, so any two IDs have an unambiguous ordering.

### Commit Log (CLOG)

Borrowed directly from PostgreSQL's `pg_xact`. The CLOG is the authoritative record of every transaction's status. Unlike PostgreSQL's memory-mapped 2-bit array (optimized for per-row visibility checks at microsecond frequency), Iceberg's CLOG is a simple catalog DB table — justified by the vastly lower access frequency (one lookup per table per query, not one per row).

```
Status values:
  0 = IN_PROGRESS    transaction is active
  1 = COMMITTED      transaction committed successfully
  2 = ABORTED        transaction was rolled back or expired
```

The CLOG is the single source of truth for visibility. A reader never needs to inspect table metadata directly — it asks the CLOG whether the transaction that wrote a given snapshot has committed.

### Subtransaction

Each table involved in a multi-table transaction is managed by a **subtransaction** — a child unit of work scoped to exactly one table. The subtransaction lifecycle is:

```
IN_PROGRESS   → the client is writing data files to object storage
SUB_COMMITTED → client has staged metadata; waiting for parent to commit
ABORTED       → subtransaction failed or parent was aborted
```

Subtransactions inherit their parent's txn_id for ordering purposes, but have their own subtxn_id for fine-grained tracking. A top-level `COMMIT` is only executed when ALL subtransactions have reached `SUB_COMMITTED`. If any subtransaction fails, the parent is aborted and all subtransactions are rolled back.

### Terminology: Two Kinds of "Snapshot"

The word "snapshot" has two distinct meanings in this system and must not be confused:

| Term | PostgreSQL Equivalent | What It Is | Lives In |
|---|---|---|---|
| **`TxnSnapshot`** | `pg_snapshot` / `SnapshotData` | The MVCC visibility state: `(xmin, xmax, xip_list)`. Determines which *transactions* are visible to a reader. | Catalog server (`txn_snapshots` table or embedded in `transactions` row) |
| **Table Snapshot** | N/A — Iceberg-native | Iceberg's point-in-time state of a table's data files, identified by `table_snapshot_id`. Created by every Iceberg write commit. | Iceberg metadata JSON on object storage |

These two are related but separate: a `TxnSnapshot` identifies a logical point in *transaction* history; a table snapshot is the concrete Iceberg metadata state that corresponds to that point for a *specific table*. The `committed_table_snapshots` mapping table bridges between them.

### TxnSnapshot (MVCC Visibility State)

A `TxnSnapshot` is taken at the moment a reader or snapshot-isolation (REPEATABLE READ) writer begins its operation. It captures the transaction landscape at that instant:

- `xmin`: the oldest in-flight `txn_id` at snapshot time
- `xmax`: the next `txn_id` to be assigned at snapshot time (exclusive upper bound)
- `xip_list`: all `txn_id`s that were in-flight at snapshot time

**Visibility rule**: a committed transaction `T` is visible to `TxnSnapshot` `RS` if and only if:

```
T.txn_id < RS.xmax
AND T.txn_id NOT IN RS.xip_list
```

This is the MVCC rule. It tells you *which transactions* a reader can see. It does not by itself tell you which Iceberg table snapshot to return — that requires the mapping described below.

### Table Snapshot (Iceberg Native)

Every time a writer commits to an Iceberg table, Iceberg creates a new **table snapshot**: an immutable record in the table's metadata JSON that captures the exact set of data files at that point. Table snapshots are identified by a `table_snapshot_id` (a long integer already part of Iceberg's existing format).

Table snapshots are what Spark, Trino, and other engines actually read from — they load the table metadata, find the current `table_snapshot_id`, and scan the corresponding data files.

### The Mapping: TxnSnapshot → Table Snapshot

The catalog server must maintain a record of which `txn_id` committed each table snapshot. This is the bridge between the MVCC world and the Iceberg world:

```
TxnSnapshot RS (xmax=5050, xip_list=[5001, 5003])
         │
         │  "which txn committed the most recent table snapshot
         │   visible to RS for table orders?"
         ▼
committed_table_snapshots
  table=orders, txn_id=5040, table_snapshot_id=42   ← most recent visible (5040 < 5050, not in xip_list)
  table=orders, txn_id=5048, table_snapshot_id=43   ← also visible
  table=orders, txn_id=5055, table_snapshot_id=44   ← NOT visible (5055 >= 5050)
         │
         │  answer: table_snapshot_id=43
         ▼
Iceberg metadata JSON for orders at table_snapshot_id=43
→ scan data files
```

The mapping query:

```sql
-- Given `TxnSnapshot` RS, find the Iceberg table snapshot to serve for a given table
SELECT table_snapshot_id, metadata_location
FROM committed_table_snapshots
WHERE table_uuid = :table_uuid
  AND committed_txn_id < :xmax           -- committed before TxnSnapshot was taken
  AND committed_txn_id != ALL(:xip_list) -- was not in-flight at TxnSnapshot time
ORDER BY committed_txn_id DESC
LIMIT 1;
```

Without this mapping table, the catalog cannot answer "which table snapshot was current at the time this `TxnSnapshot` was taken?" — it would have no way to translate the MVCC visibility state into a concrete Iceberg metadata pointer.

### Do Readers Get a TxnID?

No. A pure reader does **not** get a `txn_id`. It gets a `txn_snapshot_id` — a lightweight handle with no CLOG entry, no lock capability, and no write semantics. This is analogous to PostgreSQL's **virtual transaction IDs**: a real XID is only assigned when a transaction first writes. Read-only transactions consume no XID.

There are three cases:

| Case | Gets | Watermark contribution |
|---|---|---|
| Pure reader (SELECT) | `txn_snapshot_id` only | `MIN(txn_snapshot.xmin)` |
| Pure writer (optimistic append) | `txn_id` only | `MIN(txn_id) WHERE status=IN_PROGRESS` |
| Read-write transaction (REPEATABLE READ, multi-table) | `txn_id` + embedded `TxnSnapshot` | `MIN(txn_id) WHERE status=IN_PROGRESS` |

A read-write transaction stores its `TxnSnapshot` as `(xmin, xmax, xip_list)` columns directly in the `transactions` row. All reads within that transaction use this embedded `TxnSnapshot`, giving snapshot isolation (REPEATABLE READ) semantics and enabling read-your-own-writes.

### What Table Snapshot Does a Reader Get?

When a client reads a table, the server resolves the appropriate Iceberg table snapshot as follows:

| Request | TxnSnapshot Used | Table Snapshot Returned |
|---|---|---|
| `GET /tables/A` (no txn_snapshot_id) | Fresh: `xmax = current max committed txn_id` | Latest committed table snapshot |
| `GET /tables/A?txn_snapshot_id=RS` | RS from `txn_snapshots` table | Most recent table snapshot with `committed_txn_id` visible to RS |
| `GET /tables/A?txn_id=T` | Embedded snapshot in transaction T | Most recent visible + RYOW (staged metadata if T has SUB_COMMITTED subtxn) |

**For any query that reads more than one table, the client must acquire a `TxnSnapshot` first and pass it on every read.** Without a shared `TxnSnapshot`, two successive GETs can observe different committed states and produce a torn cross-table view.

The correct multi-table read pattern:

```
1. POST /v1/txn-snapshots                            → txn_snapshot_id=RS, xmax=5050
2. GET  /v1/tables/orders?txn_snapshot_id=RS         → table_snapshot_id=42 (visible to RS)
3. GET  /v1/tables/customers?txn_snapshot_id=RS      → table_snapshot_id=17 (visible to RS)
   Both tables are consistent at the same logical point in transaction history.
4. DELETE /v1/txn-snapshots/RS                       → release (advance watermark)
```

A `TxnSnapshot` is lightweight — no CLOG entry, no locks, no write state. Its only cost is pinning the watermark via `xmin`.

### Lock

A lock is a server-side reservation on a resource (table or partition) that prevents conflicting concurrent operations. Iceberg uses **operation-appropriate lock granularity** — the lock mode is determined by the operation being performed, not by whether the transaction is single-table or multi-table.

Locks are acquired pessimistically (before writing) for multi-table transactions and remain held until the top-level commit or abort.

---

## Data Representation

All transaction state lives in the REST catalog server's backend database. No shared memory is required. The catalog DB's own ACID transactions provide atomicity for state changes.

### `transactions` — The CLOG

Only write transactions (single-table pessimistic or multi-table) get a row here. Pure readers do not.

```sql
CREATE TABLE transactions (
    txn_id           BIGINT PRIMARY KEY,
                     -- server-assigned from monotone sequence; writers only
    txn_token        BYTEA NOT NULL,
                     -- opaque top-level bearer secret (gen_random_bytes(32));
                     -- held only by the transaction owner / orchestrator;
                     -- required to commit or abort the top-level transaction
    owner_principal  TEXT NOT NULL,
                     -- authenticated identity that called BEGIN; enforced on every operation
    status           SMALLINT NOT NULL DEFAULT 0,
                     -- 0=IN_PROGRESS, 1=COMMITTED, 2=ABORTED
    isolation        SMALLINT NOT NULL DEFAULT 1,
                     -- 0=READ_COMMITTED, 1=REPEATABLE_READ, 2=SERIALIZABLE
    -- Embedded TxnSnapshot for snapshot isolation (REPEATABLE_READ) transactions
    snap_xmin        BIGINT,      -- NULL for READ_COMMITTED (fresh snapshot per statement)
    snap_xmax        BIGINT,      -- NULL for READ_COMMITTED
    snap_xip         BIGINT[],    -- NULL for READ_COMMITTED
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at         TIMESTAMPTZ,
    expires_at       TIMESTAMPTZ NOT NULL
                     -- hard deadline; reaper aborts expired transactions
);

CREATE INDEX txn_status_idx ON transactions (status, txn_id);
```

### `subtransactions` — Per-Table Work Units

```sql
CREATE TABLE subtransactions (
    subtxn_id                BIGINT PRIMARY KEY,
    parent_txn_id            BIGINT NOT NULL REFERENCES transactions (txn_id),
    table_uuid               UUID NOT NULL,
    base_snapshot_id         BIGINT,
                             -- Iceberg snapshot_id at subtxn start; used for conflict check
    staged_metadata_location TEXT,
                             -- written by client to object storage; applied on parent commit
    assigned_staging_path    TEXT,
                             -- server-assigned path WITHIN the table's canonical metadata
                             -- directory (e.g. s3://warehouse/ns/table/metadata/<uuid>.metadata.json).
                             -- Placing the file inside the table's own tree is intentional:
                             -- the catalog pointer controls visibility (not the file's location),
                             -- so the file is invisible until commitTransaction swaps the pointer,
                             -- and credential vending / location validation work without special-casing.
    subtxn_token             BYTEA NOT NULL,
                             -- opaque per-subtxn bearer secret (gen_random_bytes(32));
                             -- authorizes only this subtxn's operations; does NOT allow
                             -- top-level commit/abort (those require transactions.txn_token)
    status                   SMALLINT NOT NULL DEFAULT 0
                             -- 0=IN_PROGRESS, 1=SUB_COMMITTED, 2=ABORTED
);

CREATE INDEX subtxn_parent_idx ON subtransactions (parent_txn_id);
CREATE INDEX subtxn_table_idx  ON subtransactions (table_uuid, parent_txn_id);
```

### `txn_snapshots` — Reader Visibility Snapshots

Only pure readers (no writes) get a row here. Read-write transactions store their `TxnSnapshot`
inline in the `transactions` table (`snap_xmin`/`snap_xmax`/`snap_xip` columns).

```sql
CREATE TABLE txn_snapshots (
    txn_snapshot_id  BIGINT PRIMARY KEY,
                      -- unique handle; does NOT need to come from the txn_id sequence
                      -- xmax already encodes the ordering relationship to txn_ids
    xmin              BIGINT NOT NULL,
                      -- oldest in-flight txn_id at snapshot time; used for watermark
    xmax              BIGINT NOT NULL,
                      -- next txn_id to be assigned at snapshot time; visibility upper bound
    xip_list          BIGINT[] NOT NULL,
                      -- in-flight txn_ids at snapshot time; excluded from visibility
    registered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at        TIMESTAMPTZ NOT NULL,
                      -- must renew or expires; prevents watermark pinning forever
    released_at       TIMESTAMPTZ
                      -- set when client explicitly releases the `TxnSnapshot`
);

CREATE INDEX txn_snapshot_xmin_idx ON txn_snapshots (xmin)
    WHERE released_at IS NULL;
```

### `committed_table_snapshots` — The MVCC-to-Iceberg Mapping

This is the bridge between the transaction system and Iceberg's native table snapshot model. Every time a transaction commits a change to a table, a row is inserted here recording which `txn_id` produced which Iceberg `table_snapshot_id`. Without this table the catalog cannot translate a `TxnSnapshot`'s `(xmax, xip_list)` into a concrete Iceberg metadata pointer.

```sql
CREATE TABLE committed_table_snapshots (
    table_uuid          UUID    NOT NULL,
    table_snapshot_id   BIGINT  NOT NULL,
                        -- Iceberg's native snapshot ID from the table metadata JSON
    committed_txn_id    BIGINT  NOT NULL,
                        -- the txn_id that committed this table snapshot
    metadata_location   TEXT    NOT NULL,
                        -- path to the Iceberg metadata JSON file for this snapshot
    committed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_uuid, table_snapshot_id)
);

-- Critical index: resolves "most recent visible table snapshot" in O(log n)
CREATE INDEX cts_visibility_idx
    ON committed_table_snapshots (table_uuid, committed_txn_id DESC);
```

The visibility resolution query — given a `TxnSnapshot` `RS`, find the Iceberg table snapshot to return for a table:

```sql
SELECT table_snapshot_id, metadata_location
FROM committed_table_snapshots
WHERE table_uuid        = :table_uuid
  AND committed_txn_id  < :rs_xmax           -- committed before TxnSnapshot was taken
  AND committed_txn_id != ALL(:rs_xip_list)  -- was not in-flight at TxnSnapshot time
ORDER BY committed_txn_id DESC
LIMIT 1;
```

This query returns the most recently committed Iceberg table snapshot that is visible to the given `TxnSnapshot` — the exact Iceberg metadata pointer the reader should use to scan data files.

For an unregistered read (no `txn_snapshot_id`), the equivalent is:

```sql
-- Latest committed: xmax = current, xip_list = [] (no in-flight exclusions)
SELECT table_snapshot_id, metadata_location
FROM committed_table_snapshots
WHERE table_uuid = :table_uuid
ORDER BY committed_txn_id DESC
LIMIT 1;
```

### `locks` — Lock Manager State

```sql
CREATE TABLE locks (
    lock_id       BIGINT PRIMARY KEY,
    txn_id        BIGINT NOT NULL REFERENCES transactions (txn_id),
    resource      TEXT NOT NULL,
                  -- e.g. 'table:<uuid>', 'partition:<uuid>/<spec_id>/<partition_value>'
    resource_type SMALLINT NOT NULL,
                  -- 0=TABLE, 1=PARTITION
    lock_mode     SMALLINT NOT NULL,
                  -- 0=APPEND, 1=ROW_DELTA, 2=OVERWRITE, 3=EXCLUSIVE, 4=ACCESS_EXCLUSIVE
    status        SMALLINT NOT NULL DEFAULT 0,
                  -- 0=WAITING, 1=GRANTED
    requested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    granted_at    TIMESTAMPTZ
);

CREATE INDEX locks_resource_idx ON locks (resource, status);
CREATE INDEX locks_txn_idx      ON locks (txn_id, status);
```

### `txn_watermark` — Purge Horizon

```sql
CREATE TABLE txn_watermark (
    id             INT PRIMARY KEY DEFAULT 1,   -- single-row table
    watermark_txn_id BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);
```

The watermark is the oldest `txn_id` that any active snapshot or in-progress transaction still references. All CLOG entries, subtransactions, and lock records with `txn_id < watermark` and `status != IN_PROGRESS` are safe to purge.

---

## Isolation Levels

### READ COMMITTED

Each statement sees only data committed before **that statement** began. Two successive reads within the same transaction can see different data if another transaction commits between them.

Implementation: take a fresh snapshot at the start of each statement. Do not pin the snapshot for the duration of the transaction.

Use case: simple read-only queries, single-statement operations where per-statement consistency is sufficient.

### REPEATABLE READ = Snapshot Isolation (Default)

All statements within a transaction see data committed before **the transaction** began. The `TxnSnapshot` is pinned at `BEGIN` time and reused for every statement. Two successive reads always return the same data regardless of concurrent commits between them.

Implementation: take one `TxnSnapshot` at `BEGIN` time and embed it in the `transactions` row. All reads in this transaction use this snapshot.

> **Note on terminology**: In this design, REPEATABLE READ *is* snapshot isolation — they are the same implementation. The SQL standard defines REPEATABLE READ as a weaker level that still permits phantom reads; snapshot isolation additionally prevents phantoms by pinning the read view at the file level. PostgreSQL made the same choice: its `REPEATABLE READ` level is implemented as snapshot isolation. We follow that convention. The column label "REPEATABLE READ" in the comparison table below means "snapshot isolation as described here."

Use case: multi-statement analytical queries, multi-table transactions, any operation where consistent cross-table reads are required.

### SERIALIZABLE (Future)

Full serializability: the result of concurrent transactions must be equivalent to some serial execution. Prevents write-skew anomalies by tracking read-sets and detecting conflicts at commit time.

Implementation: Snapshot Isolation (REPEATABLE READ) + predicate lock tracking on read operations + write-skew detection at commit time. Deferred to a later implementation phase; snapshot isolation is sufficient for the primary multi-table transaction use cases.

### Comparison

| Anomaly | READ COMMITTED | REPEATABLE READ (Snapshot Isolation) | SERIALIZABLE |
|---|---|---|---|
| Dirty read | ❌ Never | ❌ Never | ❌ Never |
| Non-repeatable read | ✅ Possible | ❌ Never | ❌ Never |
| Phantom read | ✅ Possible | ❌ Never | ❌ Never |
| Write skew | ✅ Possible | ✅ Possible | ❌ Never |

Phantom reads are prevented at snapshot isolation because the read view is fixed at the file level at `BEGIN` time — there is no mechanism by which a newly committed row can appear in a re-scan within the same transaction.

---

## Lock Hierarchy

### Lock Modes

Locks are ordered from weakest (most compatible) to strongest (least compatible):

| Mode | Code | Description | Typical Operation |
|---|---|---|---|
| `APPEND` | 0 | Add new files; never conflicts with anything | `newAppend()`, `newFastAppend()` |
| `ROW_DELTA` | 1 | Positional deletes + new data files within partitions | `newRowDelta()` (UPDATE/DELETE) |
| `OVERWRITE` | 2 | Replace entire partition(s) | `newReplacePartitions()`, `newOverwrite()` |
| `EXCLUSIVE` | 3 | Full table write serialization | `newRewrite()`, compaction |
| `ACCESS_EXCLUSIVE` | 4 | Block all access including reads | Schema evolution DDL |

### Lock Compatibility Matrix

```
                 APPEND   ROW_DELTA   OVERWRITE   EXCLUSIVE   ACCESS_EXCLUSIVE
APPEND             ✅         ✅          ✅           ✅              ❌
ROW_DELTA          ✅         ❌*         ❌           ❌              ❌
OVERWRITE          ✅         ❌          ❌           ❌              ❌
EXCLUSIVE          ✅         ❌          ❌           ❌              ❌
ACCESS_EXCLUSIVE   ❌         ❌          ❌           ❌              ❌
```

\* Two `ROW_DELTA` operations on **different partitions** of the same table are compatible even though `ROW_DELTA` conflicts with itself at the same partition. Lock resource granularity handles this: `partition:<uuid>/date=2024-01-15` and `partition:<uuid>/date=2024-01-16` are different resources.

Key properties:
- **APPEND never conflicts with any write mode** — concurrent appenders always proceed, preserving Iceberg's high-throughput ingest model.
- **Reads (snapshot acquisition) never block** — no read lock is taken; the snapshot mechanism provides read isolation without locking.
- **ACCESS_EXCLUSIVE conflicts with APPEND** — DDL (schema evolution) is the only operation that blocks readers.

### Lock Granularity

```
Catalog
  └── Namespace
        └── Table  ← TABLE-level locks (EXCLUSIVE, ACCESS_EXCLUSIVE, OVERWRITE of whole table)
              └── Partition  ← PARTITION-level locks (ROW_DELTA, OVERWRITE of specific partitions)
```

Multi-table transactions acquire locks at the granularity appropriate to each subtransaction's operation:

```
Multi-table transaction T1:
  SubTxn S1: RowDelta on orders/date=2024-01-15
             → acquire ROW_DELTA lock on partition:orders-uuid/date=2024-01-15
  SubTxn S2: Append to customers
             → no lock needed (APPEND is always optimistic)
```

The multi-table wrapper does **not** escalate lock granularity. Each subtransaction acquires only what its specific operation requires.

---

## Concurrent Operation Support

### When Locks Are Not Needed

**Append operations are always lock-free and optimistic**, regardless of whether they are inside a multi-table transaction or not. Appends add new files to the manifest without modifying or deleting existing files. There is no data-level conflict between concurrent appenders. The existing optimistic CAS (compare-and-swap on the metadata pointer) is sufficient.

This is the most important property to preserve: Iceberg's high-throughput ingest workload — thousands of concurrent Spark tasks appending to partitions — must remain completely unaffected by the addition of the transaction system.

### When Locks Are Needed

Locks are acquired for operations that can conflict at the data level:
- `ROW_DELTA`: may delete rows from a partition that another writer is also modifying
- `OVERWRITE`: replaces entire partition(s); conflicts with any other write on those partitions
- `EXCLUSIVE`: whole-table rewrite (compaction); conflicts with all other writes
- `ACCESS_EXCLUSIVE`: DDL; conflicts with all access

For **single-table operations**, locks are optional. The existing optimistic retry model remains the default. Pessimistic locking is opt-in for clients that cannot afford the cost of a retry (e.g., a long-running expensive computation).

For **multi-table operations**, pessimistic locking is mandatory for write subtransactions that involve `ROW_DELTA` or `OVERWRITE`. Without upfront locking, a subtransaction could write hours of S3 data only to discover a conflict at commit time — and it cannot retry just that one table because the other tables have already reached `SUB_COMMITTED`.

---

## Transaction Lifecycle

### Single-Table Read (No Snapshot — Current Behavior, Unchanged)

```
1. Client: GET /v1/namespaces/ns/tables/A
   → Server returns latest committed metadata pointer
   → No server-side state created
```

Simple, stateless. Equivalent to PostgreSQL `READ COMMITTED` for a single statement. Appropriate for single-table queries where per-call freshness is acceptable.

### Multi-Table Consistent Read (New)

```
1. Client: POST /v1/transactions/snapshot
   → Server atomically captures (xmin, xmax, xip_list) from current transaction state
   → Returns snapshot_id=S

2. Client: GET /v1/namespaces/ns/tables/orders?snapshot_id=S
   → Server returns latest committed metadata for orders where committed_txn_id < S.xmax
      AND committed_txn_id NOT IN S.xip_list

3. Client: GET /v1/namespaces/ns/tables/customers?snapshot_id=S
   → Same visibility rule — both tables observed at same logical point in time

4. Client: DELETE /v1/transactions/snapshots/S
   → Releases snapshot; watermark can advance past S.xmin
   → (Also released automatically when S.expires_at is reached)
```

No `txn_id` is created. No CLOG entry. The snapshot is a pure read artifact — lightweight, lock-free, and cheap to acquire. The cost is only the watermark pin: the catalog cannot purge transaction history older than `S.xmin` until S is released.

### Single-Table Optimistic Write (Current Behavior, Unchanged)

```
1. Client reads table metadata → base_snapshot_id
2. Client writes data files to S3
3. Client builds new snapshot + staged metadata locally
4. Client calls COMMIT:
   - Server checks: is base_snapshot_id still current? (CAS)
   - YES → atomically update metadata pointer → COMMITTED
   - NO  → CommitFailedException → client retries from step 1
```

No server-side transaction state needed. No locks. Unchanged from current Iceberg behavior.

### Single-Table Pessimistic (Opt-In)

```
1. Client: POST /v1/transactions           → txn_id=T1, snapshot=S1
2. Client: POST /v1/locks                  → acquire ROW_DELTA lock on table/partition
3. Client reads table metadata (guaranteed stable — lock held)
4. Client writes data files to S3
5. Client stages new metadata to S3 (unique path)
6. Client: POST /v1/transactions/T1/commit → CAS + atomic pointer update + lock release
```

### Multi-Table Transaction

```
1. Client: POST /v1/transactions           → txn_id=T1, snapshot=S1

2. For each table:
   Client: POST /v1/locks                  → acquire appropriate lock
   Client: POST /v1/transactions/T1/subtxn → subtxn_id=Sn, base_snapshot_id

3. For each subtransaction (can be parallelized):
   Client writes data files to S3
   Client stages new metadata JSON to S3
   Client: POST /v1/transactions/T1/subtxn/Sn/commit
     → Server: conflict check (was base_snapshot_id superseded?)
     → Server: marks Sn as SUB_COMMITTED

4. When ALL subtransactions are SUB_COMMITTED:
   Client: POST /v1/transactions/T1/commit
     → Server (single catalog DB transaction):
         verify all subtxns = SUB_COMMITTED
         verify no new conflicts since each subtxn's base_snapshot_id
         UPDATE table_pointers for all tables atomically
         UPDATE transactions SET status=COMMITTED
         DELETE locks WHERE txn_id=T1
     → COMMITTED — both tables visible simultaneously to all readers

5. On any failure:
   Client: POST /v1/transactions/T1/abort
     → Server: UPDATE transactions SET status=ABORTED
     → Server: DELETE locks WHERE txn_id=T1
     → Server: register staged S3 files as orphans for cleanup
```

---

## Distributed Transaction Sharing

A single `TxnSnapshot` or a single `txn_id` can be shared across multiple independent applications or Spark jobs. This enables two distinct patterns:

### Read Side: Shared TxnSnapshot Across Applications

This is the read analog of PostgreSQL's `pg_export_snapshot()` / `SET TRANSACTION SNAPSHOT`, which was designed for parallel consistent reads (e.g., parallel `pg_dump`).

**Pattern:** An orchestrator acquires one `TxnSnapshot` at pipeline start and distributes the `txn_snapshot_id` to all reader applications. Every application — regardless of when it physically starts — reads at the same logical point in time. Results from different applications can be safely joined or compared.

```
Orchestrator:
  POST /v1/txn-snapshots
  ← { txn_snapshot_id: RS, xmin: 4990, xmax: 5050, xip_list: [...], expires_at: ... }

  → passes txn_snapshot_id=RS to SparkApp1 (reads orders)
  → passes txn_snapshot_id=RS to SparkApp2 (reads customers)
  → passes txn_snapshot_id=RS to SparkApp3 (reads products)

SparkApp1: GET /tables/orders?txn_snapshot_id=RS     → table_snapshot_id=42
SparkApp2: GET /tables/customers?txn_snapshot_id=RS  → table_snapshot_id=17   (parallel)
SparkApp3: GET /tables/products?txn_snapshot_id=RS   → table_snapshot_id=31   (parallel)

  All three apps see a consistent cross-table state as of xmax=5050.
  RS.xmin pins the watermark for the duration.

Orchestrator (after all apps complete):
  DELETE /v1/txn-snapshots/RS  → release; watermark advances
```

**Key properties:**
- RS is acquired *once*, at the moment the pipeline is scheduled — not when each job happens to start.
- A job that starts 10 minutes late (cluster contention) still sees the same data as the job that started immediately.
- Single watermark pin regardless of how many applications share RS.
- RS is a read-only token: any bearer can read with it; no application can write using it.

### Write Side: Shared txn_id Across Applications (Parallel Ingestion)

**Pattern:** An orchestrator acquires a top-level transaction and creates one subtransaction per table. Each subtransaction is delegated to a separate Spark application. Applications write in parallel with no inter-app communication. The orchestrator commits atomically when all subtransactions are complete.

```
Orchestrator:
  POST /v1/transactions
  ← { txn_id: T1, txn_token: <top-level-secret>, expires_at: ... }

  POST /v1/transactions/T1/subtxn  { table: "orders" }
  ← { subtxn_id: S1, subtxn_token: <tok1>, staging_path: "s3://.../staging/T1/S1/..." }

  POST /v1/transactions/T1/subtxn  { table: "customers" }
  ← { subtxn_id: S2, subtxn_token: <tok2>, staging_path: "s3://.../staging/T1/S2/..." }

  → passes { txn_id: T1, subtxn_id: S1, subtxn_token: tok1 } to SparkApp1
  → passes { txn_id: T1, subtxn_id: S2, subtxn_token: tok2 } to SparkApp2
    (txn_token is NOT shared with apps — orchestrator only)

SparkApp1:                              SparkApp2:
  write data files to S3 (parallel)      write data files to S3 (parallel)
  stage metadata to staging_path/S1      stage metadata to staging_path/S2
  POST /v1/transactions/T1/subtxn/S1/commit   POST /v1/transactions/T1/subtxn/S2/commit
    (auth: subtxn_token=tok1)              (auth: subtxn_token=tok2)
  ← SUB_COMMITTED                        ← SUB_COMMITTED

Orchestrator (after both apps confirm SUB_COMMITTED):
  POST /v1/transactions/T1/commit  (auth: txn_token=<top-level-secret>)
  → Server atomically swaps both table pointers
  → orders and customers changes visible simultaneously ✓

On any app failure:
  POST /v1/transactions/T1/abort  (auth: txn_token=<top-level-secret>)
  → Both tables unchanged; S3 files orphaned for cleanup
```

**Key properties:**
- No inter-app communication: apps write independently and signal SUB_COMMITTED to the orchestrator.
- The `subtxn_token` scopes each app's authority to exactly one subtransaction — it cannot commit/abort the top-level transaction, and cannot touch other subtransactions.
- The `txn_token` is held exclusively by the orchestrator — only the orchestrator can finalize or cancel the transaction.
- Locks are acquired by the orchestrator at subtransaction creation time, before any app starts. This prevents deadlocks and guarantees each app finds a stable table to write to.
- All-or-nothing: if any app fails, the orchestrator aborts T1 and all written S3 files are orphaned. No partial data is ever visible.

### Token Hierarchy

```
                      Orchestrator
                      ┌──────────────────────────────┐
                      │ txn_id=T1                    │
                      │ txn_token=<top-level-secret> │  ← commit / abort / renew entire txn
                      └──────────┬───────────────────┘
               ┌─────────────────┴─────────────────┐
        SparkApp1                             SparkApp2
  ┌─────────────────────┐              ┌─────────────────────┐
  │ txn_id=T1           │              │ txn_id=T1           │
  │ subtxn_id=S1        │              │ subtxn_id=S2        │
  │ subtxn_token=<tok1> │              │ subtxn_token=<tok2> │
  └─────────────────────┘              └─────────────────────┘
  ← write S3, stage metadata,          ← write S3, stage metadata,
    call subtxn/S1/commit only           call subtxn/S2/commit only
```

### Comparison: Shared Snapshot vs. Shared Transaction

| | Shared TxnSnapshot (Read) | Shared txn_id (Write) |
|---|---|---|
| Acquired by | Orchestrator | Orchestrator |
| Distributed to | All reader apps | One subtxn credential per writer app |
| Apps share | Same `txn_snapshot_id` | Same `txn_id`; each has unique `subtxn_token` |
| Top-level token held by | Orchestrator (releases snapshot) | Orchestrator (commits/aborts txn) |
| App-level capability | Read any table at RS | Write assigned table; commit own subtxn only |
| Finalization | Orchestrator releases RS | Orchestrator calls `T1/commit` |
| Failure handling | Orchestrator releases RS early | Orchestrator calls `T1/abort` |

---

## Commit and Abort Protocol

### Why the Catalog DB Gives Atomicity for Free

The fundamental insight of this design is that the catalog server has a **transactional backend database** (PostgreSQL, MySQL, etc.). All the hard atomicity work is delegated to a single SQL transaction in that database — we do not need to implement distributed two-phase commit.

A multi-table commit is atomic because every visible side-effect happens inside one catalog DB transaction:

```sql
BEGIN;
  -- 1. Final conflict check for each subtransaction
  SELECT base_snapshot_id FROM subtransactions
    WHERE parent_txn_id = :txn_id AND status = 'SUB_COMMITTED'
    FOR UPDATE;
  -- verify each base_snapshot_id is still the current table pointer

  -- 2. Advance every table's metadata pointer
  UPDATE table_pointers SET metadata_location = :new_location
    WHERE table_uuid = :uuid AND metadata_location = :expected_location;
  -- one UPDATE per subtransaction; fail if any row is not found

  -- 3. Record the commit mapping (TxnSnapshot visibility bridge)
  INSERT INTO committed_table_snapshots
    (table_uuid, table_snapshot_id, committed_txn_id, metadata_location)
  VALUES (...), (...), ...;

  -- 4. Mark the transaction committed
  UPDATE transactions SET status = 'COMMITTED', ended_at = now()
    WHERE txn_id = :txn_id;

  -- 5. Release all locks
  DELETE FROM locks WHERE txn_id = :txn_id;
COMMIT;
```

If the database rolls back (conflict, crash, deadlock), none of the above is visible. The transaction stays `IN_PROGRESS`, locks are retained, and the reaper eventually expires it. No reader ever sees a partially committed state.

> This is why scoping multi-table transactions to the REST catalog works: a single transactional backend eliminates the need for a distributed commit coordinator and the associated failure modes of classic two-phase commit (coordinator crash, blocking).

### Two-Phase Prepare + Finalize

The protocol is conceptually two-phase, but the "prepare" phase happens on the client side and the catalog DB transaction is only the "finalize" phase:

| Phase | Who | What |
|---|---|---|
| **Prepare** | Client | Write data files to S3; write new metadata JSON to the server-assigned path within the table's canonical metadata directory; call `POST /subtxn/Sn/commit` per table → server records `staged_metadata_location`, marks `SUB_COMMITTED` |
| **Finalize** | Catalog server (single DB txn) | Conflict-check + pointer swap + CLOG update + lock release |

The prepare phase is parallelizable across tables. The finalize phase is a single short catalog DB transaction and is the only serialization point.

The staged metadata files on S3 are **pre-written but invisible** until the finalize step swaps the pointer. If finalize never happens (abort or crash), those files are orphans — they are safe to garbage-collect and do not affect any reader.

### Conflict Detection at Commit Time

For each subtransaction, the server checks:

```sql
SELECT current_metadata_location FROM table_pointers WHERE table_uuid = :uuid;
-- must equal subtxn.base_metadata_location captured at subtxn start
```

If they differ, another writer committed to that table after this subtransaction began. The appropriate response depends on the operation type:

| Operation | On Conflict |
|---|---|
| `APPEND` | Re-check: do new files overlap with ours? No overlap → safe to retry or proceed. Overlap → abort. |
| `ROW_DELTA` | Abort: a concurrent positional delete or data overwrite may have invalidated our deletes. |
| `OVERWRITE` | Abort: the data we intended to replace has changed. |

For multi-table transactions, a conflict on **any** subtransaction aborts the **entire** top-level transaction. Partial commits are not allowed.

### Abort: No Undo Log Needed

Abort is cheap because Iceberg writes are **append-only and copy-on-write**. The staged metadata on S3 and the new data files are simply never pointed to:

```
ABORT steps (server-side):
  UPDATE transactions SET status = 'ABORTED', ended_at = now()
    WHERE txn_id = :txn_id;
  DELETE FROM locks WHERE txn_id = :txn_id;
  -- Optional: INSERT INTO orphan_files SELECT staged_metadata_location ...
  -- (for proactive S3 cleanup; correctness does not depend on it)
```

This is the same model PostgreSQL uses: on `ROLLBACK`, PostgreSQL marks the XID as aborted in `pg_xact` (the CLOG) and does nothing else. The tuples remain on disk but are invisible to all future readers because any visibility check sees the XID as aborted. `VACUUM` later reclaims the space.

In Iceberg the analog is: the staged S3 files remain in object storage but are unreachable — no table pointer ever references them. A periodic orphan file cleanup job (equivalent to `VACUUM`) reclaims the storage.

**There is no undo log.** There is nothing to replay or reverse. The "rollback" is simply "never making the writes visible."

### Savepoint / Partial Rollback

A `SAVEPOINT` marks a point within the transaction to which a partial rollback can retreat. In this design, a savepoint corresponds to rolling back one or more subtransactions while keeping the rest:

```
SAVEPOINT sp1  →  record current set of SUB_COMMITTED subtxn_ids
ROLLBACK TO sp1 →  ABORT subtxns committed after sp1; release their locks;
                   those tables revert to their base_snapshot_id
                   parent txn remains IN_PROGRESS
```

The remaining subtransactions are unaffected. The client may then re-issue the rolled-back work or commit the transaction without it.

This is a forward-looking capability and is not required for the initial implementation, but the data model supports it without schema changes.

### Summary: Single-Table vs. Multi-Table Commit Comparison

| | Single-Table Optimistic | Single-Table Pessimistic | Multi-Table |
|---|---|---|---|
| Locking | None (CAS only) | Acquired at `BEGIN` | Acquired at `BEGIN` per table |
| Prepare phase | Client stages metadata | Client stages metadata | Client stages metadata per table (parallelizable) |
| Finalize | CAS pointer swap | CAS + lock release | Single DB txn: all pointer swaps + CLOG + lock release |
| Conflict handling | Retry (optimistic) | Retry (lock prevents conflict) | Abort entire txn |
| Abort cost | None (no server state) | Lock release only | Lock release + orphan registration |
| Atomicity guarantee | Single-row CAS in catalog DB | Single-row CAS in catalog DB | Multi-row DB transaction in catalog DB |

---

## Behavioral Examples

### Example 1: Reader vs. Reader

**Scenario**: A pipeline orchestrator launches two Spark apps in parallel — App1 reads `orders`, App2 reads `customers` — and the results will be joined downstream. Both apps must see the same consistent state.

#### Without a shared snapshot (broken)

```
-- Background: writer W commits to customers at t=150ms (txn_id=5011)

App1 at t=100ms: GET /tables/orders    → sees orders at table_snapshot_id=42
App1 at t=200ms: GET /tables/customers → sees customers AFTER W's commit (snapshot 18)

App1 is joining orders snapshot 42 against customers snapshot 18.
W added a new customer referenced by an existing order.
App1 sees the order but not the customer — referential inconsistency. ✗
```

Even if App1 and App2 each acquire their own snapshot independently, they still get different snapshots if any writer commits between the two acquisitions.

#### With a shared snapshot (correct)

```
-- Orchestrator acquires ONE snapshot before launching any app:
Orchestrator: POST /v1/txn-snapshots
  ← { txn_snapshot_id: RS, xmax: 5010, xip_list: [] }
  (captured before W commits at txn_id=5011)

-- Orchestrator passes RS to both apps:
App1: GET /tables/orders?txn_snapshot_id=RS
  → W's commit (txn_id=5011 ≥ xmax=5010) is NOT visible
  → returns table_snapshot_id=42  ✓

App2: GET /tables/customers?txn_snapshot_id=RS
  → Same visibility rule: W excluded
  → returns table_snapshot_id=17 (pre-W state)  ✓

Both apps see the same pre-W consistent state regardless of when each app physically started.

-- Orchestrator releases RS after both apps complete:
Orchestrator: DELETE /v1/txn-snapshots/RS  → watermark advances
```

**What if the orchestrator wants to include W?** Acquire RS *after* W commits:
```
-- W commits at txn_id=5011
Orchestrator: POST /v1/txn-snapshots
  ← { txn_snapshot_id: RS2, xmax: 5012, xip_list: [] }
  (captures state after W)

App1: GET /tables/orders?txn_snapshot_id=RS2    → W visible (5011 < 5012)
App2: GET /tables/customers?txn_snapshot_id=RS2 → W visible ✓
```

**Result**: The orchestrator controls exactly which committed state all apps observe — by choosing *when* to acquire the shared snapshot. Single watermark pin regardless of how many apps share it.

---

### Example 2: Reader vs. Writer

**Scenario**: A Spark read job runs against `orders` while a Flink pipeline appends new records.

```
T1 (reader, snapshot S1 pinned at txn_id=5000)

T2 (writer, APPEND to orders):
  writes new Parquet files to S3
  commits → txn_id=5001, new snapshot=43 visible in catalog

T1 continues reading:
  still uses snapshot S1 (pinned at txn_id=5000)
  sees orders at snapshot=42 (the state before T2 committed)
  T2's new files are in the manifest at snapshot=43
  S1's xmax=5000 < 5001 → T2 is NOT in T1's visible range
```

**Result**: T1 sees a perfectly consistent pre-T2 view for its entire duration. T2 commits without any awareness of T1. Neither blocks the other. This is the core MVCC guarantee — readers never block writers, writers never block readers.

---

### Example 3: Writer vs. Writer — Compatible (Append + Append)

**Scenario**: Two Spark jobs simultaneously append to `orders`.

```
T1 (APPEND to orders/date=2024-01-15):
  writes part-001.parquet to S3
  commits → optimistic CAS → SUCCESS, txn_id=5001

T2 (APPEND to orders/date=2024-01-16):  [concurrent with T1]
  writes part-001.parquet to S3
  commits → optimistic CAS → SUCCESS, txn_id=5002
```

Both appends write to different partitions. Even if they wrote to the same partition, appends are purely additive — manifests are merged, not replaced. Both commits succeed without conflict.

**Result**: Full concurrency. No locks, no waiting, no retries. This is the dominant Iceberg workload and must remain completely unaffected by the transaction system.

---

### Example 4: Writer vs. Writer — Conflict (RowDelta + RowDelta, Same Partition)

**Scenario**: Two jobs both try to UPDATE rows in `orders/date=2024-01-15`.

**Without pessimistic locking (optimistic, current behavior)**:

```
T1 (RowDelta, orders/date=2024-01-15):
  reads base_snapshot_id=42
  writes delete file + new data file to S3 (takes 30 minutes)
  tries commit → CAS: base=42, current=43 (T2 committed first) → CONFLICT
  retries: re-reads snapshot=43, re-applies RowDelta, re-writes S3 (another 30 minutes)
  commits → SUCCESS, txn_id=5003
```

Cost: 30 minutes of wasted work.

**With pessimistic locking (opt-in)**:

```
T1 (RowDelta, orders/date=2024-01-15, pessimistic):
  POST /v1/locks → acquire ROW_DELTA lock on orders/date=2024-01-15 → GRANTED
  reads base_snapshot_id=42 (guaranteed stable — lock held)
  writes delete file + new data file to S3 (30 minutes)
  commits → SUCCESS, txn_id=5001, lock released

T2 (RowDelta, orders/date=2024-01-15, pessimistic):
  POST /v1/locks → acquire ROW_DELTA lock on orders/date=2024-01-15 → WAITING
  ... waits 30 minutes for T1 to release ...
  lock GRANTED
  reads base_snapshot_id=43 (T1's committed state)
  writes delete file + new data file to S3 (30 minutes)
  commits → SUCCESS, txn_id=5002
```

Cost: T2 waits 30 minutes but does zero wasted work. Total wall time: 60 minutes (serialized). Compare to optimistic: potentially 90+ minutes with retries.

---

### Example 5: Multi-Table vs. Multi-Table

**Scenario**: Two ETL jobs both atomically update `orders` and `customers`.

```
T1 (multi-table: RowDelta on orders/p1 + Append to customers):
  acquire ROW_DELTA lock on orders/date=2024-01-15 → GRANTED
  no lock needed for Append to customers
  SubTxn S1 writes orders data to S3
  SubTxn S2 writes customers data to S3
  S1 → SUB_COMMITTED
  S2 → SUB_COMMITTED
  T1 commits atomically → orders+customers visible simultaneously

T2 (multi-table: RowDelta on orders/p2 + RowDelta on customers/p1):
  acquire ROW_DELTA lock on orders/date=2024-01-16 → GRANTED  [different partition from T1]
  acquire ROW_DELTA lock on customers/date=2024-01-15 → GRANTED
  SubTxn S3 writes orders data to S3
  SubTxn S4 writes customers data to S3
  S3 → SUB_COMMITTED
  S4 → SUB_COMMITTED
  T2 commits atomically
```

T1 and T2 operate on different partitions. **Both proceed fully concurrently** despite both being multi-table transactions touching the same tables.

**Conflict case** — T1 and T2 both want `orders/date=2024-01-15`:

```
T1: acquire ROW_DELTA lock on orders/date=2024-01-15 → GRANTED
T2: acquire ROW_DELTA lock on orders/date=2024-01-15 → WAITING

T2 waits until T1 commits and releases the lock.
T2 then acquires the lock and proceeds.
No data corruption. Correct serialization.
```

---

### Example 6: Multi-Table Transaction vs. Single-Table Writer

**Scenario**: A multi-table ETL (T1) holds a lock on `orders/date=2024-01-15` while a routine streaming ingest job (T2) appends to `orders`.

```
T1 (multi-table, holds ROW_DELTA lock on orders/date=2024-01-15):
  ... writing to S3 for 2 hours ...

T2 (single-table APPEND to orders/date=2024-01-16):
  No lock needed (APPEND mode)
  Writes new files to S3
  Commits via optimistic CAS → SUCCESS immediately
  [T2 is completely unaware of T1]

T3 (single-table APPEND to orders/date=2024-01-15):
  No lock needed (APPEND mode, compatible with ROW_DELTA)
  Writes new files to S3
  Commits via optimistic CAS → SUCCESS immediately
  [Appends never conflict with ROW_DELTA]

T4 (single-table RowDelta on orders/date=2024-01-15):
  Pessimistic: tries to acquire ROW_DELTA lock → WAITING for T1
  Optimistic:  writes S3, commits → CAS check: is snapshot still current?
               If T1 hasn't committed yet: base still valid → SUCCESS
               If T1 committed first: CONFLICT → retry
```

**Result**: Appends on any partition of any table proceed freely. Only writers doing `ROW_DELTA`/`OVERWRITE` on the same partition as T1's lock are affected. The blast radius of a multi-table transaction is exactly the same as an equivalent single-table pessimistic transaction.

---

### Example 7: Single-Table vs. Single-Table — DDL + DML

**Scenario**: A schema evolution operation runs concurrently with a data append.

```
T1 (ALTER TABLE orders ADD COLUMN discount DECIMAL(5,2)):
  acquire ACCESS_EXCLUSIVE lock on orders (table-level) → GRANTED
  updates schema metadata
  commits → releases lock

T2 (APPEND to orders):
  no lock needed for Append normally
  BUT: T1 holds ACCESS_EXCLUSIVE, which conflicts with APPEND
  T2's commit: CAS check will succeed (appends don't conflict with schema changes at metadata level)
```

Actually, Iceberg schema evolution is a metadata-only operation — no data files are rewritten. The schema change creates a new metadata JSON file with an updated schema definition. Data files use column IDs (not names), so they remain valid under the new schema without rewriting.

In practice, DDL in Iceberg does **not** block concurrent DML:

```
T1 (schema evolution, addColumn):
  writes new metadata JSON (schema version N+1)
  commits → txn_id=5001

T2 (APPEND, writes files using old schema N):
  writes Parquet files to S3
  commits → txn_id=5002 (base metadata may now be at N+1)
  CAS: base=N, current=N+1 → CONFLICT? No — column addition is backward compatible
       Iceberg allows appending old-schema files to new-schema tables
       → SUCCESS
```

The `ACCESS_EXCLUSIVE` lock for DDL is only needed for **destructive schema changes** that would make existing data files invalid — operations that don't currently exist in Iceberg's schema evolution model (Iceberg prohibits non-backward-compatible changes like column type narrowing).

---

### Example 8: Orchestrator Parallel Write — Shared txn_id Across Applications

**Scenario**: A nightly ETL pipeline loads three tables (`orders`, `customers`, `products`) from source systems. All three must be updated atomically — either all three land together or none do — so downstream analytics never see a partial load. Each table load is a separate Spark application for independent resource management and fault isolation.

```
-- Orchestrator starts the transaction and acquires all locks upfront:
Orchestrator:
  POST /v1/transactions  { isolation: REPEATABLE_READ, lease_seconds: 7200 }
  ← { txn_id: T1, txn_token: <TOP_SECRET>, expires_at: +2h }

  POST /v1/transactions/T1/subtxn  { table: "orders" }
  ← { subtxn_id: S1, subtxn_token: <tok1>,
      staging_path: "s3://catalog/staging/T1/S1/metadata.json",
      base_snapshot_id: 42 }
  (server acquires APPEND lock on orders)

  POST /v1/transactions/T1/subtxn  { table: "customers" }
  ← { subtxn_id: S2, subtxn_token: <tok2>,
      staging_path: "s3://catalog/staging/T1/S2/metadata.json",
      base_snapshot_id: 17 }
  (server acquires APPEND lock on customers)

  POST /v1/transactions/T1/subtxn  { table: "products" }
  ← { subtxn_id: S3, subtxn_token: <tok3>,
      staging_path: "s3://catalog/staging/T1/S3/metadata.json",
      base_snapshot_id: 31 }
  (server acquires APPEND lock on products)

-- Orchestrator distributes credentials (txn_token NOT shared):
  → SparkApp1 receives: { txn_id:T1, subtxn_id:S1, subtxn_token:tok1, staging_path:S1_path }
  → SparkApp2 receives: { txn_id:T1, subtxn_id:S2, subtxn_token:tok2, staging_path:S2_path }
  → SparkApp3 receives: { txn_id:T1, subtxn_id:S3, subtxn_token:tok3, staging_path:S3_path }

-- Three Spark apps run in parallel (no inter-app communication):
SparkApp1                   SparkApp2                   SparkApp3
  load orders from source     load customers from source  load products from source
  write Parquet to S3         write Parquet to S3         write Parquet to S3
  build metadata JSON         build metadata JSON         build metadata JSON
  PUT s3://.../S1/metadata    PUT s3://.../S2/metadata    PUT s3://.../S3/metadata
  POST T1/subtxn/S1/commit    POST T1/subtxn/S2/commit    POST T1/subtxn/S3/commit
    (auth: tok1)                (auth: tok2)                (auth: tok3)
  ← SUB_COMMITTED             ← SUB_COMMITTED             ← SUB_COMMITTED

-- Orchestrator commits after all three report success:
  POST /v1/transactions/T1/commit  (auth: txn_token=<TOP_SECRET>)
  ← Server (single DB transaction):
      conflict-check S1, S2, S3 base_snapshot_ids
      UPDATE table_pointers for orders, customers, products atomically
      INSERT committed_table_snapshots for all three
      UPDATE transactions SET status=COMMITTED
      DELETE locks for T1
  ← COMMITTED

-- All three tables become visible simultaneously. ✓
-- Readers using txn_snapshot_id acquired after commit see all three updated. ✓
```

**Failure: SparkApp2 crashes mid-run**

```
SparkApp1: SUB_COMMITTED ✓
SparkApp2: FAILED (crash — never calls subtxn/S2/commit)
SparkApp3: SUB_COMMITTED ✓

Orchestrator detects App2 failure (callback / polling):
  POST /v1/transactions/T1/abort  (auth: txn_token=<TOP_SECRET>)
  ← Server:
      UPDATE transactions SET status=ABORTED
      DELETE locks for T1
  ← ABORTED

All three tables unchanged. S3 files written by App1 and App3 are orphans
and will be cleaned up by the orphan file reaper. ✓
```

**Why locks must be acquired by the orchestrator, not by each app:**
If App1 acquired its own `APPEND` lock on orders and App2 acquired its own lock on customers, a concurrent multi-table transaction acquiring the same two tables in opposite order could deadlock. The orchestrator acquires all locks in a canonical order (e.g., sorted by `table_uuid`) before any app starts, preventing deadlocks entirely.

---

## Failure Handling and Dead Transaction Detection

### The Core Principle

**Safety does not depend on failure detection.** The CAS commit check (`base_snapshot_id must match current`) catches all data conflicts regardless of whether the catalog server correctly identifies a client as dead. The worst outcome of a false positive (incorrectly declaring a live client dead) is a wasted retry — not data corruption.

### Transaction Expiry

Every transaction is issued with a hard `expires_at` deadline at `BEGIN` time:

```
POST /v1/transactions
{ "isolation": "REPEATABLE_READ", "lease_seconds": 3600 }

← { "txn_id": 5001, "expires_at": "2025-02-20T11:00:00Z" }
```

The client must commit or abort before `expires_at`. The reaper process aborts expired transactions automatically:

```sql
UPDATE transactions
SET status = 2, ended_at = now()
WHERE status = 0 AND expires_at < now();
```

This provides a hard liveness guarantee: no dead client can hold locks or pin the watermark indefinitely. The maximum blocking duration for a waiting writer is bounded by the lease duration of the transaction it is waiting on.

### Optional Lease Renewal

Clients that genuinely need long-running transactions can renew their lease:

```
POST /v1/transactions/5001/renew
{ "extend_seconds": 3600 }
```

Renewal is opt-in. Engines that do not implement it still work correctly with a fixed deadline. This avoids the "mandatory heartbeat" buy-in problem while still supporting long-running operations.

### Orphaned Object Storage Files

When a transaction is aborted (client crash, expiry, explicit abort), data files already written to object storage become orphaned — they are not referenced by any committed snapshot. The catalog server registers these for cleanup:

```sql
SELECT staged_metadata_location
FROM subtransactions
WHERE parent_txn_id IN (
    SELECT txn_id FROM transactions
    WHERE status = 2
      AND ended_at > now() - interval '1 hour'
)
AND staged_metadata_location IS NOT NULL;
```

The staged metadata files reference their data files. A cleanup job reads the staged metadata and deletes both the metadata file and its referenced data files from object storage.

---

## Transaction Table Maintenance

### The Watermark

The watermark is the oldest `txn_id` that any active snapshot or in-progress transaction still references. CLOG entries, subtransactions, and lock records older than the watermark can be safely purged.

Two sources pin the watermark:

1. **Active txn snapshots** (`snapshots` table) — a Spark read job that acquired a snapshot pins the watermark at `snapshot.xmin` for its job duration. Long-running read jobs are the primary source of watermark lag.
2. **In-progress write transactions** (`transactions` table) — an in-progress write pins at its own `txn_id`. Released automatically on commit, abort, or expiry.

```sql
-- Compute new watermark: minimum across both sources
UPDATE txn_watermark
SET watermark_txn_id = (
    SELECT LEAST(
        -- Pure readers: txn_snapshots pin at xmin
        COALESCE(
            MIN(s.xmin) FROM txn_snapshots s
            WHERE s.released_at IS NULL AND s.expires_at > now(),
            9223372036854775807
        ),
        -- Write transactions pin at txn_id
        COALESCE(
            MIN(t.txn_id) FILTER (WHERE t.status = 0),
            9223372036854775807
        )
    )
),
updated_at = now()
WHERE id = 1;
```

Long-running read jobs holding a snapshot pin the watermark and delay purging of old history. This is an **operational cost, not a correctness or liveness problem** — new transactions commit freely regardless. The `expires_at` on snapshots bounds how long any job can pin the watermark.

### Purging

```sql
-- Safe to delete: committed/aborted transactions below the watermark
DELETE FROM transactions
WHERE txn_id < (SELECT watermark_txn_id FROM txn_watermark)
  AND status IN (1, 2);

DELETE FROM subtransactions
WHERE parent_txn_id < (SELECT watermark_txn_id FROM txn_watermark);

-- Purge released/expired txn snapshots whose xmax is fully below the watermark
DELETE FROM txn_snapshots
WHERE xmax < (SELECT watermark_txn_id FROM txn_watermark)
  AND (released_at IS NOT NULL OR expires_at < now());

DELETE FROM locks
WHERE txn_id < (SELECT watermark_txn_id FROM txn_watermark);

-- Purge the MVCC-to-Iceberg mapping for table snapshots superseded below the watermark.
-- Keep at least one row per table (the most recent) even if it is below the watermark,
-- so that a fresh unregistered reader can always find the current table snapshot.
DELETE FROM committed_table_snapshots c1
WHERE committed_txn_id < (SELECT watermark_txn_id FROM txn_watermark)
  AND EXISTS (
      -- A more recent committed snapshot for this table exists
      SELECT 1 FROM committed_table_snapshots c2
      WHERE c2.table_uuid = c1.table_uuid
        AND c2.committed_txn_id > c1.committed_txn_id
  );
```

Long-running read jobs holding a `TxnSnapshot` pin the watermark and slow purging of both the transaction history and the `committed_table_snapshots` mapping — an operational cost, not a correctness or availability problem. The `expires_at` on `TxnSnapshot` expires_at bounds how long any job can pin the watermark.

---

## REST API

### TxnSnapshot Management

Used by pure readers that need a consistent multi-table view. No `txn_id` or CLOG entry is created. The `TxnSnapshot` captures `(xmin, xmax, xip_list)` and is used by the server to resolve the correct Iceberg table snapshot via `committed_table_snapshots`.

```
POST   /v1/txn-snapshots
  ← { txn_snapshot_id, xmin, xmax, xip_list, expires_at }

DELETE /v1/txn-snapshots/{txn_snapshot_id}          Release (advance watermark)
POST   /v1/txn-snapshots/{txn_snapshot_id}/renew    Extend expiry for long-running jobs
```

Pass `txn_snapshot_id` on every table read to get a consistent point-in-time view across all tables:

```
GET /v1/{prefix}/namespaces/{ns}/tables/{table}?txn_snapshot_id={txn_snapshot_id}
```

The server resolves the Iceberg table snapshot using:
```sql
SELECT table_snapshot_id, metadata_location
FROM committed_table_snapshots
WHERE table_uuid = <table>
  AND committed_txn_id < xmax
  AND committed_txn_id != ALL(xip_list)
ORDER BY committed_txn_id DESC LIMIT 1;
```

### Write Transaction Management

Used by the orchestrator to begin, finalize, or cancel a top-level transaction. Creates a CLOG entry. Returns `txn_token` — hold it securely; it is never returned again.

```
POST   /v1/transactions                              Begin a write transaction
  → { isolation: "REPEATABLE_READ", lease_seconds: 3600 }
  ← { txn_id, txn_token, expires_at }
    -- txn_token is the top-level bearer secret (orchestrator only)
    -- TxnSnapshot embedded server-side for REPEATABLE_READ

GET    /v1/transactions/{txn_id}                     Get transaction status (no token required)
POST   /v1/transactions/{txn_id}/commit              Commit top-level transaction
  → requires txn_token
POST   /v1/transactions/{txn_id}/abort               Abort transaction
  → requires txn_token
POST   /v1/transactions/{txn_id}/renew               Extend lease
  → requires txn_token
```

### Subtransaction Management

The orchestrator creates subtransactions (requires `txn_token`). Each subtransaction returns its own `subtxn_token` — distributed to the assigned application; the orchestrator does not need to retain it.

```
POST   /v1/transactions/{txn_id}/subtxns             Create a subtransaction for a table
  → requires txn_token
  → { table_uuid, operation: "APPEND" | "ROW_DELTA" | "OVERWRITE" }
  ← { subtxn_id, subtxn_token,
      base_snapshot_id,            -- Iceberg snapshot at subtxn start
      staging_path }               -- server-assigned S3 path for staged metadata
    -- subtxn_token: per-subtxn bearer secret; distribute to assigned app only

-- The following require subtxn_token (not txn_token):
POST   /v1/transactions/{txn_id}/subtxns/{id}/commit Stage subtransaction (SUB_COMMITTED)
  → requires subtxn_token
  → { staged_metadata_location }   -- must equal the server-assigned staging_path
POST   /v1/transactions/{txn_id}/subtxns/{id}/renew  Extend subtxn lease
  → requires subtxn_token
POST   /v1/transactions/{txn_id}/subtxns/{id}/abort  Abort one subtransaction
  → requires subtxn_token OR txn_token
```

### Lock Management

```
POST   /v1/locks                                     Acquire a lock
DELETE /v1/locks/{lock_id}                           Release a lock (rarely needed; commit/abort handles this)
GET    /v1/tables/{table_uuid}/locks                 Inspect active locks on a table
```

### Modified Table Metadata Endpoint

```
GET /v1/{prefix}/namespaces/{namespace}/tables/{table}
    ?snapshot_id={snapshot_id}   optional: read at a specific snapshot point
    &txn_id={txn_id}             optional: read-your-own-writes within a transaction
```

When `txn_id` is provided, the server returns the staged metadata for that transaction's subtransaction (if `SUB_COMMITTED`), enabling read-your-own-writes within a multi-table transaction.

---

## Design Decisions and Trade-offs

### Why Pessimistic Locking for Multi-Table Writes?

Multi-table transactions cannot use pure optimistic concurrency because partial retry is not possible. If a 3-hour write to table A succeeds (SUB_COMMITTED) but the write to table B conflicts, the client cannot simply retry table B — it would need to re-execute the entire transaction from scratch, wasting the table A work. Acquiring locks upfront prevents this wasted work entirely.

Single-table operations retain the optimistic model as the default, with pessimistic locking available as an opt-in.

### Why 64-bit Transaction IDs?

PostgreSQL's 32-bit XID wraparound is one of the most operationally painful limitations in production PostgreSQL deployments. At 1 billion transactions per second, a 64-bit counter runs for 292,000 years without wrapping. The transaction ID format for Iceberg is not stored in data files (unlike PostgreSQL's `xmin`/`xmax` stored in every heap tuple), making this a catalog schema decision with zero migration cost. There is no reason to repeat PostgreSQL's 32-bit mistake.

### Why Not Distributed Catalog?

The REST catalog server is a single logical node backed by a relational database. This is not a limitation — it is the architectural advantage that makes the entire transaction model tractable. All lock state changes, CLOG updates, and snapshot acquisitions are serialized by the catalog DB's own ACID transactions. There is no shared-memory contention, no distributed consensus protocol needed for lock acquisition, and no WAL replay on server restart. The "startup recovery" problem that drove much of PostgreSQL's complexity simply does not exist.

### What Happens to Existing Single-Table Optimistic Commits?

Nothing changes. The existing `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` commit endpoint continues to work exactly as today via optimistic CAS. The new transaction system is purely additive. Engines that do not implement the new transaction protocol continue to work without modification.

### Scope: REST Catalog Only

Multi-table transaction support is scoped to the REST catalog. Hive Metastore, AWS Glue, and JDBC catalog implementations would require their own approaches and are out of scope for this design. The REST catalog's stateful server model is what makes the transaction coordinator pattern feasible.

### The Server Process Stays Stateless Even Though Transactions Are Stateful

A common objection to this design is that it makes the catalog server "stateful," which conflicts with Iceberg's operational philosophy of keeping the catalog thin and horizontally scalable.

The resolution is a precise distinction:

- **Transaction state** is stateful: lock records, CLOG entries, `TxnSnapshot` rows, subtransaction progress. These must persist and be globally consistent.
- **The catalog server process** is stateless: it holds no in-memory transaction state between requests. Every request reads and writes exclusively from the shared backing database.

Because all state lives in the catalog DB, any server replica can handle any request for any transaction — the load balancer needs no session affinity, replicas can be added or removed freely, and a server crash loses no state (the DB survives). This is not meaningfully different from how a stateless web service backed by a shared database works today.

The operational cost is the **backing database**, which must be highly available and scalable. This is already a known requirement for production REST catalog deployments.

### Why Transaction Coordination Cannot Be Pushed to the Engine or Client Side

Iceberg's philosophy pushes coordination to the engines (Spark, Flink, Trino) and keeps the catalog thin. A reasonable question is: can multi-table atomicity be handled by the engine without catalog changes?

**It cannot, for three fundamental reasons:**

**1. The catalog is the only source of truth for table pointers.**
Atomically swapping metadata pointers for two tables requires a single transactional write that updates both pointers simultaneously. Engines see the catalog as a remote API. There is no mechanism for an engine to issue a "swap A and B atomically" call — and if we added such a call, we would have invented a stateful catalog transaction endpoint anyway.

**2. Conflict detection requires global visibility the engine does not have.**
To detect that another writer committed to table B between our subtransaction start and our commit, the server must compare the current metadata pointer against the `base_snapshot_id` captured at `BEGIN` time. The engine knows its own `base_snapshot_id` but cannot atomically check-and-swap against the live catalog pointer without a round-trip that the server must serialize. Two engines racing to commit to the same table cannot resolve the conflict among themselves — only the catalog, as the single writer of table pointers, can adjudicate.

**3. Lock coordination requires a neutral third party.**
If engine A holds a `ROW_DELTA` lock on table T and engine B wants `OVERWRITE` on the same table, they cannot negotiate directly — engines have no knowledge of each other. A central lock table in the catalog is the only place where this can be tracked consistently. Peer-to-peer locking between engines would require either a distributed lock service (ZooKeeper, etcd) or a gossip protocol — both of which are far heavier than a lock table in an existing relational DB.

**In short:** multi-table atomicity, conflict serialization, and lock coordination all require a single serialization point with global visibility over table state. The catalog server — already the canonical authority on table metadata — is the only viable place for that serialization point. Pushing it to the engine side would require rebuilding the same mechanism in a distributed, multi-engine context, which is strictly harder.

---

## Security Considerations

### Transaction Ownership and Token Hierarchy

The most direct attack: writer B learns writer A's `txn_id=T1` and calls `POST /v1/transactions/T1/commit` to finalize data under A's identity. Or, in a distributed pipeline, one Spark app uses its `subtxn_token` to commit the entire top-level transaction, bypassing the orchestrator.

**Two-tier token model:**

The design issues two distinct token types with different authority scopes:

| Token | Held by | Authorizes |
|---|---|---|
| `txn_token` | Orchestrator only | `POST T1/commit`, `POST T1/abort`, `POST T1/subtxn` (create new subtxns), `POST T1/renew` |
| `subtxn_token` | Individual app | `POST T1/subtxn/Sn/commit`, `POST T1/subtxn/Sn/renew` — for its own subtxn only |

An app holding only a `subtxn_token` cannot:
- Commit or abort the top-level transaction.
- Create additional subtransactions.
- Access or modify another app's subtransaction.

This mirrors the principle of least privilege: each participant holds only the capability it needs to do its specific work.

**Implementation:**

```sql
-- Already in the schema (transactions table):
txn_token   BYTEA NOT NULL  -- gen_random_bytes(32); orchestrator only

-- Already in the schema (subtransactions table):
subtxn_token BYTEA NOT NULL -- gen_random_bytes(32); per-app; scoped to one subtxn
```

Server enforcement per endpoint:

```
POST /v1/transactions/T1/commit        → require txn_token
POST /v1/transactions/T1/abort         → require txn_token
POST /v1/transactions/T1/subtxn        → require txn_token
POST /v1/transactions/T1/renew         → require txn_token
POST /v1/transactions/T1/subtxn/Sn/commit → require subtxn_token for Sn
POST /v1/transactions/T1/subtxn/Sn/renew  → require subtxn_token for Sn
```

**Sequential `txn_id` as an oracle:** `txn_id` values are monotonically increasing and guessable. The `txn_token` requirement makes enumeration attacks useless — knowing the ID without the token grants no capability. Similarly for `subtxn_id` and `subtxn_token`.

**`owner_principal` as backstop:** even with the token model, the server also records `owner_principal` (the authenticated identity that called `BEGIN`) in the `transactions` table. This enables audit logging, admin override (force-abort by an operator), and serves as a fallback check when token-based auth is not in use.

---

### Staged Metadata Trust Boundary

The client writes staged metadata JSON to object storage and passes the `staged_metadata_location` path to the server. The server then atomically sets that path as the live table pointer on commit.

**Attacks:**
- Pass a path to a pre-existing valid metadata file (rolling back another writer's committed work).
- Pass a path to a maliciously crafted metadata file that: drops columns, changes partition spec, rewrites file lists to reference files from another table, or inflates statistics.
- Pass a path the client does not own (e.g., another namespace or another table's staging area).

**Mitigations:**

1. **Server-assigned staging paths within the table's canonical metadata directory.**
   Rather than letting the client choose where to stage metadata, the server assigns a path at subtransaction creation time:

   ```
   POST /v1/transactions/T1/subtxn
   ← { subtxn_id, staging_path: "s3://warehouse/ns/table/metadata/<txn_id>-<subtxn_id>-<uuid>.metadata.json" }
   ```

   The assigned path lives **inside the table's own metadata directory** — not in a separate global staging area. This is correct because:
   - Iceberg visibility is pointer-based: the file is unreachable to all readers until `commitTransaction` updates the catalog pointer. Its physical location is irrelevant to visibility.
   - Credential vending and location validation work without special-casing: the path is already within the table's declared storage boundary.
   - Orphan cleanup on abort uses standard Iceberg `DeleteOrphanFiles` — the same mechanism that handles failed single-table commits today.
   - Collision between concurrent transactions is prevented by including `txn_id` and `subtxn_id` in the filename.

   The client must write to exactly that path. The server validates `staged_metadata_location` starts with `assigned_staging_path` at subtxn commit time. This prevents path traversal to arbitrary catalog metadata locations.

2. **Server-side metadata validation.** Before accepting a subtxn commit, the server reads and parses the staged metadata JSON and verifies:
   - It is valid Iceberg table metadata.
   - The `table-uuid` matches the table being written.
   - The `parent-snapshot-id` matches `base_snapshot_id` captured at subtxn start (no hidden intermediate snapshots).
   - Schema changes are within the principal's permitted DDL operations.
   - Referenced data files are within the table's declared warehouse path.

3. **Object storage ACLs.** The staging path prefix (`catalog-bucket/staging/T1/`) should be writable only by the client that holds `T1`, enforced by IAM roles or presigned URLs issued by the catalog at subtxn creation time. Paths outside the staging prefix should be read-only to catalog clients.

---

### Lock-Based Denial of Service

A malicious or buggy client acquires many locks (e.g., `EXCLUSIVE` on dozens of tables) and then either crashes or never commits. Other writers are blocked for the duration of the transaction's `expires_at`.

**Mitigations:**

- **Short default TTL.** Transactions default to a short `expires_at` (e.g., 30 minutes). Long-running transactions must explicitly opt in and renew leases.
- **Lock count limit.** The server enforces a maximum number of locks per transaction (e.g., 100 tables) to prevent bulk lock acquisition attacks.
- **Rate limiting on `BEGIN`.** Limit the number of concurrent `IN_PROGRESS` transactions per principal.
- **Reaper.** The background reaper aborts expired transactions and releases their locks, bounding the maximum blast radius of a hung or malicious client.

Note that the reaper is the same mechanism that handles crashed clients — there is no separate "malicious client" code path.

---

### TxnSnapshot Watermark Pinning

A client creates many `TxnSnapshot`s with the maximum allowed `expires_at`, then never releases them. The watermark (`xmin` across all active snapshots) is pinned at an old value, preventing purging of the CLOG, `committed_table_snapshots`, and `subtransactions` tables. The catalog DB grows unboundedly.

**Mitigations:**

- **Maximum snapshot TTL.** Cap `expires_at` to a reasonable maximum (e.g., 24 hours for batch jobs, configurable by admin).
- **Snapshot count limit per principal.** Prevent a single client from holding an unlimited number of active snapshots simultaneously.
- **Admin force-expiry.** Operators can explicitly release a snapshot via an admin API, advancing the watermark regardless of `expires_at`.

---

### Privilege Escalation via Subtransaction Table Access

A writer may be authorized to write to table A but not table B. By creating a multi-table transaction and adding a subtransaction for table B, can they write to B?

**No — table-level authorization is enforced independently at subtransaction creation time.**

When `POST /v1/transactions/T1/subtxn` is called for table B, the server checks whether the authenticated principal has `WRITE` privilege on table B, using the same authorization path as a single-table commit. The transaction envelope does not grant any additional privileges. A subtransaction to a table the caller cannot write to is rejected with `403 Forbidden`.

This means the overall authorization model for multi-table transactions is:

```
Can the principal commit T1?
  ← principal == T1.owner_principal
  AND for each subtxn in T1:
      principal has WRITE on subtxn.table_uuid
```

Both conditions are checked at subtransaction creation, not only at final commit.

---

### Summary

| Threat | Mitigation |
|---|---|
| TxnID hijacking (use another writer's txn) | `owner_principal` bound at `BEGIN`; opaque `txn_token` as bearer secret |
| Sequential TxnID guessing | Opaque `txn_token` required alongside `txn_id` |
| Staged metadata path traversal | Server-assigned staging paths; presigned S3 URLs |
| Malicious metadata content | Server-side metadata validation before subtxn commit |
| Lock-based DoS | Short TTL; lock count limit; rate limiting; reaper |
| Watermark pinning via snapshot accumulation | Max snapshot TTL; count limit per principal; admin force-expiry |
| Privilege escalation to unauthorized tables | Table-level `WRITE` authorization checked at subtxn creation |
