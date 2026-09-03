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

> **Key insight — two kinds of "snapshot"**: The word "snapshot" has two distinct meanings in this system. A **`TxnSnapshot`** carries a single **`read_point`** — a monotonic commit-order value (a *commit timestamp*) captured when the snapshot is taken. It determines which committed table states a reader can see: a committed table snapshot is visible iff its `commit_scn <= read_point`. An **Iceberg table snapshot** (`table_snapshot_id`) is the native Iceberg point-in-time state of a table's data files. The catalog server maintains a `committed_table_snapshots` mapping table that translates a `read_point` into the specific Iceberg table snapshot to serve for each table. Pure readers receive an opaque **`txn_snapshot` handle** — lightweight, lock-free, no CLOG entry — and pass it on every table read; the server decodes the handle and resolves the correct Iceberg table snapshot automatically. A query that reads multiple tables **must** use a shared `TxnSnapshot`; without one, successive reads can observe different committed states and produce a torn cross-table view.
>
> This is deliberately *simpler* than PostgreSQL's `pg_snapshot` `(xmin, xmax, xip_list)` triple. See [Why a single `read_point`](#why-a-single-read_point-and-not-xmin-xmax-xip) for why the triple is unnecessary in this model.

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

The CLOG is the authoritative record of transaction *lifecycle* — it governs commit/abort and write-conflict decisions. It is **not** consulted on the read path. Because a `committed_table_snapshots` row is written **only at commit** (an aborted or in-flight transaction leaves no row — see [Abort: No Undo Log Needed](#abort-no-undo-log-needed)), the mere existence of a mapping row already means "committed." A reader therefore never needs to ask the CLOG whether a writer committed: it compares `commit_scn <= read_point` against the mapping table and is done. This is the structural difference from PostgreSQL that lets us drop `(xmin, xmax, xip)` — see [Why a single `read_point`](#why-a-single-read_point-and-not-xmin-xmax-xip).

### Subtransaction + Operations

Each table involved in a multi-table transaction is staged by an **operation** — a child unit of work scoped to exactly one table. An operation's lifecycle is:

```
IN_PROGRESS   → the client is writing data files to object storage
COMMITTED     → client has staged metadata; waiting for parent to commit  (a.k.a. SUB_COMMITTED)
ABORTED       → operation failed, its subtransaction was rolled back, or the parent was aborted
```

Operations inherit their parent's `txn_id` for ordering, but each has its own `op_id` (and per-operation `op_token`) for fine-grained tracking. A top-level `COMMIT` is only executed when every *live* operation has reached `COMMITTED`.

**Why the extra subtransaction level.** A single per-table row cannot express "these three tables succeed or fail *together*, and can be retried as a group by a different worker." The **subtransaction** is that succeed-or-fail / retry boundary:

```
OPEN     → accepting operations; the subtransaction's tables are being written
STAGED   → all operations COMMITTED and locks released; ready for parent COMMIT
ABORTED  → the subtransaction was rolled back; its operations are discarded and skipped at COMMIT
```

Rolling a subtransaction back aborts **exactly its operations** (and releases their locks), leaving other subtransactions' staged work untouched — so only the failed tables are re-run, not the whole transaction. A rollback can optionally **re-open** the subtransaction (`attempt++`) with *fresh operation rows and fresh staging paths*, so the same logical subtransaction can be re-staged (possibly by a different worker) without colliding with the orphaned files from the failed attempt. This one object serves both:

- **Cross-process parallel ingestion** (Use Case 2): different workers write the subtransaction's operations; any of them can retry the subtransaction.
- **Single-session PostgreSQL-style savepoints**: one session creates a named subtransaction (`SAVEPOINT <name>`), stages it, and can `ROLLBACK TO` it before `COMMIT`.

A **bare inline operation** (Use Case 1, single-session commit) has `subtxn_id = NULL` — it belongs to no subtransaction and is finalized directly at the top-level `COMMIT`.

### Terminology: Two Kinds of "Snapshot"

The word "snapshot" has two distinct meanings in this system and must not be confused:

| Term | PostgreSQL Equivalent | What It Is | Lives In |
|---|---|---|---|
| **`TxnSnapshot`** | a commit timestamp (cf. Percolator's timestamp oracle) | The read visibility point: a single **`read_point`**. Determines which committed table states are visible to a reader. | Catalog server (`txn_snapshots` table or embedded in `transactions` row) |
| **Table Snapshot** | N/A — Iceberg-native | Iceberg's point-in-time state of a table's data files, identified by `table_snapshot_id`. Created by every Iceberg write commit. | Iceberg metadata JSON on object storage |

These two are related but separate: a `TxnSnapshot` identifies a logical point in *transaction* history; a table snapshot is the concrete Iceberg metadata state that corresponds to that point for a *specific table*. The `committed_table_snapshots` mapping table bridges between them.

### TxnSnapshot (read visibility point)

A `TxnSnapshot` is taken at the moment a reader or snapshot-isolation (REPEATABLE READ) writer begins its operation. It captures a single value:

- `read_point`: the commit-order high-water-mark at snapshot time — the largest `commit_scn` that has durably committed. (Concretely, the current value of the catalog's `commit_scn_counter`.)

**Visibility rule**: a committed table snapshot `C` is visible to `TxnSnapshot` `RS` if and only if:

```
C.commit_scn <= RS.read_point
```

This tells you which committed states a reader can see. To turn it into the concrete Iceberg table snapshot to return, apply it against the mapping table described below (take the newest visible row per table).

### Why a single `read_point` (and not xmin/xmax/xip)

PostgreSQL needs the `(xmin, xmax, xip_list)` triple because visibility is judged **per row**: every heap tuple carries the XID that created it and the XID that deleted it, and aborted/in-flight tuples physically coexist with committed ones. A snapshot must therefore ask, per tuple, "did my creator commit as-of me, and my deleter not?" — and `xip` is how it excludes transactions that were still in flight when the snapshot was taken.

Polaris has no such coexistence. The unit of visibility is a whole-table commit, recorded in `committed_table_snapshots` **only at commit time**; abort writes nothing there. So:

- **Existence of a mapping row already means "committed"** — there is nothing aborted to filter out, so `xip` has no job.
- The only remaining question is **ordering** — did this commit land before or after my snapshot — which a single monotonic cutoff answers completely.
- The purge horizon is served directly and more precisely by `min(read_point)` over live snapshots — see [`txn_watermark`](#txn_watermark--purge-horizon).

**Commit-timestamp invariant (load-bearing).** `read_point` is exact *iff* the number on each mapping row — `commit_scn` — is a *commit-order* value assigned **atomically with the durable metadata-pointer swap**, never a begin-time `txn_id`. Two hazards make this non-trivial, and both trace to the same root: nothing inherently serializes the *allocate → commit* interval, so a drawn number's order need not match commit order.

1. *Assign-before-visible window*: if the number is drawn before the row commits, a reader can capture `read_point >= commit_scn` while that row is still invisible.
2. *Out-of-order-commit anomaly*: if two writers draw ids `n < n+1` but commit in the reverse order, a pinned `REPEATABLE_READ` snapshot can later admit the lower id that actually committed *after* it — an anomaly a single cutoff cannot exclude without an `xip` set.

A non-transactional SQL `SEQUENCE` cannot fix either: its `nextval` advances outside the caller's transaction (visible immediately, never rolled back), so it is an *allocator*, not a commit clock. (This is also why the earlier `xip` was always inert: it tracked begin-time `txn_id`s while mapping rows carried commit-time ids — two disjoint id spaces, so `NOT IN (xip)` never matched. The anomaly predates and survives dropping `xip`.)

**Implementation — `commit_scn` is a transactional per-catalog counter.** Each `(realm, catalog)` has a `commit_scn_counter` row. A commit does `UPDATE commit_scn_counter SET scn = scn + 1 RETURNING scn` **inside the same transaction as the pointer swap and the CTS-row insert**. The `UPDATE` takes a row lock held until that transaction commits, so a higher `commit_scn` is guaranteed to become visible only *after* every lower one — **`commit_scn` order == commit order, by construction**. Then `read_point = current(commit_scn_counter)` (a plain committed read) can never exceed a not-yet-visible commit, and `commit_scn <= read_point` ⇔ "committed at or before my read point." Both hazards are closed; no `xip` is needed. The trade is that commits **serialize on the counter within a catalog** (different catalogs commit concurrently) — the SQL analog of Oracle's SCN / Percolator's timestamp oracle. This fits Iceberg's atomic-pointer-swap storage model exactly.

### Table Snapshot (Iceberg Native)

Every time a writer commits to an Iceberg table, Iceberg creates a new **table snapshot**: an immutable record in the table's metadata JSON that captures the exact set of data files at that point. Table snapshots are identified by a `table_snapshot_id` (a long integer already part of Iceberg's existing format).

Table snapshots are what Spark, Trino, and other engines actually read from — they load the table metadata, find the current `table_snapshot_id`, and scan the corresponding data files.

### The Mapping: TxnSnapshot → Table Snapshot

The catalog server maintains a record of which `commit_scn` produced each table snapshot. This is the bridge between the transaction ordering and the Iceberg world:

```
TxnSnapshot RS (read_point=5050)
         │
         │  "which commit produced the most recent table snapshot
         │   visible to RS for table orders?"
         ▼
committed_table_snapshots
  table=orders, commit_scn=5040, iceberg_snapshot_id=42   ← visible (5040 <= 5050)
  table=orders, commit_scn=5048, iceberg_snapshot_id=43   ← most recent visible (5048 <= 5050)
  table=orders, commit_scn=5055, iceberg_snapshot_id=44   ← NOT visible (5055 > 5050)
         │
         │  answer: iceberg_snapshot_id=43
         ▼
Iceberg metadata JSON for orders at iceberg_snapshot_id=43
→ scan data files
```

The mapping query:

```sql
-- Given `TxnSnapshot` RS, find the Iceberg table snapshot to serve for a given table
SELECT iceberg_snapshot_id, metadata_location
FROM committed_table_snapshots
WHERE table_uuid = :table_uuid
  AND commit_scn <= :read_point    -- committed at or before RS was taken
ORDER BY commit_scn DESC
LIMIT 1;
```

Without this mapping table, the catalog cannot answer "which table snapshot was current at the time this `TxnSnapshot` was taken?" — it would have no way to translate the `read_point` into a concrete Iceberg metadata pointer.

### Do Readers Get a TxnID?

No. A pure reader does **not** get a `txn_id`. It gets a **`txn_snapshot` handle** — an opaque string encoding `{v, r}` (version + `read_point`) with no CLOG entry, no lock capability, and no write semantics. This is analogous to PostgreSQL's **virtual transaction IDs**: a real XID is only assigned when a transaction first writes. Read-only transactions consume no XID.

The handle is `base64url(JSON {"v":1,"r":<read_point>})` — URL-safe, unpadded, and opaque to the client. It encodes the read-point for opacity and evolvability (like a pagination cursor), not security. In a trusted-engine PoC, the handle is unsigned; a multi-tenant deployment would add an HMAC. Fail-closed: any malformed or tampered handle is rejected with `400 Bad Request`, never silently resolved to a default.

There are three cases:

| Case | Gets | Watermark contribution |
|---|---|---|
| Pure reader (SELECT) | `txn_snapshot` handle only (no server row) | read floor: `MAX(commit_scn WHERE committed_at <= now - L)` |
| Pure writer (optimistic append) | `txn_id` only | `MIN(txn_id) WHERE status=IN_PROGRESS` |
| Read-write transaction (REPEATABLE READ, multi-table) | `txn_id` + embedded `read_point` (server row kept) | `MIN(read_point)` |

A read-write transaction stores its `read_point` directly in the `transactions` row. All reads within that transaction use this embedded `read_point`, giving snapshot isolation (REPEATABLE READ) semantics. Read-your-own-writes is layered on top independently (staged metadata for the transaction's own `SUB_COMMITTED` subtransactions) and does not depend on the `read_point` — see the `txn_id` row in the next table.

### What Table Snapshot Does a Reader Get?

When a client reads a table, the server resolves the appropriate Iceberg table snapshot as follows:

| Request | read_point Used | Table Snapshot Returned |
|---|---|---|
| `GET /tables/A` (no txn_snapshot) | Fresh: `read_point = current commit high-water-mark` | Latest committed table snapshot |
| `GET /tables/A?txn_snapshot=<handle>` | Decoded from handle (fail-closed) | Most recent table snapshot with `commit_scn <= decoded.read_point` |
| `GET /tables/A?txn_id=T` | Embedded `read_point` in transaction T's server row | Most recent visible + RYOW (staged metadata if T has SUB_COMMITTED subtxn) |

**For any query that reads more than one table, the client must acquire a `TxnSnapshot` first and pass it on every read.** Without a shared `TxnSnapshot`, two successive GETs can observe different committed states and produce a torn cross-table view.

The correct multi-table read pattern:

```
1. POST /v1/txn-snapshots                              → txn_snapshot=<handle>, read_point=5050
2. GET  /v1/tables/orders?txn_snapshot=<handle>        → table_snapshot_id=42 (visible to RS)
3. GET  /v1/tables/customers?txn_snapshot=<handle>     → table_snapshot_id=17 (visible to RS)
   Both tables are consistent at the same logical point in transaction history.
   (No DELETE needed — handles are reclaimed by TTL; L=3600s default)
```

A `TxnSnapshot` handle is lightweight — no CLOG entry, no locks, no write state, no server row for standalone reads. Its only cost is advancing the GC watermark's read floor, bounded by `L` (default 3600 s).

### Relationship to Iceberg Time Travel

A common question: *Iceberg already has time travel (`FOR VERSION AS OF <snapshot-id>`, `FOR TIMESTAMP AS OF <ts>`) — doesn't that already give consistent multi-table reads?* The answer clarifies exactly what this design contributes. A consistent multi-table read decomposes into two independent problems:

1. **Resolution** — *for one logical instant, which snapshot of **each** table is the right one?* → produces a per-table map `{orders → 42, customers → 17, products → 31}`.
2. **Execution** — *read table X at its resolved snapshot* → scan the data files for that snapshot.

**Iceberg time travel solves only problem 2.** It is a per-table executor: given a snapshot-id (or a timestamp) *for a single table*, it reads that table at that snapshot. It has no notion of a cross-table instant, so on its own it cannot answer problem 1 — it cannot tell you *which* per-table snapshots line up into one consistent view.

This is why native `FOR TIMESTAMP AS OF ts1` does **not** give a consistent cross-table read: a single wall-clock `ts1` is a poor way to *derive* the resolution map. Snapshot `timestamp-ms` is the writer's wall-clock stamp (independent clocks, write-time ≠ commit-time), so the two halves of one logical multi-table transaction can straddle `ts1` → a torn view. The failure is in resolution, not execution.

**`committed_table_snapshots` + `read_point` is precisely the resolver for problem 1** that Iceberg lacks: a catalog-wide, commit-ordered index that turns one `read_point` into the correct snapshot for *every* table at that logical instant, using a single global order instead of unsynchronized wall clocks. Given a correct resolution map, native time travel *is* a perfectly consistent executor.

So the two are complementary — resolver + executor — and there are two valid ways to execute once `read_point` has resolved the map:

| Execution strategy | How | Notes |
|---|---|---|
| **Metadata-pointer pin** (default) | server returns the resolved `metadata_location`; client reads that immutable `metadata.json` directly (Spark plugin wraps it in `PinnedTableOperations`) | Strongest, engine-agnostic; no `AS OF` clause needed. The metadata file already carries schema/spec/current-snapshot as of that commit. |
| **Native time travel** | server returns the resolved `iceberg_snapshot_id` per table; client issues `FOR VERSION AS OF <id>` per table | Uses Iceberg's built-in mechanism (e.g. the Trino demo). The `iceberg_snapshot_id` column in `committed_table_snapshots` exists to supply this map. |

**Prefer snapshot-ids over timestamps for the time-travel path.** `FOR VERSION AS OF <snapshot-id>` reads an exact snapshot and is always safe. `FOR TIMESTAMP AS OF t` resolves to the newest snapshot with `timestamp-ms <= t`, so to hit a specific snapshot `S` you must choose `t` in the half-open interval `[S.timestamp-ms, next_snapshot.timestamp-ms)` — fragile under non-monotonic writer clocks, same-millisecond snapshots, or a snapshot landing between resolution and read. A per-table timestamp is therefore only safe when *derived from* the already-resolved snapshot-id — i.e. once you have solved problem 1 anyway, the timestamp is just a lossy encoding of the snapshot-id you already hold.

**Consistent `AS OF <timestamp>`, done correctly.** The design *can* offer a genuinely consistent wall-clock `AS OF ts1` — by routing it through the resolver: translate `ts1` once into a single cutoff, `read_point = MAX(commit_scn) WHERE committed_at <= ts1`, then apply that one `read_point` uniformly to all tables. This converts the unreliable per-table wall-clock lookup into the reliable global commit order. Caveat: this is only as precise as the `committed_at` clock resolution and assumes commits are timestamped monotonically at the catalog; for transactional correctness prefer handing out a `txn_snapshot` handle (which encodes the `read_point`) directly, and treat the `ts1` path as a convenience for humans who think in timestamps.

### Lock

A lock is a server-side reservation on a resource (table or partition) that prevents conflicting concurrent operations. Iceberg uses **operation-appropriate lock granularity** — the lock mode is determined by the operation being performed, not by whether the transaction is single-table or multi-table.

Locks are acquired pessimistically (before writing) for multi-table transactions and remain held until the top-level commit or abort.

---

## Data Representation

All transaction state lives in the REST catalog server's backend database. No shared memory is required. The catalog DB's own ACID transactions provide atomicity for state changes.

> The authoritative schema is `persistence/relational-jdbc/src/main/resources/{postgres,h2}/schema-v5.sql`; implementation status is tracked in [multi-table_transactions.plan.md](multi-table_transactions.plan.md).

### `transactions` — The CLOG

Only write transactions (single-table pessimistic or multi-table) get a row here. Pure readers do not.

```sql
CREATE TABLE transactions (
    txn_id           BIGINT PRIMARY KEY,
                     -- server-assigned from monotone sequence; writers only
    parent_txn_id    BIGINT REFERENCES transactions(txn_id),
                     -- NULL for top-level; non-NULL for nested transactions
    realm_id         TEXT NOT NULL,
    catalog_id       BIGINT NOT NULL,
    txn_token        TEXT NOT NULL,
                     -- opaque top-level bearer secret (random 32-byte hex);
                     -- held only by the transaction owner / orchestrator;
                     -- required to commit or abort the top-level transaction
    owner_principal  TEXT NOT NULL,
                     -- authenticated identity that called BEGIN; enforced on every operation
    status           SMALLINT NOT NULL DEFAULT 0,
                     -- 0=IN_PROGRESS, 1=COMMITTED, 2=ABORTED
    isolation        TEXT NOT NULL DEFAULT 'REPEATABLE_READ',
                     -- 'READ_COMMITTED' or 'REPEATABLE_READ'
    txn_snapshot_id  BIGINT,
                     -- FK to txn_snapshots; NULL for READ_COMMITTED.
                     -- The read_point (visibility cutoff) lives in the referenced txn_snapshots row.
    started_at       TIMESTAMP NOT NULL DEFAULT now(),
    ended_at         TIMESTAMP,
    expires_at       TIMESTAMP NOT NULL
                     -- hard deadline; reaper aborts expired transactions
);

CREATE INDEX idx_txns_realm_status ON transactions (realm_id, catalog_id, status, expires_at);
CREATE UNIQUE INDEX idx_txns_token ON transactions (txn_token);
```

### `subtransactions` — The Retry/Rollback Boundary

A **subtransaction** groups 1..N per-table operations under a single boundary that can be committed, rolled back, or re-opened for retry as a group. It is what makes both cross-process parallel ingestion and single-session `SAVEPOINT` work.

```sql
CREATE TABLE subtransactions (
    subtxn_id    BIGINT PRIMARY KEY,
    txn_id       BIGINT NOT NULL REFERENCES transactions (txn_id),
    realm_id     TEXT   NOT NULL,
    catalog_id   BIGINT NOT NULL,
    name         TEXT,                    -- optional user label; the SAVEPOINT name.
                                          -- unique per txn while the subtransaction is not aborted.
    subtxn_token TEXT   NOT NULL,         -- random 32-byte hex; held by the subtransaction driver
                                          -- (the worker/session that owns this subtransaction's operations)
    status       SMALLINT NOT NULL DEFAULT 0,
                                          -- 0=OPEN     (accepting operations)
                                          -- 1=STAGED   (all operations COMMITTED, locks released,
                                          --             ready for the parent COMMIT)
                                          -- 2=ABORTED  (rolled back; operations discarded/skipped at COMMIT)
    attempt      INT NOT NULL DEFAULT 1,  -- incremented each time a rollback re-opens the subtransaction for retry
    created_at   TIMESTAMP NOT NULL DEFAULT now(),
    expires_at   TIMESTAMP NOT NULL,
    ended_at     TIMESTAMP
);

-- a SAVEPOINT name is unique within a txn, but only among live (non-aborted) subtransactions,
-- so ROLLBACK TO + a fresh SAVEPOINT of the same name is allowed
CREATE UNIQUE INDEX subtxn_name_idx ON subtransactions (realm_id, txn_id, name)
    WHERE name IS NOT NULL AND status <> 2;
```

### `transaction_operations` — Per-Table Staging

One operation stages exactly one table. `subtxn_id = NULL` means a **bare inline operation** (the single-session commit path, Use Case 1, finalized directly at the top-level `COMMIT`); a non-null `subtxn_id` makes it a child of a subtransaction (Use Case 2).

```sql
CREATE TABLE transaction_operations (
    op_id                         BIGINT PRIMARY KEY,
    txn_id                        BIGINT NOT NULL REFERENCES transactions (txn_id),
    subtxn_id                     BIGINT REFERENCES subtransactions (subtxn_id),
                                  -- NULL = bare inline op; non-null = child of a subtransaction
    realm_id                      TEXT   NOT NULL,
    catalog_id                    BIGINT NOT NULL,
    table_uuid                    TEXT   NOT NULL,
    lock_mode                     TEXT   NOT NULL DEFAULT 'ROW_EXCLUSIVE',
    base_snapshot_id              BIGINT,
                                  -- Iceberg snapshot_id at operation start; used for conflict check
    server_base_metadata_location TEXT,
                                  -- committed metadata-location captured at operation-create time;
                                  -- commit-time conflict = current committed location has diverged from this
    assigned_staging_path         TEXT NOT NULL,
                                  -- server-assigned path WITHIN the table's canonical metadata directory
                                  -- (e.g. s3://warehouse/ns/table/metadata/<txnId>-<opId>-<uuid>.metadata.json).
                                  -- Placing the file inside the table's own tree is intentional:
                                  -- the catalog pointer controls visibility (not the file's location),
                                  -- so the file is invisible until commit swaps the pointer, and
                                  -- credential vending / location validation work without special-casing.
    staged_metadata_location      TEXT,
                                  -- written by client to object storage; applied on parent commit
    op_token                      TEXT NOT NULL,
                                  -- opaque per-operation bearer secret (32-byte hex);
                                  -- authorizes only this operation; does NOT allow top-level
                                  -- commit/abort (those require transactions.txn_token)
    status                        SMALLINT NOT NULL DEFAULT 0,
                                  -- 0=IN_PROGRESS, 1=COMMITTED (a.k.a. SUB_COMMITTED), 2=ABORTED
    retry_of_op_id                BIGINT,
                                  -- when a rollback re-opened the subtransaction, the fresh op row points at
                                  -- the failed predecessor it replaces (lineage; new staging path
                                  -- avoids FileAlreadyExists on the orphaned attempt's files)
    created_at                    TIMESTAMP NOT NULL DEFAULT now(),
    expires_at                    TIMESTAMP NOT NULL,
    ended_at                      TIMESTAMP
);

CREATE INDEX txn_op_txn_idx   ON transaction_operations (txn_id);
CREATE INDEX txn_op_subtxn_idx ON transaction_operations (subtxn_id);
CREATE INDEX txn_op_table_idx ON transaction_operations (table_uuid, txn_id);
```

### `txn_snapshots` — Reader Visibility Snapshots (Write Path Only)

**Standalone readers no longer get a row here.** They receive an opaque `txn_snapshot` handle (encoded `{v, r}`) and the server decodes it on each `loadTable` call — no insert, no server row. The table is used exclusively by the **write path**: a `REPEATABLE_READ` write transaction gets a row and the `transactions` row references it via `txn_snapshot_id`. `READ_COMMITTED` transactions do not pin a snapshot.

```sql
CREATE TABLE txn_snapshots (
    txn_snapshot_id  BIGINT PRIMARY KEY,
                      -- unique handle; drawn from the shared commit sequence
    read_point        BIGINT NOT NULL,
                      -- commit high-water-mark at snapshot time: the largest
                      -- commit_scn durably committed when this snapshot was taken.
                      -- Sole visibility bound (see § Why a single read_point for why the
                      -- PostgreSQL xmin/xmax/xip triple is unnecessary here) and
                      -- the value that pins the purge watermark while the snapshot is live.
    registered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at        TIMESTAMPTZ NOT NULL,
                      -- must renew or expires; prevents watermark pinning forever
    released_at       TIMESTAMPTZ
                      -- set when client explicitly releases the `TxnSnapshot`
);

CREATE INDEX txn_snapshot_read_point_idx ON txn_snapshots (read_point)
    WHERE released_at IS NULL;
```

### `committed_table_snapshots` — The MVCC-to-Iceberg Mapping

This is the bridge between the transaction system and Iceberg's native table snapshot model. Every time a transaction commits a change to a table, a row is inserted here recording which `commit_scn` produced which Iceberg `table_snapshot_id`. Without this table the catalog cannot translate a `read_point` into a concrete Iceberg metadata pointer.

> **Commit-timestamp invariant.** `commit_scn` is a *commit-order stamp* allocated from the per-catalog `commit_scn_counter` **inside the same transaction as the metadata-pointer swap** — never a begin-time `txn_id` and never a value from a non-transactional sequence. This is what makes `read_point` (a single cutoff) sufficient; see [Why a single `read_point`](#why-a-single-read_point-and-not-xmin-xmax-xip).

```sql
CREATE TABLE committed_table_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    realm_id            TEXT    NOT NULL,
    catalog_id          BIGINT  NOT NULL,
    table_uuid          TEXT    NOT NULL,
    commit_scn          BIGINT  NOT NULL,
                        -- commit-order stamp from commit_scn_counter, allocated atomically
                        -- with the pointer swap that published this snapshot (NOT a txn_id)
    iceberg_snapshot_id BIGINT,
                        -- Iceberg's native snapshot ID from the table metadata JSON;
                        -- NULL for DROP or schema-only changes
    metadata_location   TEXT    NOT NULL,
                        -- path to the Iceberg metadata JSON file for this snapshot
    committed_at        TIMESTAMP NOT NULL DEFAULT now()
);

-- Critical index: resolves "most recent visible table snapshot" in O(log n)
CREATE INDEX idx_cts_table_txn
    ON committed_table_snapshots (realm_id, catalog_id, table_uuid, commit_scn DESC);
```

**`commit_scn_counter` — the per-catalog commit clock.** A single row per `(realm, catalog)` whose `scn` is the source of every `commit_scn`. It is bumped transactionally (see the invariant above), which both orders commits and closes the assign-before-visible window:

```sql
CREATE TABLE commit_scn_counter (
    realm_id   TEXT   NOT NULL,
    catalog_id BIGINT NOT NULL,
    scn        BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (realm_id, catalog_id)
);
-- Allocation (inside the commit transaction; RETURNING on Postgres):
--   UPDATE commit_scn_counter SET scn = scn + 1 WHERE realm_id=? AND catalog_id=? RETURNING scn;
-- A fresh read_point is just the current committed value:
--   SELECT scn FROM commit_scn_counter WHERE realm_id=? AND catalog_id=?;
```

The visibility resolution query — given a `TxnSnapshot` `RS`, find the Iceberg table snapshot to return for a table:

```sql
SELECT iceberg_snapshot_id, metadata_location
FROM committed_table_snapshots
WHERE table_uuid        = :table_uuid
  AND commit_scn       <= :rs_read_point     -- committed at or before RS was taken
ORDER BY commit_scn DESC
LIMIT 1;
```

This query returns the most recently committed Iceberg table snapshot that is visible to the given `TxnSnapshot` — the exact Iceberg metadata pointer the reader should use to scan data files.

For an unregistered read (no `txn_snapshot` handle), the equivalent is simply the latest committed row (no cutoff):

```sql
SELECT iceberg_snapshot_id, metadata_location
FROM committed_table_snapshots
WHERE table_uuid = :table_uuid
ORDER BY commit_scn DESC
LIMIT 1;
```

### `locks` — Lock Manager State

```sql
CREATE TABLE locks (
    lock_id      BIGSERIAL PRIMARY KEY,
    txn_id       BIGINT NOT NULL REFERENCES transactions (txn_id),
    realm_id     TEXT NOT NULL,
    catalog_id   BIGINT NOT NULL,
    resource     TEXT NOT NULL,
                 -- "{table_uuid}" or "{table_uuid}:{partition_spec_id}:{partition_value}"
    lock_mode    TEXT NOT NULL,
                 -- 'ROW_EXCLUSIVE' (default, OCC, self-compatible),
                 -- 'EXCLUSIVE' (table pessimistic write), 'ACCESS_EXCLUSIVE' (blocks all)
    status       SMALLINT NOT NULL DEFAULT 0,
                 -- 0=WAITING, 1=GRANTED
    requested_at TIMESTAMP NOT NULL DEFAULT now(),
    granted_at   TIMESTAMP
);

CREATE INDEX idx_locks_resource ON locks (realm_id, catalog_id, resource, status);
CREATE INDEX idx_locks_txn      ON locks (txn_id, status);
```

### `lock_resource` — Per-Resource Serialization Anchor

One always-present row per lockable `(realm, catalog, resource)`, used exclusively as a `SELECT … FOR UPDATE` gate around the check-then-insert critical section in `LockManager.acquire`. Without it, two Polaris nodes could each observe zero conflicts and both insert conflicting lock rows (a phantom insert that locking the `locks` table itself cannot prevent). Locking this anchor row serializes the grant decision fleet-wide; the anchor lock is held only for the duration of the critical section (released when that short DB transaction commits). The long-lived lock remains the `locks` row. Per-resource granularity means acquisitions on different tables never serialize against each other.

```sql
CREATE TABLE lock_resource (
    realm_id   TEXT   NOT NULL,
    catalog_id BIGINT NOT NULL,
    resource   TEXT   NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (realm_id, catalog_id, resource)
);
```

### `txn_watermark` — Purge Horizon

```sql
CREATE TABLE txn_watermark (
    realm_id         TEXT   NOT NULL,
    catalog_id       BIGINT NOT NULL,
    watermark_txn_id BIGINT NOT NULL,          -- a commit_scn value (per catalog)
    updated_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (realm_id, catalog_id)
);
```

**Two horizons in two id spaces.** Since `commit_scn` (per-catalog commit clock) and `txn_id` (global begin-time identity) are now separate id spaces, they drive *different* purges and must not be mixed into one number:

- **`committed_table_snapshots` horizon (a `commit_scn`, per catalog)** — the oldest commit point any live reader could still resolve. In-progress `REPEATABLE_READ` writers are covered automatically because their linked snapshot row stays live until the transaction ends:

  ```
  watermark(realm, catalog) = MIN(read_point) over live (unreleased, unexpired)
                              txn_snapshots for that catalog
  ```

  With no live snapshots, everything below the current `commit_scn` is purgeable (the per-table survivor rule below still keeps each table's latest row). The reaper runs this per catalog (`commit_scn` is not comparable across catalogs).

- **Lifecycle-table horizon (a `txn_id`)** — CLOG rows, subtransactions, and lock records keyed by `txn_id` are purgeable below `MIN(txn_id)` over IN_PROGRESS transactions once `status != IN_PROGRESS`. This is a separate horizon in the `txn_id` space; it is *not* comparable to `commit_scn` and does not gate CTS purging.

> **Purge caveat for `committed_table_snapshots`.** A snapshot pinned exactly at the watermark must still resolve tables that have not changed recently, so the purge must **keep, per table, the newest row with `commit_scn <= watermark`** and only delete rows strictly older than that per-table survivor. Deleting all rows below the watermark unconditionally would strand an unchanged table with no visible snapshot.

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

Three PostgreSQL-style modes, ordered from least to most restrictive:

| Mode | Description | Typical Operation |
|---|---|---|
| `ROW_EXCLUSIVE` | Default OCC mode. A lock row is recorded so that a concurrent `EXCLUSIVE` acquirer can detect it. Self-compatible: multiple `ROW_EXCLUSIVE` holders on the same resource coexist. Conflicts detected at commit via metadata-location divergence (merge-and-retry for the subtransaction path; first-committer-wins abort for the inline path). Plays the role a non-blocking append lock would: ingest-level concurrency is preserved, but the record exists for EXCLUSIVE to inspect. | Default for all write operations |
| `EXCLUSIVE` | Full-table pessimistic write serialization. Blocks all other writers (any mode). Used when the cost of an OCC retry is unacceptable (e.g. compaction, long-running rewrites). | `newRewrite()`, compaction |
| `ACCESS_EXCLUSIVE` | Blocks all access, including other `ROW_EXCLUSIVE` holders. Reserved for operations that must prevent all concurrent writes (e.g. destructive DDL). | Schema evolution DDL (destructive) |

`LockManager.acquire` serializes its check-then-insert across all Polaris nodes via the `lock_resource` anchor table (`SELECT … FOR UPDATE`), preventing phantom-insert races on the `locks` table.

### Lock Compatibility Matrix

```
                   ROW_EXCLUSIVE   EXCLUSIVE   ACCESS_EXCLUSIVE
ROW_EXCLUSIVE           ✅             ❌              ❌
EXCLUSIVE               ❌             ❌              ❌
ACCESS_EXCLUSIVE        ❌             ❌              ❌
```

(Source: `LockManager.COMPAT` in `LockManager.java` — rows = held mode, columns = requested mode.)

Key properties:
- **`ROW_EXCLUSIVE` is self-compatible** — multiple concurrent holders can coexist; conflict is resolved at commit time rather than at lock-acquire time.
- **`EXCLUSIVE` and `ACCESS_EXCLUSIVE` block everything** — once granted, no other lock on the same resource can be acquired until the holding transaction ends.
- **Reads (snapshot acquisition) never block** — no read lock is taken; the snapshot mechanism provides read isolation without locking.

### Conflict Routing

Conflict detection at `COMMIT` compares a per-table **baseline** metadata-pointer against the table's current committed pointer. A difference means another writer advanced the table during this transaction's window.

| Lock mode | On metadata conflict at commit |
|---|---|
| `ROW_EXCLUSIVE` (subtransaction path) | **Merge-and-retry** — server re-derives the added data files and re-applies them onto the current table state (`extractAddedDataFiles` → fresh `newAppend()`). Commit succeeds without the client having to retry. |
| `ROW_EXCLUSIVE` (inline path) | **First-committer-wins abort** (HTTP 409) — the inline path has no per-table subtransaction to re-parent, so abort is the only correct response. |
| `EXCLUSIVE` / `ACCESS_EXCLUSIVE` | **First-committer-wins abort** in all cases. |

### Lock Granularity

```
Catalog
  └── Namespace
        └── Table  ← TABLE-level locks (EXCLUSIVE, ACCESS_EXCLUSIVE)
              └── Partition  ← PARTITION-level locks (ROW_EXCLUSIVE on specific partitions)
```

Multi-table transactions acquire locks at the granularity appropriate to each operation:

```
Multi-table transaction T1:
  Op S1: update rows in orders/date=2024-01-15
         → acquire ROW_EXCLUSIVE lock on partition:orders-uuid/date=2024-01-15
  Op S2: append to customers
         → acquire ROW_EXCLUSIVE lock on customers (recorded for EXCLUSIVE detection)
```

The multi-table wrapper does **not** escalate lock granularity. Each operation acquires only what its specific work requires.

---

## Concurrent Operation Support

### When Locks Are Not Needed

**Append operations run lock-free and optimistic by default**, regardless of whether they are inside a multi-table transaction or not. Appends add new data files to the manifest without modifying or deleting existing files. There is no data-level conflict between concurrent appenders; the existing optimistic CAS (compare-and-swap on the metadata pointer) is sufficient. A `ROW_EXCLUSIVE` lock row is still recorded for append operations inside a multi-table transaction so that a concurrent `EXCLUSIVE` acquirer can detect the active writer, but this does not block other appenders.

This is the most important property to preserve: Iceberg's high-throughput ingest workload — thousands of concurrent Spark tasks appending to partitions — must remain completely unaffected by the addition of the transaction system.

### When Locks Are Needed

Locks are acquired for operations that can conflict at the data level:
- **Positional-delete / row-delta writes**: may delete rows from a partition that another writer is also modifying — requires `ROW_EXCLUSIVE` lock on the partition; use `EXCLUSIVE` if the client cannot tolerate an OCC retry.
- **Partition overwrite**: replaces entire partition(s); use `EXCLUSIVE` to block all other writers on those partitions.
- **Full-table rewrite / compaction**: whole-table operation; requires `EXCLUSIVE` lock on the table.
- **Destructive DDL**: requires `ACCESS_EXCLUSIVE` lock on the table.

For **single-table operations**, `ROW_EXCLUSIVE` (the default) uses OCC and the optimistic retry model. `EXCLUSIVE` / `ACCESS_EXCLUSIVE` are opt-in for clients that cannot afford the cost of a retry.

For **multi-table operations**, upfront locking is mandatory for write operations that modify or replace existing data. Without it, an operation could write hours of data to object storage only to discover a conflict at commit time — and it cannot retry just that one table because the other operations have already reached `COMMITTED`.

### Conflict Resolution at Commit: Two Paths

Conflict detection at `COMMIT` compares a per-table **baseline** metadata-pointer against the table's *current* committed pointer. A difference means some other writer advanced the table during this transaction's window. The two paths capture the baseline at different moments:

- **Inline path** — the baseline is the pointer visible under the transaction's `read_point` (its `BEGIN`-time snapshot), resolved via `resolveTableMetadataLocation`.
- **Subtransaction path** — the baseline is `server_base_metadata_location`, captured when the *operation* was created (i.e. at lock-acquire / `createOperation` time, which for a long-running worker can be well after `BEGIN`). A `null` baseline with a non-`null` current pointer is *also* treated as a conflict, so a concurrent commit that lands in the lock-acquire→`COMMIT` window cannot be silently overwritten.

What happens on a conflict then depends on **which commit path** produced the staged metadata — and the two paths deliberately behave differently:

| | **Inline path** (Use Case 1) | **Subtransaction path** (Use Case 2) |
|---|---|---|
| Who staged the metadata | A single client session, built locally on top of its pinned `BEGIN` snapshot | A worker per table, whose added data files the **server** re-derives from the staged file |
| Conflict → `ROW_EXCLUSIVE` (default) | **First-committer-wins abort** (serialization failure, HTTP 409) | **Merge-and-retry** — server re-parents the added data files onto current state, commit succeeds |
| Conflict → `EXCLUSIVE` / `ACCESS_EXCLUSIVE` | First-committer-wins abort | First-committer-wins abort |
| Code | `TransactionService.prepareInlineCommit` | `TransactionService.prepareCommit` |

**Why inline can't merge-and-retry.** The subtransaction path can merge because the server holds each table's staged metadata as a distinct operation and can extract just that operation's added data files (`extractAddedDataFiles`) and re-apply them onto the live table via a fresh `newAppend()` — a genuine server-side re-derivation. The inline path has no such per-table subtransaction: one client session staged the entire multi-table write against one pinned view, and the server only receives file *locations*. There is nothing to re-parent, so the only correct response to a conflict is to abort. (There is a second, structural reason: at finalize time the server must call `ops.commit(base, staged)` with `base == ops.current()` because `IcebergCatalog.commit` requires reference equality on `base`. It therefore cannot hand Iceberg's own OCC the client's pinned base, so Iceberg's native CAS cannot detect the concurrent write on our behalf — the divergence check in `prepareInlineCommit` *is* the OCC for this path.)

**First-committer-wins is one-directional — it defends your snapshot, it does not block others.** The divergence check aborts a transaction only if the pointer moved *between its own `BEGIN` and its own `COMMIT`*. It is not mutual exclusion. This produces an intentional asymmetry with a concurrent *non-transactional* writer W on the same table:

- **Someone else commits first.** If W commits inside the inline transaction's window, the inline transaction sees `current != baseline` at `COMMIT` and **aborts**. W already won.
- **The inline transaction commits first.** At its `COMMIT`, nothing has moved yet, so it **succeeds**. W then commits afterward — but W never went through `prepareInlineCommit`; it is an ordinary `IcebergCatalog.commit`, governed solely by Iceberg's native OCC. If W is an **append**, Iceberg refreshes and re-parents onto the inline transaction's new snapshot and W **also succeeds** (both writes land). If W is a stale **overwrite/delete**, Iceberg's native OCC rejects it with `CommitFailedException`.

So `ROW_EXCLUSIVE` (the default) gives snapshot-isolation self-defense, not exclusion: a transaction aborts itself if the world changed under it, but it never prevents an independent writer from proceeding. A client that needs to actually *block* other writers — not merely detect that it lost the race — must request an `EXCLUSIVE` / `ACCESS_EXCLUSIVE` lock, which is enforced by the `LockManager` up front (at `createOperation` / `createSubtransaction`), independently of this commit-time OCC check.

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
1. Client: POST /v1/txn-snapshots
   → Server captures read_point = current commit high-water-mark
   → Encodes it into handle = base64url({"v":1,"r":<read_point>})
   → Returns { txn_snapshot: "<handle>", read_point: <informational> }

2. Client: GET /v1/namespaces/ns/tables/orders?txn_snapshot=<handle>
   → Server decodes handle → extracts read_point (fail-closed: malformed → 400)
   → Returns latest committed metadata for orders where commit_scn <= read_point

3. Client: GET /v1/namespaces/ns/tables/customers?txn_snapshot=<handle>
   → Same visibility rule — both tables observed at same logical point in time

   (No DELETE needed — handles are TTL-reclaimed; L = 3600 s)
```

No `txn_id` is created. No CLOG entry. No server row for standalone reads. The handle is a pure read artifact — lightweight, lock-free, and cheap to acquire (one `SELECT` of the commit counter, zero inserts). The GC cost is one `read_floor` advance per catalog per reaper tick, not one row per handle.

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
2. Client: POST /v1/locks                  → acquire ROW_EXCLUSIVE lock on table/partition
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

**Pattern:** An orchestrator acquires one `TxnSnapshot` at pipeline start and distributes the `txn_snapshot` handle to all reader applications. Every application — regardless of when it physically starts — reads at the same logical point in time. Results from different applications can be safely joined or compared.

```
Orchestrator:
  POST /v1/txn-snapshots
  ← { txn_snapshot: "eyJ2IjoxLCJyIjo1MDUwfQ", read_point: 5050 }
  (handle = base64url({"v":1,"r":5050}))

  → passes handle to SparkApp1 (reads orders)
  → passes handle to SparkApp2 (reads customers)
  → passes handle to SparkApp3 (reads products)

SparkApp1: GET /tables/orders?txn_snapshot=<handle>     → table_snapshot_id=42
SparkApp2: GET /tables/customers?txn_snapshot=<handle>  → table_snapshot_id=17   (parallel)
SparkApp3: GET /tables/products?txn_snapshot=<handle>   → table_snapshot_id=31   (parallel)

  All three apps see a consistent cross-table state as of read_point=5050.
  The handle is self-contained — no server row to release; the read floor
  advances automatically after L seconds.
```

**Key properties:**
- The handle is acquired *once*, at the moment the pipeline is scheduled — not when each job happens to start.
- A job that starts 10 minutes late (cluster contention) still sees the same data as the job that started immediately.
- The handle is a plain string — any bearer can read with it; no application can write using it.
- No explicit release needed: GC is TTL-based (`L = 3600 s`); the orchestrator may discard the handle after all apps complete.

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
| Apps share | Same `txn_snapshot` handle (opaque string) | Same `txn_id`; each has unique `subtxn_token` |
| Top-level token held by | Orchestrator (discards handle when done) | Orchestrator (commits/aborts txn) |
| App-level capability | Read any table at the encoded read_point | Write assigned table; commit own subtxn only |
| Finalization | No action needed; TTL-reclaimed after `L` s | Orchestrator calls `T1/commit` |
| Failure handling | No action needed; handle expires naturally | Orchestrator calls `T1/abort` |

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
    (table_uuid, iceberg_snapshot_id, commit_scn, metadata_location)
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

If they differ, another writer committed to that table after this subtransaction began. The response depends on the lock mode and commit path:

| Lock mode | Commit path | On conflict |
|---|---|---|
| `ROW_EXCLUSIVE` | Subtransaction path | **Merge-and-retry** — server re-derives added data files and re-applies them onto the current table state (`extractAddedDataFiles` + fresh `newAppend()`). Commit succeeds. |
| `ROW_EXCLUSIVE` | Inline path | **First-committer-wins abort** (HTTP 409) — no per-table subtransaction to re-parent. |
| `EXCLUSIVE` / `ACCESS_EXCLUSIVE` | Either | **First-committer-wins abort** in all cases. |

For multi-table transactions, a conflict on **any** operation that cannot be auto-merged aborts the **entire** top-level transaction. Partial commits are not allowed.

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

A `SAVEPOINT` marks a point within the transaction to which a partial rollback can retreat. The **subtransaction** *is* the savepoint: a named subtransaction (`CREATE SUBTRANSACTION FOR … name=<sp>`) groups the operations done under it, and rolling that subtransaction back retreats exactly those operations while leaving every other subtransaction's staged work intact — the parent transaction stays `IN_PROGRESS`.

```
CREATE subtxn "sp1" for {A, B} →  a subtransactions row (OPEN), one operation per table
stage sp1                     →  operations COMMITTED, subtransaction STAGED
ROLLBACK TO sp1               →  abort sp1's operations; release their locks;
                                 A and B revert to their pre-subtransaction committed state;
                                 the parent txn remains IN_PROGRESS
```

Rollback has two flavours, distinguished by whether the subtransaction is reopened for another attempt:

| Wire | SQL (Spark) | Effect |
|---|---|---|
| `POST …/rollback { reopen_for_retry: true }` (default) | `ROLLBACK SUBTRANSACTION` | Abort the subtransaction's operations + release locks, then **reopen** the subtransaction (`attempt++`) with fresh operation rows and staging paths so it can be re-staged (possibly by a different worker). `retry_of_op_id` links each new op to its aborted predecessor; a fresh staging path avoids `FileAlreadyExists` on the orphaned attempt's files. |
| `POST …/rollback { reopen_for_retry: false }` or `POST …/abort` | `ABORT SUBTRANSACTION` | Leave the subtransaction `ABORTED`. Its operations are discarded and **skipped** at the parent `COMMIT` — the guard in `prepareCommit` excludes operations of any `ABORTED` subtransaction. |

Because a subtransaction's `name` is unique only among **non-aborted** subtransactions of the same transaction, `ROLLBACK TO <name>` followed by a fresh `SAVEPOINT <name>` (a new subtransaction reusing the freed name) is allowed. Reopening keeps the same `subtxn_id` and bumps `attempt`; re-creating under the same name after an `ABORT` produces a new `subtxn_id`.

The remaining subtransactions are unaffected. The client may then re-issue the rolled-back work or commit the transaction without it.

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
  ← { txn_snapshot: "eyJ2IjoxLCJyIjo1MDEwfQ", read_point: 5010 }
  (captured before W commits at commit_scn=5011; handle encodes read_point=5010)

-- Orchestrator passes the handle to both apps:
App1: GET /tables/orders?txn_snapshot=<handle>
  → W's commit (commit_scn=5011 > read_point=5010) is NOT visible
  → returns table_snapshot_id=42  ✓

App2: GET /tables/customers?txn_snapshot=<handle>
  → Same visibility rule: W excluded
  → returns table_snapshot_id=17 (pre-W state)  ✓

Both apps see the same pre-W consistent state regardless of when each app physically started.
No explicit release needed — handle expires after L seconds.
```

**What if the orchestrator wants to include W?** Acquire a handle *after* W commits:
```
-- W commits at commit_scn=5011
Orchestrator: POST /v1/txn-snapshots
  ← { txn_snapshot: "eyJ2IjoxLCJyIjo1MDExfQ", read_point: 5011 }
  (captures state after W)

App1: GET /tables/orders?txn_snapshot=<handle2>    → W visible (5011 <= 5011)
App2: GET /tables/customers?txn_snapshot=<handle2> → W visible ✓
```

**Result**: The orchestrator controls exactly which committed state all apps observe — by choosing *when* to acquire the shared snapshot. The handle is a plain string; any number of apps can share it with zero server-side coordination.

---

### Example 2: Reader vs. Writer

**Scenario**: A Spark read job runs against `orders` while a Flink pipeline appends new records.

```
T1 (reader, snapshot S1 pinned at read_point=5000)

T2 (writer, APPEND to orders):
  writes new Parquet files to S3
  commits → commit_scn=5001, new snapshot=43 visible in catalog

T1 continues reading:
  still uses snapshot S1 (pinned at read_point=5000)
  sees orders at snapshot=42 (the state before T2 committed)
  T2's new files are in the manifest at snapshot=43
  T2's commit_scn=5001 > S1.read_point=5000 → NOT in T1's visible range
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

**With pessimistic locking (EXCLUSIVE mode, opt-in)**:

```
T1 (RowDelta, orders/date=2024-01-15, EXCLUSIVE lock):
  POST /v1/locks → acquire EXCLUSIVE lock on orders/date=2024-01-15 → GRANTED
  reads base_snapshot_id=42 (guaranteed stable — lock held)
  writes delete file + new data file to S3 (30 minutes)
  commits → SUCCESS, txn_id=5001, lock released

T2 (RowDelta, orders/date=2024-01-15, EXCLUSIVE lock):
  POST /v1/locks → acquire EXCLUSIVE lock on orders/date=2024-01-15 → WAITING
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
T1 (multi-table: row-delta on orders/p1 + append to customers):
  acquire ROW_EXCLUSIVE lock on orders/date=2024-01-15 → GRANTED
  acquire ROW_EXCLUSIVE lock on customers → GRANTED
  Op S1 writes orders data to S3
  Op S2 writes customers data to S3
  S1 → COMMITTED
  S2 → COMMITTED
  T1 commits atomically → orders+customers visible simultaneously

T2 (multi-table: row-delta on orders/p2 + row-delta on customers/p1):
  acquire ROW_EXCLUSIVE lock on orders/date=2024-01-16 → GRANTED  [different partition from T1]
  acquire ROW_EXCLUSIVE lock on customers/date=2024-01-15 → GRANTED
  Op S3 writes orders data to S3
  Op S4 writes customers data to S3
  S3 → COMMITTED
  S4 → COMMITTED
  T2 commits atomically
```

T1 and T2 operate on different partitions. **Both proceed fully concurrently** despite both being multi-table transactions touching the same tables. At commit time each uses the subtransaction (merge-and-retry) path, so if either encounters a metadata-location divergence the server re-applies just the added files onto the current state.

**Conflict case** — T1 and T2 both want `orders/date=2024-01-15` with `EXCLUSIVE` (pessimistic) locks:

```
T1: acquire EXCLUSIVE lock on orders/date=2024-01-15 → GRANTED
T2: acquire EXCLUSIVE lock on orders/date=2024-01-15 → WAITING

T2 waits until T1 commits and releases the lock.
T2 then acquires the lock and proceeds.
No data corruption. Correct serialization.
```

---

### Example 6: Multi-Table Transaction vs. Single-Table Writer

**Scenario**: A multi-table ETL (T1) holds a lock on `orders/date=2024-01-15` while a routine streaming ingest job (T2) appends to `orders`.

```
T1 (multi-table, holds ROW_EXCLUSIVE lock on orders/date=2024-01-15):
  ... writing to S3 for 2 hours ...

T2 (single-table append to orders/date=2024-01-16):
  Writes new files to S3
  Commits via optimistic CAS → SUCCESS immediately
  [T2 is completely unaware of T1; ROW_EXCLUSIVE is self-compatible]

T3 (single-table append to orders/date=2024-01-15):
  Writes new files to S3
  Commits via optimistic CAS → SUCCESS immediately
  [ROW_EXCLUSIVE is self-compatible; appends never block each other]

T4 (single-table row-delta on orders/date=2024-01-15):
  Pessimistic (EXCLUSIVE): tries to acquire EXCLUSIVE lock → CONFLICT (T1 holds ROW_EXCLUSIVE,
               which is not compatible with EXCLUSIVE) → WAITING for T1
  Optimistic (ROW_EXCLUSIVE): writes S3, commits → CAS/divergence check at commit:
               If T1 hasn't committed yet: base still valid → SUCCESS (merge path)
               If T1 committed first: metadata diverged → merge-and-retry or abort (inline path)
```

**Result**: Appends on any partition of any table proceed freely. Only writers requesting `EXCLUSIVE` or `ACCESS_EXCLUSIVE` on the same partition as T1's lock are blocked. The blast radius of a multi-table transaction is exactly the same as an equivalent single-table pessimistic transaction.

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
  (server acquires ROW_EXCLUSIVE lock on orders)

  POST /v1/transactions/T1/subtxn  { table: "customers" }
  ← { subtxn_id: S2, subtxn_token: <tok2>,
      staging_path: "s3://catalog/staging/T1/S2/metadata.json",
      base_snapshot_id: 17 }
  (server acquires ROW_EXCLUSIVE lock on customers)

  POST /v1/transactions/T1/subtxn  { table: "products" }
  ← { subtxn_id: S3, subtxn_token: <tok3>,
      staging_path: "s3://catalog/staging/T1/S3/metadata.json",
      base_snapshot_id: 31 }
  (server acquires ROW_EXCLUSIVE lock on products)

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
-- Readers using a txn_snapshot handle acquired after commit see all three updated. ✓
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
If App1 acquired its own lock on orders and App2 acquired its own lock on customers, a concurrent multi-table transaction acquiring the same two tables in opposite order could deadlock. The orchestrator acquires all locks in a canonical order (sorted by `table_uuid` lexicographically — see `LockManager.canonicalOrder`) before any app starts, preventing deadlocks entirely.

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

### Two purge horizons (as implemented)

Because `commit_scn` (per-catalog commit clock) and `txn_id` (global begin-time identity) are separate id spaces, maintenance uses **two horizons**, not one blended `txn_id` watermark (see [`txn_watermark`](#txn_watermark--purge-horizon)):

1. **CTS horizon — a `commit_scn`, per catalog.** The oldest commit point any live reader could still resolve: `MIN(read_point)` over live (unreleased, unexpired) `txn_snapshots` for that catalog. In-progress REPEATABLE_READ writers are covered automatically (their linked snapshot row stays live until the transaction ends). With no live snapshots, the horizon is the catalog's current `commit_scn` (everything below the per-table survivor is purgeable). This is what `txn_watermark.watermark_txn_id` stores (per `(realm, catalog)`), and it gates purging of `committed_table_snapshots` only.
2. **Lifecycle horizon — a `txn_id`.** CLOG rows, subtransactions/operations, and lock records keyed by `txn_id` are purgeable below `MIN(txn_id)` over IN_PROGRESS transactions once their status is terminal. This is a separate horizon in the `txn_id` space and is **not** comparable to `commit_scn`.

The reaper (`TransactionReaper.advanceWatermarkAndPurge`) runs the CTS horizon **per catalog** (`commit_scn` is not comparable across catalogs). Long-running read jobs holding a `TxnSnapshot` pin the CTS horizon and slow purging of the mapping table — an **operational cost, not a correctness or liveness problem**; new transactions commit freely regardless, and `expires_at` bounds how long any job can pin it.

### Purging

```sql
-- (A) committed_table_snapshots — gated by the per-catalog CTS horizon (a commit_scn).
--     Keep, PER TABLE, the newest row with commit_scn <= watermark so a reader pinned exactly
--     at the watermark can still resolve a table that has not changed recently; delete only rows
--     strictly older than that per-table survivor. (Implemented in
--     JdbcTransactionPersistence.purgeCommittedSnapshotsBelowWatermark.)
DELETE FROM committed_table_snapshots c
WHERE c.realm_id = :realm AND c.catalog_id = :catalog
  AND c.commit_scn < (
      SELECT MAX(c2.commit_scn) FROM committed_table_snapshots c2
      WHERE c2.realm_id = c.realm_id AND c2.catalog_id = c.catalog_id
        AND c2.table_uuid = c.table_uuid
        AND c2.commit_scn <= :cts_watermark
  );

-- (B) lifecycle tables — gated by the txn_id horizon (a different id space).
DELETE FROM transactions
WHERE txn_id < :txnid_horizon AND status IN (1, 2);       -- COMMITTED / ABORTED

DELETE FROM transaction_operations
WHERE txn_id < :txnid_horizon;

DELETE FROM locks
WHERE txn_id < :txnid_horizon;

-- Released/expired reader snapshots are safe to purge once below the CTS horizon.
DELETE FROM txn_snapshots
WHERE read_point < :cts_watermark
  AND (released_at IS NOT NULL OR expires_at < now());
```

---

## REST API

### TxnSnapshot Management

Used by pure readers that need a consistent multi-table view. No `txn_id` or CLOG entry is created. The server captures a `read_point`, encodes it into an opaque string handle, and returns it. No server row is inserted — the handle is self-contained and reclaimed purely by TTL (`L = 3600 s` hardcoded; the reaper advances the read floor once all handles older than `L + skew_pad` are past their horizon).

```
POST   /v1/txn-snapshots
  ← { txn_snapshot: "<opaque handle>", read_point: <informational>, expires_at: null }
  -- txn_snapshot is base64url({"v":1,"r":<read_point>}); ~18–46 chars
  -- expires_at is null for standalone read handles (TTL is server-side, not per-handle)
  -- No DELETE or renew endpoint for standalone read handles
```

Pass `txn_snapshot` on every table read to get a consistent point-in-time view across all tables:

```
GET /v1/{prefix}/namespaces/{ns}/tables/{table}?txn_snapshot={handle}
```

The server decodes the handle (fail-closed: malformed → `400`), extracts `read_point`, and resolves the Iceberg table snapshot:
```sql
SELECT iceberg_snapshot_id, metadata_location
FROM committed_table_snapshots
WHERE table_uuid = <table>
  AND commit_scn <= :read_point   -- from decoded handle
ORDER BY commit_scn DESC LIMIT 1;
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

`createSubtransaction` creates a **subtransaction** (`subtransactions`) that groups one or more per-table **operations** (`transaction_operations`) — see [Subtransaction + Operations](#subtransaction--operations). The `subtxn_id`/`subtxn_token` in the API are the **subtransaction's** id/token; the returned `operations[]` array carries each table's `op_id`, `op_token`, and `assigned_staging_path`. A single-table request is the degenerate one-operation subtransaction; the top-level `table_uuid`/`base_snapshot_id`/`assigned_staging_path` response fields mirror the first operation for that shorthand.

The orchestrator creates subtransactions (requires `txn_token`). Each subtransaction returns its own `subtxn_token` — distributed to the assigned application; the orchestrator does not need to retain it.

```
POST   /v1/transactions/{txn_id}/subtxns             Create a subtransaction (savepoint)
  → requires txn_token
  → {                              -- preferred multi-table form:
      tables: [ { table_identifier, operation: "ROW_EXCLUSIVE"|"EXCLUSIVE"|"ACCESS_EXCLUSIVE",
                  base_snapshot_id? }, ... ],
      name?,                       -- optional savepoint name / worker task key (unique per txn
                                   --   among non-aborted subtransactions; a name freed by rollback is reusable)
      lease_seconds?
    }
    -- single-table shorthand: { table_identifier, operation } instead of tables[]
  ← { subtxn_id,
      subtxn_token,                -- per-subtransaction bearer secret, to the assigned app only
      name, attempt,               -- attempt is 1-based; incremented on each rollback-and-retry
      operations: [ { op_id, op_token, table_uuid, operation,
                      base_snapshot_id, assigned_staging_path, status }, ... ] }

-- Subtransaction-scoped operations. commit/rollback accept the subtxn_token; rollback and abort
-- also accept the parent txn_token (orchestrator-driven).
POST   /v1/transactions/{txn_id}/subtxns/{id}/commit   Stage the WHOLE subtransaction (body-less)
  → requires subtxn_token
  → (no body)                      -- staging paths were assigned at create time; the worker has
                                   --   already written each table's metadata there. The server
                                   --   verifies each file exists, marks every operation COMMITTED,
                                   --   and marks the subtransaction STAGED. All-or-nothing (one worker per subtransaction).
POST   /v1/transactions/{txn_id}/subtxns/{id}/rollback Roll the subtransaction back (retryable)
  → requires subtxn_token OR txn_token
  → { reopen_for_retry: true }     -- default true: abort the subtransaction's operations + release locks, then
                                   --   reopen the subtransaction (attempt++) with FRESH operation rows and
                                   --   staging paths so it can be re-staged (possibly by a different
                                   --   worker) without colliding with the orphaned attempt's files.
                                   --   false ⇒ leave it ABORTED; the parent COMMIT skips it.
POST   /v1/transactions/{txn_id}/subtxns/{id}/abort    Abandon the subtransaction permanently
  → requires subtxn_token OR txn_token
                                   -- equivalent to rollback with reopen_for_retry=false.
POST   /v1/transactions/{txn_id}/subtxns/{id}/renew    Extend the subtransaction's (and parent's) lease
  → requires subtxn_token
GET    /v1/transactions/subtxns/{id}                   Look up a subtransaction by id + subtxn_token
  → requires subtxn_token          -- id-only addressing; returns the full subtransaction + operations[]
                                   --   (each with its assigned_staging_path) so a resuming worker
                                   --   can continue. Response's parent_txn_id names the owning txn.
```

`commitSubtransaction` carries no request body: the server assigned every operation's staging path at create time, so ownership of the `subtxn_token` is sufficient and the whole subtransaction stages atomically.

### Lock Management

```
POST   /v1/locks                                     Acquire a lock
DELETE /v1/locks/{lock_id}                           Release a lock (rarely needed; commit/abort handles this)
GET    /v1/tables/{table_uuid}/locks                 Inspect active locks on a table
```

### Modified Table Metadata Endpoint

```
GET /v1/{prefix}/namespaces/{namespace}/tables/{table}
    ?txn_snapshot={handle}       optional: consistent pinned read (opaque handle from POST /txn-snapshots)
    &txn_id={txn_id}             optional: read-your-own-writes within a transaction
```

When `txn_id` is provided, the server returns the staged metadata for that transaction's own operation on the table (if the operation has reached `COMMITTED`/`SUB_COMMITTED`), enabling read-your-own-writes within a multi-table transaction.

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
If engine A holds an `EXCLUSIVE` lock on table T and engine B wants an `EXCLUSIVE` lock on the same table, they cannot negotiate directly — engines have no knowledge of each other. A central lock table in the catalog is the only place where this can be tracked consistently. Peer-to-peer locking between engines would require either a distributed lock service (ZooKeeper, etcd) or a gossip protocol — both of which are far heavier than a lock table in an existing relational DB.

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

With the handle-based read path, standalone reads create **no server row** and therefore cannot pin the watermark indefinitely. The read floor (`MAX(commit_scn WHERE committed_at <= now - L)`) advances automatically every reaper tick — it is bounded by `L` (3600 s default), not by whether clients release handles.

The remaining pinning risk is from **write-path** `txn_snapshots` rows held by long-running `REPEATABLE_READ` transactions. Mitigations are the same as for any long-running transaction:

- **Maximum transaction TTL.** Cap `expires_at` to a reasonable maximum (e.g., 24 hours for large ETL jobs, configurable by admin).
- **Active-transaction count limit per principal.** Prevent a single client from holding an unlimited number of concurrent `IN_PROGRESS` transactions.
- **Reaper.** Expires transactions (and their linked snapshot rows) past `expires_at`, bounding the maximum watermark pin duration.

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
