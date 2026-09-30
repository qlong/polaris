<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Demo — The Two Superpowers

Companion to [`demo_interactive.md`](demo_interactive.md). That file demos the *table
stakes* — REPEATABLE READ multi-table reads (Part 1) and atomic multi-table writes
(Part 2). This file demos the two capabilities the talk calls **superpowers**, the ones
that don't exist in a plain Iceberg REST catalog:

- **Superpower 1 — Exportable `TxnSnapshot`.** One consistent read point, *created once*
  and *handed to any number of engines*. Below, an extra Spark session acts as the
  **coordinator**: it mints a snapshot via REST and hands the id to **two different real
  engines** — a Spark reader and a Trino reader. A writer commits underneath them. Both
  engines keep returning the *same* pinned counts, because they share one `read_point`.

- **Superpower 2 — Distributable / retryable subtransactions.** A multi-table write is a
  set of independent, server-tracked units. When one unit hits a conflict, you retry
  *that one unit* — the other workers' staged work is untouched, and the parent commit
  still lands atomically. Below extends `demo_interactive.md` Part 2 with a forced
  conflict on one worker.

> **Scope note.** These scripts are deliberately *separate* from the slide deck and
> speaker notes. Nothing here edits `docs/multi-table-transactions-slides.md` or the
> speaker notes.

---

## Prerequisites (once)

```bash
cd demos/multi-table-transaction

# 1. Start Polaris (PostgreSQL + relational-jdbc persistence — required for txns).
./start-polaris.sh

# 2. Seed 3 customers / 5 orders (idempotent; re-run before every demo).
./setup_spark_tables.sh

# 3. A Trino server (local fork with the polaris_read_snapshot session property)
#    connected to Polaris as catalog `polaris`, schema `demo_ns`. Bump the idle
#    transaction timeout so a paused REPEATABLE READ transaction survives the demo:
#      transaction.idle-timeout=60m   (in the Trino iceberg catalog / coordinator config)
```

Every Spark terminal below starts the same shell. Define a helper so the preamble is one
line:

```bash
# Run once per terminal.
source spark-env.sh
spark() {
  "$SPARK_HOME/bin/spark-sql" \
    --jars "$ICEBERG_JAR,$POLARIS_PLUGIN_JAR" \
    --properties-file spark-defaults.conf \
    --conf 'spark.sql.shuffle.partitions=2' \
    --conf 'spark.ui.enabled=false' \
    --master 'local[2]'
}
```

> **Plugin build.** Superpower 1's Spark reader uses the `USE SNAPSHOT <id>` verb. Make
> sure `$POLARIS_PLUGIN_JAR` is a fresh bundle:
> `./gradlew :polaris-spark-3.5_2.13:build -x test` and re-`source spark-env.sh`.

---

## Superpower 1 — One Snapshot, Two Engines

**Cast (4 terminals):**

| Terminal | Role | Engine |
|----------|------|--------|
| **Coordinator** | Mints the snapshot via REST, hands out the id | shell + `curl` (an extra Spark session's driver process) |
| **Reader S** | Consumes the id | **Spark** (`USE SNAPSHOT <id>`) |
| **Reader T** | Consumes the *same* id | **Trino** (`SET SESSION polaris.polaris_read_snapshot`) |
| **Writer** | Commits new rows underneath both readers | Spark |

The point on screen: **Reader S and Reader T are different engines, holding the same
`txn_snapshot_id`, and they return identical counts before *and* after the writer
commits.** Neither engine started its own transaction; both simply attached to the
coordinator's read point.

### Coordinator — mint and export the snapshot

`spark-env.sh` already set `POLARIS_TOKEN` and `POLARIS_BASE`
(`.../api/catalog/polaris/v1/$POLARIS_CATALOG`).

```bash
# Establish a baseline we can watch: 5 orders, 3 customers (from setup).
# Mint a consistent read point valid for 30 minutes.
SNAP=$(curl -s -X POST "$POLARIS_BASE/transaction-snapshots" \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"lease_seconds": 1800}')

echo "$SNAP"
# → {"txn_snapshot_id": 4242, "read_point": 137, "expires_at": "..."}

SNAP_ID=$(echo "$SNAP" | python3 -c "import sys,json; print(json.load(sys.stdin)['txn_snapshot_id'])")
echo "Exported snapshot id = $SNAP_ID   (hand this to both readers)"
```

`txn_snapshot_id` is the exportable handle. It is a `read_point` cutoff — **no locks, no
writes, no owning transaction.** Anyone holding it reads the catalog as of that instant.

### Reader S (Spark) — attach with `USE SNAPSHOT`

```bash
spark   # from a fresh terminal
```

```sql
-- Attach this session's reads to the coordinator's snapshot. No BEGIN — this
-- session does not own a transaction; it merely consumes the shared read_point.
-- (Replace 4242 with $SNAP_ID from the coordinator.)
spark-sql> USE SNAPSHOT 4242;

spark-sql> SELECT count(*) FROM polaris.demo_ns.orders;     -- 5
spark-sql> SELECT count(*) FROM polaris.demo_ns.customers;  -- 3
```

### Reader T (Trino) — attach with `SET SESSION`

```bash
trino --server http://localhost:8080 --catalog polaris --schema demo_ns
```

```sql
-- Same read point, different engine. The connector threads txn_snapshot_id on
-- every loadTable for this session.  (Replace 4242 with $SNAP_ID.)
trino> SET SESSION polaris.polaris_read_snapshot = 4242;

trino> SELECT count(*) FROM orders;     -- 5
trino> SELECT count(*) FROM customers;  -- 3
```

Both engines agree: 5 / 3. Same numbers, because same `read_point`.

### Writer — commit underneath both readers

```bash
spark   # a fourth terminal — an ordinary session, no transaction
```

```sql
-- Land new rows the normal way (auto-commit). This advances commit_scn past the
-- readers' read_point.
spark-sql> INSERT INTO polaris.demo_ns.orders    VALUES (2001, 3, 999.00, 'paid');
spark-sql> INSERT INTO polaris.demo_ns.customers VALUES (4, 'Dave', 'dave@example.com');

-- Prove the write is live for anyone NOT pinned:
spark-sql> SELECT count(*) FROM polaris.demo_ns.orders;     -- 6
spark-sql> SELECT count(*) FROM polaris.demo_ns.customers;  -- 4
```

### The reveal — re-run both readers

**Reader S (Spark):**

```sql
-- Spark caches table metadata per session; refresh so a naive reader would see
-- the write. The snapshot pin still holds.
spark-sql> REFRESH TABLE polaris.demo_ns.orders;
spark-sql> REFRESH TABLE polaris.demo_ns.customers;

spark-sql> SELECT count(*) FROM polaris.demo_ns.orders;     -- 5  (still pinned)
spark-sql> SELECT count(*) FROM polaris.demo_ns.customers;  -- 3  (still pinned)
```

**Reader T (Trino):**

```sql
trino> SELECT count(*) FROM orders;     -- 5  (still pinned)
trino> SELECT count(*) FROM customers;  -- 3  (still pinned)
```

Two different engines, one exported snapshot, unchanged view — while the world moved to
6 / 4 underneath them. **That is the exportable `TxnSnapshot`.**

### Detach and see the world catch up

```sql
-- Spark
spark-sql> USE SNAPSHOT NONE;
spark-sql> SELECT count(*) FROM polaris.demo_ns.orders;     -- 6
spark-sql> SELECT count(*) FROM polaris.demo_ns.customers;  -- 4
```

```sql
-- Trino
trino> RESET SESSION polaris.polaris_read_snapshot;   -- unset = latest (the property default is null)
trino> SELECT count(*) FROM orders;                   -- 6
```

> **Talking point.** The coordinator was "an extra Spark session," but the snapshot is
> just an id behind a REST call — a coordinator can be a notebook, an Airflow task, or a
> shell. What matters is that a *single* read point is minted once and consumed by
> heterogeneous engines. That is impossible in a stock Iceberg REST catalog, where each
> engine time-travels each table independently and nothing guarantees the same instant
> across tables *or* across engines.

---

## Superpower 2 — Retry One Unit, Not the Whole Write

This extends `demo_interactive.md` **Part 2** (atomic multi-table write). Read that first;
the terminals and the orchestrator/worker verbs are identical. Here we add a **forced
conflict** on one worker to show that a subtransaction is an independent, retryable unit —
the other worker's staged metadata is untouched, and the parent commit still lands
atomically once the conflicted unit succeeds.

**Cast (4 terminals):** Orchestrator (A), Worker-orders (B), Worker-customers (C), plus an
**Interloper (D)** whose only job is to make B's first commit conflict.

### A — Orchestrator: begin and allocate

```sql
spark-sql> BEGIN TRANSACTION;                          -- REPEATABLE READ, PENDING
-- Lock mode EXCLUSIVE on orders so the interloper's concurrent commit collides.
spark-sql> CREATE SUBTRANSACTION FOR demo_ns.orders    WITH EXCLUSIVE;   -- note SUBTXN1_*
spark-sql> CREATE SUBTRANSACTION FOR demo_ns.customers;                  -- note SUBTXN2_*
```

### C — Worker: stage customers (this unit never has to change)

```sql
spark-sql> BEGIN SUBTRANSACTION demo_ns.customers
             SUBTXN_ID=<SUBTXN2_ID>, SUBTXN_TOKEN='<SUBTXN2_TOKEN>';
spark-sql> INSERT INTO polaris.demo_ns.customers VALUES (5, 'Eve', 'eve@example.com');
spark-sql> COMMIT SUBTRANSACTION;      -- staged + committed; pointer NOT yet swapped
```

Customers' work is now safely staged on the server. Whatever happens to the orders worker,
**this unit is done and will not be re-run.**

### D — Interloper: move `orders` out from under worker B

```sql
-- An ordinary auto-commit write that advances orders past the subtransaction's base.
spark-sql> INSERT INTO polaris.demo_ns.orders VALUES (9999, 1, 1.00, 'noise');
```

### B — Worker: stage orders, hit the conflict, retry just this unit

```sql
spark-sql> BEGIN SUBTRANSACTION demo_ns.orders
             SUBTXN_ID=<SUBTXN1_ID>, SUBTXN_TOKEN='<SUBTXN1_TOKEN>';
spark-sql> INSERT INTO polaris.demo_ns.orders VALUES (1007, 2, 350.00, 'pending');

-- Commit the unit. Because the interloper advanced orders, the staged base is now
-- stale — the commit conflicts.
spark-sql> COMMIT SUBTRANSACTION;      -- ✗ conflict: base snapshot moved

-- Retry is scoped to THIS subtransaction. ROLLBACK SUBTRANSACTION reopens it
-- (attempt++) against the latest orders state; the customers unit in Terminal C
-- is never touched.
spark-sql> ROLLBACK SUBTRANSACTION;
spark-sql> BEGIN SUBTRANSACTION demo_ns.orders
             SUBTXN_ID=<SUBTXN1_ID>, SUBTXN_TOKEN='<SUBTXN1_TOKEN>';
spark-sql> INSERT INTO polaris.demo_ns.orders VALUES (1007, 2, 350.00, 'pending');
spark-sql> COMMIT SUBTRANSACTION;      -- ✓ succeeds against the new base
```

> If your build surfaces the conflict at a different verb (some paths fail fast on the
> reopened `BEGIN SUBTRANSACTION` rather than on `COMMIT SUBTRANSACTION`), the retry shape
> is the same: `ROLLBACK SUBTRANSACTION` → re-`BEGIN` → re-`INSERT` → `COMMIT
> SUBTRANSACTION`. Only the orders unit repeats.

### A — Orchestrator: commit the whole write atomically

```sql
spark-sql> COMMIT;     -- swaps orders + customers pointers in one DB transaction
```

### A — Verify

```sql
spark-sql> REFRESH TABLE polaris.demo_ns.orders;
spark-sql> REFRESH TABLE polaris.demo_ns.customers;
-- orders: original 5 + interloper's 9999 + retried 1007 = 7
spark-sql> SELECT count(*) FROM polaris.demo_ns.orders;     -- 7
-- customers: original 3 + Eve = 4  (that unit was staged ONCE, never retried)
spark-sql> SELECT count(*) FROM polaris.demo_ns.customers;  -- 4
```

**What the audience sees:** the customers worker committed its unit exactly once and went
home. The orders worker fought a conflict and retried *its unit alone*. The parent commit
still published both tables together. Retry is per-unit; atomicity is whole-transaction.

---

## Mapping back to the pillars

| Superpower | Pillar | Verb / API used here |
|------------|--------|----------------------|
| Exportable `TxnSnapshot` | **Pillar 2** | Coordinator `POST /transaction-snapshots`; Spark `USE SNAPSHOT <id>` / `USE SNAPSHOT NONE`; Trino `SET SESSION polaris.polaris_read_snapshot = <id>` |
| Distributable / retryable subtxn | **Pillar 4** | `CREATE SUBTRANSACTION FOR … WITH EXCLUSIVE`; `BEGIN SUBTRANSACTION` / `COMMIT SUBTRANSACTION` / `ROLLBACK SUBTRANSACTION`; orchestrator `COMMIT` |

Both rest on the same server-side machinery from `demo_interactive.md`: a single
`read_point` cutoff plus a transactional `commit_scn` counter for reads, and
server-allocated staging paths guarded by locks for writes.
