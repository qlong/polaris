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

# Multi-Table Transaction Interactive Demo

Demonstrates the core transaction semantics using live Spark sessions, plus cross-engine
consistent reads with Trino sharing the same Polaris catalog:

1. **Consistent multi-table reads** (no torn reads) — REPEATABLE READ isolation (Spark)
2. **Atomic multi-table writes** — subtransaction staging + atomic commit (Spark)
3. **Cross-engine, multi-table consistent read** — one Trino REPEATABLE READ transaction pins
   `orders` + `customers` at a single instant while Spark commits a torn multi-table write
   underneath (Spark writes two tables, Trino reads the same catalog with no torn read)
4. **The isolation contrast** — in the *same* Trino session, flip REPEATABLE READ → READ COMMITTED
   and watch the same query move on a concurrent Spark commit (one word changed, no new terminal)

Parts 1–2 need only Spark. Parts 3–4 additionally need a Trino server on the same Polaris
catalog and the Trino CLI (see Part 3 prerequisites).

---

## Prerequisites

```bash
# 1. Polaris running
curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d 'grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL'
# should return 200

# 2. Tables seeded with initial data
cd demos/multi-table-transaction
./setup_spark_tables.sh
# Expected output includes: "orders count: 5", "customers count: 3"

# 3. Load env (fetches POLARIS_TOKEN, sets POLARIS_BASE / ICEBERG_BASE)
source spark-env.sh
```

---

## Part 1 — Consistent Multi-Table Read (No Torn Read)

**What REPEATABLE READ gives you:** the first query in a transaction pins an MVCC snapshot on the
server. Every subsequent `loadTable` in that session resolves through the pinned snapshot, so all
queries see a consistent point-in-time view of every table — regardless of concurrent writes.
No snapshot IDs, no `VERSION AS OF`, no manual coordination: the Polaris Spark plugin handles
everything internally.

Open **2 terminal windows**. Both run from `demos/multi-table-transaction/`.

### Terminal A — Start a REPEATABLE READ transaction

```bash
source spark-env.sh
$SPARK_HOME/bin/spark-sql \
  --jars $ICEBERG_JAR,$POLARIS_PLUGIN_JAR \
  --properties-file spark-defaults.conf \
  --conf 'spark.sql.shuffle.partitions=2' \
  --conf 'spark.ui.enabled=false' \
  --master 'local[2]'
```

```sql
-- Begin a REPEATABLE READ transaction (default isolation — no need to specify).
-- The server-side snapshot is pinned lazily on the first query.
spark-sql> BEGIN TRANSACTION;

-- First query: triggers beginTransaction REST call, pins the snapshot.
spark-sql> SELECT count(*) FROM polaris.demo_ns.orders;
-- 5

-- Second query: same pinned snapshot — consistent with orders.
spark-sql> SELECT count(*) FROM polaris.demo_ns.customers;
-- 3
```

### Terminal B — Concurrent writer (separate Spark session)

```bash
source spark-env.sh
$SPARK_HOME/bin/spark-sql \
  --jars $ICEBERG_JAR,$POLARIS_PLUGIN_JAR \
  --properties-file spark-defaults.conf \
  --conf 'spark.sql.shuffle.partitions=2' \
  --conf 'spark.ui.enabled=false' \
  --master 'local[2]'
```

Insert into orders, then — after a pause — into customers. This simulates a multi-table write
with a gap between the two commits: exactly the scenario that produces a torn read without isolation.

```sql
spark-sql> INSERT INTO polaris.demo_ns.orders VALUES (1006, 1, 200.00, 'new');
-- orders is now 6, customers still 3 — torn-read window open

spark-sql> INSERT INTO polaris.demo_ns.customers VALUES (4, 'Dave', 'dave@example.com');
-- customers is now 4 — torn-read window closed
```

### Terminal A — Pinned snapshot still sees the consistent old state

Back in Terminal A's open transaction:

```sql
-- Still inside the REPEATABLE READ transaction.
-- Both queries return the values from BEFORE Terminal B's writes — no torn read.
spark-sql> SELECT count(*) FROM polaris.demo_ns.orders;
-- 5  ← pinned, not 6

spark-sql> SELECT count(*) FROM polaris.demo_ns.customers;
-- 3  ← pinned, not 4

-- A reader WITHOUT a transaction would see orders=6 but customers=3 between
-- Terminal B's two INSERTs — a classic torn read. REPEATABLE READ prevents this.

spark-sql> COMMIT;

-- After COMMIT the transaction is closed; unpinned reads see the latest data.
spark-sql> SELECT count(*) FROM polaris.demo_ns.orders;
-- 6
spark-sql> SELECT count(*) FROM polaris.demo_ns.customers;
-- 4
```

---

## Part 2 — Atomic Multi-Table Write

**What atomicity gives you:** an orchestrator allocates a server-assigned
`assigned_staging_path` per table via `CREATE SUBTRANSACTION FOR`. Each worker session
writes Iceberg metadata to exactly that path — invisible to all readers while in flight.
Only when `COMMIT` fires in the orchestrator does Polaris atomically swap every table's
metadata pointer in a single DB operation, making all changes visible simultaneously.

Open **3 terminal windows**. All run from `demos/multi-table-transaction/`.

### Terminal A — Orchestrator: begin and allocate subtransactions

Start a spark-sql shell:

```bash
source spark-env.sh
$SPARK_HOME/bin/spark-sql \
  --jars $ICEBERG_JAR,$POLARIS_PLUGIN_JAR \
  --properties-file spark-defaults.conf \
  --conf 'spark.sql.shuffle.partitions=2' \
  --conf 'spark.ui.enabled=false' \
  --master 'local[2]'
```

```sql
-- Begin the transaction (PENDING state — no REST call yet).
spark-sql> BEGIN TRANSACTION;

-- The first CREATE SUBTRANSACTION FOR fires the lazy beginTransaction REST call,
-- pinning the REPEATABLE READ snapshot at this statement — exactly like PostgreSQL.
-- No dummy SELECT required.
-- The result row shows:  subtxn_id=<N>, subtxn_token=<S>, staging_path=<P>
-- staging_path is shown for observability only; workers fetch it from the server.
-- Note subtxn_id and subtxn_token for the orders worker (call them SUBTXN1_*).
spark-sql> CREATE SUBTRANSACTION FOR demo_ns.orders;

-- Note subtxn_id and subtxn_token for the customers worker (SUBTXN2_*).
spark-sql> CREATE SUBTRANSACTION FOR demo_ns.customers;
```

Leave Terminal A open. Pass the respective `subtxn_id` and `subtxn_token` to
each worker — that is all they need. Workers fetch the staging path and snapshot
ID automatically from the server using the `GET /transactions/subtransactions/{id}`
endpoint.

### Terminal B — Worker: write to orders

Start a second spark-sql shell:

```bash
source spark-env.sh
$SPARK_HOME/bin/spark-sql \
  --jars $ICEBERG_JAR,$POLARIS_PLUGIN_JAR \
  --properties-file spark-defaults.conf \
  --conf 'spark.sql.shuffle.partitions=2' \
  --conf 'spark.ui.enabled=false' \
  --master 'local[2]'
```

Fill in the values from the orchestrator output:

```sql
-- Enter worker mode. The plugin calls GET /transactions/subtransactions/{id}
-- to fetch the staging path and snapshot ID — only subtxn_id and subtxn_token
-- are needed here.
spark-sql> BEGIN SUBTRANSACTION demo_ns.orders
             SUBTXN_ID=<SUBTXN1_ID>, SUBTXN_TOKEN='<SUBTXN1_TOKEN>';

-- Write staged (invisible to all other sessions).
-- PinnedTableOperations writes the Iceberg metadata.json to the server-assigned
-- staging path without touching the live catalog pointer.
spark-sql> INSERT INTO polaris.demo_ns.orders VALUES (1007, 2, 350.00, 'pending');

-- Commit this subtransaction — sends the staged metadata path to the server.
-- The catalog pointer is still unchanged.
spark-sql> COMMIT SUBTRANSACTION;
```

### Terminal C — Worker: write to customers

Start a third spark-sql shell (same preamble as Terminal B), then:

```sql
spark-sql> BEGIN SUBTRANSACTION demo_ns.customers
             SUBTXN_ID=<SUBTXN2_ID>, SUBTXN_TOKEN='<SUBTXN2_TOKEN>';

spark-sql> INSERT INTO polaris.demo_ns.customers VALUES (5, 'Eve', 'eve@example.com');

spark-sql> COMMIT SUBTRANSACTION;
```

### Terminal A — Orchestrator: commit atomically

After both workers have issued `COMMIT SUBTRANSACTION`:

```sql
-- Polaris swaps both tables' metadata pointers in a single DB transaction.
-- Both changes become visible simultaneously.
spark-sql> COMMIT;
```

### Terminal A — Verify atomicity

```sql
-- Refresh Spark's catalog cache to pick up the new metadata pointers.
spark-sql> REFRESH TABLE polaris.demo_ns.orders;
spark-sql> REFRESH TABLE polaris.demo_ns.customers;

spark-sql> SELECT count(*) FROM polaris.demo_ns.orders;     -- 7
spark-sql> SELECT count(*) FROM polaris.demo_ns.customers;  -- 5

-- A reader polling between the two COMMIT SUBTRANSACTION calls (in the
-- window where orders has committed its subtransaction but customers has
-- not yet) sees NEITHER update — atomic visibility guaranteed.
```

---

## What Was Demonstrated

| Scenario | Semantic | How it works |
|----------|----------|--------------|
| Part 1: REPEATABLE READ | Consistent multi-table reads, no torn reads | `BEGIN TRANSACTION` (REPEATABLE READ by default) pins an MVCC snapshot at the first statement; all `loadTable` calls in the session resolve through it transparently |
| Part 2: Atomic write | Both tables visible together or not at all | Orchestrator issues `CREATE SUBTRANSACTION FOR` per table; workers use `BEGIN SUBTRANSACTION` (subtxn_id + subtxn_token only) + INSERT + `COMMIT SUBTRANSACTION`; `COMMIT` swaps all pointers atomically |

These semantics map directly to MVCC concepts familiar from PostgreSQL:
- REPEATABLE READ ≈ `BEGIN TRANSACTION; SELECT ...` (REPEATABLE READ is the default isolation)
- Atomic write ≈ `COMMIT` with a single atomic pointer swap across all tables

---

## Part 3 — Cross-Engine, Multi-Table Consistent Read (Trino pins two tables Spark writes)

**What this shows:** Trino and Spark share the same Polaris REST catalog. A single Trino
transaction opened with `START TRANSACTION ISOLATION LEVEL REPEATABLE READ` pins a Polaris
snapshot on its first read — and that one snapshot covers **every** table the transaction touches.
While the transaction stays open, a Spark session commits a **torn multi-table write** underneath
(an `orders` row referencing a `customers` row that doesn't exist yet, then the matching customer).
The pinned Trino transaction keeps seeing the consistent *pair* — no torn read — until it commits.
This is the same REPEATABLE READ guarantee as Part 1, but now **across engines and across tables**.

> Verified live: a single Trino REPEATABLE READ transaction resolves both `orders` and `customers`
> through the same `read_point`. The counts stay `5 / 3` across the concurrent Spark write, and the
> dangling-FK invariant stays `0` inside the pin while a fresh read sees `1`.

### Prerequisites (Trino)

- A Trino server running with the Polaris REST catalog connector (catalog name `polaris`,
  schema `demo_ns`). For local dev this is the `trino-server-dev` build started via
  `testing/trino-server-dev/start-polaris-dev.sh` in the Trino repo.
- The Trino CLI jar. Build it once from the Trino repo:
  ```bash
  cd <trino-repo>
  JAVA_HOME=<jdk25> ./mvnw -pl client/trino-cli -am -q -DskipTests package
  # jar: client/trino-cli/target/trino-cli-*-executable.jar
  ```
- **Idle timeout:** Trino's default idle transaction timeout is **5 minutes**
  (`TransactionManagerConfig.idleTimeout`). If you pause too long between statements during the
  demo, Trino evicts the transaction and the next statement fails with:
  `Unknown transaction ID: ... Possibly expired? Commands ignored until end of transaction block`
  The `testing/trino-server-dev/etc/config-polaris.properties` used by `start-polaris-dev.sh`
  sets `transaction.idle-timeout=60m` to give the demo enough headroom. If you hit this error,
  restart Trino (`pkill -f DevelopmentServer`, then re-run `start-polaris-dev.sh`) and redo the
  part from `START TRANSACTION`.

### IMPORTANT — isolation level MUST be set in SQL

Trino only forwards a transaction's isolation level to a connector when it is written in the
**SQL statement**:

```sql
START TRANSACTION ISOLATION LEVEL REPEATABLE READ
```

Setting isolation via a client connection property or header (e.g. the Python `trino` client's
`isolation_level=`, or JDBC `setTransactionIsolation`) does **NOT** reach the Polaris connector —
Trino uses its internal default (`READ_UNCOMMITTED`) in that case, and no snapshot is pinned.
Always type the isolation level in the SQL, and keep the CLI session open at the prompt so the
transaction stays alive across the concurrent Spark write.

### Terminal A — Trino: pin BOTH tables in one REPEATABLE READ transaction

```bash
java -jar <trino-repo>/client/trino-cli/target/trino-cli-*-executable.jar \
  --server localhost:8080 --catalog polaris --schema demo_ns --user test
```

```sql
-- Isolation level in the SQL is REQUIRED (see note above). The FIRST read pins a Polaris
-- snapshot; every table read later in this transaction resolves through that same snapshot.
trino:demo_ns> START TRANSACTION ISOLATION LEVEL REPEATABLE READ;

trino:demo_ns> SELECT count(*) FROM orders;      -- 5  — pins the snapshot HERE
trino:demo_ns> SELECT count(*) FROM customers;   -- 3  — same pinned instant

-- Baseline cross-table invariant: no order references a missing customer.
trino:demo_ns> SELECT count(*) AS dangling FROM orders o
                 LEFT JOIN customers c ON o.customer_id = c.customer_id
                 WHERE c.customer_id IS NULL;    -- 0
```

**IMPORTANT — wait until Trino returns `5` and `3` before switching to Terminal B.** The snapshot
is pinned on the first *read*, not on `START TRANSACTION`. Leave this prompt open.

### Terminal B — Spark: commit a torn multi-table write

Insert the **orphan order first** (opening the torn-read window), then — after glancing back at
Terminal A — the matching customer. Running these as two separate statements lets you control the
window live.

```bash
source spark-env.sh
$SPARK_HOME/bin/spark-sql \
  --jars $ICEBERG_JAR,$POLARIS_PLUGIN_JAR \
  --properties-file spark-defaults.conf \
  --conf 'spark.ui.enabled=false' --master 'local[2]'
```

```sql
-- Step 1: an order for customer 6, who does NOT exist yet → dangling foreign key.
spark-sql> INSERT INTO polaris.demo_ns.orders VALUES (9500, 6, 42.00, 'live');
--         orders is now 6 in the live catalog; customers still 3 — window OPEN.

--   ⏸  Run the "still pinned" reads in Terminal A now, then come back.

-- Step 2: close the window by inserting the matching customer.
spark-sql> INSERT INTO polaris.demo_ns.customers VALUES (6, 'Frank', 'frank@example.com');
```

### Terminal A — Trino: pinned view is unmoved (no torn read), then commit → fresh

```sql
-- Still inside the same REPEATABLE READ transaction — pinned to the original instant.
trino:demo_ns> SELECT count(*) FROM orders;      -- STILL 5 — does NOT see Spark's commit
trino:demo_ns> SELECT count(*) FROM customers;   -- STILL 3 — consistent with orders

-- The killer line: the cross-table invariant HOLDS on the pinned snapshot, even though a
-- dangling FK now exists in the live catalog. The pinned reader is shielded from it.
trino:demo_ns> SELECT count(*) AS dangling FROM orders o
                 LEFT JOIN customers c ON o.customer_id = c.customer_id
                 WHERE c.customer_id IS NULL;    -- 0  ← no torn read

trino:demo_ns> COMMIT;

-- New (autocommit) statements — fresh reads see the latest committed data.
trino:demo_ns> SELECT count(*) FROM orders;      -- 6
trino:demo_ns> SELECT count(*) FROM customers;   -- 4

-- Run the invariant BETWEEN Spark's two INSERTs (window still open) to see it fail:
--   SELECT count(*) AS dangling ... WHERE c.customer_id IS NULL;   -- 1  ← the torn window is REAL
-- After Step 2 (customer 6 inserted) the fresh invariant returns 0 again.
```

> **The `0 → 1 → 0` story.** Inside the pinned transaction the invariant is always `0`. A fresh
> (unpinned) read taken *between* Spark's two INSERTs returns `1` — proof the dangling FK genuinely
> exists in the catalog. REPEATABLE READ is the difference between a reader that catches the torn
> state and one that never does — the same guarantee Part 1 gives Spark, now delivered to Trino.

---

## Part 4 — READ COMMITTED: flip one word, same session (the contrast)

**Behavior:** `START TRANSACTION ISOLATION LEVEL READ COMMITTED` does NOT pin a snapshot — each
statement reads the latest committed data, so a read that follows a concurrent commit reflects it,
even within the same open transaction. This is the direct contrast to REPEATABLE READ in Part 3.

**No new terminals.** Right after the Part 3 `COMMIT`, stay in the **same Trino session** and change
only the isolation level. Same query, different answer — that's the whole point.

Verified live via the CLI (RR stayed `5` across a Spark commit; RC then jumped `5 → 7`):

```sql
-- Terminal A (Trino) — same session as Part 3, immediately after COMMIT.
trino:demo_ns> START TRANSACTION ISOLATION LEVEL READ COMMITTED;   -- was REPEATABLE READ
trino:demo_ns> SELECT count(*) FROM orders;   -- baseline, e.g. 6

--   Terminal B (Spark): INSERT another row + commit  → latest is now 7

trino:demo_ns> SELECT count(*) FROM orders;   -- 7  ← MOVES: sees the concurrent commit (latest)
trino:demo_ns> COMMIT;
trino:demo_ns> SELECT count(*) FROM orders;   -- 7
```

> **The A/B in one terminal:** REPEATABLE READ froze at its pinned instant across a concurrent Spark
> write; READ COMMITTED — same session, same query, one word changed — tracks the latest. The reader
> chooses stable-vs-fresh with a familiar SQL name, no third session and no client-side config.

**How it works:** within one Trino connector transaction, a loaded table is cached at two layers —
the connector's own per-transaction table cache (`TrinoRestCatalog.tableCache`) and the underlying
Iceberg `RESTSessionCatalog` table cache. On the non-pinning (READ COMMITTED) path,
`IcebergMetadata.getTableHandle` invalidates **both** layers before each `loadTable`
(`TrinoRestCatalog.invalidateTableCache(session, table)`), so every statement re-resolves to the
latest committed snapshot. Pinned reads (REPEATABLE READ / SERIALIZABLE) and explicit-snapshot
reads keep the cache for stable, repeatable reads.

---

## Cross-Engine Summary

| Isolation (Trino) | How to request | Behavior | Status |
|---|---|---|---|
| REPEATABLE READ | `START TRANSACTION ISOLATION LEVEL REPEATABLE READ` (in SQL) | Pins a Polaris snapshot on first read; **one snapshot covers every table in the txn** — consistent multi-table view (no torn read) across concurrent Spark commits | Works |
| READ COMMITTED | `START TRANSACTION ISOLATION LEVEL READ COMMITTED` (in SQL) | Reads latest committed data each statement (both cache layers invalidated per statement) | Works |
| Explicit snapshot | `SET SESSION polaris.polaris_read_snapshot = <id>` | Pins reads to a specific Polaris `txn_snapshot_id` (allocate via `POST /transaction-snapshots`); works from any client | Works |

Note: client-side isolation settings (Python `trino` `isolation_level=`, JDBC
`setTransactionIsolation`) are NOT conveyed to the Polaris connector — only SQL-level
`ISOLATION LEVEL` is. Use the explicit-snapshot `SET SESSION` path for engines/clients that cannot
issue SQL-level isolation.
