# Multi-Table Transaction Interactive Demo

Demonstrates two core transaction semantics using live Spark sessions:

1. **Consistent multi-table reads** (no torn reads) — REPEATABLE READ isolation
2. **Atomic multi-table writes** — subtransaction staging + atomic commit

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
