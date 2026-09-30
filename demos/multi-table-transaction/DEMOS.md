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

# Running the four multi-table transaction demos

Four demos, run live against **one Polaris catalog**, across **Spark and Trino**:

1. **RC tears, RR doesn't** — READ COMMITTED catches a torn read; REPEATABLE READ, pinned first, does not.
2. **No partial write** — an atomic multi-table write is never visible half-applied, at any isolation level.
3. **Fan-out read** — one snapshot handle, shared by Spark *and* Trino, pins both engines to the same instant.
4. **Fan-out write** — a coordinator fans one transaction out to independent workers; nothing is visible until it commits.

Prereqs (Java, the Iceberg jar, the Polaris Spark plugin, the Trino CLI) are covered in
[`README.md`](README.md). This file is only the demo run-sheet. Everything below assumes you have run
the setup once and left the servers up.

---

## Setup (once)

Run these from the Polaris repo root, in order:

```bash
# 1. PostgreSQL (Docker) + a fresh Polaris server
./demos/multi-table-transaction/start-polaris.sh

# 2. Seed the demo tables — 5 orders, 3 customers (also drops any previous state)
source ./demos/multi-table-transaction/spark-env.sh
./demos/multi-table-transaction/setup_spark_tables.sh

# 3. Start the Trino dev server (from your Trino fork checkout)
cd /path/to/trino          # the Trino fork, lakehouse-transactions branch
nohup bash testing/trino-server-dev/start-polaris-dev.sh &
```

After `setup_spark_tables.sh` the data is in a known clean state (5 orders, 3 customers), so the
counts in each demo below are exact. Re-run `setup_spark_tables.sh` before a fresh pass.

> **Why the demos need `cache-enabled=false`.** `spark-defaults.conf` sets
> `spark.sql.catalog.polaris.cache-enabled false`. Without it, the Iceberg Spark catalog caches table
> metadata client-side for ~30 s, which *looks* like REPEATABLE READ but is just a stale cache —
> masking the real isolation behavior these demos show. It is already set in the config the launch
> commands use; no action needed, just don't remove it.

---

## Terminals

The minimum is **three**: two readers and one Spark writer. Demos 3 and 4 add one more.

| Terminal | Demos 1–2 | Demo 3 | Demo 4 |
|---|---|---|---|
| **Reader A** (left) | Trino, READ COMMITTED | swapped to **Spark** reader | — |
| **Reader B** (right) | Trino, REPEATABLE READ | Trino reader | the script's reader |
| **Writer** | Spark (`spark-sql`) | Spark writer | — |
| **extra** (optional) | — | curl for the snapshot handle | runs the Demo 4 script |

**Start a Trino reader** (repeat per Trino terminal):

```bash
# $TRINO_CLI is the CLI jar you built from the Trino fork (see README.md)
java -jar "$TRINO_CLI" \
  --server localhost:8080 --catalog polaris --schema demo_ns --user test
```

**Start a Spark session** (writer, and the Demo 3 Spark reader). `spark-env.sh` must be sourced in
this terminal first:

```bash
source ./demos/multi-table-transaction/spark-env.sh
$SPARK_HOME/bin/spark-sql \
  --jars "$ICEBERG_JAR,$POLARIS_PLUGIN_JAR" \
  --properties-file demos/multi-table-transaction/spark-defaults.conf \
  --conf 'spark.sql.shuffle.partitions=2' \
  --conf 'spark.ui.enabled=false' \
  --database demo_ns \
  --master 'local[2]'
```

Isolation level is set **in the SQL** (`START TRANSACTION ISOLATION LEVEL … , READ ONLY` in Trino,
`BEGIN TRANSACTION ISOLATION LEVEL …` in the Spark plugin) — client/JDBC properties do not reach the
connector. On Trino the `READ ONLY` access mode is required for REPEATABLE READ to pin a snapshot.

---

## Demo 1 — RC tears, RR doesn't

**Shows:** READ COMMITTED catches a torn read (a dangling foreign key); REPEATABLE READ, pinned
before the write, does not. This is the demo that proves the two isolation levels actually *differ*.

Both readers use a dangling-FK check — orders whose `customer_id` has no matching customer:

```sql
SELECT count(*) AS dangling FROM orders o
  LEFT JOIN customers c ON o.customer_id = c.customer_id
  WHERE c.customer_id IS NULL;
```

**1. Both readers pin first** (this is what freezes RR before the gap):

```sql
-- Reader A (Trino, left):
START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY;
-- <run the dangling-FK check> → 0

-- Reader B (Trino, right):
START TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
-- <run the dangling-FK check> → 0
```

**2. Writer inserts an orphan order, then stops** (customer 21 does not exist yet):

```sql
-- Writer (Spark):
INSERT INTO orders VALUES (2001, 21, 9.99, 'pending');
```

**3. Re-run the dangling-FK check in both readers:**

- Reader A (READ COMMITTED) → **1** — it sees order 2001 with no customer. Torn read.
- Reader B (REPEATABLE READ) → **0** — pinned before the write, it sees neither the order nor the gap.

(`SELECT * FROM orders` confirms: A shows 2001, B does not.)

**4. Writer closes the gap:**

```sql
-- Writer (Spark):
INSERT INTO customers VALUES (21, 'mike', 'mike@example.com');
```

- Reader A → **0** again (caught up).
- Reader B → still **0** (still pinned to the original instant).

**5. Reader B commits and re-queries:**

```sql
-- Reader B (Trino):
COMMIT;
-- <re-run the check> → 0, and SELECT * now shows 2001 + customer 21
```

Committing releases the pin, so B advances to the latest state — proving the pin was holding real
committed data back, not that the rows never existed. (It also leaves B closed, ready for a fresh
pin in Demo 2.)

> **Note:** RR dodged the tear only because it pinned *before* the writer's gap. Pin mid-write and it
> would see the orphan too — same as PostgreSQL. Isolation gives one consistent instant across
> tables; it does not promise that instant is clean. A clean instant comes from the writer — Demo 2.

---

## Demo 2 — No partial write

**Shows:** an atomic multi-table write is never visible half-applied — while the transaction is open,
**neither** READ COMMITTED nor REPEATABLE READ sees the uncommitted rows; on commit both tables
appear together. This is the backbone of the whole design.

Keep the same two Trino readers (A = RC left, B = RR right) and the Spark writer.

**1. Re-pin Reader B to now** (Demo 1 committed it, so it is closed):

```sql
-- Reader B (Trino, right):
START TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
-- <SELECT *> → sees Demo 1's rows (order 2001, customer 21); dangling 0
```

A fresh pin *after* Demo 1 but *before* this write is what makes the next check meaningful: if B
stays blind to the open write, that's the transaction boundary holding, not a stale snapshot. (Reader
A stays as-is — READ COMMITTED always reads latest.)

**2. Writer opens an atomic multi-table transaction, does not commit:**

```sql
-- Writer (Spark):
BEGIN TRANSACTION;
INSERT INTO orders    VALUES (2002, 22, 9.99, 'pending');
INSERT INTO customers VALUES (22, 'joe', 'joe@example.com');
-- leave OPEN
```

**3. Both readers query while the writer is open:**

- Reader A (READ COMMITTED) → new rows **not** there; dangling **0**.
- Reader B (REPEATABLE READ) → new rows **not** there; dangling **0**.

Neither isolation level can see uncommitted work — the backbone result.

**4. Writer commits:**

```sql
-- Writer (Spark):
COMMIT;
```

- Reader A → now sees order 2002 **and** customer 22 together, in one step; dangling still **0**.
  There was never an in-between state to observe.

**5. Reader B commits and re-queries** → advances past its pin and now sees the new rows too.

Every new order deliberately points at a brand-new customer id, so any early leak would show up
immediately as a dangling FK — it never does.

---

## Demo 3 — Fan-out read

**Shows:** one snapshot handle from the catalog, shared by **two different engines** (Spark and
Trino), pins both to the exact same dataset version while a writer moves on.

Swap Reader A from Trino to a **Spark** session (so now: Spark reader left, Trino reader right).

**1. Ask the catalog for one snapshot handle** (any terminal with `spark-env.sh` sourced, for
`$POLARIS_BASE` / `$POLARIS_TOKEN`):

```bash
curl -s -X POST "$POLARIS_BASE/transaction-snapshots" \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"lease_seconds": 1800}'
# → {"txn_snapshot":"eyJ2IjoxLCJyIjo3fQ","read_point":7,"expires_at":null}
```

Copy the `txn_snapshot` value (e.g. `eyJ2IjoxLCJyIjo3fQ`).

**2. Point both readers at the same handle:**

```sql
-- Spark reader (left):
USE SNAPSHOT 'eyJ2IjoxLCJyIjo3fQ';

-- Trino reader (right):  note the single quotes
SET SESSION polaris.polaris_read_snapshot = 'eyJ2IjoxLCJyIjo3fQ';
```

**3. Writer moves on** (a non-atomic write — it doesn't matter here):

```sql
-- Writer (Spark):
INSERT INTO orders    VALUES (2003, 23, 9.99, 'pending');
INSERT INTO customers VALUES (23, 'tom', 'tom@example.com');
```

**4. Re-run `SELECT` on both readers** → neither sees order 2003 / customer 23. One handle, two
independent engines, the same instant — pinned even as the world moves on underneath them.

**5. Release the pin on both:**

```sql
-- Spark:  USE SNAPSHOT NONE;
-- Trino:  RESET SESSION polaris.polaris_read_snapshot;
```

Both readers return to reading latest and now see the new rows. (Starting a fresh Spark/Trino session
has the same effect.)

---

## Demo 4 — Fan-out write

**Shows:** a coordinator fans one transaction out to independent workers; each stages its own table's
slice, and nothing is visible to anyone until the coordinator commits the parent — then both rows
appear together, atomically.

This one is **scripted** — it drives the REST API directly so the fan-out to separate workers is
clear. Run it in its own pane; it is Enter-paced (press Enter to advance each step):

```bash
./demos/multi-table-transaction/demo_distributable_transaction.sh
```

What it walks through:

1. Coordinator opens one parent transaction and creates two subtransactions — one for `customers`,
   one for `orders` — handing each worker its own `{subtxn_id, subtxn_token}`.
2. Worker A takes the `customers` subtransaction, inserts, and commits its slice.
3. Worker B takes the `orders` subtransaction, inserts, reads its own row back
   (read-your-own-write inside the slice), and commits its slice.
4. A Trino reader sees **nothing new** — the slices are staged, but the parent has not committed.
5. The coordinator commits the parent transaction.
6. The Trino reader now sees **both** rows, together.

To confirm the data really persisted (not just the script talking to itself), query from any reader
that was never part of the script — e.g. a Trino session at READ COMMITTED (or one where you first
`RESET SESSION polaris.polaris_read_snapshot;`). A reader still holding a pinned snapshot from Demo 3
would show the old data, so make sure it is unpinned.

The same thing is expressible in SQL (`BEGIN TRANSACTION` / `CREATE SUBTRANSACTION` /
`COMMIT SUBTRANSACTION` / `COMMIT`); the script uses REST because it is clearer for fanning work out
to separate worker processes.

---

## Resetting between passes

Re-run `./demos/multi-table-transaction/setup_spark_tables.sh` to drop and reseed the tables (back to
5 orders / 3 customers). Start fresh reader/writer sessions, or clear any pinned snapshot first
(`USE SNAPSHOT NONE;` / `RESET SESSION polaris.polaris_read_snapshot;`), so no stale pin carries over.
