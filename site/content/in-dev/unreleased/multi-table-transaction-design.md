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

# Multi-Table Transactions on the Iceberg REST Catalog — Design

Iceberg's REST catalog commits one table atomically, but has no way to commit *several* tables as a
unit, and no way for a reader to see several tables at one consistent instant. This document
describes a design that adds both, implemented as an additive extension to Apache Polaris.

It is written to be read without reading the code. **Part I** is the architecture and vocabulary a
contributor needs to navigate the implementation. **Part II** is the behavior — reads, writes,
isolation, locking, lifecycle — at a level a reviewer can judge and discuss. **Part III** is the
rationale: the choices we made and the tradeoffs behind them. Appendix C maps the concepts to their
PostgreSQL analogues for readers with that background; nothing in Parts I–III depends on it.

The design borrows the shape of a classic database transaction manager, but the reasoning is always
from what the lakehouse needs — the correspondence to PostgreSQL is corroboration, not justification.

---

## Part I — What it is

### 1. The problem

A lakehouse dataset is usually several tables that only mean something together — an `orders` fact
and its `customers` dimension, an `events` log and the `state` it drives, a ledger's debits and
credits. Iceberg keeps each table individually consistent, but nothing keeps *related* tables
consistent with each other. Two failure modes follow:

- **Partial multi-table write (no cross-table atomicity).** Load new `orders` and the new `customers`
  they reference, and you write two independent commits. Between them the dataset is broken — orders
  pointing at customers that do not exist yet. No commit order avoids it: each table needs the other,
  so a reader in the gap always catches a half-applied change. And it happens on every run, not only
  on failure — a crash just makes a transient gap permanent.
- **Torn multi-table read (no cross-table read consistency).** Even when every write is atomic, a
  reader scans several tables one at a time. If a writer commits in between, the reader joins one
  table's old state against another's new state — a combination that never actually existed.

A correct system must provide two guarantees: a set of table changes becomes visible **all at once or
not at all**, and a reader can pin **one instant** that every table it reads resolves against.

### 2. The core idea: coordinate in the catalog

In a lakehouse a table's current version is a **pointer the catalog publishes** — "table `orders`
lives at this metadata file." A commit is the catalog swapping that pointer. Data files are immutable;
what is visible is entirely a function of which pointer the catalog advertises.

That fixes where a multi-table transaction can live:

> Only the authority that owns the table pointers can swap several of them atomically in one step —
> and in a lakehouse, that authority is the catalog.

An engine sees the catalog as a remote API; it cannot swap two pointers atomically, and two engines
racing on the same table cannot referee each other. A client library has the same problem. A separate
coordinator either gets authority over the pointers — at which point it *is* the catalog — or
coordinates with the catalog across two authorities, which is two-phase commit, the thing we want to
avoid. So the catalog is not a convenient choice; it is the one place a multi-table commit is a single
local transaction instead of a distributed handshake. (Full argument: [§13](#13-server-side-transaction-state).)

### 3. Architecture at a glance

```
  Spark            Trino            (any engine w/ the thin REST client)
    │                │
    │  BEGIN / COMMIT / loadTable(?txn_snapshot | ?txn_id)
    ▼                ▼
┌─────────────────────────────────────────────────────────────┐
│  Polaris REST catalog  (stateless process; scales out)       │
│    TransactionCatalogAdapter   — JAX-RS endpoints            │
│    TransactionService          — lifecycle + commit logic    │
│    LockManager                 — lock acquire/release        │
│    TransactionReaper           — lease expiry, GC            │
└─────────────────────────────────────────────────────────────┘
                     │  all state in SQL
                     ▼
        Backing database (PostgreSQL)   ← atomicity lives here
```

Engines stay **unmodified**. They talk to the catalog through the thin Polaris REST client, which
speaks the transaction verbs (`BEGIN`, `COMMIT`, `ROLLBACK`, subtransactions) and passes an optional
read handle on `loadTable`. Trino needs no plugin at all — it participates through stock `loadTable`
plus two query parameters.

The **catalog server process is stateless**: it keeps no transaction state in memory between
requests. All state lives in the backing database, so any node serves any request, and a crash loses
nothing. Atomicity is the backing DB's job — a multi-table commit is one local SQL transaction, not a
distributed protocol.

The server-side pieces a contributor will look for:

| Component | Role |
|---|---|
| `TransactionCatalogAdapter` | JAX-RS resource; the REST surface ([Appendix A](#a-rest-api-surface)) |
| `TransactionService` | begin/commit/abort, staging, conflict classification |
| `LockManager` | lock modes, compatibility, canonical ordering, multi-node serialization |
| `TransactionReaper` | expires leased transactions, releases locks, advances GC watermark |
| `JdbcTransactionPersistence` | all SQL against the transaction tables |
| `TransactionManager` (Spark plugin) | client-side state machine; lazy begin, read-your-own-writes |

### 4. Core concepts and vocabulary

Defined once, used throughout:

- **Transaction** (`txn_id`) — a write transaction. A 64-bit id from a monotonic sequence, created
  when the first real statement runs (not at a bare client `BEGIN` — see [§10](#10-transaction-lifecycle-and-failure-handling)). Read-only consumers do **not** get one.
- **Operation** (`op_id`) — one table's staged change within a transaction. The unit that acquires a
  lock and stages metadata.
- **Subtransaction** (`subtxn_id`) — a retry/rollback/savepoint boundary that groups 1..N operations
  so they succeed, fail, or retry as a unit. Also the handle a remote worker attaches to.
- **`commit_scn`** — a per-catalog commit-order counter. Every commit is stamped with the next value,
  allocated transactionally so the stamp order equals true commit order.
- **`read_point`** — a captured `commit_scn` cutoff: "everything committed at or before this." One
  number that defines a consistent instant.
- **`TxnSnapshot`** — a reader's pinned view, carrying a `read_point`. For a standalone read it is an
  **opaque handle** with no server row; only a write transaction persists one.
- **`committed_table_snapshots`** (CTS) — the mapping from `commit_scn` to the concrete Iceberg
  metadata each commit produced. Turns a `read_point` into a per-table Iceberg snapshot.

Two things are both called "snapshot" and must not be confused: a **`TxnSnapshot`** is a
transaction-level read point; an **Iceberg table snapshot** is Iceberg's native per-table
point-in-time. CTS bridges the first to the second.

**Two write use cases**, referenced throughout:

- **Use Case 1 — single-session inline write.** One client session does `BEGIN … COMMIT`, writing
  every table itself; each table is a bare inline operation (`subtxn_id = NULL`), finalized directly
  at the top-level commit. This is the **inline path**.
- **Use Case 2 — distributed / parallel ingest.** Independent workers (separate processes or engines)
  each stage one table's work; a worker attaches with only `subtxn_id` + `subtxn_token`, writes, and
  commits its slice. This is the **subtransaction path**.

### 5. The data model

All transaction state is in the catalog's backing database (schema
`persistence/relational-jdbc/src/main/resources/{postgres,h2}/schema-v5.sql`). Begin-time ids
(`txn_id`, `txn_snapshot_id`, `op_id`) come from one monotonic sequence, `txn_id_seq`. The commit
clock is a **separate** transactional counter, `commit_scn_counter` — the two id spaces are distinct
and never mixed (see [§14](#14-a-single-read_point)).

Grouped by role:

**Lifecycle — the write transaction and its parts**

| Table | Holds | Key columns |
|---|---|---|
| `transactions` | one row per write transaction | `txn_id` PK, `parent_txn_id`, `txn_token`, `owner_principal`, `status` (0/1/2), `isolation`, `txn_snapshot_id` (NULL for READ_COMMITTED), `expires_at` |
| `subtransactions` | the retry/rollback/savepoint boundary | `subtxn_id` PK, `txn_id`, `name` (optional SAVEPOINT label), `subtxn_token`, `status` (OPEN/STAGED/ABORTED), `attempt` |
| `transaction_operations` | one table's staged change | `op_id` PK, `txn_id`, `subtxn_id` (NULL = inline op), `table_uuid`, `lock_mode`, `base_snapshot_id`, `server_base_metadata_location`, `assigned_staging_path`, `staged_metadata_location`, `op_token`, `table_entity_id`, `retry_of_op_id` |

Note two identifiers on an operation: **`table_uuid`** is the qualified table name (e.g. `ns.table`)
— the key used for locking and for CTS resolution — and **`table_entity_id`** is the numeric Iceberg
entity id carried alongside it.

**Read resolution — turning a `read_point` into per-table Iceberg metadata**

| Table | Holds | Notes |
|---|---|---|
| `committed_table_snapshots` | `(table_uuid, commit_scn, iceberg_snapshot_id, metadata_location, committed_at)` per commit | resolution reads the newest row with `commit_scn <= read_point`; indexed `(realm, catalog, table_uuid, commit_scn DESC)`, plus a `committed_at` index for the GC read-floor |
| `commit_scn_counter` | one `scn` row per `(realm, catalog)` | bumped `SET scn = scn + 1 RETURNING scn` inside the commit transaction |
| `txn_snapshots` | write-path read points only | a `REPEATABLE_READ` transaction persists one row; **standalone reads persist nothing** — they carry the read point in an opaque handle |

**Locking**

| Table | Holds | Notes |
|---|---|---|
| `locks` | granted/waiting locks | `resource` = `{table_uuid}` or `{table_uuid}:{partition}`; `lock_mode`; `status` (WAITING/GRANTED) |
| `lock_resource` | one always-present anchor row per resource | `SELECT … FOR UPDATE` gate that serializes the grant decision across nodes (a lock is a *new* `locks` row, so an anchor that always exists is needed to prevent a phantom-insert race — [§9](#9-locking-and-conflict-resolution)) |

**Garbage collection**

| Table | Holds | Notes |
|---|---|---|
| `txn_watermark` | one purge-horizon row per `(realm, catalog)` | CTS rows below the horizon are purgeable, except the newest survivor per table ([§12](#12-garbage-collection-and-table-maintenance)) |

---

## Part II — How it behaves

### 6. Reads: consistent multi-table snapshots

A consistent multi-table read pins **one `read_point`** and resolves every table against it.

1. The client acquires a read point (`POST /transaction-snapshots`, or implicitly at a
   REPEATABLE_READ `BEGIN`). The server reads the current `commit_scn` and hands it back as an
   **opaque handle** — `base64url({"v":1,"r":<read_point>})`. For a standalone read that is the whole
   cost: one `SELECT` of the commit counter, **no server row written**.
2. The client passes the handle on every read: `loadTable?txn_snapshot=<handle>`.
3. The server decodes the handle, extracts `read_point`, and resolves the table:

   ```sql
   SELECT iceberg_snapshot_id, metadata_location
   FROM   committed_table_snapshots
   WHERE  realm_id = ? AND catalog_id = ? AND table_uuid = ?
     AND  commit_scn <= :read_point
   ORDER BY commit_scn DESC
   LIMIT 1;
   ```

   It returns that immutable `metadata_location`; the client reads it directly. Every table resolves
   at the same cutoff, so the view is consistent even if a writer commits mid-read (a higher
   `commit_scn` is simply `> read_point` and invisible).

**Fan-out read.** Because the handle is a self-contained string, an orchestrator acquires it once and
hands it to many workers or engines. Each decodes and resolves locally — no shared row, no
coordination. Two Spark jobs minutes apart, or a Spark job and a Trino query, all read the same
dataset version.

**The handle carries no authority.** It is a read-point reference, not an auth or capability token.
Access is gated by the request's own credentials (the catalog already authorizes the principal for
the table); the handle rides inside an already-authorized request. A forged `read_point` can at worst
corrupt the caller's *own* read — it cannot cross a tenant boundary or read anything the caller
couldn't already. Encoding is for opacity and evolvability (clients treat it as opaque, like a
pagination cursor), not secrecy; an untrusted-client deployment can add an HMAC for integrity, but
correctness never rests on it.

**Fail-closed resolve.** Resolution can miss two ways, and both return `410 read point expired`,
never stale bytes: (a) the row was purged because the GC watermark moved past `read_point`
([§12](#12-garbage-collection-and-table-maintenance)); (b) the row survives but its `metadata.json`
was physically deleted by an external Iceberg maintenance job. The object-store existence check is a
cold fallback, not on the hot path.

### 7. Writes: stage, then publish atomically

A multi-table write is two phases: **prepare quietly, then reveal all at once.**

```
BEGIN
   stage table A   (write data files + a new metadata.json to a server-assigned path — no pointer moves)
   stage table B
   stage table C
COMMIT
   one local DB transaction swaps A, B, C pointers together + stamps one commit_scn
```

The prepare phase does all the slow work — writing data files and metadata to object storage — with
**no pointer moved**, so the staged files are physically present but invisible. Multiple tables (and,
in Use Case 2, multiple workers) stage in parallel. The finalize phase is one short local DB
transaction: swap every table's pointer, insert a `committed_table_snapshots` row per table with a
single shared `commit_scn`, mark the transaction committed, release locks. Commits ⇒ all pointers
moved; rolls back ⇒ none did.

**Abort needs no undo.** Since staged files are only revealed by the pointer swap, aborting means
never doing the swap. The staged files become orphans a cleanup job reclaims later; there is nothing
to roll back.

**Two ways to drive it:**

- **Use Case 1 (inline).** One session stages every table itself and commits. Operations have
  `subtxn_id = NULL` and are finalized directly.
- **Use Case 2 (fan-out).** An orchestrator opens the transaction, creates one subtransaction per
  table, and hands each worker only its `subtxn_id` + `subtxn_token`. Workers stage independently —
  different processes, even different machines — then the orchestrator commits. There is no single
  client that holds the whole write set; the transaction *lives on the server*, which is the one
  place every worker can reach. That rendezvous is why the write path keeps server state
  ([§13](#13-server-side-transaction-state)).

### 8. Isolation levels

Two levels are implemented, named as in SQL; a third is future work.

| Level | What a reader sees | Mechanism |
|---|---|---|
| **READ COMMITTED** | latest committed data, per statement | a fresh `read_point` each statement; no pin |
| **REPEATABLE READ** *(default)* | one frozen instant for the whole transaction, across every table | one `read_point` captured at first access, reused for every read |
| **SERIALIZABLE** *(future)* | full serializability (no write skew) | REPEATABLE READ + server-side read-set tracking + write-skew detection at commit |

REPEATABLE READ here *is* snapshot isolation — the read view is fixed at the file level, so phantom
reads cannot occur within a transaction. A writer at REPEATABLE READ also gets read-your-own-writes:
it sees its own staged changes while everyone else sees the pre-transaction state. SERIALIZABLE is
deferred because it structurally needs durable server-side read-set state; snapshot isolation covers
the multi-table use cases this design targets ([§13](#13-server-side-transaction-state) notes how the
door stays open).

| Anomaly | READ COMMITTED | REPEATABLE READ | SERIALIZABLE |
|---|---|---|---|
| Dirty read | never | never | never |
| Non-repeatable read | possible | never | never |
| Phantom read | possible | never | never |
| Write skew | possible | possible | never |

### 9. Locking and conflict resolution

Locking is **optimistic by default, pessimistic on request**. There are two behavioral classes today:

| Mode | Blocks? | Used for |
|---|---|---|
| `ROW_EXCLUSIVE` | no — self-compatible | default for every write; concurrency preserved, conflicts caught at commit |
| `EXCLUSIVE` | yes — blocks all other writers on the resource | opt-in for costly destructive writes (overwrite, row-delta, compaction) |

Compatibility: only `(ROW_EXCLUSIVE, ROW_EXCLUSIVE)` is compatible; every other pair conflicts. So
appends and additive writes never block each other, and Iceberg's high-throughput ingest is
unaffected unless a client explicitly reaches for `EXCLUSIVE`.

> A third constant, `ACCESS_EXCLUSIVE`, is defined for future DDL-in-transaction use, but today its
> compatibility row is identical to `EXCLUSIVE` (blocks everything, blocked by everything) and no code
> path treats it differently — so there are effectively **two** modes. Reads take no lock at all, so
> the intended "blocks readers too" distinction is not yet meaningful.

**Acquired per operation, at operation-create time** (`createSubtransaction` / `createOperation`) —
**not** at `BEGIN`. Release timing differs by mode: a `ROW_EXCLUSIVE` lock is released early, when
its operation commits or aborts (so a waiting `EXCLUSIVE` acquirer proceeds as soon as staging is
done); an `EXCLUSIVE` lock is held until the whole transaction ends.

**Conflict routing at commit.** Conflicts are checked twice. Before finalize, the server compares
each table's baseline (`server_base_metadata_location`, captured at operation-create, or the
REPEATABLE READ read point on the inline path) against the current committed pointer; a table with no
baseline but a committed version now also counts as changed. Unchanged → direct pointer swap.
Changed →

- **`ROW_EXCLUSIVE`, subtransaction path → merge-and-retry.** The server holds the operation's staged
  metadata, re-derives the data files of every snapshot it adds, and re-applies them onto the current
  state with a fresh `newAppend()`. Commit succeeds; the client does nothing.
- **Inline path, REPEATABLE READ → first-committer-wins abort (HTTP 409)**, regardless of lock mode.
- **`EXCLUSIVE` → first-committer-wins abort**, always.

Locks do not cover every writer (a `ROW_EXCLUSIVE` lock blocks no one and is released at stage time;
an inline transaction takes none), so the table can still move after that check. Finalize therefore
re-checks each table against the exact state its pointer swap replaces: the staged metadata is
accepted only if the live metadata file appears in its metadata log, i.e. the client built on the
live state. Every commit writes a new metadata file at a unique path, so this catches any concurrent
change — data, schema, properties, snapshot expiry — not just a moved current snapshot. On a
mismatch, the subtransaction path and the inline READ COMMITTED path re-apply the staged appends onto
the live state; the inline REPEATABLE READ path aborts. Re-applying works only for additive changes:
a staged change that is destructive (overwrite, delete, replace) or metadata-only fails with HTTP 409
instead. Anything that commits after this re-check fails the entity-version check of the final batch
write, so the transaction never overwrites a commit it did not see.

This is one-directional self-defense, not mutual exclusion: `ROW_EXCLUSIVE` aborts *your* transaction
if the table moved under it, but never blocks an independent writer. A client that must actually
exclude others opts into `EXCLUSIVE`.

**Multi-node serialization.** `LockManager.acquire` runs its check-then-insert under a
`SELECT … FOR UPDATE` on the resource's `lock_resource` anchor row. A held lock is a *new* `locks`
row, so two nodes could each read "no conflict" and both insert — a phantom-insert race that locking
the `locks` table cannot prevent. The always-present anchor row closes it; it is held only for the
microsecond critical section. Acquire never blocks server-side — it grants or returns 409 with a
retry hint, and the client retries with backoff. Deadlock is prevented by never waiting inside the
server while holding a lock, plus canonical lock ordering (sort by `table_uuid`) for orchestrators
taking several locks.

### 10. Transaction lifecycle and failure handling

**Lazy begin.** In the Spark plugin a user's `BEGIN TRANSACTION` does not call the server — it records
a `PENDING` marker in session state. The server-side `txn_id` is created on the first real statement
(first `loadTable` or `CREATE SUBTRANSACTION`), transitioning `PENDING → ACTIVE`. An empty
`BEGIN … COMMIT`, or a read that turns out single-table, costs the server nothing.

**Three end paths, all symmetric.** A transaction leaves `IN_PROGRESS` exactly once — by commit,
abort, or reap (lease expiry) — and all three release every lock and the linked read snapshot. That
symmetry is what bounds lock leakage.

**Lease + reaper.** Every transaction has an `expires_at` (default 3600 s). `TransactionReaper` runs
on every node from startup, finds `IN_PROGRESS AND expires_at < now()`, marks them `ABORTED`, and
releases their locks. Running on all nodes is safe because every action is idempotent (double-abort
is a no-op, purges converge, the watermark only advances). **Safety never depends on the reaper**:
visibility is the final pointer swap, so a crashed client's un-published work is invisible regardless;
the reaper only reclaims locks and space. So an abandoned `EXCLUSIVE` lock is released within ~one
lease + one reaper interval — never held forever.

**Session-independent.** A transaction is a server-side, token-addressed, DB-durable object keyed by
`(txn_id, txn_token)`. It survives the originating session's death and even a server restart; only
the client's in-memory handle is session-scoped. Any holder of the token can drive it — which is
exactly how a Use Case 2 worker attaches to a subtransaction it did not begin.

**Two workers on one subtransaction.** The `subtxn_token` is a transferable capability with no owner,
so two workers *could* be handed the same one. The system resolves this at commit, not admission:
both stage, the first `commitSubtransaction` flips the subtransaction `OPEN → STAGED` and wins; the
loser's later commit hits the `status == OPEN` guard and is rejected. Crucially, the loser cannot
harm the winner — `rollbackSubtransaction` also requires `OPEN`, so the loser's abort of the now-
STAGED subtransaction fails fast (HTTP 400) before touching anything, while the loser's own client
session clears unconditionally. A STAGED subtransaction is immutable to everyone until the parent
commits or aborts.

### 11. Commit protocol

Commit is a client-side **prepare** followed by a server-side **finalize**:

| Phase | Who | What |
|---|---|---|
| Prepare | client / workers | write data files + staged `metadata.json` to the server-assigned path; `POST …/subtxn/{id}/commit` per subtransaction → server records `staged_metadata_location`, marks it STAGED |
| Finalize | catalog server | one local DB transaction: conflict-check each operation → swap every table pointer → insert a CTS row per table (one shared `commit_scn`) → mark transaction COMMITTED → release locks |

The prepare phase parallelizes across tables and workers; finalize is one short transaction and the
only serialization point. Atomicity comes **for free** from the backing database: if that one SQL
transaction commits, every pointer moved; if it rolls back (conflict, crash, deadlock), none did, the
transaction stays `IN_PROGRESS`, and the reaper cleans up later. No reader ever sees a partial state,
and there is no distributed two-phase commit to coordinate. `commit_scn` is allocated inside this same
transaction, so its order is exactly commit order ([§14](#14-a-single-read_point)).

### 12. Garbage collection and table maintenance

Two things are reclaimed independently, and keeping them straight is the whole GC story:

| Domain | Reclaims | Governed by | Safety |
|---|---|---|---|
| **Our rows** (`committed_table_snapshots`) | pointer history | watermark `W` (our purge) | `W` must be ≤ every live read point |
| **Iceberg files** (`metadata.json`, manifests) | object storage | Iceberg snapshot expiry, per table, owned by the table owner | fail-closed resolve → `410` |

**The watermark `W`** is the purge horizon for CTS, one durable row per `(realm, catalog)` in
`txn_watermark`. Invariant: `W ≤ read_point(t)` for every still-honorable read `t`. Over-advancing is
a torn read (bug); under-advancing is safe (just retains extra history). It is the minimum of two
floors:

```
W = MIN( write_floor, read_floor )
write_floor = MIN(read_point) over live write transactions      -- exact; we own these rows
read_floor  = MAX(commit_scn) WHERE committed_at <= now - L - skew_pad   -- conservative
```

Write transactions have rows, so their floor is exact. Standalone reads have **no rows** (the price
of the stateless handle), so `read_floor` cannot be an exact MIN — it is derived conservatively from
a **lease `L`**: the server retains pointer history for `L` after a read point could have been
issued, using the `(committed_at, commit_scn)` map already in CTS. `L` is a server-side retention
budget, not a client validity promise — a read past `L` degrades to a clean `410`, never a torn read.

**Per-table survivor.** Purge keeps, per table, the newest row with `commit_scn <= W` (plus
everything above `W`), and deletes only rows strictly older than that survivor — so a reader pinned
exactly at `W` can still resolve a table that has not changed recently.

**Two id-space horizons.** CTS purge is gated by a `commit_scn` (the watermark above). Lifecycle rows
(`transactions`, `subtransactions`, `transaction_operations`, `locks`) are keyed by `txn_id` and
purged below `MIN(txn_id)` over in-progress transactions. `commit_scn` and `txn_id` are different id
spaces and are never compared.

**Why not extend Iceberg retention to the watermark?** Because that would let a leaked or long-lived
read pin physical storage indefinitely — the exact abandoned-context liability the stateless handle
avoids. Iceberg retention stays sovereign over files; fail-closed resolve keeps reads correct when a
file is gone.

**Known gap.** No server-side cleanup of orphaned staged files exists yet — abort, rollback-retry,
reap, and worker re-stage all leak metadata/data files that no committed pointer references. This is a
storage leak, not a correctness problem (the committed table only ever references the last staged
metadata), and reclaiming them is a tracked TODO.

---

## Part III — Why (design decisions & tradeoffs)

### 13. Server-side transaction state

The central choice: multi-table *write* state lives on the server (durable rows), while the *read*
path is stateless (an opaque handle). This split is deliberate, and the reason is what the lakehouse
needs, not an appeal to how databases are built.

**Why writes need server state.** A stateless "carry a base commit id" protocol assumes one client
assembles the whole write set locally before commit. Fan-out write breaks that: independent workers —
separate processes, even separate machines — each stage part of one transaction. That needs a
rendezvous every worker can reach and attach to, and a client process is not that place. The catalog
is. The durable transaction is not overhead bolted onto a stateless design; it is the thing that lets
a transaction span independent workers at all. Locks that must outlive a worker, and abort that must
reclaim another worker's staged slice, need the same shared home.

**Why reads stay stateless.** A read needs only a cutoff value, which the client can carry itself. So
a standalone read opens no transaction and writes no row — it holds an opaque handle and resolves
against the shared commit history. This keeps the common, high-frequency path cheap and makes fan-out
read a matter of copying a string.

**What it costs, and the bound.** The server tier stays stateless (state is in the backing DB, so any
node serves any request); the operational cost is the database itself, already required for a
production catalog. Leaked or abandoned transactions are bounded by the lease + reaper.

The read path being *independent of its storage* also keeps SERIALIZABLE reachable: that level needs
durable server-side read-set tracking, which re-materializes a row behind the same `TxnSnapshot`
concept — an evolution, not a redesign. (This is where the design corresponds to a classic
transaction manager; see [Appendix C](#c-postgresql-correspondence). The correspondence is
reassurance that the shape is sound, not the reason for it.)

### 14. A single `read_point`

Reader visibility is one number — a `commit_scn` cutoff — not a richer per-transaction visibility
set. This works because of how commits are recorded:

- A `committed_table_snapshots` row is written **only at commit**; an aborted or in-flight
  transaction leaves no row. So the mere existence of a row means "committed" — there is nothing
  aborted or in-flight to filter out. The only question a reader asks is *ordering*: did this commit
  land at or before my cutoff? One monotonic value answers that completely.
- The cutoff is sound only if `commit_scn` order equals true commit order. `commit_scn` is allocated
  `UPDATE commit_scn_counter SET scn = scn + 1 RETURNING scn` **inside the same transaction as the
  pointer swap**. The row lock is held until that transaction commits, so a higher `commit_scn`
  becomes visible only after every lower one. A plain read of the counter then can never exceed a
  not-yet-visible commit — closing the assign-before-visible window a non-transactional sequence
  would leave open. The tradeoff is that commits serialize on the counter *within a catalog*
  (different catalogs are independent).

A richer visibility set (tracking specifically which in-flight transactions to exclude) is what a
system needs when uncommitted and committed versions physically coexist and must be filtered per row.
This design has no such coexistence, so it does not need one.

### 15. Optimistic by default, pessimistic opt-in

Optimistic concurrency is the model Iceberg already uses: a plain single-table write is a
compare-and-swap on the metadata pointer, and non-transactional writes keep using Iceberg's native
CAS unchanged. Transactions extend the same *idea*, but the catalog does the check itself rather than
delegating to Iceberg's CAS — at finalize the server must commit with `base == current`, so Iceberg's
own OCC can no longer see a concurrent write, and the catalog's commit-time checks (the baseline
vs. the current committed pointer, then the staged metadata's lineage vs. the live metadata file
at finalize) *are* the OCC for the transaction path.

The default `ROW_EXCLUSIVE` mode is non-blocking so that the dominant lakehouse workload — many
concurrent appends — keeps running exactly as it does without transactions. When that divergence
check fires on the subtransaction path, the conflict is repaired by server-side **merge-and-retry**
(re-parent the added files onto current state), so an additive conflict does not force re-running the
whole transaction.

Pessimistic `EXCLUSIVE` locking is worth its blocking cost only for **destructive operations that
cannot be merged** — overwrite, row-delta, compaction, destructive DDL — where an optimistic conflict
would waste hours of work with no safe merge. There, reserving the resource upfront trades a wait for
guaranteed progress. It is a per-operation choice the client reaches for, never the default tax on
every write.

### 16. Overhead: is the protocol chatty, and does it get hot?

Two questions decide whether server-side transactions are affordable: how many extra round-trips a
transaction adds (is the protocol *chatty*?), and what that work costs the database under load —
including whether it creates a hot spot. The per-phase figures below are **measured** by
`TxnPersistenceBenchmark` driving
`JdbcTransactionPersistence` against a real PostgreSQL (2000 iterations per cell, concurrency 1–32);
the zero point is a plain OCC pointer swap — exactly today's non-transactional write.

**Chatty? Reads add nothing; writes add a bounded, fixed number of calls.**

- A **READ COMMITTED** read is byte-for-byte a plain catalog load — no transaction opened, no extra
  call. A **REPEATABLE READ** consistent read adds exactly **one** call to pin the snapshot, then
  reuses that one handle across every table and engine in a fan-out. Staging a write is a
  direct-to-object-store PUT, not a catalog call, so a write statement inside a transaction issues
  **zero** extra catalog calls on its own.
- A write transaction adds three round-trips around the work — **begin** (pin a read point),
  **subtransaction** (stage + lock), **commit** (allocate `commit_scn` + swap pointers). Fan-out adds
  `~3` calls per worker, but workers run in **parallel**, so the critical path stays roughly constant
  as workers are added. This is the opposite of chatty: extra tables add throughput cost, not
  round-trip depth, and never a per-statement register/delete dance.

**What it costs the database, and whether it gets hot.** These are database-op metrics — each phase's
persistence work measured in isolation, not end-to-end RPC latency. The per-phase bookkeeping is a
fixed **~0.7–1.8 ms** at low concurrency (begin ≈ 0.7 ms, commit ≈ 1.8 ms; a consistent-read resolve
is ~0.1–0.8 ms at ~30k ops/s). The one phase that scales is **staging**, intrinsically — N tables
means N metadata writes — and it grows **linearly**: 1→8 tables roughly doubles latency (~1.9→3.6 ms),
a per-table write, not a contention cliff.

Under concurrency the numbers hold, because there is only one point that can serialize commits: the
per-`(realm, catalog)` `commit_scn` counter behind `allocateCommitScn` (a single row locked to
commit). If transactions made the DB hot, it would show there — and it doesn't: throughput holds
**~2,900 ops/s within ±13% from 1 to 32 concurrent committers**. It is a serialization point, not a
saturation point; different catalogs never contend, and everything else in a transaction sits on
disjoint primary keys. Reads are unaffected too — a REPEATABLE READ resolve under 8 background writers
is statistically indistinguishable from the same resolve with no write load. The design deliberately
keeps per-table staging *off* the counter's critical section, so fan-out costs connections and
wall-clock, never contention.

Set against the operation it rides on, this is small: a single Iceberg commit is several sequential
object-store PUTs (data files, manifests, manifest list, `metadata.json`), each ~20–100 ms — an order
of magnitude larger than all the bookkeeping combined. The tax is single-digit-percent on an
operation that is fundamentally I/O-bound, not catalog-bound. (Reproduce:
`POLARIS_BENCH=true ./gradlew :polaris-relational-jdbc:test --tests "*.TxnPersistenceBenchmark"`.)

### 17. Mixed write paths and CTS completeness

Two paths can advance a table's pointer: the **transaction path** and the ordinary **plain path**
(`createTable`, `updateTable`, `registerTable`, and any other Iceberg commit through the catalog). If
only the transaction path recorded CTS rows, a table written both ways would have a *partial*
history — a snapshot read could resolve a stale row that silently diverges from live state. Three
ways to handle it:

- **Option 1 — enrollment.** Once a table has any CTS row it is "enrolled"; subsequent plain writes
  are rejected unless inside a transaction. Zero write-path cost, but a breaking change for plain
  clients on enrolled tables.
- **Option 2 — universal CTS (implemented).** Every commit, transactional or not, allocates a
  `commit_scn` and inserts a CTS row in the same JDBC transaction as the pointer swap. No breaking
  change and CTS stays complete; the cost is a counter bump + insert on every write and more purge
  load.
- **Option 3 — resolve-time detection.** Detect the partial-history case at read time. Useful as
  observability layered on Option 1 or 2, not a standalone fix.

The implementation uses **Option 2**: plain commits go through the same atomic entity-swap + CTS
insert as transactional ones, so plain writers keep working unchanged on every table and snapshot
readers see their commits in order. When the backing store has no transaction schema (in-memory,
NoSQL, or a relational schema older than v5) no CTS rows are written, and the transaction endpoints
are unavailable.

One gap remains: a table with **no** CTS row at or below the read point resolves to its *live*
state rather than failing. That covers a table that predates the transaction schema and has not
been written since, and a table created after the read point. A pinned read of such a table is
therefore not isolated from later commits to it.

### 18. Scope and non-goals

- **Additive and opt-in by design.** There is no table-format change and no server feature flag: the
  transaction endpoints are simply present, and a client that never calls them sees a stock Iceberg
  REST catalog, unchanged. A client opts in per operation — pass a read handle for a consistent read,
  or wrap writes in `BEGIN`/`COMMIT`. Plain writes are never rejected; they are recorded in CTS like
  transactional ones ([§17](#17-mixed-write-paths-and-cts-completeness)).
- **REST catalog only.** The stateful-server coordinator model is what makes this tractable; Hive
  Metastore / Glue / JDBC catalogs are out of scope.
- **DML only.** `CREATE`/`ALTER`/`DROP` inside a transaction is a non-goal today (it is the case that
  most needs an `ACCESS_EXCLUSIVE` lock held across statements). The server does not reject a staged
  metadata-only change: it commits if nothing else changed the table, and fails with HTTP 409
  otherwise ([§9](#9-locking-and-conflict-resolution)).
- **Single catalog.** A transaction is scoped to one `(realm, catalog)`; cross-catalog is out of
  scope.
- **SERIALIZABLE is future work.** REPEATABLE READ (snapshot isolation) and READ COMMITTED are the
  built, selectable levels.
- **PoC hardening gaps** are called out where they exist (orphaned-file cleanup
  [§12](#12-garbage-collection-and-table-maintenance); single-leader reaper gating; HMAC on handles
  for untrusted clients [§6](#6-reads-consistent-multi-table-snapshots); per-transaction lock limit
  [§19](#19-security-considerations); pinned reads of tables with no CTS history
  [§17](#17-mixed-write-paths-and-cts-completeness)).

### 19. Security considerations

**Two-tier tokens.** Authority is split so each participant holds only what it needs:

- `txn_token` — held by the orchestrator only; authorizes commit/abort/renew and creating
  subtransactions.
- `subtxn_token` — handed to one worker; authorizes staging and committing *its* subtransaction only.
- `op_token` — per-operation secret; authorizes that operation, not the top-level transaction.

Tokens are capability secrets (32 random bytes, compared by equality), **not** authentication.
Authn/authz remain the request's own credentials — a worker must still be an authorized principal,
and table-level `WRITE` is checked at subtransaction-create time, so a transaction envelope grants no
extra privilege. `owner_principal` is recorded at begin for audit and admin override.

**Staged-metadata trust boundary.** The client writes metadata to object storage and hands the server
a path. What the server checks today:

- **Subtransaction path:** the server assigns the staging path (inside the table's own metadata
  directory) and rejects a commit that names any other path.
- **Inline path:** the client chooses the path and the server accepts it as given.
- **Both paths, at finalize:** the staged file must belong to the table — its `table-uuid` must match
  the live table's, else the commit fails with HTTP 400 — and it is swapped in only if it was built
  on the table's live state ([§9](#9-locking-and-conflict-resolution)). The ordinary location checks
  of an Iceberg commit still apply.

So a client cannot commit a metadata file built for another table: swapped in, it would leave the
table unreadable, and merge-retried, it would append the other table's data files.

**Bounded blast radius.** The read handle carries no authority
([§6](#6-reads-consistent-multi-table-snapshots)); lock DoS is bounded by short leases and the reaper.
There is no per-transaction limit on the number of locks yet.

### 20. Relationship to prior art & future direction

The Iceberg community has an open design discussion on multi-table transactions with two branches —
a stateless *CatalogSequenceNumber* (Option 1) and a stateful *TransactionContext* (Option 2). This
design is a **hybrid**: the read path borrows the stateless-handle idea (Option 1's thinking), while
the write-side coordination is server-side (converging with Option 2, arrived at independently). The
community leans stateless on paper but the decision is not settled — a working PoC is meant to inform
it. The full landscape, positions, and provenance live in the companion prior-art write-up; this doc
does not re-argue them.

**Future direction.** The maximal-stateless endgame would embed `commit_scn` in the Iceberg table
metadata itself, so a reader resolves against the table's own metadata-log lineage and CTS's
resolution role disappears — the catalog stays thin. This needs an Iceberg **spec** change (a
first-class, interoperable field; retention that honors the catalog's lease rather than pruning
underneath it) and still leaves the read-floor time→scn derivation needing a home. Until the spec
carries `commit_scn` durably, CTS is the catalog-side stand-in that ships today. Note this direction
is the stateless branch, and it forecloses SERIALIZABLE the same way statelessness does — a stance,
not a settled conclusion.

---

## Appendix

### A. REST API surface

Transaction endpoints are a Polaris extension under `/polaris/v1/{prefix}/`
(`spec/polaris-catalog-apis/transaction-apis.yaml`). Reads ride two optional params on the existing
`loadTable`.

**Consistent read handles** — used by pure readers; no `txn_id`, no CLOG entry.

```
POST   /polaris/v1/{prefix}/transaction-snapshots                      createTransactionSnapshot
GET    /polaris/v1/{prefix}/transaction-snapshots/{txn-snapshot-id}    getTransactionSnapshot
DELETE /polaris/v1/{prefix}/transaction-snapshots/{txn-snapshot-id}    releaseTransactionSnapshot
POST   /polaris/v1/{prefix}/transaction-snapshots/{txn-snapshot-id}/renew   renewTransactionSnapshot
```

**Write transactions** — the orchestrator holds `txn_token`.

```
POST   /polaris/v1/{prefix}/transactions                   beginTransaction   → txn_id, txn_token
GET    /polaris/v1/{prefix}/transactions/{txn-id}          getTransaction
POST   /polaris/v1/{prefix}/transactions/{txn-id}/commit   commitTransaction  (txn_token)
POST   /polaris/v1/{prefix}/transactions/{txn-id}/abort    abortTransaction   (txn_token)
POST   /polaris/v1/{prefix}/transactions/{txn-id}/renew    renewTransaction   (txn_token)
```

**Subtransactions** — created by the orchestrator; each `subtxn_token` handed to one worker.

```
POST   /polaris/v1/{prefix}/transactions/{txn-id}/subtransactions                     createSubtransaction  (txn_token)
GET    /polaris/v1/{prefix}/transactions/subtransactions/{subtxn-id}                  getSubtransaction     (subtxn_token)
POST   /polaris/v1/{prefix}/transactions/{txn-id}/subtransactions/{subtxn-id}/commit  commitSubtransaction  (subtxn_token)
POST   /polaris/v1/{prefix}/transactions/{txn-id}/subtransactions/{subtxn-id}/rollback rollbackSubtransaction  (subtxn_token | txn_token)
POST   /polaris/v1/{prefix}/transactions/{txn-id}/subtransactions/{subtxn-id}/abort   abortSubtransaction   (subtxn_token | txn_token)
POST   /polaris/v1/{prefix}/transactions/{txn-id}/subtransactions/{subtxn-id}/renew   renewSubtransaction   (subtxn_token)
```

**Reads within the isolation model** — two optional params on the standard endpoint:

```
GET /v1/{prefix}/namespaces/{ns}/tables/{table}
      ?txn_snapshot={handle}    consistent pinned read (opaque handle from createTransactionSnapshot)
      &txn_id={txn-id}          read-your-own-writes within a transaction
```

Tier for upstreaming: the two read params are a small, backward-compatible addition proposable to the
Iceberg REST spec now; the write verbs are a Polaris-namespaced extension today.

### B. Behavioral examples

**Shared snapshot vs. independent snapshots (read).** Two Spark apps join `orders` and `customers`
downstream. If each acquires its own snapshot, a writer committing between the two acquisitions gives
them different instants. Instead the orchestrator acquires **one** handle
(`read_point = 5010`, before writer W's `commit_scn = 5011`) and passes it to both; both resolve
`orders` and `customers` as of 5010, W excluded, regardless of when each app starts. To include W,
acquire the handle *after* W commits. The orchestrator controls exactly which committed state all
apps see by choosing *when* to acquire.

**Reader vs. writer (no blocking).** A reader pinned at `read_point = 5000` keeps seeing `orders` at
its pre-writer snapshot for its whole duration while a writer appends and commits at
`commit_scn = 5001` — the new commit is `> 5000`, invisible to the reader. Neither blocks the other.

**Writer vs. writer, additive (concurrent).** Two appends to `orders` (same or different partitions)
both take self-compatible `ROW_EXCLUSIVE`, both commit; manifests merge. Full concurrency, no waiting.

**Writer vs. writer, destructive (serialized).** Two row-delta writers on the same partition both opt
into `EXCLUSIVE`. The first is granted and writes; the second waits, is granted after the first
commits, reads the updated base, and proceeds. No wasted work, correct serialization.

**Fan-out write (Use Case 2).** An orchestrator opens `T1`, creates subtransactions for `orders`,
`customers`, `products`, hands each worker its `{subtxn_id, subtxn_token}`. Workers stage in parallel
(different processes). When all report STAGED, the orchestrator commits `T1`; all three tables become
visible together. If a worker crashes, the orchestrator aborts `T1` and the staged files are orphaned
— nothing partial is ever visible.

### C. PostgreSQL correspondence

This design borrows the shape of a classic MVCC transaction manager. The correspondence is provided
for readers with that background — it is orientation, not justification. Every mechanism in Parts
I–III stands on the lakehouse reasoning given there; a reader who skips this table loses nothing.

| PostgreSQL | This design | Note |
|---|---|---|
| Transaction id (`xid`) | `txn_id` | 64-bit, from a monotonic sequence; writers only |
| Commit ordering (`xid` at commit) | `commit_scn` | a *transactional* per-catalog counter, not the begin-time id |
| Snapshot horizon | `read_point` | a single cutoff, not the `(xmin, xmax, xip)` triple |
| Row version (`xmin`, tuple) | `committed_table_snapshots` row (`commit_scn`, `metadata_location`) | the versioned unit is a whole-table commit, not a row |
| Visibility test `xmin ≤ snapshot` | `commit_scn ≤ read_point` | existence of the row already means "committed" |
| `pg_export_snapshot()` | the exportable read handle | shareable across processes/engines |
| Read-only txns consume no `xid` | standalone reads get no `txn_id`, no row | just an opaque handle |
| `SAVEPOINT` / `ROLLBACK TO` | subtransaction | also the fan-out worker boundary |
| `RowExclusiveLock` / `LOCK TABLE` | `ROW_EXCLUSIVE` / `EXCLUSIVE` | optimistic default, pessimistic opt-in |
| `VACUUM` removes dead versions | `purgeCommittedSnapshotsBelowWatermark` | GC below the watermark |
| Frozen tuple (`relfrozenxid`) | per-table survivor row kept by purge | kept until the table is dropped |
| CLOG (`pg_xact`) status | `transactions.status` | status off the read path, so one cutoff suffices |

The one deliberate divergence: PostgreSQL judges visibility **per row**, with committed and
aborted/in-flight tuples coexisting, so it needs the `(xmin, xmax, xip)` triple to filter them. Here
a version exists only once committed, so ordering alone decides visibility and a single `read_point`
is enough ([§14](#14-a-single-read_point)).
