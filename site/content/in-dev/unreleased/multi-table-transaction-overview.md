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

# Multi-Table Transactions for Iceberg Tables (High-Level Overview)

This document is a **high-level** design proposal for multi-table transaction semantics for lakehouse workloads using Apache Iceberg tables, scoped to catalogs that can act as a coordinator (most naturally: an Iceberg REST catalog service such as Apache Polaris).

It intentionally avoids low-level implementation details (SQL schemas, specific queries, storage layouts). See `docs/multi-table-transaction-design.md` for a detailed design with concrete data structures and end-to-end protocols.

---

## What Problems Are We Solving?

### 1) Cross-Table Atomicity (Write-Side)

Iceberg today provides strong **single-table** atomic commit semantics. Many real pipelines, however, must update **multiple tables** as one logical unit:

- **Star schema ETL**: update facts and dimensions together (e.g., `fact_orders` + `dim_customer`).
- **Event + state**: write an `events` table and a `current_state` table that must not diverge.
- **Audit/compliance**: write `patient_records` and its `audit_log` inseparably.
- **Coordinated table swaps**: publish `*_staging` into `*_prod` atomically across several tables.

Without multi-table atomicity, readers can observe partial results (e.g., facts referencing dimension rows that are not yet visible), causing correctness issues for joins, constraints-by-convention, and regulatory workflows.

### 2) Consistent Multi-Table Reads (Read-Side)

Even if every single-table write is correct, **multi-table reads** can be inconsistent when metadata is fetched at different times:

- Job reads `orders` at time \(t_1\) and `customers` at time \(t_2\).
- A writer commits between \(t_1\) and \(t_2\).
- The job observes a **torn** view: `orders` from "before" and `customers` from "after".

This is not a "data file read consistency" problem; it's a **catalog coordination** problem across multiple table metadata pointers.

---

## Is There a Real Need?

Yes, but it is **workload-dependent**. Many lakehouse deployments tolerate eventual cross-table consistency. Others cannot.

### When you truly need it

- Pipelines that publish multiple related tables as one "dataset version".
- Downstream jobs that must join tables with referential assumptions and cannot tolerate partial updates.
- Regulatory environments where "data + audit trail" must be published atomically.
- Multi-application workflows where different compute jobs read different tables and results are merged later (e.g., separate Spark apps for `orders`, `customers`, `products`).

### Why the ecosystem often avoids it

- **Operational cost**: locks, long-running transactions, and state management.
- **Complexity**: multi-table atomicity is materially harder than single-table CAS.
- **Distributed engines**: failures (OOM/network) are common; correctness must not rely on perfect failure detection.

### Existing workarounds (and their limits)

- **Accept eventual consistency**: simplest, but incorrect for strict workflows.
- **Staging tables + "publish" step**: often works but becomes complex when the publish must span many tables and readers must never see a partial publish.
- **Nessie branches/merges**: Nessie already offers cross-table commit semantics and isolation levels, but it introduces a branch/merge operational model and requires adopting Nessie as the catalog of record. For some users, that is acceptable; for others, it is not.
- **REST multi-table commit vs. transaction semantics**: the Iceberg REST ecosystem has discussed multi-table *commit* mechanics, but community discussions emphasize that **multi-table commit is not the same as multi-table transaction semantics** (notably: consistent read snapshots and isolation rules across multiple statements).

---

## Scope and Non-Goals

### Scope

- **Iceberg tables** only (not views, not other objects initially).
- **Catalog-coordinated semantics** (most naturally: REST catalog service + transactional backend database).
- Designed to work with multiple engines (Spark/Flink/Trino) without requiring engines to coordinate with each other directly.

### Non-goals (initially)

- Row-level MVCC in data files (Iceberg remains snapshot/file-based).
- General distributed transactions across independent catalogs.
- Making every catalog implementation support this immediately.
- Serializable isolation on day one (it can be a later phase).

---

## High-Level Approach

The core idea is to treat the **catalog** as the transaction coordinator, because:

- The catalog is already the authority that publishes each table's current metadata pointer.
- Only the catalog can atomically change multiple tables' pointers "at once".
- Engines generally do not have visibility into each other; peer-to-peer coordination would require an external distributed lock service anyway.

At a high level:

- Writers perform work "off to the side" (write data files; produce new Iceberg metadata).
- Nothing becomes visible until the catalog performs a single **atomic publish** step that updates all involved tables' metadata pointers together.
- Readers get an explicit **multi-table read snapshot handle** so that all tables resolve to a consistent point in time.

This borrows the *ideas* of MVCC (monotonic transaction IDs + a pinned read view), but adapts them to Iceberg's snapshot-based tables. Because Iceberg publishes each commit as one atomic, immutable table snapshot — rather than per-row tuple versions that coexist with aborted ones — a reader's view is captured as a single **`read_point`** (a commit timestamp), not PostgreSQL's `(xmin, xmax, xip)` triple. This is closer to a timestamp-oracle model (cf. Google Percolator) than to per-row heap MVCC. See the design doc's [Why a single `read_point`](multi-table-transaction-design.md) for the full rationale.

---

## Core Concepts Introduced

### Transaction ID (TxnID)

A monotonically increasing 64-bit transaction identifier assigned by the catalog to order commits globally. This is the logical clock used for:

- Ordering committed changes across tables
- Defining consistent read snapshots
- Driving garbage-collection watermarks for catalog-side transaction history

### TxnSnapshot (Visibility Snapshot)

A **TxnSnapshot** is a read-consistency handle that represents "what set of committed TxnIDs are visible".

Properties:

- It is **read-only**.
- It can be **shared across processes/applications** (e.g., an orchestrator acquires one snapshot and distributes it to multiple Spark apps).
- It gives a consistent multi-table view regardless of when each individual table is fetched.

This is analogous to PostgreSQL's exported snapshots (`pg_export_snapshot()` / `SET TRANSACTION SNAPSHOT`) used to make parallel workers read the exact same database state.

### Table Snapshot (Iceberg-native)

Iceberg already has **table snapshots**: immutable points-in-time for a *single* table, identified by an Iceberg snapshot ID and referenced by table metadata.

This proposal does not change Iceberg's table snapshot model. Instead it introduces a way for the catalog to map a **TxnSnapshot** ("transaction visibility") to the correct **Iceberg table snapshot** to serve for each table.

### Subtransaction (Per-table unit of work)

A multi-table write transaction consists of multiple **subtransactions**, each scoped to exactly one table. Subtransactions enable:

- Independent work per table (often parallelizable)
- Tracking progress per table
- Retrying failed work for a single table while keeping the parent transaction alive (for transient job failures)

### Locking (to make retries practical)

Multi-table writes often require **pessimistic locks** (at least for destructive operations like `ROW_DELTA` / `OVERWRITE`) so that:

- A long-running job does not do hours of work only to lose to a concurrent writer at the final commit step
- A failed subtransaction can be retried safely without the world changing underneath it

Iceberg's existing optimistic concurrency model remains the default for single-table operations; multi-table transactions opt into locking where needed.

---

## What Features / Semantics Are Supported?

### Multi-Statement Transactions

A key differentiator of this proposal — and a gap in most current lakehouse offerings — is support for **genuine multi-statement transactions** over Iceberg tables.

A multi-statement transaction allows an application to issue several independent reads and writes (across one or more tables) under a single `BEGIN` / `COMMIT` envelope, with the following guarantees:

- **All statements share the same TxnSnapshot**: a `SELECT` on `orders` followed by a `SELECT` on `customers` within the same transaction always sees the same consistent point in time, regardless of concurrent commits that happen between the two statements.
- **Writes from earlier statements are visible to later statements in the same transaction** (read-your-own-writes), without being visible to other transactions yet.
- **The commit is atomic**: either all staged writes from all statements become globally visible at once, or none do.

This is important because most current options are limited in this area:

- **Amazon Redshift on Iceberg**: documents that if an Iceberg write is included inside an explicit transaction block, no other statements may run in that block; general **multi-statement** transactional workflows on Iceberg tables are not supported.
- **Snowflake on Iceberg**: DDL statements (including `ALTER ICEBERG TABLE … REFRESH`) auto-commit as individual transactions even inside an explicit transaction block, **breaking multi-statement atomicity**.
- **BigLake Iceberg in BigQuery**: BigQuery native tables support multi-statement transactions, but BigLake Iceberg tables have restrictions and any modification outside BigQuery can cause unrecoverable consistency failures.

This proposal is explicitly designed to support the multi-statement pattern with snapshot isolation semantics, aligned with how warehouse engines (and users of those engines) already expect transactions to work.

### Isolation levels (high-level)

This proposal uses the familiar SQL names, with the common PostgreSQL convention:

- **READ COMMITTED**: each statement sees a fresh view; successive reads can change.
- **REPEATABLE READ = snapshot isolation**: a transaction pins a TxnSnapshot at `BEGIN` and reuses it; reads are consistent **across statements and tables** — this is what enables true multi-statement transactions.
- **SERIALIZABLE (future)**: can be layered on later (e.g., SSI-style conflict detection) once the core APIs and behavior are stable.

### Read-your-own-writes

Within a write transaction, clients should be able to read their own staged changes (e.g., for validation or downstream steps) **before** the top-level commit publishes them to everyone else. This is an important usability requirement for complex pipelines.

### Multi-application snapshot sharing (read)

Multiple independent applications can share the same `txn_snapshot_id`:

- An orchestrator acquires one TxnSnapshot at pipeline start.
- It passes that snapshot to N Spark/Flink/Trino jobs.
- All jobs read different tables but observe the same consistent dataset version.

### Multi-application transaction sharing (write)

For large writes, different applications can update different tables in parallel under one parent transaction:

- Orchestrator creates the parent transaction.
- Orchestrator creates one subtransaction per table and delegates each subtransaction to a worker application.
- Worker applications stage table updates independently.
- Orchestrator commits only after all subtransactions succeed.

To make this safe in a multi-tenant environment, the authorization model must be capability-based:

- Orchestrator holds a top-level transaction capability (commit/abort).
- Each worker holds a per-subtransaction capability (operate on exactly one table's subtransaction).

---

## Conceptual Protocols (No Low-level Details)

### Multi-table consistent read

1. Client acquires a `TxnSnapshot` from the catalog.
2. Client reads each table, passing the `txn_snapshot_id`.
3. Catalog resolves each table to the latest committed version visible to that snapshot.
4. Client releases the snapshot (or it expires).

### Multi-table write

1. Orchestrator begins a transaction.
2. For each table: create a subtransaction and acquire the appropriate lock.
3. Each subtransaction stages its Iceberg metadata update "off to the side".
4. Orchestrator commits atomically: catalog publishes all table pointer updates together.

Abort semantics are simple: staged files remain unreachable (like PostgreSQL marking a transaction aborted; cleanup is deferred).

---

## Adapting PostgreSQL Concepts to Lakehouse Reality

This proposal borrows PostgreSQL's *ideas* (TxnIDs, visibility snapshots, commit log), but relaxes assumptions that do not fit distributed OLAP jobs.

### Must enforce (correctness)

- **Atomic publish** across tables (all-or-nothing).
- **Snapshot isolation** for multi-table reads (no torn view).
- **No dirty reads** (uncommitted staged work is never visible to others).
- **Conflict serialization** (only one winner when writers conflict).
- **Deadlock avoidance** via canonical lock ordering.

### Can relax (operational practicality)

- **Transaction tied to a single process**: in OLAP, jobs crash; transaction state must be durable in the catalog backend.
- **Any failure aborts the whole transaction**: distinguish *data conflicts* from *transient job failures*.
- **Short transactions**: hours-long transactions are common.

### Retry model (what "retry" means)

Retries are appropriate for **transient failures** (OOM/network/executor crash) because they produce no committed side effects.

The clean model is:

- Parent transaction remains `IN_PROGRESS`.
- A failed subtransaction attempt is marked failed/aborted.
- The orchestrator creates a **new subtransaction attempt** for the *same table* under the same parent transaction and retries that job.
- The top-level commit considers **one successful attempt per table** (conceptually, "the latest successful attempt").

To support auditing and debugging, implementations may record an explicit retry chain (e.g., "subtxn S2 was retried as S2′"). This chaining is helpful for traceability but is not required for the high-level semantics: the parent transaction can always derive the commit set by selecting the successful subtransaction attempt per table.

Retries are **not** appropriate for true data conflicts (the world changed); those should abort the parent transaction.

---

## Orphaned Transactions (What Can Go Wrong and How We Contain It)

Distributed systems fail silently. The catalog cannot perfectly know whether a job is dead or just slow. The design goal is therefore:

> **No orphan can block the system indefinitely.**

The main orphan patterns:

- Subtransaction becomes silent (job crashed, never reports).
- Parent transaction becomes orphaned (orchestrator crashed).
- All subtransactions succeed, but nobody finalizes the parent commit ("completed but unpublished").

The containment strategy is tiered:

- **Leases with expiry** for transactions and subtransactions (hard liveness backstop).
- **Optional renewal** for legitimate long-running jobs.
- **Operator intervention** (force-abort / force-finalize) for the rare "obviously dead" cases.
- **Orchestrator checkpointing** of the top-level commit capability so a restarted orchestrator can safely finalize a fully-prepared transaction instead of discarding hours of work.

Orphaned object storage files are expected in failure cases; they remain unreachable until cleaned up by a GC process.

---

## Relationship to Existing Iceberg / Ecosystem Work

- **Iceberg REST Catalog**: the REST protocol is the natural place to add multi-table transaction semantics because it is designed as a service boundary and already supports advanced coordination features (e.g., multi-table commits, change-based commits).
- **Multi-table "commit" vs "transaction"**: community discussion distinguishes a "commit endpoint that can update multiple tables" from full transaction semantics (isolation, consistent reads, begin/commit lifecycle). This proposal is explicitly about the latter.
- **Nessie**: provides a branch/commit model with cross-table atomicity and isolation levels. This proposal aims for similar *transaction semantics* but integrated into a REST-catalog-coordinator model without requiring git-like operational workflows.

---

## Comparison to Industry Offerings (AWS, Snowflake, GCP, Azure)

Many cloud warehouses provide multi-table transactions *inside the warehouse engine*. The hard part for lakehouse is providing multi-table transaction semantics over **open tables on object storage** that remain interoperable across multiple engines.

### AWS

- **Amazon Redshift writing Iceberg on S3**: Redshift documents ACID + snapshot isolation for Iceberg write queries, but also documents a key limitation: if you include an Iceberg write inside an explicit transaction block, **no other statements may run in that block** (otherwise the transaction fails). That prevents general **multi-statement, multi-table** transactional workflows on Iceberg tables. See [Redshift "Transaction semantics"](https://docs.aws.amazon.com/redshift/latest/dg/iceberg-writes-transaction-semantics.html).
- **Athena / Glue / Lake Formation**: these services can coordinate table-level Iceberg commits, but (today) they do not offer a general, engine-agnostic "export a consistent multi-table snapshot + atomically publish multi-table updates" protocol that other engines can participate in.

### Snowflake

- Snowflake supports multi-statement transactions and can commit multiple DML statements atomically inside Snowflake native tables, but it documents Iceberg-specific **restrictions on multi-statement transactions**:
  - DDL executes as individual transactions when encountered.
  - `ALTER ICEBERG TABLE … REFRESH` auto-commits as a single-statement transaction even inside an explicit transaction block, **breaking multi-statement atomicity** for Iceberg tables.
  - Snowflake-managed Iceberg tables are not writable from external engines.
  See [Snowflake "Transactions and Apache Iceberg tables"](https://docs.snowflake.com/en/user-guide/tables-iceberg-transactions).

These behaviors are compatible with Snowflake-as-the-primary-engine, but they do not provide a general **multi-statement, multi-engine** transaction model for open Iceberg tables where independent engines observe the same multi-table version with atomic publish semantics.

### Google Cloud (BigQuery / BigLake)

- **BigQuery (native tables)**: supports multi-statement transactions with ACID + snapshot isolation across multiple tables inside BigQuery. See [BigQuery "Multi-statement transactions"](https://cloud.google.com/bigquery/docs/transactions).
- **BigLake Iceberg tables in BigQuery (Iceberg in customer buckets)**: BigQuery/BigLake provides a managed experience over Iceberg-format tables stored in your bucket. The docs list DML mutations and "multi-statement transactions (Preview)", but they also strongly warn against modifying data files outside BigQuery (risking data loss or unrecoverable consistency failures). See [BigLake Iceberg tables in BigQuery](https://docs.cloud.google.com/biglake/docs/biglake-iceberg-tables-in-bigquery).

Net: BigLake Iceberg can look like "transactions on open tables", but operationally it resembles a managed system where BigQuery is the primary coordinator, not a general-purpose multi-engine transaction protocol.

### Azure / Microsoft

- **Microsoft Fabric Data Warehouse**: supports explicit multi-table transactions (ACID) within the warehouse engine. See [Fabric Data Warehouse transactions](https://learn.microsoft.com/en-us/fabric/data-warehouse/transactions).
- **Fabric Lakehouse / Spark**: the default lakehouse table format is Delta Lake, which is table-scoped in its open log model. See [Fabric Lakehouse and Delta tables](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-and-delta-tables).

Net: Azure provides strong warehouse-engine transactions, but there is not yet a broadly adopted, open, multi-engine transaction protocol for Iceberg-on-object-storage analogous to a REST-catalog-coordinated transaction model.

### What is unique about this proposal (vs. "warehouse transactions")

This proposal targets a different problem category:

- **Multi-engine / open-table interoperability**: the primary target is "multiple independent engines reading/writing the same Iceberg tables" via a shared catalog service.
- **Engine-neutral coordination is the feature**: Spark/Flink/Trino do not coordinate with each other directly, and they shouldn't have to. Coordination happens through a neutral catalog + DB, which is exactly what makes this model different from single-engine warehouses (e.g., BigQuery) where the engine itself is the coordinator.
- **Genuine multi-statement transactions**: a `BEGIN` / `COMMIT` block can span multiple reads and writes across tables, with full snapshot isolation between statements — something current Iceberg-on-cloud offerings explicitly restrict or do not support.
- **Explicit multi-table read snapshots**: an exportable `TxnSnapshot` so that multi-table reads (and even multi-application pipelines) can share one consistent view.
- **Atomic publish at the catalog boundary**: multi-table visibility is achieved by a single catalog-coordinated publish step that updates multiple table pointers together.
- **Long-running job realism**: retryable subtransaction attempts, bounded orphans, and lease-based liveness are first-class concerns.

---

## Related Academic Research (and How It Maps Here)

The proposal is motivated by well-studied transaction theory and by prior systems that layered transactional semantics over shared storage.

### Isolation theory (definitions and anomalies)

- **Adya et al. (ICDE 2000)**: formal, implementation-independent isolation level definitions ("generalized phenomena"), used to reason about which anomalies are prevented at each level. Paper: [Generalized Isolation Level Definitions](https://pmg.csail.mit.edu/papers/icde00.pdf).
- **Berenson et al. (SIGMOD 1995)**: clarifies that snapshot isolation is not serializable (write skew), motivating either stricter isolation or conflict tracking.
- **Ports & Grittner (VLDB 2012)**: practical "Serializable Snapshot Isolation" (SSI) in PostgreSQL. Paper/abstract: [Serializable Snapshot Isolation in PostgreSQL](https://pmg.csail.mit.edu/pubs/ports12__serial_snaps_isolat_postg-abstract.html).

**Mapping to this proposal:** we explicitly start from snapshot isolation semantics for multi-table reads and writes, and treat serializable as a later layer that can be added via conflict tracking once the core APIs are stable.

### ACID over object storage ("lakehouse table formats")

- **Delta Lake (PVLDB 2020)**: canonical design for ACID table storage over cloud object stores using an append-only transaction log and optimistic concurrency. Paper: [Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores](https://vldb.org/pvldb/vol13/p3411-armbrust.pdf).
- **Lakehouse (CIDR 2021)**: positions ACID reliability and performance on open formats as foundational to the lakehouse architecture. Paper: [Lakehouse (CIDR 2021)](https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf).

**Mapping to this proposal:** these systems generally provide **table-scoped** transactions. This proposal builds a catalog-coordination layer to safely extend from "ACID per table" to "atomic publish across tables" plus consistent multi-table reads.

### Cross-object transactional layers and commit ordering

- **Percolator (Google, 2010)**: transactional layer over shared storage using snapshot isolation and a timestamp oracle to coordinate visibility. Paper: [Large-scale Incremental Processing Using Distributed Transactions and Notifications](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/36726.pdf).
- **Calvin (SIGMOD 2012)**: deterministic transaction ordering that separates "global ordering" from "execution", reducing contention footprints. Paper: [Calvin](https://www.cs.umd.edu/~abadi/papers/calvin-sigmod12.pdf).

**Mapping to this proposal:** we similarly rely on a monotone TxnID ("global ordering") and a two-phase "stage then publish" pattern. The key simplification is scope: by using a single catalog service with a transactional backend, we can often avoid the failure modes and complexity of general distributed 2PC.

### What is unique about this proposal (vs. prior systems)

- **Multi-table semantics without a git/branch workflow**: it targets BEGIN/COMMIT-style transactional semantics for ETL and orchestration, rather than repo/branch operations.
- **Explicit exported multi-table read snapshots** for multi-application pipelines (a first-class capability in the catalog API, not an emergent property of client behavior).
- **Long-running distributed job support** as a design center: retryable per-table attempts, bounded orphans, and operational controls are treated as core semantics, not edge cases.

---

## Comparison to Git-based Nessie

Nessie is the most relevant point of comparison because it already provides cross-table atomicity and a consistent "commit log across many objects" model on top of Iceberg tables.

### What Nessie does well

- **Dataset versioning as a first-class concept**: commit hashes, branches, tags, and merges are natural building blocks for "publish version X", experimentation, and promotion workflows.
- **Cross-table atomicity**: Nessie can include multiple object changes in a single commit, and it can also publish a sequence of changes via branch merge.
- **Consistent reads**: pinning a branch to a commit hash is a simple and powerful global read handle.

### Key differences and trade-offs

This proposal and Nessie are solving overlapping but not identical problems. The "unique value" of this proposal is summarized in the sections above (industry comparison + academic mapping): an explicit **catalog-coordinated transaction protocol** with exported multi-table read snapshots, atomic multi-table publish, and long-running-job operational controls.

In contrast:

- **Primary abstraction**: Nessie's primary abstraction is a **repo/ref/commit graph** (git-like). This proposal's primary abstraction is a **BEGIN/COMMIT transaction protocol** (warehouse-like).
- **Multi-application orchestration**: both can enable "publish multiple tables together", but they do it through different user experiences: branches/merges vs. explicit transaction and exported snapshot handles.
- **Long-running write practicality**: this proposal treats retryable per-table attempts, leases, and orphan containment as core semantics. In Nessie, comparable behavior typically pushes toward a more stateful server-side protocol (their docs explicitly note pessimistic locking would require formal server-held transaction state).

### When you should prefer Nessie

- You want git-like workflows (branches/tags/merge) as the *primary* governance and promotion mechanism.
- You want a single "repo history" across many objects as the dominant abstraction.

### When you should prefer this approach

- You want **transaction semantics** (BEGIN/COMMIT, exported snapshots, read-your-own-writes) without adopting git-style operational workflows.
- You want a catalog-coordinator that aligns with how engines already interact with a REST catalog service.

### Coexistence (not either/or)

In practice, some organizations may want **both**:

- Nessie-style branching for experimentation/promotion/governance, and
- A warehouse-like transaction protocol for orchestrated ETL "publish" and exported multi-table read snapshots.

The two models can be compatible if the catalog layer cleanly separates the concerns of (a) naming/versioning (branches/tags) and (b) transactional visibility/publish semantics.

---

## Most Persuasive Incremental Feature Set

To maximize adoption and minimize controversy, the most effective sequence is to start with the smallest uncontroversial semantics and build up:

1. **Exportable TxnSnapshot for consistent multi-table reads** (share one snapshot across requests and applications).
2. **Catalog-side mapping from TxnSnapshot → Iceberg table snapshot** (so engines don't have to coordinate or guess).
3. **Multi-table atomic publish** (make multiple tables visible together).
4. **Pessimistic locking for destructive operations + retryable subtransaction attempts** (make long-running writes practical).
5. **Orchestrator-friendly delegation (multi-application write sharing) + capability-based security** (safe parallel ingestion at scale).

Each step is valuable on its own. Together they form a complete end-to-end story: "write atomically across tables and read a consistent cross-table view."

---

## Why This Approach Could Succeed (or Fail)

### Why it could succeed

- **Directly targets a real, recurring correctness bug**: torn multi-table reads are easy to reproduce and explain.
- **Incremental and opt-in**: engines and catalogs that do not participate can keep existing behavior unchanged.
- **A natural fit for REST catalogs**: the catalog already publishes table metadata pointers; it is the only place that can atomically publish across multiple tables.
- **Operational realism**: leases, retries, and orphan containment treat distributed job failure as normal rather than exceptional.

### Why it could fail

- **Perceived complexity**: anything that resembles a "transaction coordinator" triggers understandable pushback in a community that values a thin catalog.
- **Long-running lock pain**: even correct locking can cause unacceptable writer blocking if defaults, scopes, or operational controls are wrong.
- **Engine adoption inertia**: semantics only help if Spark/Flink/Trino integrations expose them; that work takes time and coordination.
- **Ecosystem fragmentation**: some stakeholders may prefer "use Nessie" rather than standardizing new REST transaction semantics.

---

## Simple Demo Workloads

The goal of demos is to show benefits that are easy to understand and hard to dispute, using tiny schemas and repeatable race conditions.

### Demo 1: Torn multi-table read (why TxnSnapshot exists)

**Setup**: two tables, `orders(order_id, customer_id, amount)` and `customers(customer_id, name)`.

**Race**: a writer commits to `customers` between two metadata reads in a pipeline.

**Demo**:
- Without `txn_snapshot_id`, a pipeline that reads `orders` then `customers` can observe different points in time (torn view).
- With one exported `TxnSnapshot` shared across both reads, both tables resolve to the same logical time.

**Minimal verification**: show that a join-based invariant can "flip" within one pipeline stage without a shared snapshot, but stays stable when both reads use the same `txn_snapshot_id`.

### Demo 2: Multi-application consistent read (exported snapshot shared across jobs)

**Setup**: an orchestrator launches two independent Spark applications:
- App A reads `orders`
- App B reads `customers`

**Demo**:
- If each app acquires its own snapshot at start time, late-starting apps can see newer commits.
- If the orchestrator acquires one `TxnSnapshot` once and passes it to both apps, both observe the same dataset version regardless of start time skew.

### Demo 3: Atomic publish across tables (multi-table write)

**Setup**: load batch `B` into both `orders` and `customers`.

**Demo**:
- Without multi-table publish, there is a window where batch `B` is visible in one table but not the other.
- With a parent transaction, batch `B` becomes visible in both tables simultaneously.

**Minimal verification query (re-run during publish)**:
`(count orders for B, count customers for B)` must transition `(0,0) → (N,M)` and never show `(N,0)` or `(0,M)`.

### Demo 4: Read-your-own-writes (within a transaction)

**Setup**: begin a multi-table write transaction T1; stage an update to `orders` as subtxn S1.

**Demo**:
- Within T1, the writer reads `orders` and should see its own staged change (even though it is not yet published globally).
- Another client reading without T1 should not see the staged change.

This demo validates that "transaction-local view" is supported, which is essential for complex pipelines that validate outputs before publish.

### Demo 5: Retryable subtransaction attempt (job failure ≠ transaction abort)

**Setup**: parent transaction T1 has two subtransactions:
- S1 (orders) succeeds and is staged
- S2 (customers) fails due to a simulated transient failure (OOM/network)

**Demo**:
- Retry only the failed table by creating a new subtransaction attempt for `customers` under the same parent.
- Commit T1 after both tables have a successful staged attempt.

This is the "lakehouse reality" demo: long-running ETL can retry failed work without discarding hours of successful work on other tables.

---

## What This Enables (In One Sentence)

**A catalog-coordinated mechanism to publish multi-table updates atomically and to read a consistent multi-table view, in a way that is realistic for long-running, failure-prone distributed lakehouse jobs.**
