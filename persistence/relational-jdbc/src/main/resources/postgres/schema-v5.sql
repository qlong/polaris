--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file--
--  distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"). You may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Changes from v4:
--  * Added multi-table transaction support tables:
--    - txn_id_seq: monotonically increasing sequence for transaction and snapshot IDs
--    - transactions: top-level write transactions; parent_txn_id enables nested transactions
--    - transaction_operations: per-table staging units within a transaction (replaces subtransactions)
--    - txn_snapshots: MVCC visibility window; shared by read-only handles and write transactions
--    - committed_table_snapshots: MVCC-to-Iceberg snapshot bridge
--    - locks: pessimistic lock state (ROW_EXCLUSIVE, EXCLUSIVE, ACCESS_EXCLUSIVE)
--    - txn_watermark: purge horizon for old transaction history

CREATE SCHEMA IF NOT EXISTS POLARIS_SCHEMA;
SET search_path TO POLARIS_SCHEMA;

CREATE TABLE IF NOT EXISTS version (
    version_key TEXT PRIMARY KEY,
    version_value INTEGER NOT NULL
);
INSERT INTO version (version_key, version_value)
VALUES ('version', 5)
ON CONFLICT (version_key) DO UPDATE
SET version_value = EXCLUDED.version_value;
COMMENT ON TABLE version IS 'the version of the JDBC schema in use';

CREATE TABLE IF NOT EXISTS entities (
    realm_id TEXT NOT NULL,
    catalog_id BIGINT NOT NULL,
    id BIGINT NOT NULL,
    parent_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    entity_version INT NOT NULL,
    type_code INT NOT NULL,
    sub_type_code INT NOT NULL,
    create_timestamp BIGINT NOT NULL,
    drop_timestamp BIGINT NOT NULL,
    purge_timestamp BIGINT NOT NULL,
    to_purge_timestamp BIGINT NOT NULL,
    last_update_timestamp BIGINT NOT NULL,
    properties JSONB not null default '{}'::JSONB,
    internal_properties JSONB not null default '{}'::JSONB,
    grant_records_version INT NOT NULL,
    location_without_scheme TEXT,
    PRIMARY KEY (realm_id, id),
    CONSTRAINT constraint_name UNIQUE (realm_id, catalog_id, parent_id, type_code, name)
);

-- TODO: create indexes based on all query pattern.
CREATE INDEX IF NOT EXISTS idx_entities ON entities (realm_id, catalog_id, id);
CREATE INDEX IF NOT EXISTS idx_entities_catalog_id_id ON entities (catalog_id, id);
CREATE INDEX IF NOT EXISTS idx_locations
    ON entities USING btree (realm_id, parent_id, location_without_scheme)
    WHERE location_without_scheme IS NOT NULL;

COMMENT ON TABLE entities IS 'all the entities';

COMMENT ON COLUMN entities.realm_id IS 'realm_id used for multi-tenancy';
COMMENT ON COLUMN entities.catalog_id IS 'catalog id';
COMMENT ON COLUMN entities.id IS 'entity id';
COMMENT ON COLUMN entities.parent_id IS 'entity id of parent';
COMMENT ON COLUMN entities.name IS 'entity name';
COMMENT ON COLUMN entities.entity_version IS 'version of the entity';
COMMENT ON COLUMN entities.type_code IS 'type code';
COMMENT ON COLUMN entities.sub_type_code IS 'sub type of entity';
COMMENT ON COLUMN entities.create_timestamp IS 'creation time of entity';
COMMENT ON COLUMN entities.drop_timestamp IS 'time of drop of entity';
COMMENT ON COLUMN entities.purge_timestamp IS 'time to start purging entity';
COMMENT ON COLUMN entities.last_update_timestamp IS 'last time the entity is touched';
COMMENT ON COLUMN entities.properties IS 'entities properties json';
COMMENT ON COLUMN entities.internal_properties IS 'entities internal properties json';
COMMENT ON COLUMN entities.grant_records_version IS 'the version of grant records change on the entity';

CREATE TABLE IF NOT EXISTS grant_records (
    realm_id TEXT NOT NULL,
    securable_catalog_id BIGINT NOT NULL,
    securable_id BIGINT NOT NULL,
    grantee_catalog_id BIGINT NOT NULL,
    grantee_id BIGINT NOT NULL,
    privilege_code INTEGER,
    PRIMARY KEY (realm_id, securable_catalog_id, securable_id, grantee_catalog_id, grantee_id, privilege_code)
);

COMMENT ON TABLE grant_records IS 'grant records for entities';

COMMENT ON COLUMN grant_records.securable_catalog_id IS 'catalog id of the securable';
COMMENT ON COLUMN grant_records.securable_id IS 'entity id of the securable';
COMMENT ON COLUMN grant_records.grantee_catalog_id IS 'catalog id of the grantee';
COMMENT ON COLUMN grant_records.grantee_id IS 'id of the grantee';
COMMENT ON COLUMN grant_records.privilege_code IS 'privilege code';

CREATE INDEX IF NOT EXISTS idx_grants_realm_grantee 
    ON grant_records (realm_id, grantee_id);
CREATE INDEX IF NOT EXISTS idx_grants_realm_securable 
    ON grant_records (realm_id, securable_id);

CREATE TABLE IF NOT EXISTS principal_authentication_data (
    realm_id TEXT NOT NULL,
    principal_id BIGINT NOT NULL,
    principal_client_id VARCHAR(255) NOT NULL,
    main_secret_hash VARCHAR(255) NOT NULL,
    secondary_secret_hash VARCHAR(255) NOT NULL,
    secret_salt VARCHAR(255) NOT NULL,
    PRIMARY KEY (realm_id, principal_client_id)
);

COMMENT ON TABLE principal_authentication_data IS 'authentication data for client';

CREATE TABLE IF NOT EXISTS policy_mapping_record (
    realm_id TEXT NOT NULL,
    target_catalog_id BIGINT NOT NULL,
    target_id BIGINT NOT NULL,
    policy_type_code INTEGER NOT NULL,
    policy_catalog_id BIGINT NOT NULL,
    policy_id BIGINT NOT NULL,
    parameters JSONB NOT NULL DEFAULT '{}'::JSONB,
    PRIMARY KEY (realm_id, target_catalog_id, target_id, policy_type_code, policy_catalog_id, policy_id)
);

CREATE INDEX IF NOT EXISTS idx_policy_mapping_record ON policy_mapping_record (realm_id, policy_type_code, policy_catalog_id, policy_id, target_catalog_id, target_id);

CREATE TABLE IF NOT EXISTS events (
    realm_id TEXT NOT NULL,
    catalog_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    request_id TEXT,
    event_type TEXT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    principal_name TEXT,
    resource_type TEXT NOT NULL,
    resource_identifier TEXT NOT NULL,
    additional_properties JSONB NOT NULL DEFAULT '{}'::JSONB,
    PRIMARY KEY (event_id)
);

-- Idempotency records (key-only idempotency; durable replay)
CREATE TABLE IF NOT EXISTS idempotency_records (
    realm_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,            -- normalized request-derived resource identifier (not a generated entity id)

    -- Finalization/replay
    http_status INTEGER,                 -- NULL while IN_PROGRESS; set only on finalized 2xx/terminal 4xx
    error_subtype TEXT,                  -- optional: e.g., already_exists, namespace_not_empty, idempotency_replay_failed
    response_summary TEXT,               -- minimal body to reproduce equivalent response (JSON string)
    response_headers TEXT,               -- small whitelisted headers to replay (JSON string)
    finalized_at TIMESTAMP,              -- when http_status was written

    -- Liveness/ops
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    heartbeat_at TIMESTAMP,              -- updated by owner while IN_PROGRESS
    executor_id TEXT,                    -- owner pod/worker id
    expires_at TIMESTAMP,

    PRIMARY KEY (realm_id, idempotency_key)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_idemp_realm_expires
    ON idempotency_records (realm_id, expires_at);

-- ============================================================================
-- SCAN METRICS REPORT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS scan_metrics_report (
    report_id TEXT NOT NULL,
    realm_id TEXT NOT NULL,
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,

    -- Report metadata
    timestamp_ms BIGINT NOT NULL,
    principal_name TEXT,
    request_id TEXT,

    -- Trace correlation
    otel_trace_id TEXT,
    otel_span_id TEXT,
    report_trace_id TEXT,

    -- Scan context
    snapshot_id BIGINT,
    schema_id INTEGER,
    filter_expression TEXT,
    projected_field_ids TEXT,
    projected_field_names TEXT,

    -- Scan metrics
    result_data_files BIGINT DEFAULT 0,
    result_delete_files BIGINT DEFAULT 0,
    total_file_size_bytes BIGINT DEFAULT 0,
    total_data_manifests BIGINT DEFAULT 0,
    total_delete_manifests BIGINT DEFAULT 0,
    scanned_data_manifests BIGINT DEFAULT 0,
    scanned_delete_manifests BIGINT DEFAULT 0,
    skipped_data_manifests BIGINT DEFAULT 0,
    skipped_delete_manifests BIGINT DEFAULT 0,
    skipped_data_files BIGINT DEFAULT 0,
    skipped_delete_files BIGINT DEFAULT 0,
    total_planning_duration_ms BIGINT DEFAULT 0,

    -- Equality/positional delete metrics
    equality_delete_files BIGINT DEFAULT 0,
    positional_delete_files BIGINT DEFAULT 0,
    indexed_delete_files BIGINT DEFAULT 0,
    total_delete_file_size_bytes BIGINT DEFAULT 0,

    -- Additional metadata (for extensibility)
    metadata JSONB DEFAULT '{}'::JSONB,

    PRIMARY KEY (realm_id, report_id)
);

COMMENT ON TABLE scan_metrics_report IS 'Scan metrics reports as first-class entities';

-- Index for retention cleanup by timestamp
CREATE INDEX IF NOT EXISTS idx_scan_report_timestamp ON scan_metrics_report(realm_id, timestamp_ms);

-- Index for query lookups by catalog_id and table_id
CREATE INDEX IF NOT EXISTS idx_scan_report_lookup ON scan_metrics_report(realm_id, catalog_id, table_id, timestamp_ms);

-- ============================================================================
-- COMMIT METRICS REPORT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS commit_metrics_report (
    report_id TEXT NOT NULL,
    realm_id TEXT NOT NULL,
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,

    -- Report metadata
    timestamp_ms BIGINT NOT NULL,
    principal_name TEXT,
    request_id TEXT,

    -- Trace correlation
    otel_trace_id TEXT,
    otel_span_id TEXT,
    report_trace_id TEXT,

    -- Commit context
    snapshot_id BIGINT NOT NULL,
    sequence_number BIGINT,
    operation TEXT NOT NULL,

    -- File metrics
    added_data_files BIGINT DEFAULT 0,
    removed_data_files BIGINT DEFAULT 0,
    total_data_files BIGINT DEFAULT 0,
    added_delete_files BIGINT DEFAULT 0,
    removed_delete_files BIGINT DEFAULT 0,
    total_delete_files BIGINT DEFAULT 0,

    -- Equality delete files
    added_equality_delete_files BIGINT DEFAULT 0,
    removed_equality_delete_files BIGINT DEFAULT 0,

    -- Positional delete files
    added_positional_delete_files BIGINT DEFAULT 0,
    removed_positional_delete_files BIGINT DEFAULT 0,

    -- Record metrics
    added_records BIGINT DEFAULT 0,
    removed_records BIGINT DEFAULT 0,
    total_records BIGINT DEFAULT 0,

    -- Size metrics
    added_file_size_bytes BIGINT DEFAULT 0,
    removed_file_size_bytes BIGINT DEFAULT 0,
    total_file_size_bytes BIGINT DEFAULT 0,

    -- Duration and attempts
    total_duration_ms BIGINT DEFAULT 0,
    attempts INTEGER DEFAULT 1,

    -- Additional metadata (for extensibility)
    metadata JSONB DEFAULT '{}'::JSONB,

    PRIMARY KEY (realm_id, report_id)
);

COMMENT ON TABLE commit_metrics_report IS 'Commit metrics reports as first-class entities';

-- Index for retention cleanup by timestamp
CREATE INDEX IF NOT EXISTS idx_commit_report_timestamp ON commit_metrics_report(realm_id, timestamp_ms);

-- Index for query lookups by catalog_id and table_id
CREATE INDEX IF NOT EXISTS idx_commit_report_lookup ON commit_metrics_report(realm_id, catalog_id, table_id, timestamp_ms);

-- ---------------------------------------------------------------------------
-- Global monotone sequence for begin-time identity: txn_id, txn_snapshot_id,
-- and op_id. Using a Postgres sequence guarantees strict monotonicity and
-- durability across server restarts. Polaris entity IDs (entities table)
-- continue to use IdGenerator (random longs) and are unaffected.
-- NOTE: this sequence no longer feeds commit stamps. The commit-order stamp
-- (commit_scn) is a *transactional* counter (commit_scn_counter) allocated
-- inside the commit transaction so it is monotonic in true commit order; a
-- non-transactional SEQUENCE cannot provide that. See commit_scn_counter below
-- and multi-table-transaction-design.md (§ Why a single read_point).
-- ---------------------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS txn_id_seq
    AS BIGINT
    START WITH 1
    INCREMENT BY 1
    NO CYCLE;

-- ---------------------------------------------------------------------------
-- transactions: top-level write transactions.
-- parent_txn_id is NULL for top-level; non-NULL for nested transactions.
-- txn_snapshot_id references txn_snapshots; NULL for READ_COMMITTED.
-- The read_point (visibility cutoff) lives in txn_snapshots, referenced via txn_snapshot_id.
-- status: 0=IN_PROGRESS, 1=COMMITTED, 2=ABORTED
-- isolation: 'READ_COMMITTED', 'REPEATABLE_READ'
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transactions (
    txn_id          BIGINT PRIMARY KEY,
    parent_txn_id   BIGINT REFERENCES transactions(txn_id),  -- NULL = top-level
    realm_id        TEXT NOT NULL,
    catalog_id      BIGINT NOT NULL,
    txn_token       TEXT NOT NULL,           -- random 32-byte hex; held by orchestrator
    owner_principal TEXT NOT NULL,
    status          SMALLINT NOT NULL DEFAULT 0,
    isolation       TEXT NOT NULL DEFAULT 'REPEATABLE_READ',
    txn_snapshot_id BIGINT,                  -- FK to txn_snapshots; NULL for READ_COMMITTED
    started_at      TIMESTAMP NOT NULL DEFAULT now(),
    expires_at      TIMESTAMP NOT NULL,
    ended_at        TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_txns_realm_status
    ON transactions (realm_id, catalog_id, status, expires_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_txns_token
    ON transactions (txn_token);

-- ---------------------------------------------------------------------------
-- transaction_operations: per-table staging units within a transaction.
-- Created explicitly (CREATE SUBTRANSACTION FOR) or implicitly by StagingTableOperations.
-- commitTransaction processes all COMMITTED rows atomically.
-- lock_mode: 'ROW_EXCLUSIVE' (default/OCC), 'EXCLUSIVE', 'ACCESS_EXCLUSIVE'
-- status: 0=IN_PROGRESS, 1=COMMITTED, 2=ABORTED
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transaction_operations (
    op_id                         BIGINT PRIMARY KEY,
    txn_id                        BIGINT NOT NULL REFERENCES transactions(txn_id),
    realm_id                      TEXT NOT NULL,
    catalog_id                    BIGINT NOT NULL,
    table_uuid                    TEXT NOT NULL,   -- stable UUID of the Iceberg table entity
    lock_mode                     TEXT NOT NULL DEFAULT 'ROW_EXCLUSIVE',
    base_snapshot_id              BIGINT,          -- Iceberg snapshot ID at op creation time
    server_base_metadata_location TEXT,            -- table's metadata-location at op creation (server-captured)
    assigned_staging_path         TEXT NOT NULL,   -- server-assigned staging path for metadata
    staged_metadata_location      TEXT,            -- filled in by commitOperation
    op_token                      TEXT NOT NULL,   -- random 32-byte hex; given to worker app
    status                        SMALLINT NOT NULL DEFAULT 0,
    retry_of_op_id                BIGINT,          -- set when this op retries a failed predecessor
    created_at                    TIMESTAMP NOT NULL DEFAULT now(),
    expires_at                    TIMESTAMP NOT NULL,
    ended_at                      TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ops_txn
    ON transaction_operations (txn_id, status);
CREATE INDEX IF NOT EXISTS idx_ops_table
    ON transaction_operations (realm_id, catalog_id, table_uuid, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ops_token
    ON transaction_operations (op_token);

-- ---------------------------------------------------------------------------
-- txn_snapshots: exportable consistent-read handles.
-- Clients acquire one to get a pinned view across multiple tables. read_point is
-- a single commit-order cutoff (a commit SCN): a committed table snapshot is
-- visible iff its commit_scn <= read_point. read_point is captured as the current
-- value of the catalog's commit_scn_counter, so it can never exceed a not-yet-
-- committed commit_scn. This replaces the PostgreSQL-style (snap_xmin, snap_xmax,
-- snap_xip_list) triple; see multi-table-transaction-design.md
-- (§ Why a single read_point). released_at is set on DELETE (explicit release) or reaper.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS txn_snapshots (
    txn_snapshot_id BIGINT PRIMARY KEY,
    realm_id        TEXT NOT NULL,
    catalog_id      BIGINT NOT NULL,
    read_point      BIGINT NOT NULL,   -- commit high-water-mark at snapshot time; sole visibility bound
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    expires_at      TIMESTAMP NOT NULL,
    released_at     TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_snapshots_realm_expires
    ON txn_snapshots (realm_id, catalog_id, expires_at)
    WHERE released_at IS NULL;

-- ---------------------------------------------------------------------------
-- committed_table_snapshots: MVCC-to-Iceberg bridge
-- Every successful table commit (old or new API) records the Iceberg snapshot
-- ID that became visible at commit_scn. Used to resolve a TxnSnapshot to
-- per-table Iceberg snapshot IDs.
-- commit_scn is a commit-order stamp allocated from commit_scn_counter *inside*
-- the same transaction as the metadata-pointer swap, so it is monotonic in true
-- commit order and durable atomically with the row (never a begin-time txn_id).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS committed_table_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    realm_id            TEXT NOT NULL,
    catalog_id          BIGINT NOT NULL,
    table_uuid          TEXT NOT NULL,
    commit_scn          BIGINT NOT NULL,          -- from commit_scn_counter (transactional)
    iceberg_snapshot_id BIGINT,                   -- NULL for DROP or schema-only changes
    metadata_location   TEXT NOT NULL,
    committed_at        TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cts_table_txn
    ON committed_table_snapshots (realm_id, catalog_id, table_uuid, commit_scn DESC);

CREATE INDEX IF NOT EXISTS idx_cts_txn
    ON committed_table_snapshots (commit_scn);

-- ---------------------------------------------------------------------------
-- commit_scn_counter: per-(realm, catalog) transactional commit clock.
-- Allocated as `UPDATE ... SET scn = scn + 1 RETURNING scn` INSIDE the commit
-- transaction; the row lock is held until that transaction commits, so a higher
-- commit_scn is guaranteed to become visible after a lower one (commit order ==
-- scn order). This is what makes read_point (a single cutoff) sound without an
-- xip set, and closes the assign-before-visible window that a non-transactional
-- SEQUENCE would leave open. Serializes commits within a catalog.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS commit_scn_counter (
    realm_id   TEXT NOT NULL,
    catalog_id BIGINT NOT NULL,
    scn        BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (realm_id, catalog_id)
);

-- ---------------------------------------------------------------------------
-- locks: pessimistic lock state.
-- resource: "{table_uuid}" or "{table_uuid}:{partition_spec_id}:{partition_value}"
-- lock_mode: 'ROW_EXCLUSIVE' (default/OCC, self-compatible), 'EXCLUSIVE', 'ACCESS_EXCLUSIVE'
-- status: 0=WAITING, 1=GRANTED
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS locks (
    lock_id      BIGSERIAL PRIMARY KEY,
    txn_id       BIGINT NOT NULL REFERENCES transactions(txn_id),
    realm_id     TEXT NOT NULL,
    catalog_id   BIGINT NOT NULL,
    resource     TEXT NOT NULL,
    lock_mode    TEXT NOT NULL,
    status       SMALLINT NOT NULL DEFAULT 0,
    requested_at TIMESTAMP NOT NULL DEFAULT now(),
    granted_at   TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_locks_resource
    ON locks (realm_id, catalog_id, resource, status);
CREATE INDEX IF NOT EXISTS idx_locks_txn
    ON locks (txn_id, status);

-- ---------------------------------------------------------------------------
-- lock_resource: per-(realm, catalog, resource) serialization anchor.
-- One always-present row per lockable resource, used ONLY as a SELECT ... FOR
-- UPDATE gate around the check-then-insert critical section in LockManager.acquire.
-- A held EXCLUSIVE/ACCESS_EXCLUSIVE lock is a NEW row in `locks`; two acquirers on
-- two Polaris nodes could both SELECT zero conflicts and both insert (phantom) —
-- locking rows that don't exist yet cannot prevent that. Locking this anchor row
-- (which always exists) serializes the grant decision fleet-wide. The anchor lock
-- is held only for the microsecond critical section (released at that txn's commit);
-- the long-lived lock remains the `locks` row. Per-resource granularity means
-- acquisitions on different tables never serialize against each other.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lock_resource (
    realm_id   TEXT NOT NULL,
    catalog_id BIGINT NOT NULL,
    resource   TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (realm_id, catalog_id, resource)
);

-- ---------------------------------------------------------------------------
-- txn_watermark: per-(realm, catalog) purge horizon
-- committed_table_snapshots rows with commit_scn < watermark_txn_id are safe to
-- purge, EXCEPT the newest surviving row per table (see reaper: a snapshot pinned
-- exactly at the watermark must still resolve tables that have not changed since).
-- The horizon is MIN(read_point over live snapshots, txn_id over in-progress txns).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS txn_watermark (
    realm_id         TEXT NOT NULL,
    catalog_id       BIGINT NOT NULL,
    watermark_txn_id BIGINT NOT NULL DEFAULT 0,
    updated_at       TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (realm_id, catalog_id)
);