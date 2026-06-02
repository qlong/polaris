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
--    - transactions: transaction CLOG with isolation level and expiry
--    - subtransactions: per-table units of work within a transaction
--    - txn_snapshots: exportable read-consistency handles (MVCC-style)
--    - committed_table_snapshots: MVCC-to-Iceberg snapshot bridge
--    - locks: pessimistic lock state
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
-- Global monotone sequence for TxnIDs and TxnSnapshot IDs.
-- Using a Postgres sequence guarantees strict monotonicity and durability
-- across server restarts. Polaris entity IDs (entities table) continue to
-- use IdGenerator (random longs) and are unaffected.
-- ---------------------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS txn_id_seq
    AS BIGINT
    START WITH 1
    INCREMENT BY 1
    NO CYCLE;

-- ---------------------------------------------------------------------------
-- transactions: transaction commit log (CLOG)
-- status: 0=IN_PROGRESS, 1=COMMITTED, 2=ABORTED
-- isolation: 'READ_COMMITTED', 'REPEATABLE_READ'
-- snap_xmin/xmax/xip: captured pg_snapshot at BEGIN (for REPEATABLE_READ)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transactions (
    txn_id          BIGINT PRIMARY KEY,
    realm_id        TEXT NOT NULL,
    catalog_id      BIGINT NOT NULL,
    txn_token       TEXT NOT NULL,           -- random 32-byte hex; held by orchestrator
    owner_principal TEXT NOT NULL,
    status          SMALLINT NOT NULL DEFAULT 0,
    isolation       TEXT NOT NULL DEFAULT 'REPEATABLE_READ',
    snap_xmin       BIGINT,                  -- populated for REPEATABLE_READ
    snap_xmax       BIGINT,
    snap_xip_list   TEXT,                    -- comma-separated in-progress txn IDs at BEGIN
    txn_snapshot_id BIGINT,                  -- ID of the txn_snapshots row created at BEGIN (REPEATABLE_READ only)
    started_at      TIMESTAMP NOT NULL DEFAULT now(),
    expires_at      TIMESTAMP NOT NULL,
    ended_at        TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_txns_realm_status
    ON transactions (realm_id, catalog_id, status, expires_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_txns_token
    ON transactions (txn_token);

-- ---------------------------------------------------------------------------
-- subtransactions: per-table units of work inside a parent transaction
-- status: 0=IN_PROGRESS, 1=SUB_COMMITTED, 2=SUB_ABORTED
-- operation: 'APPEND', 'ROW_DELTA', 'OVERWRITE', 'EXCLUSIVE'
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS subtransactions (
    subtxn_id               BIGINT PRIMARY KEY,
    parent_txn_id           BIGINT NOT NULL REFERENCES transactions(txn_id),
    realm_id                TEXT NOT NULL,
    catalog_id              BIGINT NOT NULL,
    table_uuid              TEXT NOT NULL,   -- stable UUID of the Iceberg table entity
    operation               TEXT NOT NULL,
    base_snapshot_id        BIGINT,          -- Iceberg snapshot ID at subtxn creation time
    assigned_staging_path   TEXT NOT NULL,   -- server-assigned s3://…/staging/{txn_id}/{subtxn_id}/
    staged_metadata_location TEXT,           -- filled in by commitSubtransaction
    subtxn_token            TEXT NOT NULL,   -- random 32-byte hex; given to worker app
    status                  SMALLINT NOT NULL DEFAULT 0,
    retry_of_subtxn_id      BIGINT,          -- set when this subtxn retries a failed predecessor
    created_at              TIMESTAMP NOT NULL DEFAULT now(),
    expires_at              TIMESTAMP NOT NULL,
    ended_at                TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_subtxns_parent
    ON subtransactions (parent_txn_id, status);
CREATE INDEX IF NOT EXISTS idx_subtxns_table
    ON subtransactions (realm_id, catalog_id, table_uuid, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_subtxns_token
    ON subtransactions (subtxn_token);

-- ---------------------------------------------------------------------------
-- txn_snapshots: exportable consistent-read handles
-- Analogous to pg_snapshot. Clients acquire one to get a pinned view across
-- multiple tables. released_at is set on DELETE (explicit release) or reaper.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS txn_snapshots (
    txn_snapshot_id BIGINT PRIMARY KEY,
    realm_id        TEXT NOT NULL,
    catalog_id      BIGINT NOT NULL,
    snap_xmin       BIGINT NOT NULL,   -- lowest txn_id that may be in-progress
    snap_xmax       BIGINT NOT NULL,   -- first txn_id not yet assigned at snapshot time
    snap_xip_list   TEXT NOT NULL,     -- comma-separated txn_ids in-progress at snapshot time
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
-- ID that became visible at committed_txn_id. Used to resolve a TxnSnapshot
-- to per-table Iceberg snapshot IDs.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS committed_table_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    realm_id            TEXT NOT NULL,
    catalog_id          BIGINT NOT NULL,
    table_uuid          TEXT NOT NULL,
    committed_txn_id    BIGINT NOT NULL,          -- from txn_id_seq
    iceberg_snapshot_id BIGINT,                   -- NULL for DROP or schema-only changes
    metadata_location   TEXT NOT NULL,
    committed_at        TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cts_table_txn
    ON committed_table_snapshots (realm_id, catalog_id, table_uuid, committed_txn_id DESC);

CREATE INDEX IF NOT EXISTS idx_cts_txn
    ON committed_table_snapshots (committed_txn_id);

-- ---------------------------------------------------------------------------
-- locks: pessimistic lock manager state
-- resource format: "{table_uuid}" or "{table_uuid}:{partition_spec_id}:{partition_value}"
-- lock_mode: 'APPEND', 'ROW_DELTA', 'OVERWRITE', 'EXCLUSIVE', 'ACCESS_EXCLUSIVE'
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
-- txn_watermark: single-row purge horizon
-- All committed_table_snapshots rows with committed_txn_id < watermark_txn_id
-- are safe to purge (no live TxnSnapshot references them any more).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS txn_watermark (
    realm_id         TEXT NOT NULL,
    catalog_id       BIGINT NOT NULL,
    watermark_txn_id BIGINT NOT NULL DEFAULT 0,
    updated_at       TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (realm_id, catalog_id)
);