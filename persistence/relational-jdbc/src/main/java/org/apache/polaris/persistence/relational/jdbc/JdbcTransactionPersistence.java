/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.persistence.relational.jdbc;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raw JDBC persistence for the multi-table transaction extension tables introduced in schema v5:
 * {@code transactions}, {@code transaction_operations}, {@code txn_snapshots}, {@code
 * committed_table_snapshots}, {@code locks}, {@code txn_watermark}.
 *
 * <p>Uses {@link DataSource} directly (borrowed from the same pool as {@link
 * JdbcBasePersistenceImpl}) rather than going through the {@link DatasourceOperations} helper,
 * because all writes here need explicit connection control (autocommit off, manual commit/rollback)
 * for multi-statement atomicity that the {@link DatasourceOperations.TransactionCallback} interface
 * does not support for cross-statement reads.
 */
public class JdbcTransactionPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcTransactionPersistence.class);

  /** Status codes for the {@code transactions} table. */
  public static final int TXN_STATUS_IN_PROGRESS = 0;

  public static final int TXN_STATUS_COMMITTED = 1;
  public static final int TXN_STATUS_ABORTED = 2;

  /** Status codes for the {@code transaction_operations} table. */
  public static final int OP_STATUS_IN_PROGRESS = 0;

  public static final int OP_STATUS_COMMITTED = 1;
  public static final int OP_STATUS_ABORTED = 2;

  /** Status codes for the {@code subtransactions} table (retry/rollback/savepoint boundary). */
  public static final int SUBTXN_STATUS_OPEN = 0;

  public static final int SUBTXN_STATUS_STAGED = 1;
  public static final int SUBTXN_STATUS_ABORTED = 2;

  /** Status codes for the {@code locks} table. */
  public static final int LOCK_STATUS_WAITING = 0;

  public static final int LOCK_STATUS_GRANTED = 1;

  private final DataSource dataSource;
  private final DatabaseType dbType;
  private final String realmId;
  private final long catalogId;

  public JdbcTransactionPersistence(
      @Nonnull DataSource dataSource,
      @Nonnull DatabaseType dbType,
      @Nonnull String realmId,
      long catalogId) {
    this.dataSource = dataSource;
    this.dbType = dbType;
    this.realmId = realmId;
    this.catalogId = catalogId;
  }

  // ---------------------------------------------------------------------------
  // Accessors
  // ---------------------------------------------------------------------------

  public String getRealmId() {
    return realmId;
  }

  public long getCatalogId() {
    return catalogId;
  }

  // ---------------------------------------------------------------------------
  // Sequence: txn_id_seq
  // ---------------------------------------------------------------------------

  /**
   * Draws the next value from {@code txn_id_seq}. Strictly monotonic and DB-durable. Used for both
   * TxnIDs and TxnSnapshot IDs.
   */
  public long nextTxnId() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT nextval('POLARIS_SCHEMA.txn_id_seq')");
        ResultSet rs = ps.executeQuery()) {
      if (rs.next()) return rs.getLong(1);
      throw new SQLException("nextval('txn_id_seq') returned no rows");
    }
  }

  // ---------------------------------------------------------------------------
  // commit_scn_counter: per-(realm, catalog) transactional commit clock
  // ---------------------------------------------------------------------------

  /**
   * Allocates the next {@code commit_scn} for this realm+catalog on the <b>caller's</b> connection
   * — it MUST be invoked inside the same transaction that performs the durable metadata-pointer
   * swap (autocommit off). The {@code UPDATE ... SET scn = scn + 1} takes a row lock on the counter
   * that is held until the caller commits, so a higher {@code commit_scn} is guaranteed to become
   * visible only after every lower one has committed — i.e. {@code commit_scn} order equals true
   * commit order. This is the coordination that makes {@code read_point} (a single cutoff) sound
   * and closes the assign-before-visible window a non-transactional sequence would leave open.
   * Commits within a catalog serialize on this row.
   *
   * <p>The counter row is created lazily (initialized to {@code 0}) the first time a catalog
   * commits, so no bootstrap seeding is required.
   */
  public long allocateCommitScn(@Nonnull Connection conn) throws SQLException {
    if (dbType == DatabaseType.H2) {
      // H2 has no ON CONFLICT ... RETURNING; init-if-absent, then increment, then read — all on the
      // same connection/transaction so the UPDATE's row lock is held to commit.
      try (PreparedStatement init =
          conn.prepareStatement(
              "INSERT INTO POLARIS_SCHEMA.commit_scn_counter (realm_id, catalog_id, scn) "
                  + "SELECT ?, ?, 0 WHERE NOT EXISTS ("
                  + "SELECT 1 FROM POLARIS_SCHEMA.commit_scn_counter "
                  + "WHERE realm_id = ? AND catalog_id = ?)")) {
        init.setString(1, realmId);
        init.setLong(2, catalogId);
        init.setString(3, realmId);
        init.setLong(4, catalogId);
        init.executeUpdate();
      }
      try (PreparedStatement upd =
          conn.prepareStatement(
              "UPDATE POLARIS_SCHEMA.commit_scn_counter SET scn = scn + 1, updated_at = now() "
                  + "WHERE realm_id = ? AND catalog_id = ?")) {
        upd.setString(1, realmId);
        upd.setLong(2, catalogId);
        upd.executeUpdate();
      }
      try (PreparedStatement sel =
          conn.prepareStatement(
              "SELECT scn FROM POLARIS_SCHEMA.commit_scn_counter "
                  + "WHERE realm_id = ? AND catalog_id = ?")) {
        sel.setString(1, realmId);
        sel.setLong(2, catalogId);
        try (ResultSet rs = sel.executeQuery()) {
          if (rs.next()) return rs.getLong(1);
          throw new SQLException("commit_scn_counter row missing after upsert");
        }
      }
    } else {
      String sql =
          "INSERT INTO POLARIS_SCHEMA.commit_scn_counter (realm_id, catalog_id, scn) "
              + "VALUES (?, ?, 1) "
              + "ON CONFLICT (realm_id, catalog_id) DO UPDATE "
              + "SET scn = POLARIS_SCHEMA.commit_scn_counter.scn + 1, updated_at = now() "
              + "RETURNING scn";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, realmId);
        ps.setLong(2, catalogId);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) return rs.getLong(1);
          throw new SQLException("commit_scn_counter upsert returned no rows");
        }
      }
    }
  }

  /**
   * Returns the current committed {@code commit_scn} for this realm+catalog (the value used as a
   * fresh {@code read_point}), or {@code 0} if the catalog has never committed. This is a plain
   * (non-locking) read: under MVCC it returns the last committed {@code scn} without blocking on an
   * in-flight committer's row lock, and — because the counter advances atomically with the CTS row
   * — it can never exceed a {@code commit_scn} whose row is not yet visible.
   */
  public long currentCommitScn(@Nonnull Connection conn) throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT scn FROM POLARIS_SCHEMA.commit_scn_counter "
                + "WHERE realm_id = ? AND catalog_id = ?")) {
      ps.setString(1, realmId);
      ps.setLong(2, catalogId);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 0L;
      }
    }
  }

  // ---------------------------------------------------------------------------
  // transactions
  // ---------------------------------------------------------------------------

  public record TransactionRow(
      long txnId,
      @Nullable Long parentTxnId,
      String realmId,
      long catalogId,
      String txnToken,
      String ownerPrincipal,
      int status,
      String isolation,
      @Nullable Long txnSnapshotId,
      Instant startedAt,
      Instant expiresAt,
      @Nullable Instant endedAt) {}

  public void insertTransaction(@Nonnull TransactionRow row) throws SQLException {
    String sql =
        "INSERT INTO POLARIS_SCHEMA.transactions "
            + "(txn_id, parent_txn_id, realm_id, catalog_id, txn_token, owner_principal, "
            + " status, isolation, txn_snapshot_id, started_at, expires_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setLong(1, row.txnId());
      setNullableLong(ps, 2, row.parentTxnId());
      ps.setString(3, row.realmId());
      ps.setLong(4, row.catalogId());
      ps.setString(5, row.txnToken());
      ps.setString(6, row.ownerPrincipal());
      ps.setInt(7, row.status());
      ps.setString(8, row.isolation());
      setNullableLong(ps, 9, row.txnSnapshotId());
      ps.setTimestamp(10, Timestamp.from(row.startedAt()));
      ps.setTimestamp(11, Timestamp.from(row.expiresAt()));
      ps.executeUpdate();
    }
  }

  public Optional<TransactionRow> findTransaction(long txnId) throws SQLException {
    String sql =
        "SELECT txn_id, parent_txn_id, realm_id, catalog_id, txn_token, owner_principal, "
            + "status, isolation, txn_snapshot_id, started_at, expires_at, ended_at "
            + "FROM POLARIS_SCHEMA.transactions WHERE txn_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, txnId);
      ps.setString(2, realmId);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rowToTransaction(rs)) : Optional.empty();
      }
    }
  }

  public void updateTransactionStatus(long txnId, int newStatus) throws SQLException {
    String sql =
        "UPDATE POLARIS_SCHEMA.transactions SET status = ?, ended_at = now() "
            + "WHERE txn_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setInt(1, newStatus);
      ps.setLong(2, txnId);
      ps.setString(3, realmId);
      ps.executeUpdate();
    }
  }

  public void renewTransaction(long txnId, Instant newExpiresAt) throws SQLException {
    String sql =
        "UPDATE POLARIS_SCHEMA.transactions SET expires_at = ? "
            + "WHERE txn_id = ? AND realm_id = ? AND status = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setTimestamp(1, Timestamp.from(newExpiresAt));
      ps.setLong(2, txnId);
      ps.setString(3, realmId);
      ps.setInt(4, TXN_STATUS_IN_PROGRESS);
      ps.executeUpdate();
    }
  }

  /**
   * Expired in-progress transactions for this realm across all catalogs (for background reaping).
   */
  public List<Long> findExpiredTransactionIdsForRealm() throws SQLException {
    String sql =
        "SELECT txn_id FROM POLARIS_SCHEMA.transactions "
            + "WHERE realm_id = ? AND status = ? AND expires_at < now()";
    List<Long> ids = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, realmId);
      ps.setInt(2, TXN_STATUS_IN_PROGRESS);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) ids.add(rs.getLong(1));
      }
    }
    return ids;
  }

  public List<Long> findExpiredTransactionIds() throws SQLException {
    String sql =
        "SELECT txn_id FROM POLARIS_SCHEMA.transactions "
            + "WHERE realm_id = ? AND catalog_id = ? AND status = ? AND expires_at < now()";
    List<Long> ids = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, realmId);
      ps.setLong(2, catalogId);
      ps.setInt(3, TXN_STATUS_IN_PROGRESS);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) ids.add(rs.getLong(1));
      }
    }
    return ids;
  }

  private TransactionRow rowToTransaction(ResultSet rs) throws SQLException {
    return new TransactionRow(
        rs.getLong("txn_id"),
        getNullableLong(rs, "parent_txn_id"),
        rs.getString("realm_id"),
        rs.getLong("catalog_id"),
        rs.getString("txn_token"),
        rs.getString("owner_principal"),
        rs.getInt("status"),
        rs.getString("isolation"),
        getNullableLong(rs, "txn_snapshot_id"),
        toInstant(rs.getTimestamp("started_at")),
        toInstant(rs.getTimestamp("expires_at")),
        toInstant(rs.getTimestamp("ended_at")));
  }

  // ---------------------------------------------------------------------------
  // subtransactions: the retry / rollback / savepoint boundary
  // ---------------------------------------------------------------------------

  public record SubtxnRow(
      long subtxnId,
      long txnId,
      String realmId,
      long catalogId,
      @Nullable String name,
      String subtxnToken,
      int status,
      int attempt,
      Instant createdAt,
      Instant expiresAt,
      @Nullable Instant endedAt) {}

  public void insertSubtxn(@Nonnull SubtxnRow row) throws SQLException {
    String sql =
        "INSERT INTO POLARIS_SCHEMA.subtransactions "
            + "(subtxn_id, txn_id, realm_id, catalog_id, name, subtxn_token, status, attempt, "
            + " created_at, expires_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setLong(1, row.subtxnId());
      ps.setLong(2, row.txnId());
      ps.setString(3, row.realmId());
      ps.setLong(4, row.catalogId());
      ps.setString(5, row.name());
      ps.setString(6, row.subtxnToken());
      ps.setInt(7, row.status());
      ps.setInt(8, row.attempt());
      ps.setTimestamp(9, Timestamp.from(row.createdAt()));
      ps.setTimestamp(10, Timestamp.from(row.expiresAt()));
      ps.executeUpdate();
    }
  }

  private static final String SUBTXN_COLUMNS =
      "subtxn_id, txn_id, realm_id, catalog_id, name, subtxn_token, status, attempt, "
          + "created_at, expires_at, ended_at";

  public Optional<SubtxnRow> findSubtxn(long subtxnId) throws SQLException {
    String sql =
        "SELECT "
            + SUBTXN_COLUMNS
            + " FROM POLARIS_SCHEMA.subtransactions WHERE subtxn_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, subtxnId);
      ps.setString(2, realmId);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rowToSubtxn(rs)) : Optional.empty();
      }
    }
  }

  /** All subtransactions of a transaction, in creation order. */
  public List<SubtxnRow> findSubtxnsForTransaction(long txnId) throws SQLException {
    String sql =
        "SELECT "
            + SUBTXN_COLUMNS
            + " FROM POLARIS_SCHEMA.subtransactions "
            + "WHERE txn_id = ? AND realm_id = ? ORDER BY subtxn_id";
    List<SubtxnRow> rows = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, txnId);
      ps.setString(2, realmId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) rows.add(rowToSubtxn(rs));
      }
    }
    return rows;
  }

  public void updateSubtxnStatus(long subtxnId, int newStatus) throws SQLException {
    String sql =
        "UPDATE POLARIS_SCHEMA.subtransactions "
            + "SET status = ?, ended_at = CASE WHEN ? IN (1, 2) THEN now() ELSE ended_at END "
            + "WHERE subtxn_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setInt(1, newStatus);
      ps.setInt(2, newStatus);
      ps.setLong(3, subtxnId);
      ps.setString(4, realmId);
      ps.executeUpdate();
    }
  }

  /**
   * Atomically aborts a subtransaction and all of its operations in one JDBC transaction: marks the
   * subtransaction ABORTED and every one of its {@code transaction_operations} rows ABORTED. Lock
   * release is handled by the caller (service layer) so it can consult each op's lock mode. Returns
   * the ops that were aborted (their prior state), so the caller knows which locks to release.
   */
  public List<OperationRow> abortSubtxn(long subtxnId) throws SQLException {
    List<OperationRow> ops = findOperationsForSubtxn(subtxnId);
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        try (PreparedStatement ps =
            conn.prepareStatement(
                "UPDATE POLARIS_SCHEMA.transaction_operations "
                    + "SET status = ?, ended_at = now() "
                    + "WHERE subtxn_id = ? AND realm_id = ? AND status = ?")) {
          ps.setInt(1, OP_STATUS_ABORTED);
          ps.setLong(2, subtxnId);
          ps.setString(3, realmId);
          ps.setInt(4, OP_STATUS_IN_PROGRESS);
          ps.executeUpdate();
        }
        // COMMITTED (staged) ops of the subtransaction are also discarded on rollback — their
        // staged
        // metadata files simply become orphans (pointer-based visibility never referenced them).
        try (PreparedStatement ps =
            conn.prepareStatement(
                "UPDATE POLARIS_SCHEMA.transaction_operations "
                    + "SET status = ?, ended_at = now() "
                    + "WHERE subtxn_id = ? AND realm_id = ? AND status = ?")) {
          ps.setInt(1, OP_STATUS_ABORTED);
          ps.setLong(2, subtxnId);
          ps.setString(3, realmId);
          ps.setInt(4, OP_STATUS_COMMITTED);
          ps.executeUpdate();
        }
        try (PreparedStatement ps =
            conn.prepareStatement(
                "UPDATE POLARIS_SCHEMA.subtransactions "
                    + "SET status = ?, ended_at = now() WHERE subtxn_id = ? AND realm_id = ?")) {
          ps.setInt(1, SUBTXN_STATUS_ABORTED);
          ps.setLong(2, subtxnId);
          ps.setString(3, realmId);
          ps.executeUpdate();
        }
        conn.commit();
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      }
    }
    return ops;
  }

  /**
   * Re-opens a previously-aborted subtransaction for another attempt: sets status back to OPEN,
   * clears {@code ended_at}, bumps {@code attempt}, and extends the lease. Used by the
   * rollback-and-retry path so the same subtxn id/name can be re-staged (possibly by a different
   * worker).
   */
  public void reopenSubtxn(long subtxnId, Instant expiresAt) throws SQLException {
    String sql =
        "UPDATE POLARIS_SCHEMA.subtransactions "
            + "SET status = ?, attempt = attempt + 1, ended_at = NULL, expires_at = ? "
            + "WHERE subtxn_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setInt(1, SUBTXN_STATUS_OPEN);
      ps.setTimestamp(2, Timestamp.from(expiresAt));
      ps.setLong(3, subtxnId);
      ps.setString(4, realmId);
      ps.executeUpdate();
    }
  }

  /**
   * Extends a subtransaction's lease without changing its status or attempt counter. Used by the
   * lease-renewal path to keep a long-running subtransaction (and its held locks) alive.
   */
  public void renewSubtxn(long subtxnId, Instant expiresAt) throws SQLException {
    String sql =
        "UPDATE POLARIS_SCHEMA.subtransactions "
            + "SET expires_at = ? WHERE subtxn_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setTimestamp(1, Timestamp.from(expiresAt));
      ps.setLong(2, subtxnId);
      ps.setString(3, realmId);
      ps.executeUpdate();
    }
  }

  private SubtxnRow rowToSubtxn(ResultSet rs) throws SQLException {
    return new SubtxnRow(
        rs.getLong("subtxn_id"),
        rs.getLong("txn_id"),
        rs.getString("realm_id"),
        rs.getLong("catalog_id"),
        rs.getString("name"),
        rs.getString("subtxn_token"),
        rs.getInt("status"),
        rs.getInt("attempt"),
        toInstant(rs.getTimestamp("created_at")),
        toInstant(rs.getTimestamp("expires_at")),
        toInstant(rs.getTimestamp("ended_at")));
  }

  // ---------------------------------------------------------------------------
  // transaction_operations
  // ---------------------------------------------------------------------------

  public record OperationRow(
      long opId,
      long txnId,
      @Nullable Long subtxnId,
      String realmId,
      long catalogId,
      String tableUuid,
      String lockMode,
      @Nullable String tableEntityId,
      @Nullable Long baseSnapshotId,
      @Nullable String serverBaseMetadataLocation,
      String assignedStagingPath,
      @Nullable String stagedMetadataLocation,
      String opToken,
      int status,
      @Nullable Long retryOfOpId,
      Instant createdAt,
      Instant expiresAt,
      @Nullable Instant endedAt) {}

  public void insertOperation(@Nonnull OperationRow row) throws SQLException {
    String sql =
        "INSERT INTO POLARIS_SCHEMA.transaction_operations "
            + "(op_id, txn_id, subtxn_id, realm_id, catalog_id, table_uuid, lock_mode, "
            + " table_entity_id, base_snapshot_id, server_base_metadata_location, "
            + " assigned_staging_path, op_token, status, retry_of_op_id, created_at, expires_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setLong(1, row.opId());
      ps.setLong(2, row.txnId());
      setNullableLong(ps, 3, row.subtxnId());
      ps.setString(4, row.realmId());
      ps.setLong(5, row.catalogId());
      ps.setString(6, row.tableUuid());
      ps.setString(7, row.lockMode());
      ps.setString(8, row.tableEntityId());
      setNullableLong(ps, 9, row.baseSnapshotId());
      ps.setString(10, row.serverBaseMetadataLocation());
      ps.setString(11, row.assignedStagingPath());
      ps.setString(12, row.opToken());
      ps.setInt(13, row.status());
      setNullableLong(ps, 14, row.retryOfOpId());
      ps.setTimestamp(15, Timestamp.from(row.createdAt()));
      ps.setTimestamp(16, Timestamp.from(row.expiresAt()));
      ps.executeUpdate();
    }
  }

  private static final String OP_COLUMNS =
      "op_id, txn_id, subtxn_id, realm_id, catalog_id, table_uuid, lock_mode, "
          + "table_entity_id, base_snapshot_id, server_base_metadata_location, assigned_staging_path, "
          + "staged_metadata_location, op_token, status, retry_of_op_id, "
          + "created_at, expires_at, ended_at";

  public Optional<OperationRow> findOperation(long opId) throws SQLException {
    String sql =
        "SELECT "
            + OP_COLUMNS
            + " FROM POLARIS_SCHEMA.transaction_operations WHERE op_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, opId);
      ps.setString(2, realmId);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rowToOperation(rs)) : Optional.empty();
      }
    }
  }

  public List<OperationRow> findOperationsForTransaction(long txnId) throws SQLException {
    String sql =
        "SELECT "
            + OP_COLUMNS
            + " FROM POLARIS_SCHEMA.transaction_operations WHERE txn_id = ? AND realm_id = ?";
    List<OperationRow> rows = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, txnId);
      ps.setString(2, realmId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) rows.add(rowToOperation(rs));
      }
    }
    return rows;
  }

  /** All operations belonging to a subtransaction, in creation order. */
  public List<OperationRow> findOperationsForSubtxn(long subtxnId) throws SQLException {
    String sql =
        "SELECT "
            + OP_COLUMNS
            + " FROM POLARIS_SCHEMA.transaction_operations "
            + "WHERE subtxn_id = ? AND realm_id = ? AND status != ? ORDER BY op_id";
    List<OperationRow> rows = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, subtxnId);
      ps.setString(2, realmId);
      ps.setInt(3, OP_STATUS_ABORTED);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) rows.add(rowToOperation(rs));
      }
    }
    return rows;
  }

  public void markOperationCommitted(long opId, String stagedMetadataLocation) throws SQLException {
    String sql =
        "UPDATE POLARIS_SCHEMA.transaction_operations "
            + "SET status = ?, staged_metadata_location = ?, ended_at = now() "
            + "WHERE op_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setInt(1, OP_STATUS_COMMITTED);
      ps.setString(2, stagedMetadataLocation);
      ps.setLong(3, opId);
      ps.setString(4, realmId);
      ps.executeUpdate();
    }
  }

  public void markOperationAborted(long opId) throws SQLException {
    String sql =
        "UPDATE POLARIS_SCHEMA.transaction_operations SET status = ?, ended_at = now() "
            + "WHERE op_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setInt(1, OP_STATUS_ABORTED);
      ps.setLong(2, opId);
      ps.setString(3, realmId);
      ps.executeUpdate();
    }
  }

  private OperationRow rowToOperation(ResultSet rs) throws SQLException {
    return new OperationRow(
        rs.getLong("op_id"),
        rs.getLong("txn_id"),
        getNullableLong(rs, "subtxn_id"),
        rs.getString("realm_id"),
        rs.getLong("catalog_id"),
        rs.getString("table_uuid"),
        rs.getString("lock_mode"),
        rs.getString("table_entity_id"),
        getNullableLong(rs, "base_snapshot_id"),
        rs.getString("server_base_metadata_location"),
        rs.getString("assigned_staging_path"),
        rs.getString("staged_metadata_location"),
        rs.getString("op_token"),
        rs.getInt("status"),
        getNullableLong(rs, "retry_of_op_id"),
        toInstant(rs.getTimestamp("created_at")),
        toInstant(rs.getTimestamp("expires_at")),
        toInstant(rs.getTimestamp("ended_at")));
  }

  // ---------------------------------------------------------------------------
  // txn_snapshots
  // ---------------------------------------------------------------------------

  /**
   * A read visibility handle. {@code readPoint} is a single commit-order cutoff (a commit SCN): a
   * committed table snapshot is visible iff {@code commit_scn <= readPoint}. This replaces the
   * former PostgreSQL-style {@code (xmin, xmax, xipList)} triple.
   */
  public record TxnSnapshotRow(
      long txnSnapshotId,
      String realmId,
      long catalogId,
      long readPoint,
      Instant createdAt,
      Instant expiresAt,
      @Nullable Instant releasedAt) {}

  public void insertTxnSnapshot(@Nonnull TxnSnapshotRow row) throws SQLException {
    String sql =
        "INSERT INTO POLARIS_SCHEMA.txn_snapshots "
            + "(txn_snapshot_id, realm_id, catalog_id, read_point, created_at, expires_at) "
            + "VALUES (?, ?, ?, ?, ?, ?)";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setLong(1, row.txnSnapshotId());
      ps.setString(2, row.realmId());
      ps.setLong(3, row.catalogId());
      ps.setLong(4, row.readPoint());
      ps.setTimestamp(5, Timestamp.from(row.createdAt()));
      ps.setTimestamp(6, Timestamp.from(row.expiresAt()));
      ps.executeUpdate();
    }
  }

  public Optional<TxnSnapshotRow> findTxnSnapshot(long txnSnapshotId) throws SQLException {
    String sql =
        "SELECT txn_snapshot_id, realm_id, catalog_id, read_point, "
            + "created_at, expires_at, released_at "
            + "FROM POLARIS_SCHEMA.txn_snapshots "
            + "WHERE txn_snapshot_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, txnSnapshotId);
      ps.setString(2, realmId);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rowToTxnSnapshot(rs)) : Optional.empty();
      }
    }
  }

  public void releaseTxnSnapshot(long txnSnapshotId) throws SQLException {
    String sql =
        "UPDATE POLARIS_SCHEMA.txn_snapshots SET released_at = now() "
            + "WHERE txn_snapshot_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setLong(1, txnSnapshotId);
      ps.setString(2, realmId);
      ps.executeUpdate();
    }
  }

  public void renewTxnSnapshot(long txnSnapshotId, Instant newExpiresAt) throws SQLException {
    String sql =
        "UPDATE POLARIS_SCHEMA.txn_snapshots SET expires_at = ? "
            + "WHERE txn_snapshot_id = ? AND realm_id = ? AND released_at IS NULL";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setTimestamp(1, Timestamp.from(newExpiresAt));
      ps.setLong(2, txnSnapshotId);
      ps.setString(3, realmId);
      ps.executeUpdate();
    }
  }

  /**
   * Captures the current {@code read_point}: the highest {@code commit_scn} that has durably
   * committed in this realm+catalog. A committed table snapshot is visible to this snapshot iff
   * {@code commit_scn <= read_point}.
   *
   * <p>Read directly from {@code commit_scn_counter} (the current committed value). Because that
   * counter advances atomically with the CTS row inside the commit transaction, the captured {@code
   * read_point} can never exceed a {@code commit_scn} whose row is not yet visible — the
   * assign-before-visible window is closed.
   */
  public long captureReadPoint() throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      return currentCommitScn(conn);
    }
  }

  private TxnSnapshotRow rowToTxnSnapshot(ResultSet rs) throws SQLException {
    return new TxnSnapshotRow(
        rs.getLong("txn_snapshot_id"),
        rs.getString("realm_id"),
        rs.getLong("catalog_id"),
        rs.getLong("read_point"),
        toInstant(rs.getTimestamp("created_at")),
        toInstant(rs.getTimestamp("expires_at")),
        toInstant(rs.getTimestamp("released_at")));
  }

  // ---------------------------------------------------------------------------
  // committed_table_snapshots
  // ---------------------------------------------------------------------------

  /**
   * A committed table state. {@code commitScn} is the commit-order stamp allocated from {@code
   * commit_scn_counter}. When a caller builds a row as a <i>template</i> for insertion (before the
   * scn is known), it passes {@link #UNASSIGNED_SCN}; the insert path allocates the real scn inside
   * the commit transaction and stamps it on the persisted row.
   */
  public record CommittedTableSnapshotRow(
      String tableUuid,
      long commitScn,
      @Nullable Long icebergSnapshotId,
      String metadataLocation) {}

  /** Placeholder scn for rows built before the commit transaction allocates the real value. */
  public static final long UNASSIGNED_SCN = 0L;

  /**
   * Records a table commit on a connection that is already inside a transaction (autocommit=false).
   * The commit/rollback is owned by the caller. {@code row.commitScn()} must already be a value
   * allocated via {@link #allocateCommitScn(Connection)} <b>on the same connection/transaction</b>,
   * so the scn advance and this row become durable together.
   */
  public void insertCommittedTableSnapshot(
      @Nonnull Connection conn, @Nonnull CommittedTableSnapshotRow row) throws SQLException {
    String sql =
        "INSERT INTO POLARIS_SCHEMA.committed_table_snapshots "
            + "(realm_id, catalog_id, table_uuid, commit_scn, iceberg_snapshot_id, metadata_location) "
            + "VALUES (?, ?, ?, ?, ?, ?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, realmId);
      ps.setLong(2, catalogId);
      ps.setString(3, row.tableUuid());
      ps.setLong(4, row.commitScn());
      setNullableLong(ps, 5, row.icebergSnapshotId());
      ps.setString(6, row.metadataLocation());
      ps.executeUpdate();
    }
  }

  /**
   * Best-effort fallback for the non-JDBC / in-memory metastore path (which cannot share a
   * transaction with the entity write). Allocates a {@code commit_scn} and inserts the row in its
   * own transaction, so the scn advance and the row are at least durable together. The {@code
   * commitScn} on the input row is ignored — a fresh one is allocated here.
   */
  public void insertCommittedTableSnapshotRow(@Nonnull CommittedTableSnapshotRow row)
      throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        long scn = allocateCommitScn(conn);
        insertCommittedTableSnapshot(
            conn,
            new CommittedTableSnapshotRow(
                row.tableUuid(), scn, row.icebergSnapshotId(), row.metadataLocation()));
        conn.commit();
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      }
    }
  }

  /**
   * Resolves a TxnSnapshot to the per-table metadata location visible under {@code snapshot}.
   *
   * <p>Visibility rule: the newest commit with {@code commit_scn <= read_point}. A {@code
   * committed_table_snapshots} row exists only for committed transactions, and {@code commit_scn}
   * is monotonic in commit order, so a single commit-order cutoff is sufficient — there is nothing
   * in-flight/aborted to exclude.
   */
  public Optional<CommittedTableSnapshotRow> resolveTableSnapshot(
      @Nonnull String tableUuid, @Nonnull TxnSnapshotRow snapshot) throws SQLException {
    return resolveTableSnapshot(tableUuid, snapshot.readPoint());
  }

  /**
   * Resolves the per-table metadata location visible at commit-order cutoff {@code readPoint} — the
   * newest commit with {@code commit_scn <= readPoint}. This is the representation-independent
   * form: the read point may come from a live {@code txn_snapshots} row (write path) or be decoded
   * from an opaque handle (standalone read path); both resolve identically.
   */
  public Optional<CommittedTableSnapshotRow> resolveTableSnapshot(
      @Nonnull String tableUuid, long readPoint) throws SQLException {

    String sql =
        "SELECT table_uuid, commit_scn, iceberg_snapshot_id, metadata_location "
            + "FROM POLARIS_SCHEMA.committed_table_snapshots "
            + "WHERE realm_id = ? AND catalog_id = ? AND table_uuid = ? "
            + "AND commit_scn <= ?"
            + " ORDER BY commit_scn DESC LIMIT 1";

    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      int idx = 1;
      ps.setString(idx++, realmId);
      ps.setLong(idx++, catalogId);
      ps.setString(idx++, tableUuid);
      ps.setLong(idx++, readPoint);

      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return Optional.of(
              new CommittedTableSnapshotRow(
                  rs.getString("table_uuid"),
                  rs.getLong("commit_scn"),
                  getNullableLong(rs, "iceberg_snapshot_id"),
                  rs.getString("metadata_location")));
        }
        return Optional.empty();
      }
    }
  }

  /**
   * Returns the most-recently-committed {@code metadata_location} for {@code tableUuid}, or {@link
   * Optional#empty()} if the table has no committed history yet.
   */
  public Optional<String> findLatestCommittedMetadataLocation(String tableUuid)
      throws SQLException {
    String sql =
        "SELECT metadata_location FROM POLARIS_SCHEMA.committed_table_snapshots "
            + "WHERE realm_id = ? AND catalog_id = ? AND table_uuid = ? "
            + "ORDER BY commit_scn DESC LIMIT 1";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, realmId);
      ps.setLong(2, catalogId);
      ps.setString(3, tableUuid);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rs.getString("metadata_location")) : Optional.empty();
      }
    }
  }

  // ---------------------------------------------------------------------------
  // locks
  // ---------------------------------------------------------------------------

  public record LockRow(
      long lockId,
      long txnId,
      String realmId,
      long catalogId,
      String resource,
      String lockMode,
      int status,
      Instant requestedAt,
      @Nullable Instant grantedAt) {}

  public long insertLock(long txnId, @Nonnull String resource, @Nonnull String lockMode)
      throws SQLException {
    String sql =
        "INSERT INTO POLARIS_SCHEMA.locks "
            + "(txn_id, realm_id, catalog_id, resource, lock_mode, status, requested_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, now())";
    AtomicLong generatedId = new AtomicLong(-1L);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql, new String[] {"lock_id"})) {
      conn.setAutoCommit(true);
      ps.setLong(1, txnId);
      ps.setString(2, realmId);
      ps.setLong(3, catalogId);
      ps.setString(4, resource);
      ps.setString(5, lockMode);
      ps.setInt(6, LOCK_STATUS_WAITING);
      ps.executeUpdate();
      try (ResultSet keys = ps.getGeneratedKeys()) {
        if (keys.next()) generatedId.set(keys.getLong(1));
      }
    }
    return generatedId.get();
  }

  public void grantLock(long lockId) throws SQLException {
    String sql = "UPDATE POLARIS_SCHEMA.locks SET status = ?, granted_at = now() WHERE lock_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setInt(1, LOCK_STATUS_GRANTED);
      ps.setLong(2, lockId);
      ps.executeUpdate();
    }
  }

  public void deleteLocksForTransaction(long txnId) throws SQLException {
    String sql = "DELETE FROM POLARIS_SCHEMA.locks WHERE txn_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setLong(1, txnId);
      ps.setString(2, realmId);
      ps.executeUpdate();
    }
  }

  public void deleteLockForResource(long txnId, @Nonnull String resource) throws SQLException {
    String sql =
        "DELETE FROM POLARIS_SCHEMA.locks WHERE txn_id = ? AND realm_id = ? AND resource = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setLong(1, txnId);
      ps.setString(2, realmId);
      ps.setString(3, resource);
      ps.executeUpdate();
    }
  }

  public List<LockRow> findGrantedLocksOnResource(@Nonnull String resource, long excludeTxnId)
      throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      return findGrantedLocksOnResource(conn, resource, excludeTxnId);
    }
  }

  /**
   * Connection-scoped variant of {@link #findGrantedLocksOnResource(String, long)} — reads on the
   * caller's connection so it can participate in the anchor-locked critical section (see {@link
   * #runWithResourceAnchor}).
   */
  public List<LockRow> findGrantedLocksOnResource(
      @Nonnull Connection conn, @Nonnull String resource, long excludeTxnId) throws SQLException {
    String sql =
        "SELECT lock_id, txn_id, realm_id, catalog_id, resource, lock_mode, status, "
            + "requested_at, granted_at "
            + "FROM POLARIS_SCHEMA.locks "
            + "WHERE realm_id = ? AND catalog_id = ? AND resource = ? "
            + "AND status = ? AND txn_id <> ?";
    List<LockRow> rows = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, realmId);
      ps.setLong(2, catalogId);
      ps.setString(3, resource);
      ps.setInt(4, LOCK_STATUS_GRANTED);
      ps.setLong(5, excludeTxnId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) rows.add(rowToLock(rs));
      }
    }
    return rows;
  }

  /**
   * Inserts an already-GRANTED lock row on the caller's connection (for use inside {@link
   * #runWithResourceAnchor}, where the grant decision has already been made under the anchor lock).
   * Returns the generated {@code lock_id}.
   */
  public long insertGrantedLock(
      @Nonnull Connection conn, long txnId, @Nonnull String resource, @Nonnull String lockMode)
      throws SQLException {
    String sql =
        "INSERT INTO POLARIS_SCHEMA.locks "
            + "(txn_id, realm_id, catalog_id, resource, lock_mode, status, requested_at, granted_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, now(), now())";
    try (PreparedStatement ps = conn.prepareStatement(sql, new String[] {"lock_id"})) {
      ps.setLong(1, txnId);
      ps.setString(2, realmId);
      ps.setLong(3, catalogId);
      ps.setString(4, resource);
      ps.setString(5, lockMode);
      ps.setInt(6, LOCK_STATUS_GRANTED);
      ps.executeUpdate();
      try (ResultSet keys = ps.getGeneratedKeys()) {
        if (keys.next()) return keys.getLong(1);
      }
    }
    throw new SQLException("insertGrantedLock returned no generated key");
  }

  /** A unit of work executed while holding the per-resource anchor lock. */
  @FunctionalInterface
  public interface ResourceAnchorCallback<T> {
    T run(@Nonnull Connection conn) throws SQLException;
  }

  /**
   * Runs {@code cb} inside a single transaction that holds an exclusive row lock on the {@code
   * lock_resource} anchor for {@code (realmId, catalogId, resource)}, serializing the lock-grant
   * critical section across all Polaris nodes. The anchor row is created on first use. The anchor
   * lock is released when this transaction commits (right after {@code cb} returns) — it guards
   * only the check-then-insert, not the lifetime of the granted lock.
   *
   * <p>Because every acquirer of the same resource must take this same row lock, two nodes can no
   * longer both observe "no conflict" and both insert a conflicting {@code locks} row (the phantom
   * a plain {@code SELECT ... FOR UPDATE} on {@code locks} cannot prevent).
   */
  public <T> T runWithResourceAnchor(
      @Nonnull String resource, @Nonnull ResourceAnchorCallback<T> cb) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        lockResourceAnchor(conn, resource);
        T result = cb.run(conn);
        conn.commit();
        return result;
      } catch (SQLException | RuntimeException e) {
        conn.rollback();
        throw e;
      }
    }
  }

  /** Ensures the anchor row exists and takes its row lock (held until the caller commits). */
  private void lockResourceAnchor(@Nonnull Connection conn, @Nonnull String resource)
      throws SQLException {
    if (dbType == DatabaseType.H2) {
      try (PreparedStatement init =
          conn.prepareStatement(
              "INSERT INTO POLARIS_SCHEMA.lock_resource (realm_id, catalog_id, resource) "
                  + "SELECT ?, ?, ? WHERE NOT EXISTS ("
                  + "SELECT 1 FROM POLARIS_SCHEMA.lock_resource "
                  + "WHERE realm_id = ? AND catalog_id = ? AND resource = ?)")) {
        init.setString(1, realmId);
        init.setLong(2, catalogId);
        init.setString(3, resource);
        init.setString(4, realmId);
        init.setLong(5, catalogId);
        init.setString(6, resource);
        init.executeUpdate();
      }
      // H2 holds a write lock on the updated row until commit.
      try (PreparedStatement lock =
          conn.prepareStatement(
              "UPDATE POLARIS_SCHEMA.lock_resource SET updated_at = now() "
                  + "WHERE realm_id = ? AND catalog_id = ? AND resource = ?")) {
        lock.setString(1, realmId);
        lock.setLong(2, catalogId);
        lock.setString(3, resource);
        lock.executeUpdate();
      }
    } else {
      // Postgres: ON CONFLICT DO UPDATE creates-or-locks the row atomically and holds the row lock
      // to commit, even on the conflict (existing-row) path.
      try (PreparedStatement ps =
          conn.prepareStatement(
              "INSERT INTO POLARIS_SCHEMA.lock_resource (realm_id, catalog_id, resource) "
                  + "VALUES (?, ?, ?) "
                  + "ON CONFLICT (realm_id, catalog_id, resource) DO UPDATE SET updated_at = now()")) {
        ps.setString(1, realmId);
        ps.setLong(2, catalogId);
        ps.setString(3, resource);
        ps.executeUpdate();
      }
    }
  }

  private LockRow rowToLock(ResultSet rs) throws SQLException {
    return new LockRow(
        rs.getLong("lock_id"),
        rs.getLong("txn_id"),
        rs.getString("realm_id"),
        rs.getLong("catalog_id"),
        rs.getString("resource"),
        rs.getString("lock_mode"),
        rs.getInt("status"),
        toInstant(rs.getTimestamp("requested_at")),
        toInstant(rs.getTimestamp("granted_at")));
  }

  // ---------------------------------------------------------------------------
  // txn_watermark — purge horizon (in commit_scn space)
  // ---------------------------------------------------------------------------

  /**
   * Distinct {@code catalog_id}s that have transaction state (committed snapshots or live
   * snapshots) in this realm. The reaper iterates these to run per-catalog purges, since {@code
   * commit_scn} is per-catalog and not comparable across catalogs.
   */
  public List<Long> findCatalogIdsForRealm() throws SQLException {
    String sql =
        "SELECT DISTINCT catalog_id FROM POLARIS_SCHEMA.committed_table_snapshots WHERE realm_id = ? "
            + "UNION "
            + "SELECT DISTINCT catalog_id FROM POLARIS_SCHEMA.txn_snapshots WHERE realm_id = ?";
    List<Long> ids = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, realmId);
      ps.setString(2, realmId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) ids.add(rs.getLong(1));
      }
    }
    return ids;
  }

  /**
   * The {@code MIN(read_point)} over live (unreleased, unexpired) {@code txn_snapshots} for this
   * realm+catalog, or {@code -1} if there are none. This is the retention horizon: no live reader
   * can resolve a {@code commit_scn} below it (in-progress REPEATABLE_READ writers are covered
   * because their linked snapshot row stays live until the transaction ends).
   */
  public long minLiveReadPointForCatalog() throws SQLException {
    String sql =
        "SELECT MIN(read_point) FROM POLARIS_SCHEMA.txn_snapshots "
            + "WHERE realm_id = ? AND catalog_id = ? AND released_at IS NULL AND expires_at > now()";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, realmId);
      ps.setLong(2, catalogId);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          long v = rs.getLong(1);
          if (!rs.wasNull()) {
            return v;
          }
        }
        return -1L;
      }
    }
  }

  /**
   * The {@code read_floor}: the highest {@code commit_scn} whose row is old enough that any
   * standalone read handle referencing it must already have exceeded its lease. Standalone reads no
   * longer persist a {@code txn_snapshots} row, so they contribute nothing to {@link
   * #minLiveReadPointForCatalog()}; instead their retention is bounded by time. A handle minted at
   * or below this scn was issued more than {@code (leaseSeconds + skewPadSeconds)} ago, so its
   * lease has expired and the rows it could resolve are safe to purge.
   *
   * <p>Uses {@code committed_table_snapshots.(committed_at, commit_scn)} directly as the time→scn
   * map — no separate checkpoint table. Returns {@code 0} when no row is old enough (NULL from
   * {@code MAX}).
   */
  public long readFloor(long leaseSeconds, long skewPadSeconds) throws SQLException {
    long cutoffSeconds = leaseSeconds + skewPadSeconds;
    String interval =
        dbType == DatabaseType.H2
            ? "DATEADD('SECOND', -?, now())"
            : "now() - make_interval(secs => ?)";
    String sql =
        "SELECT MAX(commit_scn) FROM POLARIS_SCHEMA.committed_table_snapshots "
            + "WHERE realm_id = ? AND catalog_id = ? AND committed_at <= "
            + interval;
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, realmId);
      ps.setLong(2, catalogId);
      ps.setLong(3, cutoffSeconds);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          long v = rs.getLong(1);
          if (!rs.wasNull()) {
            return v;
          }
        }
        return 0L;
      }
    }
  }

  public long getWatermark() throws SQLException {
    String sql =
        "SELECT watermark_txn_id FROM POLARIS_SCHEMA.txn_watermark "
            + "WHERE realm_id = ? AND catalog_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, realmId);
      ps.setLong(2, catalogId);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 0L;
      }
    }
  }

  /** Records the current purge horizon for this realm+catalog (monotonically non-decreasing). */
  public void upsertWatermark(long newWatermark) throws SQLException {
    String sql;
    if (dbType == DatabaseType.H2) {
      sql =
          "MERGE INTO POLARIS_SCHEMA.txn_watermark (realm_id, catalog_id, watermark_txn_id, updated_at) "
              + "KEY (realm_id, catalog_id) VALUES (?, ?, ?, now())";
    } else {
      sql =
          "INSERT INTO POLARIS_SCHEMA.txn_watermark (realm_id, catalog_id, watermark_txn_id, updated_at) "
              + "VALUES (?, ?, ?, now()) "
              + "ON CONFLICT (realm_id, catalog_id) DO UPDATE "
              + "SET watermark_txn_id = EXCLUDED.watermark_txn_id, updated_at = now() "
              + "WHERE POLARIS_SCHEMA.txn_watermark.watermark_txn_id < EXCLUDED.watermark_txn_id";
    }
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setString(1, realmId);
      ps.setLong(2, catalogId);
      ps.setLong(3, newWatermark);
      ps.executeUpdate();
    }
  }

  /**
   * Purges {@code committed_table_snapshots} rows for this realm+catalog that no live snapshot can
   * resolve, while honoring the per-table survivor rule: for each table it keeps the newest row
   * with {@code commit_scn <= watermark} (a snapshot pinned exactly at the watermark must still
   * resolve tables that have not changed since), plus every row above the watermark, and deletes
   * only the rows strictly older than that per-table survivor.
   */
  public void purgeCommittedSnapshotsBelowWatermark(long watermark) throws SQLException {
    if (watermark <= 0) {
      return;
    }
    String sql =
        "DELETE FROM POLARIS_SCHEMA.committed_table_snapshots c "
            + "WHERE c.realm_id = ? AND c.catalog_id = ? AND c.commit_scn < ("
            + "  SELECT MAX(c2.commit_scn) FROM POLARIS_SCHEMA.committed_table_snapshots c2 "
            + "  WHERE c2.realm_id = c.realm_id AND c2.catalog_id = c.catalog_id "
            + "    AND c2.table_uuid = c.table_uuid AND c2.commit_scn <= ?)";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setString(1, realmId);
      ps.setLong(2, catalogId);
      ps.setLong(3, watermark);
      int deleted = ps.executeUpdate();
      LOGGER.debug(
          "Purged {} committed_table_snapshots rows for catalog {} below watermark {} (per-table survivor kept)",
          deleted,
          catalogId,
          watermark);
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static void setNullableLong(PreparedStatement ps, int idx, @Nullable Long value)
      throws SQLException {
    if (value == null) ps.setNull(idx, java.sql.Types.BIGINT);
    else ps.setLong(idx, value);
  }

  private static @Nullable Long getNullableLong(ResultSet rs, String col) throws SQLException {
    long v = rs.getLong(col);
    return rs.wasNull() ? null : v;
  }

  private static @Nullable Instant toInstant(@Nullable Timestamp ts) {
    return ts == null ? null : ts.toInstant();
  }
}
