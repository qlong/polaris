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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raw JDBC persistence for the multi-table transaction extension tables introduced in schema v5:
 * {@code transactions}, {@code subtransactions}, {@code txn_snapshots}, {@code
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

  /** Status codes for the {@code subtransactions} table. */
  public static final int SUBTXN_STATUS_IN_PROGRESS = 0;

  public static final int SUBTXN_STATUS_SUB_COMMITTED = 1;
  public static final int SUBTXN_STATUS_SUB_ABORTED = 2;

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

  /**
   * Returns the next-to-be-allocated value of {@code txn_id_seq} (i.e., xmax) without advancing it.
   *
   * <p>PostgreSQL: {@code last_value + 1} (last_value is the last allocated value). H2: {@code
   * BASE_VALUE} from INFORMATION_SCHEMA.SEQUENCES, which is already the next value. Returns 1 if
   * the sequence has never been used.
   */
  private long sequenceXmax(Connection conn) throws SQLException {
    if (dbType == DatabaseType.H2) {
      String sql =
          "SELECT BASE_VALUE FROM INFORMATION_SCHEMA.SEQUENCES"
              + " WHERE SEQUENCE_SCHEMA = 'POLARIS_SCHEMA' AND SEQUENCE_NAME = 'txn_id_seq'";
      try (PreparedStatement ps = conn.prepareStatement(sql);
          ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 1L;
      }
    } else {
      String sql = "SELECT last_value FROM POLARIS_SCHEMA.txn_id_seq";
      try (PreparedStatement ps = conn.prepareStatement(sql);
          ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) + 1 : 1L;
      }
    }
  }

  // ---------------------------------------------------------------------------
  // transactions
  // ---------------------------------------------------------------------------

  public record TransactionRow(
      long txnId,
      String realmId,
      long catalogId,
      String txnToken,
      String ownerPrincipal,
      int status,
      String isolation,
      @Nullable Long snapXmin,
      @Nullable Long snapXmax,
      @Nullable String snapXipList,
      @Nullable Long txnSnapshotId,
      Instant startedAt,
      Instant expiresAt,
      @Nullable Instant endedAt) {}

  public void insertTransaction(@Nonnull TransactionRow row) throws SQLException {
    String sql =
        "INSERT INTO POLARIS_SCHEMA.transactions "
            + "(txn_id, realm_id, catalog_id, txn_token, owner_principal, status, isolation, "
            + " snap_xmin, snap_xmax, snap_xip_list, txn_snapshot_id, started_at, expires_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setLong(1, row.txnId());
      ps.setString(2, row.realmId());
      ps.setLong(3, row.catalogId());
      ps.setString(4, row.txnToken());
      ps.setString(5, row.ownerPrincipal());
      ps.setInt(6, row.status());
      ps.setString(7, row.isolation());
      setNullableLong(ps, 8, row.snapXmin());
      setNullableLong(ps, 9, row.snapXmax());
      ps.setString(10, row.snapXipList());
      setNullableLong(ps, 11, row.txnSnapshotId());
      ps.setTimestamp(12, Timestamp.from(row.startedAt()));
      ps.setTimestamp(13, Timestamp.from(row.expiresAt()));
      ps.executeUpdate();
    }
  }

  public Optional<TransactionRow> findTransaction(long txnId) throws SQLException {
    String sql =
        "SELECT txn_id, realm_id, catalog_id, txn_token, owner_principal, status, isolation, "
            + "snap_xmin, snap_xmax, snap_xip_list, txn_snapshot_id, started_at, expires_at, ended_at "
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
        rs.getString("realm_id"),
        rs.getLong("catalog_id"),
        rs.getString("txn_token"),
        rs.getString("owner_principal"),
        rs.getInt("status"),
        rs.getString("isolation"),
        getNullableLong(rs, "snap_xmin"),
        getNullableLong(rs, "snap_xmax"),
        rs.getString("snap_xip_list"),
        getNullableLong(rs, "txn_snapshot_id"),
        toInstant(rs.getTimestamp("started_at")),
        toInstant(rs.getTimestamp("expires_at")),
        toInstant(rs.getTimestamp("ended_at")));
  }

  // ---------------------------------------------------------------------------
  // subtransactions
  // ---------------------------------------------------------------------------

  public record SubtransactionRow(
      long subtxnId,
      long parentTxnId,
      String realmId,
      long catalogId,
      String tableUuid,
      String operation,
      @Nullable Long baseSnapshotId,
      @Nullable String serverBaseMetadataLocation,
      String assignedStagingPath,
      @Nullable String stagedMetadataLocation,
      String subtxnToken,
      int status,
      @Nullable Long retryOfSubtxnId,
      Instant createdAt,
      Instant expiresAt,
      @Nullable Instant endedAt) {}

  public void insertSubtransaction(@Nonnull SubtransactionRow row) throws SQLException {
    String sql =
        "INSERT INTO POLARIS_SCHEMA.subtransactions "
            + "(subtxn_id, parent_txn_id, realm_id, catalog_id, table_uuid, operation, "
            + " base_snapshot_id, server_base_metadata_location, assigned_staging_path, "
            + " subtxn_token, status, retry_of_subtxn_id, created_at, expires_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setLong(1, row.subtxnId());
      ps.setLong(2, row.parentTxnId());
      ps.setString(3, row.realmId());
      ps.setLong(4, row.catalogId());
      ps.setString(5, row.tableUuid());
      ps.setString(6, row.operation());
      setNullableLong(ps, 7, row.baseSnapshotId());
      ps.setString(8, row.serverBaseMetadataLocation());
      ps.setString(9, row.assignedStagingPath());
      ps.setString(10, row.subtxnToken());
      ps.setInt(11, row.status());
      setNullableLong(ps, 12, row.retryOfSubtxnId());
      ps.setTimestamp(13, Timestamp.from(row.createdAt()));
      ps.setTimestamp(14, Timestamp.from(row.expiresAt()));
      ps.executeUpdate();
    }
  }

  public Optional<SubtransactionRow> findSubtransaction(long subtxnId) throws SQLException {
    String sql =
        "SELECT subtxn_id, parent_txn_id, realm_id, catalog_id, table_uuid, operation, "
            + "base_snapshot_id, server_base_metadata_location, assigned_staging_path, "
            + "staged_metadata_location, subtxn_token, status, retry_of_subtxn_id, "
            + "created_at, expires_at, ended_at "
            + "FROM POLARIS_SCHEMA.subtransactions WHERE subtxn_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, subtxnId);
      ps.setString(2, realmId);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rowToSubtransaction(rs)) : Optional.empty();
      }
    }
  }

  public List<SubtransactionRow> findSubtransactionsForTransaction(long txnId) throws SQLException {
    String sql =
        "SELECT subtxn_id, parent_txn_id, realm_id, catalog_id, table_uuid, operation, "
            + "base_snapshot_id, server_base_metadata_location, assigned_staging_path, "
            + "staged_metadata_location, subtxn_token, status, retry_of_subtxn_id, "
            + "created_at, expires_at, ended_at "
            + "FROM POLARIS_SCHEMA.subtransactions WHERE parent_txn_id = ? AND realm_id = ?";
    List<SubtransactionRow> rows = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, txnId);
      ps.setString(2, realmId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) rows.add(rowToSubtransaction(rs));
      }
    }
    return rows;
  }

  public void markSubtransactionCommitted(long subtxnId, String stagedMetadataLocation)
      throws SQLException {
    String sql =
        "UPDATE POLARIS_SCHEMA.subtransactions "
            + "SET status = ?, staged_metadata_location = ?, ended_at = now() "
            + "WHERE subtxn_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setInt(1, SUBTXN_STATUS_SUB_COMMITTED);
      ps.setString(2, stagedMetadataLocation);
      ps.setLong(3, subtxnId);
      ps.setString(4, realmId);
      ps.executeUpdate();
    }
  }

  public void markSubtransactionAborted(long subtxnId) throws SQLException {
    String sql =
        "UPDATE POLARIS_SCHEMA.subtransactions SET status = ?, ended_at = now() "
            + "WHERE subtxn_id = ? AND realm_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setInt(1, SUBTXN_STATUS_SUB_ABORTED);
      ps.setLong(2, subtxnId);
      ps.setString(3, realmId);
      ps.executeUpdate();
    }
  }

  private SubtransactionRow rowToSubtransaction(ResultSet rs) throws SQLException {
    return new SubtransactionRow(
        rs.getLong("subtxn_id"),
        rs.getLong("parent_txn_id"),
        rs.getString("realm_id"),
        rs.getLong("catalog_id"),
        rs.getString("table_uuid"),
        rs.getString("operation"),
        getNullableLong(rs, "base_snapshot_id"),
        rs.getString("server_base_metadata_location"),
        rs.getString("assigned_staging_path"),
        rs.getString("staged_metadata_location"),
        rs.getString("subtxn_token"),
        rs.getInt("status"),
        getNullableLong(rs, "retry_of_subtxn_id"),
        toInstant(rs.getTimestamp("created_at")),
        toInstant(rs.getTimestamp("expires_at")),
        toInstant(rs.getTimestamp("ended_at")));
  }

  // ---------------------------------------------------------------------------
  // txn_snapshots
  // ---------------------------------------------------------------------------

  public record TxnSnapshotRow(
      long txnSnapshotId,
      String realmId,
      long catalogId,
      long xmin,
      long xmax,
      String xipList,
      Instant createdAt,
      Instant expiresAt,
      @Nullable Instant releasedAt) {}

  public void insertTxnSnapshot(@Nonnull TxnSnapshotRow row) throws SQLException {
    String sql =
        "INSERT INTO POLARIS_SCHEMA.txn_snapshots "
            + "(txn_snapshot_id, realm_id, catalog_id, snap_xmin, snap_xmax, snap_xip_list, created_at, expires_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setLong(1, row.txnSnapshotId());
      ps.setString(2, row.realmId());
      ps.setLong(3, row.catalogId());
      ps.setLong(4, row.xmin());
      ps.setLong(5, row.xmax());
      ps.setString(6, row.xipList());
      ps.setTimestamp(7, Timestamp.from(row.createdAt()));
      ps.setTimestamp(8, Timestamp.from(row.expiresAt()));
      ps.executeUpdate();
    }
  }

  public Optional<TxnSnapshotRow> findTxnSnapshot(long txnSnapshotId) throws SQLException {
    String sql =
        "SELECT txn_snapshot_id, realm_id, catalog_id, snap_xmin, snap_xmax, snap_xip_list, "
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
   * Captures the current MVCC visibility state: (xmin, xmax, xip_list) atomically.
   *
   * <p>Result layout: {@code [xmin, xmax, xip0, xip1, ...]}.
   */
  public long[] captureXminXmax() throws SQLException {
    String xipSql =
        "SELECT txn_id FROM POLARIS_SCHEMA.transactions "
            + "WHERE realm_id = ? AND catalog_id = ? AND status = ?";

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        long xmax = sequenceXmax(conn);
        List<Long> xip = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(xipSql)) {
          ps.setString(1, realmId);
          ps.setLong(2, catalogId);
          ps.setInt(3, TXN_STATUS_IN_PROGRESS);
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) xip.add(rs.getLong(1));
          }
        }
        conn.commit();
        long xmin = xip.stream().mapToLong(Long::longValue).min().orElse(1L);
        long[] result = new long[2 + xip.size()];
        result[0] = xmin;
        result[1] = xmax;
        for (int i = 0; i < xip.size(); i++) result[2 + i] = xip.get(i);
        return result;
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      }
    }
  }

  /**
   * Like {@link #captureXminXmax()} but includes in-progress transactions for this {@link #realmId}
   * across all catalogs (ignores {@link #catalogId}).
   */
  public long[] captureXminXmaxForRealm() throws SQLException {
    String xipSql =
        "SELECT txn_id FROM POLARIS_SCHEMA.transactions " + "WHERE realm_id = ? AND status = ?";

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        long xmax = sequenceXmax(conn);
        List<Long> xip = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(xipSql)) {
          ps.setString(1, realmId);
          ps.setInt(2, TXN_STATUS_IN_PROGRESS);
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) xip.add(rs.getLong(1));
          }
        }
        conn.commit();
        long xmin = xip.stream().mapToLong(Long::longValue).min().orElse(1L);
        long[] result = new long[2 + xip.size()];
        result[0] = xmin;
        result[1] = xmax;
        for (int i = 0; i < xip.size(); i++) result[2 + i] = xip.get(i);
        return result;
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      }
    }
  }

  private TxnSnapshotRow rowToTxnSnapshot(ResultSet rs) throws SQLException {
    return new TxnSnapshotRow(
        rs.getLong("txn_snapshot_id"),
        rs.getString("realm_id"),
        rs.getLong("catalog_id"),
        rs.getLong("snap_xmin"),
        rs.getLong("snap_xmax"),
        rs.getString("snap_xip_list"),
        toInstant(rs.getTimestamp("created_at")),
        toInstant(rs.getTimestamp("expires_at")),
        toInstant(rs.getTimestamp("released_at")));
  }

  // ---------------------------------------------------------------------------
  // committed_table_snapshots
  // ---------------------------------------------------------------------------

  public record CommittedTableSnapshotRow(
      String tableUuid,
      long committedTxnId,
      @Nullable Long icebergSnapshotId,
      String metadataLocation) {}

  /**
   * Records a table commit atomically with the entity update. Must be called on a connection that
   * is already inside a transaction (autocommit=false). The commit/rollback is owned by the caller.
   */
  public void insertCommittedTableSnapshot(
      @Nonnull Connection conn, @Nonnull CommittedTableSnapshotRow row) throws SQLException {
    String sql =
        "INSERT INTO POLARIS_SCHEMA.committed_table_snapshots "
            + "(realm_id, catalog_id, table_uuid, committed_txn_id, iceberg_snapshot_id, metadata_location) "
            + "VALUES (?, ?, ?, ?, ?, ?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, realmId);
      ps.setLong(2, catalogId);
      ps.setString(3, row.tableUuid());
      ps.setLong(4, row.committedTxnId());
      setNullableLong(ps, 5, row.icebergSnapshotId());
      ps.setString(6, row.metadataLocation());
      ps.executeUpdate();
    }
  }

  /**
   * Inserts a committed snapshot row in its own transaction. Used after the Polaris entity row has
   * been updated so the JDBC transaction scope does not have to match the metastore transaction.
   */
  public void insertCommittedTableSnapshotRow(@Nonnull CommittedTableSnapshotRow row)
      throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        insertCommittedTableSnapshot(conn, row);
        conn.commit();
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      }
    }
  }

  /**
   * Inserts all CTS rows for a multi-table transaction atomically: one {@code nextTxnId()} call
   * shared across every row, all rows inserted in a single JDBC transaction. This ensures an MVCC
   * snapshot taken before this call sees none of the tables' new states, and a snapshot taken after
   * sees all of them.
   */
  public void insertBatchCommittedTableSnapshots(@Nonnull List<CommittedTableSnapshotRow> rows)
      throws SQLException {
    if (rows.isEmpty()) {
      return;
    }
    long sharedTxnId = nextTxnId();
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        for (CommittedTableSnapshotRow row : rows) {
          insertCommittedTableSnapshot(
              conn,
              new CommittedTableSnapshotRow(
                  row.tableUuid(), sharedTxnId, row.icebergSnapshotId(), row.metadataLocation()));
        }
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
   * <p>Visibility rule: {@code committed_txn_id >= xmin AND < xmax AND NOT IN (xip_list)}.
   */
  public Optional<CommittedTableSnapshotRow> resolveTableSnapshot(
      @Nonnull String tableUuid, @Nonnull TxnSnapshotRow snapshot) throws SQLException {

    List<Long> xip = parseXipList(snapshot.xipList());
    String notInClause =
        xip.isEmpty() ? "" : " AND committed_txn_id NOT IN (" + placeholders(xip.size()) + ")";

    // MVCC visibility: a commit is visible if committed_txn_id < xmax and not in xip_list.
    // The xmin lower-bound is NOT used as a filter — it would incorrectly exclude rows committed
    // before any transaction was in-flight at snapshot time.
    String sql =
        "SELECT table_uuid, committed_txn_id, iceberg_snapshot_id, metadata_location "
            + "FROM POLARIS_SCHEMA.committed_table_snapshots "
            + "WHERE realm_id = ? AND catalog_id = ? AND table_uuid = ? "
            + "AND committed_txn_id < ?"
            + notInClause
            + " ORDER BY committed_txn_id DESC LIMIT 1";

    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      int idx = 1;
      ps.setString(idx++, realmId);
      ps.setLong(idx++, catalogId);
      ps.setString(idx++, tableUuid);
      ps.setLong(idx++, snapshot.xmax());
      for (long xipId : xip) ps.setLong(idx++, xipId);

      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return Optional.of(
              new CommittedTableSnapshotRow(
                  rs.getString("table_uuid"),
                  rs.getLong("committed_txn_id"),
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
            + "ORDER BY committed_txn_id DESC LIMIT 1";
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

  public List<LockRow> findGrantedLocksOnResource(@Nonnull String resource, long excludeTxnId)
      throws SQLException {
    String sql =
        "SELECT lock_id, txn_id, realm_id, catalog_id, resource, lock_mode, status, "
            + "requested_at, granted_at "
            + "FROM POLARIS_SCHEMA.locks "
            + "WHERE realm_id = ? AND catalog_id = ? AND resource = ? "
            + "AND status = ? AND txn_id <> ?";
    List<LockRow> rows = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
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
  // txn_watermark
  // ---------------------------------------------------------------------------

  /**
   * Raises {@code txn_watermark.watermark_txn_id} for every catalog in this realm up to {@code
   * newWatermark} when the current value is lower. Used by the background reaper.
   */
  public void raiseWatermarkAcrossRealm(long newWatermark) throws SQLException {
    if (newWatermark <= 0) {
      return;
    }
    String sql =
        "UPDATE POLARIS_SCHEMA.txn_watermark SET watermark_txn_id = ?, updated_at = now() "
            + "WHERE realm_id = ? AND watermark_txn_id < ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setLong(1, newWatermark);
      ps.setString(2, realmId);
      ps.setLong(3, newWatermark);
      ps.executeUpdate();
    }
  }

  /** Deletes committed snapshot rows for this realm older than {@code watermark} (any catalog). */
  public void purgeCommittedSnapshotsRealmWideBelow(long watermark) throws SQLException {
    String sql =
        "DELETE FROM POLARIS_SCHEMA.committed_table_snapshots "
            + "WHERE realm_id = ? AND committed_txn_id < ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setString(1, realmId);
      ps.setLong(2, watermark);
      int deleted = ps.executeUpdate();
      LOGGER.debug(
          "Purged {} committed_table_snapshots rows realm-wide below watermark {}",
          deleted,
          watermark);
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

  public void purgeCommittedSnapshotsBelowWatermark(long watermark) throws SQLException {
    String sql =
        "DELETE FROM POLARIS_SCHEMA.committed_table_snapshots "
            + "WHERE realm_id = ? AND catalog_id = ? AND committed_txn_id < ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      conn.setAutoCommit(true);
      ps.setString(1, realmId);
      ps.setLong(2, catalogId);
      ps.setLong(3, watermark);
      int deleted = ps.executeUpdate();
      LOGGER.debug(
          "Purged {} committed_table_snapshots rows below watermark {}", deleted, watermark);
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

  public static List<Long> parseXipList(@Nullable String xipList) {
    if (xipList == null || xipList.isBlank()) return List.of();
    return Arrays.stream(xipList.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(Long::parseLong)
        .collect(Collectors.toList());
  }

  private static String placeholders(int count) {
    return "?,".repeat(count).replaceAll(",$", "");
  }
}
