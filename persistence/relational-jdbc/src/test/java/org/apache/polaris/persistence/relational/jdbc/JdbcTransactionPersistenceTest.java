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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JdbcTransactionPersistence} using an in-memory H2 database bootstrapped
 * from {@code h2/schema-v5.sql}. H2 is run in PostgreSQL compatibility mode so that {@code
 * nextval()} works correctly.
 */
class JdbcTransactionPersistenceTest {

  private static final String REALM = "test-realm";
  private static final long CATALOG_ID = 42L;

  private DataSource dataSource;
  private JdbcTransactionPersistence persistence;

  @BeforeEach
  void setUp() throws Exception {
    // Run H2 in PostgreSQL mode so that nextval('seq') works.
    String jdbcUrl =
        "jdbc:h2:mem:txn_test_"
            + UUID.randomUUID()
            + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_UPPER=false";
    dataSource = JdbcConnectionPool.create(jdbcUrl, "sa", "");

    DatasourceOperations ops = new DatasourceOperations(dataSource, new SimpleJdbcConfig());
    InputStream schema =
        DatasourceOperations.class.getClassLoader().getResourceAsStream("h2/schema-v5.sql");
    ops.executeScript(schema);

    persistence = new JdbcTransactionPersistence(dataSource, DatabaseType.H2, REALM, CATALOG_ID);
  }

  // ---------------------------------------------------------------------------
  // nextTxnId
  // ---------------------------------------------------------------------------

  @Test
  void nextTxnIdReturnsMonotonicallyIncreasingValues() throws SQLException {
    long first = persistence.nextTxnId();
    long second = persistence.nextTxnId();
    long third = persistence.nextTxnId();

    assertThat(first).isGreaterThan(0);
    assertThat(second).isGreaterThan(first);
    assertThat(third).isGreaterThan(second);
  }

  // ---------------------------------------------------------------------------
  // transactions
  // ---------------------------------------------------------------------------

  @Test
  void insertAndFindTransaction() throws SQLException {
    long txnId = persistence.nextTxnId();
    Instant now = Instant.now();
    Instant expires = now.plusSeconds(300);

    JdbcTransactionPersistence.TransactionRow row =
        new JdbcTransactionPersistence.TransactionRow(
            txnId,
            null,
            REALM,
            CATALOG_ID,
            "tok-" + txnId,
            "alice",
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            "REPEATABLE_READ",
            null,
            now,
            expires,
            null);

    persistence.insertTransaction(row);

    Optional<JdbcTransactionPersistence.TransactionRow> found = persistence.findTransaction(txnId);
    assertThat(found).isPresent();
    JdbcTransactionPersistence.TransactionRow r = found.get();
    assertThat(r.txnId()).isEqualTo(txnId);
    assertThat(r.realmId()).isEqualTo(REALM);
    assertThat(r.catalogId()).isEqualTo(CATALOG_ID);
    assertThat(r.ownerPrincipal()).isEqualTo("alice");
    assertThat(r.status()).isEqualTo(JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS);
    assertThat(r.isolation()).isEqualTo("REPEATABLE_READ");
    assertThat(r.endedAt()).isNull();
  }

  @Test
  void findTransactionReturnsEmptyForUnknownId() throws SQLException {
    assertThat(persistence.findTransaction(99999L)).isEmpty();
  }

  @Test
  void updateTransactionStatusSetsEndedAt() throws SQLException {
    long txnId = persistence.nextTxnId();
    insertSimpleTransaction(txnId);

    persistence.updateTransactionStatus(txnId, JdbcTransactionPersistence.TXN_STATUS_COMMITTED);

    Optional<JdbcTransactionPersistence.TransactionRow> found = persistence.findTransaction(txnId);
    assertThat(found).isPresent();
    assertThat(found.get().status()).isEqualTo(JdbcTransactionPersistence.TXN_STATUS_COMMITTED);
    assertThat(found.get().endedAt()).isNotNull();
  }

  @Test
  void renewTransactionUpdatesExpiresAt() throws SQLException {
    long txnId = persistence.nextTxnId();
    insertSimpleTransaction(txnId);

    Instant newExpiry = Instant.now().plusSeconds(3600);
    persistence.renewTransaction(txnId, newExpiry);

    Optional<JdbcTransactionPersistence.TransactionRow> found = persistence.findTransaction(txnId);
    assertThat(found).isPresent();
    // The renewal updates expires_at; verify it's no longer in the past.
    assertThat(found.get().expiresAt()).isAfter(Instant.now().minusSeconds(10));
  }

  @Test
  void findExpiredTransactionIdsReturnsExpiredRows() throws SQLException {
    long expiredId = persistence.nextTxnId();
    long activeId = persistence.nextTxnId();

    // Insert one transaction already expired (expires_at in the past)
    JdbcTransactionPersistence.TransactionRow expired =
        new JdbcTransactionPersistence.TransactionRow(
            expiredId,
            null,
            REALM,
            CATALOG_ID,
            "tok-" + expiredId,
            "bob",
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            "REPEATABLE_READ",
            null,
            Instant.now().minusSeconds(600),
            Instant.now().minusSeconds(1), // already expired
            null);
    persistence.insertTransaction(expired);

    // Insert one with a future expiry
    JdbcTransactionPersistence.TransactionRow active =
        new JdbcTransactionPersistence.TransactionRow(
            activeId,
            null,
            REALM,
            CATALOG_ID,
            "tok-" + activeId,
            "carol",
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            "REPEATABLE_READ",
            null,
            Instant.now(),
            Instant.now().plusSeconds(300),
            null);
    persistence.insertTransaction(active);

    List<Long> expired1 = persistence.findExpiredTransactionIds();
    assertThat(expired1).contains(expiredId).doesNotContain(activeId);
  }

  // ---------------------------------------------------------------------------
  // transaction_operations
  // ---------------------------------------------------------------------------

  @Test
  void insertAndFindOperation() throws SQLException {
    long txnId = persistence.nextTxnId();
    insertSimpleTransaction(txnId);

    long opId = persistence.nextTxnId();
    Instant now = Instant.now();
    JdbcTransactionPersistence.OperationRow op =
        new JdbcTransactionPersistence.OperationRow(
            opId,
            txnId,
            REALM,
            CATALOG_ID,
            "table-uuid-1",
            "ROW_EXCLUSIVE",
            null,
            null,
            "s3://staging/path",
            null,
            "subtok-" + opId,
            JdbcTransactionPersistence.OP_STATUS_IN_PROGRESS,
            null,
            now,
            now.plusSeconds(60),
            null);

    persistence.insertOperation(op);

    Optional<JdbcTransactionPersistence.OperationRow> found = persistence.findOperation(opId);
    assertThat(found).isPresent();
    JdbcTransactionPersistence.OperationRow r = found.get();
    assertThat(r.opId()).isEqualTo(opId);
    assertThat(r.txnId()).isEqualTo(txnId);
    assertThat(r.tableUuid()).isEqualTo("table-uuid-1");
    assertThat(r.lockMode()).isEqualTo("ROW_EXCLUSIVE");
    assertThat(r.assignedStagingPath()).isEqualTo("s3://staging/path");
    assertThat(r.stagedMetadataLocation()).isNull();
    assertThat(r.status()).isEqualTo(JdbcTransactionPersistence.OP_STATUS_IN_PROGRESS);
  }

  @Test
  void findOperationsForTransactionReturnsAllChildren() throws SQLException {
    long txnId = persistence.nextTxnId();
    insertSimpleTransaction(txnId);

    long op1 = persistence.nextTxnId();
    long op2 = persistence.nextTxnId();
    insertSimpleOperation(op1, txnId);
    insertSimpleOperation(op2, txnId);

    List<JdbcTransactionPersistence.OperationRow> rows =
        persistence.findOperationsForTransaction(txnId);
    assertThat(rows).hasSize(2);
    assertThat(rows.stream().map(JdbcTransactionPersistence.OperationRow::opId))
        .containsExactlyInAnyOrder(op1, op2);
  }

  @Test
  void markOperationCommittedUpdatesStatus() throws SQLException {
    long txnId = persistence.nextTxnId();
    insertSimpleTransaction(txnId);
    long opId = persistence.nextTxnId();
    insertSimpleOperation(opId, txnId);

    persistence.markOperationCommitted(opId, "s3://staging/metadata.json");

    Optional<JdbcTransactionPersistence.OperationRow> found = persistence.findOperation(opId);
    assertThat(found).isPresent();
    assertThat(found.get().status()).isEqualTo(JdbcTransactionPersistence.OP_STATUS_COMMITTED);
    assertThat(found.get().stagedMetadataLocation()).isEqualTo("s3://staging/metadata.json");
    assertThat(found.get().endedAt()).isNotNull();
  }

  @Test
  void markOperationAbortedUpdatesStatus() throws SQLException {
    long txnId = persistence.nextTxnId();
    insertSimpleTransaction(txnId);
    long opId = persistence.nextTxnId();
    insertSimpleOperation(opId, txnId);

    persistence.markOperationAborted(opId);

    Optional<JdbcTransactionPersistence.OperationRow> found = persistence.findOperation(opId);
    assertThat(found).isPresent();
    assertThat(found.get().status()).isEqualTo(JdbcTransactionPersistence.OP_STATUS_ABORTED);
    assertThat(found.get().endedAt()).isNotNull();
  }

  // ---------------------------------------------------------------------------
  // txn_snapshots
  // ---------------------------------------------------------------------------

  @Test
  void insertAndFindTxnSnapshot() throws SQLException {
    long snapId = persistence.nextTxnId();
    Instant now = Instant.now();
    JdbcTransactionPersistence.TxnSnapshotRow snap =
        new JdbcTransactionPersistence.TxnSnapshotRow(
            snapId, REALM, CATALOG_ID, 20L, now, now.plusSeconds(60), null);

    persistence.insertTxnSnapshot(snap);

    Optional<JdbcTransactionPersistence.TxnSnapshotRow> found = persistence.findTxnSnapshot(snapId);
    assertThat(found).isPresent();
    JdbcTransactionPersistence.TxnSnapshotRow r = found.get();
    assertThat(r.txnSnapshotId()).isEqualTo(snapId);
    assertThat(r.readPoint()).isEqualTo(20L);
    assertThat(r.releasedAt()).isNull();
  }

  @Test
  void releaseTxnSnapshotSetsReleasedAt() throws SQLException {
    long snapId = persistence.nextTxnId();
    Instant now = Instant.now();
    persistence.insertTxnSnapshot(
        new JdbcTransactionPersistence.TxnSnapshotRow(
            snapId, REALM, CATALOG_ID, 2L, now, now.plusSeconds(60), null));

    persistence.releaseTxnSnapshot(snapId);

    Optional<JdbcTransactionPersistence.TxnSnapshotRow> found = persistence.findTxnSnapshot(snapId);
    assertThat(found).isPresent();
    assertThat(found.get().releasedAt()).isNotNull();
  }

  @Test
  void renewTxnSnapshotUpdatesExpiresAt() throws SQLException {
    long snapId = persistence.nextTxnId();
    Instant now = Instant.now();
    persistence.insertTxnSnapshot(
        new JdbcTransactionPersistence.TxnSnapshotRow(
            snapId, REALM, CATALOG_ID, 2L, now, now.plusSeconds(60), null));

    Instant newExpiry = now.plusSeconds(7200);
    persistence.renewTxnSnapshot(snapId, newExpiry);

    // Should not throw and the row should still be findable.
    assertThat(persistence.findTxnSnapshot(snapId)).isPresent();
  }

  // ---------------------------------------------------------------------------
  // committed_table_snapshots
  // ---------------------------------------------------------------------------

  @Test
  void insertCommittedTableSnapshotRowAndResolveSnapshot() throws SQLException {
    // Insert with an explicit commit_scn via the connection primitive
    // (insertCommittedTableSnapshotRow
    // auto-allocates, so use the primitive when the test needs a known scn).
    long scn = 100L;
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      persistence.insertCommittedTableSnapshot(
          conn,
          new JdbcTransactionPersistence.CommittedTableSnapshotRow(
              "uuid-tbl-1", scn, 555L, "s3://bucket/metadata.json"));
      conn.commit();
    }

    Instant now = Instant.now();
    JdbcTransactionPersistence.TxnSnapshotRow snap =
        new JdbcTransactionPersistence.TxnSnapshotRow(
            persistence.nextTxnId(),
            REALM,
            CATALOG_ID,
            scn + 10, // read_point >= commit_scn → visible
            now,
            now.plusSeconds(60),
            null);

    Optional<JdbcTransactionPersistence.CommittedTableSnapshotRow> resolved =
        persistence.resolveTableSnapshot("uuid-tbl-1", snap);
    assertThat(resolved).isPresent();
    assertThat(resolved.get().tableUuid()).isEqualTo("uuid-tbl-1");
    assertThat(resolved.get().commitScn()).isEqualTo(scn);
    assertThat(resolved.get().metadataLocation()).isEqualTo("s3://bucket/metadata.json");
  }

  @Test
  void resolveTableSnapshotExcludesCommitsAboveReadPoint() throws SQLException {
    long scn = 100L;
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      persistence.insertCommittedTableSnapshot(
          conn,
          new JdbcTransactionPersistence.CommittedTableSnapshotRow(
              "uuid-tbl-above", scn, null, "s3://bucket/v1.json"));
      conn.commit();
    }

    // read_point is below the commit's commit_scn → the commit is not yet visible.
    Instant now = Instant.now();
    JdbcTransactionPersistence.TxnSnapshotRow snap =
        new JdbcTransactionPersistence.TxnSnapshotRow(
            persistence.nextTxnId(),
            REALM,
            CATALOG_ID,
            scn - 1, // read_point < commit_scn → not visible
            now,
            now.plusSeconds(60),
            null);

    Optional<JdbcTransactionPersistence.CommittedTableSnapshotRow> resolved =
        persistence.resolveTableSnapshot("uuid-tbl-above", snap);
    assertThat(resolved).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // locks
  // ---------------------------------------------------------------------------

  @Test
  void insertLockAndGrantThenFind() throws SQLException {
    long txnId = persistence.nextTxnId();
    insertSimpleTransaction(txnId);

    long lockId = persistence.insertLock(txnId, "resource-A", "ROW_EXCLUSIVE");
    assertThat(lockId).isGreaterThan(0);

    persistence.grantLock(lockId);

    List<JdbcTransactionPersistence.LockRow> granted =
        persistence.findGrantedLocksOnResource("resource-A", -1L);
    assertThat(granted).hasSize(1);
    assertThat(granted.get(0).lockId()).isEqualTo(lockId);
    assertThat(granted.get(0).status()).isEqualTo(JdbcTransactionPersistence.LOCK_STATUS_GRANTED);
  }

  @Test
  void findGrantedLocksExcludesGivenTxnId() throws SQLException {
    long txn1 = persistence.nextTxnId();
    long txn2 = persistence.nextTxnId();
    insertSimpleTransaction(txn1);
    insertSimpleTransaction(txn2);

    long lock1 = persistence.insertLock(txn1, "resource-B", "ROW_EXCLUSIVE");
    long lock2 = persistence.insertLock(txn2, "resource-B", "ROW_EXCLUSIVE");
    persistence.grantLock(lock1);
    persistence.grantLock(lock2);

    // Exclude txn1's lock
    List<JdbcTransactionPersistence.LockRow> result =
        persistence.findGrantedLocksOnResource("resource-B", txn1);
    assertThat(result).hasSize(1);
    assertThat(result.get(0).txnId()).isEqualTo(txn2);
  }

  @Test
  void deleteLocksForTransactionRemovesAllLocks() throws SQLException {
    long txnId = persistence.nextTxnId();
    insertSimpleTransaction(txnId);

    persistence.insertLock(txnId, "resource-C", "ROW_EXCLUSIVE");
    persistence.insertLock(txnId, "resource-D", "EXCLUSIVE");

    persistence.deleteLocksForTransaction(txnId);

    assertThat(persistence.findGrantedLocksOnResource("resource-C", -1L)).isEmpty();
    assertThat(persistence.findGrantedLocksOnResource("resource-D", -1L)).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // txn_watermark
  // ---------------------------------------------------------------------------

  @Test
  void upsertAndGetWatermark() throws SQLException {
    persistence.upsertWatermark(100L);
    assertThat(persistence.getWatermark()).isEqualTo(100L);

    // Upsert again with a higher value
    persistence.upsertWatermark(200L);
    assertThat(persistence.getWatermark()).isEqualTo(200L);
  }

  @Test
  void getWatermarkReturnsZeroWhenNoRow() throws SQLException {
    assertThat(persistence.getWatermark()).isEqualTo(0L);
  }

  @Test
  void purgeCommittedSnapshotsBelowWatermarkKeepsPerTableSurvivor() throws SQLException {
    // Three versions of the same table at scn 10, 20, 30, plus another table's row at 15.
    insertCtsWithScn("uuid-purge", 10L, "s3://v10.json");
    insertCtsWithScn("uuid-purge", 20L, "s3://v20.json");
    insertCtsWithScn("uuid-purge", 30L, "s3://v30.json");
    insertCtsWithScn("uuid-other", 15L, "s3://other15.json");

    // Watermark = 25. For uuid-purge, the newest row <= 25 is scn=20 (the survivor); scn=10 is
    // strictly older and must be deleted; scn=30 is above the watermark and must be kept. For
    // uuid-other, the only row (15) is the survivor and is kept.
    persistence.purgeCommittedSnapshotsBelowWatermark(25L);

    // A snapshot pinned exactly at the watermark still resolves uuid-purge (to the survivor, v20)
    // and uuid-other (v15).
    Instant now = Instant.now();
    JdbcTransactionPersistence.TxnSnapshotRow atWatermark =
        new JdbcTransactionPersistence.TxnSnapshotRow(
            persistence.nextTxnId(), REALM, CATALOG_ID, 25L, now, now.plusSeconds(60), null);
    assertThat(persistence.resolveTableSnapshot("uuid-purge", atWatermark))
        .hasValueSatisfying(r -> assertThat(r.metadataLocation()).isEqualTo("s3://v20.json"));
    assertThat(persistence.resolveTableSnapshot("uuid-other", atWatermark))
        .hasValueSatisfying(r -> assertThat(r.metadataLocation()).isEqualTo("s3://other15.json"));

    // A snapshot above everything sees the latest uuid-purge (v30, kept because > watermark).
    JdbcTransactionPersistence.TxnSnapshotRow above =
        new JdbcTransactionPersistence.TxnSnapshotRow(
            persistence.nextTxnId(), REALM, CATALOG_ID, 40L, now, now.plusSeconds(60), null);
    assertThat(persistence.resolveTableSnapshot("uuid-purge", above))
        .hasValueSatisfying(r -> assertThat(r.metadataLocation()).isEqualTo("s3://v30.json"));

    // The purged scn=10 row is gone: a snapshot at read_point=10 no longer resolves it.
    JdbcTransactionPersistence.TxnSnapshotRow atTen =
        new JdbcTransactionPersistence.TxnSnapshotRow(
            persistence.nextTxnId(), REALM, CATALOG_ID, 10L, now, now.plusSeconds(60), null);
    assertThat(persistence.resolveTableSnapshot("uuid-purge", atTen)).isEmpty();
  }

  /** Inserts a committed_table_snapshots row with an explicit commit_scn (via the primitive). */
  private void insertCtsWithScn(String tableUuid, long scn, String metadataLocation)
      throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      persistence.insertCommittedTableSnapshot(
          conn,
          new JdbcTransactionPersistence.CommittedTableSnapshotRow(
              tableUuid, scn, null, metadataLocation));
      conn.commit();
    }
  }

  // ---------------------------------------------------------------------------
  // insertCommittedTableSnapshot on Connection (called within a JDBC transaction)
  // ---------------------------------------------------------------------------

  @Test
  void insertCommittedTableSnapshotOnConnectionWithinTransaction() throws SQLException {
    long txnId = persistence.nextTxnId();

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      persistence.insertCommittedTableSnapshot(
          conn,
          new JdbcTransactionPersistence.CommittedTableSnapshotRow(
              "uuid-conn-test", txnId, 777L, "s3://conn-test.json"));
      conn.commit();
    }

    Instant now = Instant.now();
    JdbcTransactionPersistence.TxnSnapshotRow snap =
        new JdbcTransactionPersistence.TxnSnapshotRow(
            persistence.nextTxnId(),
            REALM,
            CATALOG_ID,
            txnId + 5, // read_point above the committed txn
            now,
            now.plusSeconds(60),
            null);
    Optional<JdbcTransactionPersistence.CommittedTableSnapshotRow> resolved =
        persistence.resolveTableSnapshot("uuid-conn-test", snap);
    assertThat(resolved).isPresent();
    assertThat(resolved.get().icebergSnapshotId()).isEqualTo(777L);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private void insertSimpleTransaction(long txnId) throws SQLException {
    Instant now = Instant.now();
    persistence.insertTransaction(
        new JdbcTransactionPersistence.TransactionRow(
            txnId,
            null,
            REALM,
            CATALOG_ID,
            "tok-" + txnId,
            "test-user",
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            "REPEATABLE_READ",
            null,
            now,
            now.plusSeconds(300),
            null));
  }

  private void insertSimpleOperation(long opId, long txnId) throws SQLException {
    Instant now = Instant.now();
    persistence.insertOperation(
        new JdbcTransactionPersistence.OperationRow(
            opId,
            txnId,
            REALM,
            CATALOG_ID,
            "table-uuid-sub",
            "ROW_EXCLUSIVE",
            null,
            null,
            "s3://staging/" + opId,
            null,
            "subtok-" + opId,
            JdbcTransactionPersistence.OP_STATUS_IN_PROGRESS,
            null,
            now,
            now.plusSeconds(60),
            null));
  }

  private static class SimpleJdbcConfig implements RelationalJdbcConfiguration {
    @Override
    public java.util.Optional<Integer> maxRetries() {
      return java.util.Optional.of(1);
    }

    @Override
    public java.util.Optional<Long> maxDurationInMs() {
      return java.util.Optional.of(100L);
    }

    @Override
    public java.util.Optional<Long> initialDelayInMs() {
      return java.util.Optional.of(10L);
    }

    @Override
    public java.util.Optional<String> databaseType() {
      return java.util.Optional.empty();
    }
  }
}
