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
            REALM,
            CATALOG_ID,
            "tok-" + txnId,
            "alice",
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            "REPEATABLE_READ",
            null,
            null,
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
            REALM,
            CATALOG_ID,
            "tok-" + expiredId,
            "bob",
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            "REPEATABLE_READ",
            null,
            null,
            null,
            Instant.now().minusSeconds(600),
            Instant.now().minusSeconds(1), // already expired
            null);
    persistence.insertTransaction(expired);

    // Insert one with a future expiry
    JdbcTransactionPersistence.TransactionRow active =
        new JdbcTransactionPersistence.TransactionRow(
            activeId,
            REALM,
            CATALOG_ID,
            "tok-" + activeId,
            "carol",
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            "REPEATABLE_READ",
            null,
            null,
            null,
            Instant.now(),
            Instant.now().plusSeconds(300),
            null);
    persistence.insertTransaction(active);

    List<Long> expired1 = persistence.findExpiredTransactionIds();
    assertThat(expired1).contains(expiredId).doesNotContain(activeId);
  }

  // ---------------------------------------------------------------------------
  // subtransactions
  // ---------------------------------------------------------------------------

  @Test
  void insertAndFindSubtransaction() throws SQLException {
    long txnId = persistence.nextTxnId();
    insertSimpleTransaction(txnId);

    long subtxnId = persistence.nextTxnId();
    Instant now = Instant.now();
    JdbcTransactionPersistence.SubtransactionRow sub =
        new JdbcTransactionPersistence.SubtransactionRow(
            subtxnId,
            txnId,
            REALM,
            CATALOG_ID,
            "table-uuid-1",
            "APPEND",
            null,
            "s3://staging/path",
            null,
            "subtok-" + subtxnId,
            JdbcTransactionPersistence.SUBTXN_STATUS_IN_PROGRESS,
            null,
            now,
            now.plusSeconds(60),
            null);

    persistence.insertSubtransaction(sub);

    Optional<JdbcTransactionPersistence.SubtransactionRow> found =
        persistence.findSubtransaction(subtxnId);
    assertThat(found).isPresent();
    JdbcTransactionPersistence.SubtransactionRow r = found.get();
    assertThat(r.subtxnId()).isEqualTo(subtxnId);
    assertThat(r.parentTxnId()).isEqualTo(txnId);
    assertThat(r.tableUuid()).isEqualTo("table-uuid-1");
    assertThat(r.operation()).isEqualTo("APPEND");
    assertThat(r.assignedStagingPath()).isEqualTo("s3://staging/path");
    assertThat(r.stagedMetadataLocation()).isNull();
    assertThat(r.status()).isEqualTo(JdbcTransactionPersistence.SUBTXN_STATUS_IN_PROGRESS);
  }

  @Test
  void findSubtransactionsForTransactionReturnsAllChildren() throws SQLException {
    long txnId = persistence.nextTxnId();
    insertSimpleTransaction(txnId);

    long sub1 = persistence.nextTxnId();
    long sub2 = persistence.nextTxnId();
    insertSimpleSubtransaction(sub1, txnId);
    insertSimpleSubtransaction(sub2, txnId);

    List<JdbcTransactionPersistence.SubtransactionRow> rows =
        persistence.findSubtransactionsForTransaction(txnId);
    assertThat(rows).hasSize(2);
    assertThat(rows.stream().map(JdbcTransactionPersistence.SubtransactionRow::subtxnId))
        .containsExactlyInAnyOrder(sub1, sub2);
  }

  @Test
  void markSubtransactionCommittedUpdatesStatus() throws SQLException {
    long txnId = persistence.nextTxnId();
    insertSimpleTransaction(txnId);
    long subtxnId = persistence.nextTxnId();
    insertSimpleSubtransaction(subtxnId, txnId);

    persistence.markSubtransactionCommitted(subtxnId, "s3://staging/metadata.json");

    Optional<JdbcTransactionPersistence.SubtransactionRow> found =
        persistence.findSubtransaction(subtxnId);
    assertThat(found).isPresent();
    assertThat(found.get().status())
        .isEqualTo(JdbcTransactionPersistence.SUBTXN_STATUS_SUB_COMMITTED);
    assertThat(found.get().stagedMetadataLocation()).isEqualTo("s3://staging/metadata.json");
    assertThat(found.get().endedAt()).isNotNull();
  }

  @Test
  void markSubtransactionAbortedUpdatesStatus() throws SQLException {
    long txnId = persistence.nextTxnId();
    insertSimpleTransaction(txnId);
    long subtxnId = persistence.nextTxnId();
    insertSimpleSubtransaction(subtxnId, txnId);

    persistence.markSubtransactionAborted(subtxnId);

    Optional<JdbcTransactionPersistence.SubtransactionRow> found =
        persistence.findSubtransaction(subtxnId);
    assertThat(found).isPresent();
    assertThat(found.get().status())
        .isEqualTo(JdbcTransactionPersistence.SUBTXN_STATUS_SUB_ABORTED);
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
            snapId, REALM, CATALOG_ID, 10L, 20L, "11,12", now, now.plusSeconds(60), null);

    persistence.insertTxnSnapshot(snap);

    Optional<JdbcTransactionPersistence.TxnSnapshotRow> found = persistence.findTxnSnapshot(snapId);
    assertThat(found).isPresent();
    JdbcTransactionPersistence.TxnSnapshotRow r = found.get();
    assertThat(r.txnSnapshotId()).isEqualTo(snapId);
    assertThat(r.xmin()).isEqualTo(10L);
    assertThat(r.xmax()).isEqualTo(20L);
    assertThat(r.xipList()).isEqualTo("11,12");
    assertThat(r.releasedAt()).isNull();
  }

  @Test
  void releaseTxnSnapshotSetsReleasedAt() throws SQLException {
    long snapId = persistence.nextTxnId();
    Instant now = Instant.now();
    persistence.insertTxnSnapshot(
        new JdbcTransactionPersistence.TxnSnapshotRow(
            snapId, REALM, CATALOG_ID, 1L, 2L, "", now, now.plusSeconds(60), null));

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
            snapId, REALM, CATALOG_ID, 1L, 2L, "", now, now.plusSeconds(60), null));

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
    long txnId = persistence.nextTxnId();
    JdbcTransactionPersistence.CommittedTableSnapshotRow cts =
        new JdbcTransactionPersistence.CommittedTableSnapshotRow(
            "uuid-tbl-1", txnId, 555L, "s3://bucket/metadata.json");
    persistence.insertCommittedTableSnapshotRow(cts);

    Instant now = Instant.now();
    JdbcTransactionPersistence.TxnSnapshotRow snap =
        new JdbcTransactionPersistence.TxnSnapshotRow(
            persistence.nextTxnId(),
            REALM,
            CATALOG_ID,
            txnId, // xmin
            txnId + 10, // xmax
            "", // no xip
            now,
            now.plusSeconds(60),
            null);

    Optional<JdbcTransactionPersistence.CommittedTableSnapshotRow> resolved =
        persistence.resolveTableSnapshot("uuid-tbl-1", snap);
    assertThat(resolved).isPresent();
    assertThat(resolved.get().tableUuid()).isEqualTo("uuid-tbl-1");
    assertThat(resolved.get().committedTxnId()).isEqualTo(txnId);
    assertThat(resolved.get().metadataLocation()).isEqualTo("s3://bucket/metadata.json");
  }

  @Test
  void resolveTableSnapshotRespectsXipList() throws SQLException {
    long txnId = persistence.nextTxnId();
    JdbcTransactionPersistence.CommittedTableSnapshotRow cts =
        new JdbcTransactionPersistence.CommittedTableSnapshotRow(
            "uuid-tbl-xip", txnId, null, "s3://bucket/v1.json");
    persistence.insertCommittedTableSnapshotRow(cts);

    // Snapshot that has txnId in the xip_list (in-progress, not visible)
    Instant now = Instant.now();
    JdbcTransactionPersistence.TxnSnapshotRow snap =
        new JdbcTransactionPersistence.TxnSnapshotRow(
            persistence.nextTxnId(),
            REALM,
            CATALOG_ID,
            txnId, // xmin
            txnId + 10, // xmax
            String.valueOf(txnId), // txnId is in xip → not visible
            now,
            now.plusSeconds(60),
            null);

    Optional<JdbcTransactionPersistence.CommittedTableSnapshotRow> resolved =
        persistence.resolveTableSnapshot("uuid-tbl-xip", snap);
    assertThat(resolved).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // locks
  // ---------------------------------------------------------------------------

  @Test
  void insertLockAndGrantThenFind() throws SQLException {
    long txnId = persistence.nextTxnId();
    insertSimpleTransaction(txnId);

    long lockId = persistence.insertLock(txnId, "resource-A", "APPEND");
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

    long lock1 = persistence.insertLock(txn1, "resource-B", "APPEND");
    long lock2 = persistence.insertLock(txn2, "resource-B", "APPEND");
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

    persistence.insertLock(txnId, "resource-C", "APPEND");
    persistence.insertLock(txnId, "resource-D", "OVERWRITE");

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
  void purgeCommittedSnapshotsBelowWatermarkDeletesOldRows() throws SQLException {
    long old = persistence.nextTxnId();
    long recent = persistence.nextTxnId();

    persistence.insertCommittedTableSnapshotRow(
        new JdbcTransactionPersistence.CommittedTableSnapshotRow(
            "uuid-purge", old, null, "s3://old.json"));
    persistence.insertCommittedTableSnapshotRow(
        new JdbcTransactionPersistence.CommittedTableSnapshotRow(
            "uuid-purge", recent, null, "s3://recent.json"));

    persistence.upsertWatermark(old);
    // Purge everything below old (exclusive), so only `old` row remains
    persistence.purgeCommittedSnapshotsBelowWatermark(old);

    // recent > old should still be resolvable; old should too since purge is < watermark
    Instant now = Instant.now();
    JdbcTransactionPersistence.TxnSnapshotRow snapForOld =
        new JdbcTransactionPersistence.TxnSnapshotRow(
            persistence.nextTxnId(),
            REALM,
            CATALOG_ID,
            old,
            recent + 1,
            "",
            now,
            now.plusSeconds(60),
            null);
    assertThat(persistence.resolveTableSnapshot("uuid-purge", snapForOld)).isPresent();
  }

  // ---------------------------------------------------------------------------
  // parseXipList helper
  // ---------------------------------------------------------------------------

  @Test
  void parseXipListHandlesNullAndEmpty() {
    assertThat(JdbcTransactionPersistence.parseXipList(null)).isEmpty();
    assertThat(JdbcTransactionPersistence.parseXipList("")).isEmpty();
    assertThat(JdbcTransactionPersistence.parseXipList("  ")).isEmpty();
  }

  @Test
  void parseXipListParsesCommaSeparatedIds() {
    List<Long> ids = JdbcTransactionPersistence.parseXipList("10,20,30");
    assertThat(ids).containsExactly(10L, 20L, 30L);
  }

  @Test
  void parseXipListHandlesWhitespace() {
    List<Long> ids = JdbcTransactionPersistence.parseXipList(" 5 , 6 , 7 ");
    assertThat(ids).containsExactly(5L, 6L, 7L);
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
            txnId,
            txnId + 5,
            "",
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
            REALM,
            CATALOG_ID,
            "tok-" + txnId,
            "test-user",
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            "REPEATABLE_READ",
            null,
            null,
            null,
            now,
            now.plusSeconds(300),
            null));
  }

  private void insertSimpleSubtransaction(long subtxnId, long parentTxnId) throws SQLException {
    Instant now = Instant.now();
    persistence.insertSubtransaction(
        new JdbcTransactionPersistence.SubtransactionRow(
            subtxnId,
            parentTxnId,
            REALM,
            CATALOG_ID,
            "table-uuid-sub",
            "APPEND",
            null,
            "s3://staging/" + subtxnId,
            null,
            "subtok-" + subtxnId,
            JdbcTransactionPersistence.SUBTXN_STATUS_IN_PROGRESS,
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
