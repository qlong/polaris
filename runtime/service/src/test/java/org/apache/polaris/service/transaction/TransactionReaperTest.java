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
package org.apache.polaris.service.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.CommittedTableSnapshotRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.LockRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.TransactionRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.TxnSnapshotRow;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the reaper logic: expired transactions are aborted and their locks
 * released.
 *
 * <p>These tests exercise the same paths that {@link TransactionReaper} calls ( {@link
 * JdbcTransactionPersistence#findExpiredTransactionIdsForRealm}, {@link
 * JdbcTransactionPersistence#updateTransactionStatus}, {@link LockManager#releaseAll}) directly,
 * without CDI wiring.
 */
class TransactionReaperTest {

  private static final String REALM = "reaper-realm";
  private static final long CATALOG_ID = 88L;
  private static final String RESOURCE = "table-uuid-reaper";
  private static final String OWNER = "reaper-principal";

  private JdbcTransactionPersistence persistence;
  private LockManager lockManager;
  private DataSource dataSource;
  private DatasourceOperations ops;

  @BeforeEach
  void setUp() throws Exception {
    String jdbcUrl =
        "jdbc:h2:mem:reaper_integ_"
            + UUID.randomUUID()
            + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_UPPER=false";
    dataSource = JdbcConnectionPool.create(jdbcUrl, "sa", "");

    ops =
        new DatasourceOperations(
            dataSource,
            new RelationalJdbcConfiguration() {
              @Override
              public Optional<Integer> maxRetries() {
                return Optional.of(1);
              }

              @Override
              public Optional<Long> maxDurationInMs() {
                return Optional.of(5000L);
              }

              @Override
              public Optional<Long> initialDelayInMs() {
                return Optional.empty();
              }

              @Override
              public Optional<String> databaseType() {
                return Optional.of("h2");
              }
            });
    InputStream schema =
        DatasourceOperations.class.getClassLoader().getResourceAsStream("h2/schema-v5.sql");
    ops.executeScript(schema);

    persistence = new JdbcTransactionPersistence(dataSource, DatabaseType.H2, REALM, CATALOG_ID);
    lockManager = new LockManager(persistence);
  }

  /** Inserts a transaction that has already expired (expiresAt in the past). */
  private TransactionRow insertExpiredTransaction() throws Exception {
    long txnId = persistence.nextTxnId();
    Instant now = Instant.now();
    TransactionRow row =
        new TransactionRow(
            txnId,
            null, // parentTxnId
            REALM,
            CATALOG_ID,
            "token-" + txnId,
            OWNER,
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            "REPEATABLE_READ",
            null, // txnSnapshotId
            now,
            now.minus(1, ChronoUnit.MINUTES), // already expired
            null);
    persistence.insertTransaction(row);
    return row;
  }

  @Test
  void expiredTransactionIsFoundByReaper() throws Exception {
    TransactionRow txn = insertExpiredTransaction();

    List<Long> expired = persistence.findExpiredTransactionIdsForRealm();

    assertThat(expired).contains(txn.txnId());
  }

  @Test
  void nonExpiredTransactionIsNotFoundByReaper() throws Exception {
    long txnId = persistence.nextTxnId();
    Instant now = Instant.now();
    TransactionRow row =
        new TransactionRow(
            txnId,
            null, // parentTxnId
            REALM,
            CATALOG_ID,
            "token-" + txnId,
            OWNER,
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            "REPEATABLE_READ",
            null, // txnSnapshotId
            now,
            now.plus(1, ChronoUnit.HOURS), // not yet expired
            null);
    persistence.insertTransaction(row);

    List<Long> expired = persistence.findExpiredTransactionIdsForRealm();

    assertThat(expired).doesNotContain(txnId);
  }

  @Test
  void reaperAbortsExpiredTransactionAndReleasesLocks() throws Exception {
    TransactionRow txn = insertExpiredTransaction();
    // Give the expired transaction a lock
    lockManager.acquire(txn.txnId(), RESOURCE, LockManager.MODE_EXCLUSIVE);

    // Simulate what TransactionReaper.reapTransaction does
    List<Long> expired = persistence.findExpiredTransactionIdsForRealm();
    assertThat(expired).contains(txn.txnId());

    for (long id : expired) {
      persistence.updateTransactionStatus(id, JdbcTransactionPersistence.TXN_STATUS_ABORTED);
      lockManager.releaseAll(id);
    }

    // Transaction is now ABORTED
    TransactionRow aborted = persistence.findTransaction(txn.txnId()).orElseThrow();
    assertThat(aborted.status()).isEqualTo(JdbcTransactionPersistence.TXN_STATUS_ABORTED);

    // Lock is released
    List<LockRow> remaining = persistence.findGrantedLocksOnResource(RESOURCE, -1L);
    assertThat(remaining).isEmpty();
  }

  @Test
  void reaperSkipsAlreadyAbortedTransaction() throws Exception {
    TransactionRow txn = insertExpiredTransaction();
    persistence.updateTransactionStatus(txn.txnId(), JdbcTransactionPersistence.TXN_STATUS_ABORTED);

    // Already aborted — reaper should not find it in the expired list again
    List<Long> expired = persistence.findExpiredTransactionIdsForRealm();
    assertThat(expired).doesNotContain(txn.txnId());
  }

  @Test
  void reaperReleasesLocksForExpiredTransactionEnablingNewAcquire() throws Exception {
    TransactionRow expiredTxn = insertExpiredTransaction();
    lockManager.acquire(expiredTxn.txnId(), RESOURCE, LockManager.MODE_EXCLUSIVE);

    // Reap the expired transaction
    persistence.updateTransactionStatus(
        expiredTxn.txnId(), JdbcTransactionPersistence.TXN_STATUS_ABORTED);
    lockManager.releaseAll(expiredTxn.txnId());

    // A new transaction should now be able to acquire the same resource
    long newTxnId = persistence.nextTxnId();
    long lockId = lockManager.acquire(newTxnId, RESOURCE, LockManager.MODE_EXCLUSIVE);
    assertThat(lockId).isGreaterThan(0);
  }

  // ---------------------------------------------------------------------------
  // runOnce(): end-to-end reap + per-catalog watermark advance and survivor purge
  // ---------------------------------------------------------------------------

  /**
   * Exercises {@link TransactionReaper#runOnce()} through its real orchestration — catalog
   * discovery, per-catalog watermark {@code W = MIN(write_floor, read_floor)}, watermark upsert,
   * and per-table survivor purge — plus the expired-transaction reap, all in one pass.
   *
   * <p>Standalone read snapshots are now opaque handles that persist <em>no</em> {@code
   * txn_snapshots} row, so a catalog with only standalone readers has {@code write_floor = -1}
   * (treated as +infinity) and its horizon is driven entirely by {@code read_floor}: the highest
   * {@code commit_scn} whose {@code committed_at} is older than the lease+skew, i.e. past the point
   * where any live read handle could still reference it. This test ages the older commits past that
   * horizon and leaves the newest fresh.
   *
   * <p>Setup, all in realm {@link #REALM}:
   *
   * <ul>
   *   <li><b>Catalog A</b> ({@link #CATALOG_ID}): table t1 committed at commit_scn 1, 2, 3; scn 1
   *       and 2 aged past the lease horizon, scn 3 left fresh. read_floor = 2 → keep scn 2
   *       (survivor) and 3 (above, still protected), delete scn 1.
   *   <li><b>Catalog B</b> ({@link #CATALOG_ID_B}): table t2 committed at 1, 2; both aged past the
   *       horizon. read_floor = 2 → keep scn 2 (survivor), delete scn 1.
   *   <li>One expired IN_PROGRESS transaction in catalog A holding an EXCLUSIVE lock, to confirm
   *       the reap path runs in the same pass.
   * </ul>
   */
  @Test
  void runOnceReapsExpiredAndPurgesPerCatalogWithSurvivor() throws Exception {
    JdbcTransactionPersistence persistenceB =
        new JdbcTransactionPersistence(dataSource, DatabaseType.H2, REALM, CATALOG_ID_B);

    // Catalog A: t1 @1, @2, @3.
    commitTable(persistence, "t1", "s3://a/t1-v1.json"); // scn 1
    commitTable(persistence, "t1", "s3://a/t1-v2.json"); // scn 2
    commitTable(persistence, "t1", "s3://a/t1-v3.json"); // scn 3

    // Catalog B: t2 @1, @2.
    commitTable(persistenceB, "t2", "s3://b/t2-v1.json"); // scn 1
    commitTable(persistenceB, "t2", "s3://b/t2-v2.json"); // scn 2

    // Age scn 1 and 2 of both catalogs past the lease+skew horizon so read_floor = 2 for each;
    // leave
    // catalog A's scn 3 fresh (within the lease) so it stays above the horizon and is never purged.
    long pastHorizonSeconds =
        TransactionReaper.SNAPSHOT_LEASE_SECONDS + TransactionReaper.SKEW_PAD_SECONDS + 60;
    ageCommittedRow(CATALOG_ID, 1L, pastHorizonSeconds);
    ageCommittedRow(CATALOG_ID, 2L, pastHorizonSeconds);
    ageCommittedRow(CATALOG_ID_B, 1L, pastHorizonSeconds);
    ageCommittedRow(CATALOG_ID_B, 2L, pastHorizonSeconds);

    // An expired transaction in catalog A holding a lock, to verify reap runs too.
    TransactionRow expired = insertExpiredTransaction();
    lockManager.acquire(expired.txnId(), RESOURCE, LockManager.MODE_EXCLUSIVE);

    // Run the reaper end-to-end.
    TransactionReaper reaper = newReaper();
    int reaped = reaper.runOnce();

    // --- Reap path ---
    assertThat(reaped).as("the one expired transaction is reaped").isEqualTo(1);
    assertThat(persistence.findTransaction(expired.txnId()).orElseThrow().status())
        .isEqualTo(JdbcTransactionPersistence.TXN_STATUS_ABORTED);
    assertThat(persistence.findGrantedLocksOnResource(RESOURCE, -1L)).isEmpty();

    // --- Catalog A watermark + survivor purge (read_floor = 2; scn 3 fresh, above the horizon) ---
    assertThat(persistence.getWatermark())
        .as("catalog A watermark = read_floor over aged commits")
        .isEqualTo(2L);
    // scn 1 purged (strictly below the per-table survivor at scn 2)
    assertThat(persistence.resolveTableSnapshot("t1", syntheticSnapshot(CATALOG_ID, 1L))).isEmpty();
    // scn 2 survives (newest <= watermark) — a reader pinned at the watermark still resolves t1
    assertThat(persistence.resolveTableSnapshot("t1", syntheticSnapshot(CATALOG_ID, 2L)))
        .hasValueSatisfying(r -> assertThat(r.metadataLocation()).isEqualTo("s3://a/t1-v2.json"));
    // scn 3 (above watermark) is kept
    assertThat(persistence.resolveTableSnapshot("t1", syntheticSnapshot(CATALOG_ID, 3L)))
        .hasValueSatisfying(r -> assertThat(r.metadataLocation()).isEqualTo("s3://a/t1-v3.json"));

    // --- Catalog B watermark + survivor purge (no live write snapshot → watermark = read_floor =
    // 2) ---
    assertThat(persistenceB.getWatermark())
        .as("catalog B watermark = read_floor over aged commits")
        .isEqualTo(2L);
    assertThat(persistenceB.resolveTableSnapshot("t2", syntheticSnapshot(CATALOG_ID_B, 1L)))
        .isEmpty();
    assertThat(persistenceB.resolveTableSnapshot("t2", syntheticSnapshot(CATALOG_ID_B, 2L)))
        .hasValueSatisfying(r -> assertThat(r.metadataLocation()).isEqualTo("s3://b/t2-v2.json"));
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static final long CATALOG_ID_B = 89L;

  /** Builds a reaper wired to this test's datasource/ops for {@link #REALM}. */
  @SuppressWarnings("unchecked")
  private TransactionReaper newReaper() {
    Instance<DataSource> dsInstance = mock(Instance.class);
    when(dsInstance.isUnsatisfied()).thenReturn(false);
    when(dsInstance.get()).thenReturn(dataSource);
    return new TransactionReaper(dsInstance, ops, REALM, true);
  }

  /**
   * Commits one table version: allocates a fresh commit_scn (advancing the per-catalog counter) and
   * inserts the CTS row in the same transaction, mirroring a real commit.
   */
  private static void commitTable(
      JdbcTransactionPersistence p, String tableUuid, String metadataLocation) throws Exception {
    p.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(
            tableUuid, JdbcTransactionPersistence.UNASSIGNED_SCN, null, metadataLocation));
  }

  /** A synthetic (non-persisted) snapshot handle pinned at the given read_point for resolution. */
  private static TxnSnapshotRow syntheticSnapshot(long catalogId, long readPoint) {
    Instant now = Instant.now();
    return new TxnSnapshotRow(
        1L, REALM, catalogId, readPoint, now, now.plus(1, ChronoUnit.HOURS), null);
  }

  /**
   * Backdates a committed_table_snapshots row's {@code committed_at} by {@code seconds} so it falls
   * past the reaper's lease+skew horizon, making {@code read_floor} include it.
   */
  private void ageCommittedRow(long catalogId, long commitScn, long seconds) throws Exception {
    String sql =
        "UPDATE POLARIS_SCHEMA.committed_table_snapshots "
            + "SET committed_at = DATEADD('SECOND', -?, now()) "
            + "WHERE realm_id = ? AND catalog_id = ? AND commit_scn = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, seconds);
      ps.setString(2, REALM);
      ps.setLong(3, catalogId);
      ps.setLong(4, commitScn);
      ps.executeUpdate();
    }
  }
}
