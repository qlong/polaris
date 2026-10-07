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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.CommittedTableSnapshotRow;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the Phase 4 conflict-detection logic in {@link TransactionService#prepareCommit}.
 *
 * <p>Three scenarios exercised:
 *
 * <ol>
 *   <li><b>No conflict</b> — operation's {@code server_base_metadata_location} matches the current
 *       committed state → operation routed to {@code direct}.
 *   <li><b>ROW_EXCLUSIVE conflict → merge-retry</b> — a concurrent writer advanced the table
 *       between operation creation and {@code prepareCommit}; lock mode is ROW_EXCLUSIVE →
 *       operation routed to {@code mergeRetry}.
 *   <li><b>EXCLUSIVE conflict → SerializationFailure</b> — same concurrent-write scenario but lock
 *       mode is EXCLUSIVE or ACCESS_EXCLUSIVE → {@link
 *       TransactionService.SerializationFailureException} is thrown and transaction is ABORTED.
 * </ol>
 */
class TransactionConflictTest {

  private static final long CATALOG_ID = 42L;
  private static final String OWNER = "test-principal";
  private static final String TABLE_META_DIR = "s3://bucket/ns/orders/metadata";
  private static final String TABLE_UUID = "ns.orders";

  private JdbcTransactionPersistence persistence;
  private TransactionService service;

  @BeforeEach
  void setUp() throws Exception {
    String jdbcUrl =
        "jdbc:h2:mem:conflict_test_"
            + UUID.randomUUID()
            + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_UPPER=false";
    DataSource dataSource = JdbcConnectionPool.create(jdbcUrl, "sa", "");

    DatasourceOperations ops =
        new DatasourceOperations(
            dataSource,
            new RelationalJdbcConfiguration() {
              @Override
              public java.util.Optional<Integer> maxRetries() {
                return java.util.Optional.of(1);
              }

              @Override
              public java.util.Optional<Long> maxDurationInMs() {
                return java.util.Optional.of(5000L);
              }

              @Override
              public java.util.Optional<Long> initialDelayInMs() {
                return java.util.Optional.empty();
              }

              @Override
              public java.util.Optional<String> databaseType() {
                return java.util.Optional.of("h2");
              }
            });
    InputStream schema =
        DatasourceOperations.class.getClassLoader().getResourceAsStream("h2/schema-v5.sql");
    ops.executeScript(schema);

    persistence =
        new JdbcTransactionPersistence(dataSource, DatabaseType.H2, "realm-test", CATALOG_ID);
    service = new TransactionService(persistence, new LockManager(persistence));
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Records a committed_table_snapshots row, simulating a prior writer. */
  private void commitTableVersion(String tableUuid, String metadataLocation) throws Exception {
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(tableUuid, persistence.nextTxnId(), null, metadataLocation));
  }

  /**
   * Creates an operation for {@code TABLE_UUID} and marks it COMMITTED with the staged path. The
   * {@code server_base_metadata_location} is set by {@code createOperation} at creation time — so
   * call this BEFORE simulating concurrent commits.
   */
  private long beginOpAndStage(long txnId, String txnToken, String lockMode) throws Exception {
    var op =
        service.createOperation(
            txnId, txnToken, TABLE_UUID, lockMode, null, null, 3600, TABLE_META_DIR);
    service.commitOperation(txnId, op.opId(), op.opToken(), op.assignedStagingPath());
    return op.opId();
  }

  // ---------------------------------------------------------------------------
  // 1. No conflict: server_base == current committed → routed to direct
  // ---------------------------------------------------------------------------

  @Test
  void noConflict_routesToDirect() throws Exception {
    // Establish a committed version that the operation will see as its base
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    // createOperation captures v1.json as server_base; no further commits happen
    beginOpAndStage(txn.txnId(), txn.txnToken(), "ROW_EXCLUSIVE");

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());

    assertThat(result.direct()).hasSize(1);
    assertThat(result.mergeRetry()).isEmpty();
    assertThat(result.direct().get(0).tableUuid()).isEqualTo(TABLE_UUID);
  }

  @Test
  void noConflict_nullServerBase_alwaysRoutesToDirect() throws Exception {
    // Table has never been committed — server_base will be null; no conflict possible
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    beginOpAndStage(txn.txnId(), txn.txnToken(), "EXCLUSIVE");

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());

    assertThat(result.direct()).hasSize(1);
    assertThat(result.mergeRetry()).isEmpty();
  }

  @Test
  void multiTable_noConflict_allRoutedToDirect() throws Exception {
    String tableA = "ns.tableA";
    String tableB = "ns.tableB";
    commitTableVersion(tableA, "s3://bucket/a/v1.json");
    commitTableVersion(tableB, "s3://bucket/b/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);

    // Create and stage operations for both tables before any new commit
    var subA =
        service.createOperation(
            txn.txnId(),
            txn.txnToken(),
            tableA,
            "ROW_EXCLUSIVE",
            null,
            null,
            3600,
            "s3://bucket/a/metadata");
    service.commitOperation(txn.txnId(), subA.opId(), subA.opToken(), subA.assignedStagingPath());

    var subB =
        service.createOperation(
            txn.txnId(),
            txn.txnToken(),
            tableB,
            "ROW_EXCLUSIVE",
            null,
            null,
            3600,
            "s3://bucket/b/metadata");
    service.commitOperation(txn.txnId(), subB.opId(), subB.opToken(), subB.assignedStagingPath());

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());

    assertThat(result.direct()).hasSize(2);
    assertThat(result.mergeRetry()).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // 2. ROW_EXCLUSIVE conflict → mergeRetry
  // ---------------------------------------------------------------------------

  @Test
  void rowExclusiveConflict_routesToMergeRetry() throws Exception {
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    // Operation created while v1 is current (server_base = v1)
    beginOpAndStage(txn.txnId(), txn.txnToken(), "ROW_EXCLUSIVE");

    // Concurrent writer commits v2 between operation creation and prepareCommit
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());

    assertThat(result.mergeRetry()).hasSize(1);
    assertThat(result.direct()).isEmpty();
    assertThat(result.mergeRetry().get(0).tableUuid()).isEqualTo(TABLE_UUID);
  }

  @Test
  void rowExclusiveConflict_txnRemainsInProgress() throws Exception {
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    beginOpAndStage(txn.txnId(), txn.txnToken(), "ROW_EXCLUSIVE");
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());

    // ROW_EXCLUSIVE conflict does NOT abort the transaction — it goes to merge-retry
    assertThat(result.mergeRetry()).hasSize(1);
    JdbcTransactionPersistence.TransactionRow txnRow =
        persistence.findTransaction(txn.txnId()).orElseThrow();
    assertThat(txnRow.status()).isEqualTo(JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS);
  }

  @Test
  void mixedTables_oneConflictOneClean_rowExclusiveRouting() throws Exception {
    String cleanTable = "ns.cleanTable";
    String dirtyTable = "ns.dirtyTable";
    commitTableVersion(cleanTable, "s3://bucket/clean/v1.json");
    commitTableVersion(dirtyTable, "s3://bucket/dirty/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);

    var subClean =
        service.createOperation(
            txn.txnId(),
            txn.txnToken(),
            cleanTable,
            "ROW_EXCLUSIVE",
            null,
            null,
            3600,
            "s3://bucket/clean/metadata");
    service.commitOperation(
        txn.txnId(), subClean.opId(), subClean.opToken(), subClean.assignedStagingPath());

    var subDirty =
        service.createOperation(
            txn.txnId(),
            txn.txnToken(),
            dirtyTable,
            "ROW_EXCLUSIVE",
            null,
            null,
            3600,
            "s3://bucket/dirty/metadata");
    service.commitOperation(
        txn.txnId(), subDirty.opId(), subDirty.opToken(), subDirty.assignedStagingPath());

    // Concurrent writer advances only dirtyTable
    commitTableVersion(dirtyTable, "s3://bucket/dirty/v2.json");

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());

    assertThat(result.direct()).hasSize(1);
    assertThat(result.direct().get(0).tableUuid()).isEqualTo(cleanTable);
    assertThat(result.mergeRetry()).hasSize(1);
    assertThat(result.mergeRetry().get(0).tableUuid()).isEqualTo(dirtyTable);
  }

  // ---------------------------------------------------------------------------
  // 3. EXCLUSIVE conflict → SerializationFailureException + transaction ABORTED
  // ---------------------------------------------------------------------------

  @Test
  void exclusiveConflict_throwsSerializationFailure() throws Exception {
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    beginOpAndStage(txn.txnId(), txn.txnToken(), "EXCLUSIVE");

    // Concurrent writer commits v2
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    assertThatThrownBy(() -> service.prepareCommit(txn.txnId(), txn.txnToken()))
        .isInstanceOf(TransactionService.SerializationFailureException.class)
        .hasMessageContaining(TABLE_UUID);
  }

  @Test
  void exclusiveConflict_transactionIsAborted() throws Exception {
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    beginOpAndStage(txn.txnId(), txn.txnToken(), "EXCLUSIVE");
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    try {
      service.prepareCommit(txn.txnId(), txn.txnToken());
    } catch (TransactionService.SerializationFailureException ignored) {
    }

    JdbcTransactionPersistence.TransactionRow txnRow =
        persistence.findTransaction(txn.txnId()).orElseThrow();
    assertThat(txnRow.status())
        .as("transaction must be ABORTED after serialization failure")
        .isEqualTo(JdbcTransactionPersistence.TXN_STATUS_ABORTED);
  }

  @Test
  void accessExclusiveConflict_throwsSerializationFailure() throws Exception {
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    beginOpAndStage(txn.txnId(), txn.txnToken(), "ACCESS_EXCLUSIVE");
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    assertThatThrownBy(() -> service.prepareCommit(txn.txnId(), txn.txnToken()))
        .isInstanceOf(TransactionService.SerializationFailureException.class);
  }

  @Test
  void serializationFailure_subsequentCommitRejected() throws Exception {
    // After a serialization failure the transaction is ABORTED; prepareCommit again must reject.
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    JdbcTransactionPersistence.TransactionRow txn =
        service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    long txnId = txn.txnId();
    String txnToken = txn.txnToken();
    beginOpAndStage(txnId, txnToken, "EXCLUSIVE");
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    try {
      service.prepareCommit(txnId, txnToken);
    } catch (TransactionService.SerializationFailureException ignored) {
    }

    // The transaction is now ABORTED; a retry of prepareCommit must fail with BadRequest
    assertThatThrownBy(() -> service.prepareCommit(txnId, txnToken))
        .isInstanceOf(jakarta.ws.rs.BadRequestException.class)
        .hasMessageContaining("not IN_PROGRESS");
  }

  // ---------------------------------------------------------------------------
  // 4. Keyspace-split regression: committed_table_snapshots is keyed by the numeric entity id,
  //    but the operation's tableUuid is the qualified name. prepareCommit must look up CTS by the
  //    op's tableEntityId (not the name), or conflict detection silently mis-fires and a
  //    destructive op is routed to direct instead of raising SerializationFailure.
  //
  //    Before Fix 1, prepareCommit looked up CTS by tableUuid (the name) → found nothing (CTS was
  //    written under the numeric id) → server_base == null → no conflict → routed to direct. These
  //    cases fail (no exception thrown / wrong routing) before the fix and pass after it.
  // ---------------------------------------------------------------------------

  private static final String ENTITY_ID = "9001"; // numeric id ≠ TABLE_UUID ("ns.orders")

  /**
   * Creates a subtransaction operation carrying an explicit {@code tableEntityId} (the numeric CTS
   * key), then marks it COMMITTED. Mirrors {@link #beginOpAndStage} but exercises the extension
   * path that threads the entity id, and uses a name that differs from the CTS key.
   */
  private long beginOpWithEntityId(long txnId, String txnToken, String lockMode) throws Exception {
    var op =
        service.createOperation(
            txnId,
            txnToken,
            null,
            TABLE_UUID,
            lockMode,
            ENTITY_ID,
            null,
            null,
            3600,
            TABLE_META_DIR);
    service.commitOperation(txnId, op.opId(), op.opToken(), op.assignedStagingPath());
    return op.opId();
  }

  @Test
  void exclusiveConflict_ctsKeyedByEntityId_throwsSerializationFailure() throws Exception {
    // CTS written under the NUMERIC id, not the name.
    commitTableVersion(ENTITY_ID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    beginOpWithEntityId(txn.txnId(), txn.txnToken(), "EXCLUSIVE");

    // Concurrent writer commits v2 under the same numeric id.
    commitTableVersion(ENTITY_ID, "s3://bucket/ns/orders/metadata/v2.json");

    assertThatThrownBy(() -> service.prepareCommit(txn.txnId(), txn.txnToken()))
        .as("conflict must be detected via the numeric CTS key, not the qualified name")
        .isInstanceOf(TransactionService.SerializationFailureException.class)
        .hasMessageContaining(TABLE_UUID);
  }

  @Test
  void rowExclusiveConflict_ctsKeyedByEntityId_routesToMergeRetry() throws Exception {
    commitTableVersion(ENTITY_ID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    beginOpWithEntityId(txn.txnId(), txn.txnToken(), "ROW_EXCLUSIVE");

    commitTableVersion(ENTITY_ID, "s3://bucket/ns/orders/metadata/v2.json");

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());

    assertThat(result.direct()).isEmpty();
    assertThat(result.mergeRetry()).hasSize(1);
    assertThat(result.mergeRetry().get(0).tableUuid()).isEqualTo(TABLE_UUID);
    assertThat(result.mergeRetry().get(0).tableEntityId()).isEqualTo(ENTITY_ID);
  }

  // ---------------------------------------------------------------------------
  // 4. Inline path (prepareInlineCommit), REPEATABLE_READ
  // ---------------------------------------------------------------------------

  private static List<TransactionService.StagedTableInfo> inlineStaged() {
    return List.of(
        new TransactionService.StagedTableInfo(
            TABLE_UUID, TABLE_META_DIR + "/staged.metadata.json", null, "ROW_EXCLUSIVE"));
  }

  @Test
  void inlineRepeatableRead_concurrentCommitAfterBegin_throwsSerializationFailure()
      throws Exception {
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);

    // A concurrent writer advances the table after the transaction's read point.
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    assertThatThrownBy(
            () -> service.prepareInlineCommit(txn.txnId(), txn.txnToken(), inlineStaged()))
        .isInstanceOf(TransactionService.SerializationFailureException.class);
  }

  @Test
  void inlineRepeatableRead_firstCommitAfterBegin_nullBaseline_throwsSerializationFailure()
      throws Exception {
    // The table has no committed_table_snapshots row when the transaction begins (e.g. it predates
    // the transaction schema), so the baseline resolved under the read point is null.
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);

    // A concurrent writer then commits the table's first recorded version.
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    assertThatThrownBy(
            () -> service.prepareInlineCommit(txn.txnId(), txn.txnToken(), inlineStaged()))
        .as("a commit after the read point must be detected even when the baseline is null")
        .isInstanceOf(TransactionService.SerializationFailureException.class);
  }
}
