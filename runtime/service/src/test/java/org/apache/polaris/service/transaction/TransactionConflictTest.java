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
 *   <li><b>No conflict</b> — subtransaction's {@code server_base_metadata_location} matches the
 *       current committed state → subtxn routed to {@code direct}.
 *   <li><b>APPEND conflict → merge-retry</b> — a concurrent writer advanced the table between
 *       subtxn creation and {@code prepareCommit}; operation is APPEND → subtxn routed to {@code
 *       mergeRetry}.
 *   <li><b>Non-APPEND conflict → SerializationFailure</b> — same concurrent-write scenario but
 *       operation is OVERWRITE / ROW_DELTA → {@link
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

    persistence = new JdbcTransactionPersistence(dataSource, DatabaseType.H2, "realm-test", CATALOG_ID);
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
   * Creates a subtransaction for {@code tableUuid} and marks it SUB_COMMITTED with the given
   * staged path. The {@code server_base_metadata_location} is set by {@code createSubtransaction}
   * at creation time — so call this BEFORE simulating concurrent commits.
   */
  private long beginSubtxnAndStage(long txnId, String txnToken, String operation) throws Exception {
    var subtxn =
        service.createSubtransaction(
            txnId, txnToken, TABLE_UUID, operation, null, null, 3600, TABLE_META_DIR);
    service.commitSubtransaction(
        txnId, subtxn.subtxnId(), subtxn.subtxnToken(), subtxn.assignedStagingPath());
    return subtxn.subtxnId();
  }

  // ---------------------------------------------------------------------------
  // 1. No conflict: server_base == current committed → routed to direct
  // ---------------------------------------------------------------------------

  @Test
  void noConflict_routesToDirect() throws Exception {
    // Establish a committed version that the subtxn will see as its base
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    // createSubtransaction captures v1.json as server_base; no further commits happen
    beginSubtxnAndStage(txn.txnId(), txn.txnToken(), "APPEND");

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());

    assertThat(result.direct()).hasSize(1);
    assertThat(result.mergeRetry()).isEmpty();
    assertThat(result.direct().get(0).tableUuid()).isEqualTo(TABLE_UUID);
  }

  @Test
  void noConflict_nullServerBase_alwaysRoutesToDirect() throws Exception {
    // Table has never been committed — server_base will be null; no conflict possible
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    beginSubtxnAndStage(txn.txnId(), txn.txnToken(), "OVERWRITE");

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

    // Create and stage subtxns for both tables before any new commit
    var subA = service.createSubtransaction(txn.txnId(), txn.txnToken(), tableA, "APPEND", null, null, 3600, "s3://bucket/a/metadata");
    service.commitSubtransaction(txn.txnId(), subA.subtxnId(), subA.subtxnToken(), subA.assignedStagingPath());

    var subB = service.createSubtransaction(txn.txnId(), txn.txnToken(), tableB, "APPEND", null, null, 3600, "s3://bucket/b/metadata");
    service.commitSubtransaction(txn.txnId(), subB.subtxnId(), subB.subtxnToken(), subB.assignedStagingPath());

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());

    assertThat(result.direct()).hasSize(2);
    assertThat(result.mergeRetry()).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // 2. APPEND conflict → mergeRetry
  // ---------------------------------------------------------------------------

  @Test
  void appendConflict_routesToMergeRetry() throws Exception {
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    // Subtxn created while v1 is current (server_base = v1)
    beginSubtxnAndStage(txn.txnId(), txn.txnToken(), "APPEND");

    // Concurrent writer commits v2 between subtxn creation and prepareCommit
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());

    assertThat(result.mergeRetry()).hasSize(1);
    assertThat(result.direct()).isEmpty();
    assertThat(result.mergeRetry().get(0).tableUuid()).isEqualTo(TABLE_UUID);
  }

  @Test
  void appendConflict_txnRemainsInProgress() throws Exception {
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    beginSubtxnAndStage(txn.txnId(), txn.txnToken(), "APPEND");
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());

    // APPEND conflict does NOT abort the transaction — it goes to merge-retry
    assertThat(result.mergeRetry()).hasSize(1);
    JdbcTransactionPersistence.TransactionRow txnRow = persistence.findTransaction(txn.txnId()).orElseThrow();
    assertThat(txnRow.status()).isEqualTo(JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS);
  }

  @Test
  void mixedTables_oneConflictOneClean_appendRouting() throws Exception {
    String cleanTable = "ns.cleanTable";
    String dirtyTable = "ns.dirtyTable";
    commitTableVersion(cleanTable, "s3://bucket/clean/v1.json");
    commitTableVersion(dirtyTable, "s3://bucket/dirty/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);

    var subClean = service.createSubtransaction(txn.txnId(), txn.txnToken(), cleanTable, "APPEND", null, null, 3600, "s3://bucket/clean/metadata");
    service.commitSubtransaction(txn.txnId(), subClean.subtxnId(), subClean.subtxnToken(), subClean.assignedStagingPath());

    var subDirty = service.createSubtransaction(txn.txnId(), txn.txnToken(), dirtyTable, "APPEND", null, null, 3600, "s3://bucket/dirty/metadata");
    service.commitSubtransaction(txn.txnId(), subDirty.subtxnId(), subDirty.subtxnToken(), subDirty.assignedStagingPath());

    // Concurrent writer advances only dirtyTable
    commitTableVersion(dirtyTable, "s3://bucket/dirty/v2.json");

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());

    assertThat(result.direct()).hasSize(1);
    assertThat(result.direct().get(0).tableUuid()).isEqualTo(cleanTable);
    assertThat(result.mergeRetry()).hasSize(1);
    assertThat(result.mergeRetry().get(0).tableUuid()).isEqualTo(dirtyTable);
  }

  // ---------------------------------------------------------------------------
  // 3. Non-APPEND conflict → SerializationFailureException + transaction ABORTED
  // ---------------------------------------------------------------------------

  @Test
  void overwriteConflict_throwsSerializationFailure() throws Exception {
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    beginSubtxnAndStage(txn.txnId(), txn.txnToken(), "OVERWRITE");

    // Concurrent writer commits v2
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    assertThatThrownBy(() -> service.prepareCommit(txn.txnId(), txn.txnToken()))
        .isInstanceOf(TransactionService.SerializationFailureException.class)
        .hasMessageContaining(TABLE_UUID);
  }

  @Test
  void overwriteConflict_transactionIsAborted() throws Exception {
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    beginSubtxnAndStage(txn.txnId(), txn.txnToken(), "OVERWRITE");
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    try {
      service.prepareCommit(txn.txnId(), txn.txnToken());
    } catch (TransactionService.SerializationFailureException ignored) {
    }

    JdbcTransactionPersistence.TransactionRow txnRow = persistence.findTransaction(txn.txnId()).orElseThrow();
    assertThat(txnRow.status())
        .as("transaction must be ABORTED after serialization failure")
        .isEqualTo(JdbcTransactionPersistence.TXN_STATUS_ABORTED);
  }

  @Test
  void rowDeltaConflict_throwsSerializationFailure() throws Exception {
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    beginSubtxnAndStage(txn.txnId(), txn.txnToken(), "ROW_DELTA");
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    assertThatThrownBy(() -> service.prepareCommit(txn.txnId(), txn.txnToken()))
        .isInstanceOf(TransactionService.SerializationFailureException.class);
  }

  @Test
  void exclusiveConflict_throwsSerializationFailure() throws Exception {
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    beginSubtxnAndStage(txn.txnId(), txn.txnToken(), "EXCLUSIVE");
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v2.json");

    assertThatThrownBy(() -> service.prepareCommit(txn.txnId(), txn.txnToken()))
        .isInstanceOf(TransactionService.SerializationFailureException.class);
  }

  @Test
  void serializationFailure_subsequentCommitRejected() throws Exception {
    // After a serialization failure the transaction is ABORTED; prepareCommit again must reject.
    commitTableVersion(TABLE_UUID, "s3://bucket/ns/orders/metadata/v1.json");

    JdbcTransactionPersistence.TransactionRow txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    long txnId = txn.txnId();
    String txnToken = txn.txnToken();
    beginSubtxnAndStage(txnId, txnToken, "OVERWRITE");
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
}
