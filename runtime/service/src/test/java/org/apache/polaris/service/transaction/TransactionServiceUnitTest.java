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

import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotFoundException;
import java.io.InputStream;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link TransactionService} state-machine transitions using an in-memory H2 database.
 * Verifies: IN_PROGRESS → COMMITTED, IN_PROGRESS → ABORTED, double-commit rejected,
 * commit-after-abort rejected.
 */
class TransactionServiceUnitTest {

  private static final String REALM = "test-realm";
  private static final long CATALOG_ID = 99L;
  private static final String OWNER = "test-principal";
  private static final String TABLE_METADATA_DIR = "s3://test-bucket/ns/table/metadata";

  private JdbcTransactionPersistence persistence;
  private TransactionService service;

  @BeforeEach
  void setUp() throws Exception {
    String jdbcUrl =
        "jdbc:h2:mem:txnsvc_test_"
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

    persistence = new JdbcTransactionPersistence(dataSource, DatabaseType.H2, REALM, CATALOG_ID);
    service = new TransactionService(persistence, new LockManager(persistence));
  }

  // ---------------------------------------------------------------------------
  // State machine: begin → commit
  // ---------------------------------------------------------------------------

  @Test
  void beginTransactionCreatesInProgressTransaction() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);

    assertThat(txn.txnId()).isGreaterThan(0);
    assertThat(txn.txnToken()).isNotBlank();
    assertThat(txn.status()).isEqualTo(JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS);
    assertThat(txn.ownerPrincipal()).isEqualTo(OWNER);
  }

  @Test
  void prepareAndFinalizeCommitTransitionsToCommitted() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());
    assertThat(result.direct()).isEmpty();
    assertThat(result.mergeRetry()).isEmpty();

    service.finalizeCommit(txn.txnId());

    var committed = persistence.findTransaction(txn.txnId()).orElseThrow();
    assertThat(committed.status()).isEqualTo(JdbcTransactionPersistence.TXN_STATUS_COMMITTED);
  }

  @Test
  void abortTransactionTransitionsToAborted() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);

    service.abortTransaction(txn.txnId(), txn.txnToken());

    var aborted = persistence.findTransaction(txn.txnId()).orElseThrow();
    assertThat(aborted.status()).isEqualTo(JdbcTransactionPersistence.TXN_STATUS_ABORTED);
  }

  @Test
  void doubleCommitIsRejected() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    service.prepareCommit(txn.txnId(), txn.txnToken());
    service.finalizeCommit(txn.txnId());

    // A second prepareCommit must fail because the txn is no longer IN_PROGRESS
    assertThatThrownBy(() -> service.prepareCommit(txn.txnId(), txn.txnToken()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("not IN_PROGRESS");
  }

  @Test
  void commitAfterAbortIsRejected() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    service.abortTransaction(txn.txnId(), txn.txnToken());

    assertThatThrownBy(() -> service.prepareCommit(txn.txnId(), txn.txnToken()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("not IN_PROGRESS");
  }

  @Test
  void commitWithWrongTokenIsRejected() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);

    assertThatThrownBy(() -> service.prepareCommit(txn.txnId(), "wrong-token"))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("token");
  }

  @Test
  void abortAlreadyCommittedIsRejected() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    service.prepareCommit(txn.txnId(), txn.txnToken());
    service.finalizeCommit(txn.txnId());

    assertThatThrownBy(() -> service.abortTransaction(txn.txnId(), txn.txnToken()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("already committed");
  }

  // ---------------------------------------------------------------------------
  // State machine: operations
  // ---------------------------------------------------------------------------

  @Test
  void createOperationCreatesInProgressOp() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);

    var op =
        service.createOperation(
            txn.txnId(), txn.txnToken(), "table-uuid-1", "ROW_EXCLUSIVE", null, null, 3600, TABLE_METADATA_DIR);

    assertThat(op.opId()).isGreaterThan(0);
    assertThat(op.opToken()).isNotBlank();
    assertThat(op.status()).isEqualTo(JdbcTransactionPersistence.OP_STATUS_IN_PROGRESS);
    assertThat(op.assignedStagingPath()).startsWith(TABLE_METADATA_DIR);
    assertThat(op.assignedStagingPath()).endsWith(".metadata.json");
  }

  @Test
  void commitOperationTransitionsToCommitted() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    var op =
        service.createOperation(
            txn.txnId(), txn.txnToken(), "table-uuid-2", "ROW_EXCLUSIVE", null, null, 3600, TABLE_METADATA_DIR);

    // assignedStagingPath is the exact file path — pass it directly
    String metadataLocation = op.assignedStagingPath();
    service.commitOperation(txn.txnId(), op.opId(), op.opToken(), metadataLocation);

    var result = service.prepareCommit(txn.txnId(), txn.txnToken());
    assertThat(result.direct()).hasSize(1);
    assertThat(result.direct().get(0).status())
        .isEqualTo(JdbcTransactionPersistence.OP_STATUS_COMMITTED);
  }

  @Test
  void prepareCommitFailsWhenOpStillInProgress() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    service.createOperation(
        txn.txnId(), txn.txnToken(), "table-uuid-3", "ROW_EXCLUSIVE", null, null, 3600, TABLE_METADATA_DIR);

    assertThatThrownBy(() -> service.prepareCommit(txn.txnId(), txn.txnToken()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("IN_PROGRESS");
  }

  @Test
  void getOperationReturnsParentTxnAndStagingPath() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    var op =
        service.createOperation(
            txn.txnId(), txn.txnToken(), "ns.orders", "ROW_EXCLUSIVE", null, null, 3600, TABLE_METADATA_DIR);

    var info = service.getOperation(op.opId(), op.opToken());

    assertThat(info.txnId()).isEqualTo(txn.txnId());
    assertThat(info.txnSnapshotId()).isEqualTo(txn.txnSnapshotId());
    assertThat(info.assignedStagingPath()).isEqualTo(op.assignedStagingPath());
    assertThat(info.tableUuid()).isEqualTo("ns.orders");
  }

  @Test
  void getOperationWithWrongTokenThrowsForbidden() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    var op =
        service.createOperation(
            txn.txnId(), txn.txnToken(), "ns.orders", "ROW_EXCLUSIVE", null, null, 3600, TABLE_METADATA_DIR);

    assertThatThrownBy(() -> service.getOperation(op.opId(), "wrong-token"))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("op_token");
  }

  @Test
  void getOperationWithUnknownIdThrowsNotFound() {
    assertThatThrownBy(() -> service.getOperation(99999L, "any-token"))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("99999");
  }

  // ---------------------------------------------------------------------------
  // Linked TxnSnapshot (REPEATABLE_READ creates a real txn_snapshots row)
  // ---------------------------------------------------------------------------

  @Test
  void beginRepeatableReadCreatesLinkedTxnSnapshot() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);

    assertThat(txn.txnSnapshotId()).isNotNull();
    // The linked snapshot row must exist in txn_snapshots
    var snap = persistence.findTxnSnapshot(txn.txnSnapshotId());
    assertThat(snap).isPresent();
    assertThat(snap.get().txnSnapshotId()).isEqualTo(txn.txnSnapshotId());
    assertThat(snap.get().xmax()).isGreaterThan(0);
    assertThat(snap.get().releasedAt()).isNull();
  }

  @Test
  void beginReadCommittedHasNoLinkedTxnSnapshot() throws Exception {
    var txn = service.beginTransaction("READ_COMMITTED", 3600, OWNER);

    assertThat(txn.txnSnapshotId()).isNull();
  }

  @Test
  void commitReleasesLinkedTxnSnapshot() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    long snapId = txn.txnSnapshotId();

    service.prepareCommit(txn.txnId(), txn.txnToken());
    service.finalizeCommit(txn.txnId());

    var snap = persistence.findTxnSnapshot(snapId);
    assertThat(snap).isPresent();
    assertThat(snap.get().releasedAt()).isNotNull();
  }

  @Test
  void abortReleasesLinkedTxnSnapshot() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);
    long snapId = txn.txnSnapshotId();

    service.abortTransaction(txn.txnId(), txn.txnToken());

    var snap = persistence.findTxnSnapshot(snapId);
    assertThat(snap).isPresent();
    assertThat(snap.get().releasedAt()).isNotNull();
  }

  @Test
  void linkedSnapshotIdIsDistinctFromTxnId() throws Exception {
    // txn_id and txn_snapshot_id both come from the same sequence, so they must differ
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, OWNER);

    assertThat(txn.txnSnapshotId()).isNotEqualTo(txn.txnId());
  }
}
