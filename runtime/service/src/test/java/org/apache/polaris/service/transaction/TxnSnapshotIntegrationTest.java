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
import java.io.InputStream;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.CommittedTableSnapshotRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.TxnSnapshotRow;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for TxnSnapshot creation and visibility with concurrent commits.
 *
 * <p>Verifies the full round-trip: {@code createSnapshot} → concurrent {@code doCommit}-style
 * inserts → {@code resolveTableSnapshot} returns the correct historical pointer.
 */
class TxnSnapshotIntegrationTest {

  private static final String REALM = "snap-realm";
  private static final long CATALOG_ID = 55L;
  private static final String TABLE_UUID = "integ-table-uuid";

  private JdbcTransactionPersistence persistence;
  private TransactionService service;

  @BeforeEach
  void setUp() throws Exception {
    String jdbcUrl =
        "jdbc:h2:mem:snap_integ_"
            + UUID.randomUUID()
            + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_UPPER=false";
    DataSource dataSource = JdbcConnectionPool.create(jdbcUrl, "sa", "");

    DatasourceOperations ops =
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
    service = new TransactionService(persistence, new LockManager(persistence));
  }

  @Test
  void snapshotSeesOnlyCommitsBeforeItWasAcquired() throws Exception {
    // Insert a committed snapshot before acquiring the read snapshot
    long txnIdBefore = persistence.nextTxnId();
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(TABLE_UUID, txnIdBefore, null, "s3://bucket/v-before.json"));

    // Acquire a read snapshot — its xmax will be > txnIdBefore
    TxnSnapshotRow snap = service.createTransactionSnapshot(3600);

    // Insert a committed snapshot after acquiring the read snapshot
    long txnIdAfter = persistence.nextTxnId();
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(TABLE_UUID, txnIdAfter, null, "s3://bucket/v-after.json"));

    // Resolve: should see v-before but not v-after
    Optional<String> resolved =
        service.resolveTableMetadataLocation(TABLE_UUID, snap.txnSnapshotId());
    assertThat(resolved).isPresent();
    assertThat(resolved.get()).isEqualTo("s3://bucket/v-before.json");
  }

  @Test
  void snapshotSeesCommitsWithinItsWindow() throws Exception {
    // Insert multiple commits
    long id1 = persistence.nextTxnId();
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(TABLE_UUID, id1, null, "s3://bucket/v1.json"));
    long id2 = persistence.nextTxnId();
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(TABLE_UUID, id2, null, "s3://bucket/v2.json"));

    // Acquire snapshot — captures xmax = id2 + 1 or so
    TxnSnapshotRow snap = service.createTransactionSnapshot(3600);

    // Latest visible commit should be v2
    Optional<String> resolved =
        service.resolveTableMetadataLocation(TABLE_UUID, snap.txnSnapshotId());
    assertThat(resolved).isPresent();
    assertThat(resolved.get()).isEqualTo("s3://bucket/v2.json");
  }

  @Test
  void releasedSnapshotIsRejected() throws Exception {
    TxnSnapshotRow snap = service.createTransactionSnapshot(3600);
    service.releaseTransactionSnapshot(snap.txnSnapshotId());

    // resolveTableMetadataLocation should throw because the snapshot is released
    assertThatThrownBy(() -> service.resolveTableMetadataLocation(TABLE_UUID, snap.txnSnapshotId()))
        .isInstanceOf(BadRequestException.class);
  }

  // ---------------------------------------------------------------------------
  // beginTransaction(REPEATABLE_READ) linked snapshot visibility
  // ---------------------------------------------------------------------------

  @Test
  void transactionLinkedSnapshotSeesCommitBeforeBeginNotAfter() throws Exception {
    // Commit a row BEFORE beginning the transaction
    long preTxnCommit = persistence.nextTxnId();
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(
            TABLE_UUID, preTxnCommit, null, "s3://bucket/pre-begin.json"));

    // Begin a REPEATABLE_READ transaction — captures TxnSnapshot at this moment
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, "owner");
    assertThat(txn.txnSnapshotId()).isNotNull();

    // Commit a row AFTER the transaction began
    long postTxnCommit = persistence.nextTxnId();
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(
            TABLE_UUID, postTxnCommit, null, "s3://bucket/post-begin.json"));

    // The linked snapshot must see the pre-begin commit but not the post-begin one
    Optional<String> resolved =
        service.resolveTableMetadataLocation(TABLE_UUID, txn.txnSnapshotId());
    assertThat(resolved).isPresent();
    assertThat(resolved.get()).isEqualTo("s3://bucket/pre-begin.json");
  }

  @Test
  void twoTransactionsGetDistinctLinkedSnapshots() throws Exception {
    var txn1 = service.beginTransaction("REPEATABLE_READ", 3600, "owner1");
    var txn2 = service.beginTransaction("REPEATABLE_READ", 3600, "owner2");

    assertThat(txn1.txnSnapshotId()).isNotNull();
    assertThat(txn2.txnSnapshotId()).isNotNull();
    assertThat(txn1.txnSnapshotId()).isNotEqualTo(txn2.txnSnapshotId());
  }

  @Test
  void transactionLinkedSnapshotReleasedOnAbort() throws Exception {
    var txn = service.beginTransaction("REPEATABLE_READ", 3600, "owner");
    long snapId = txn.txnSnapshotId();

    service.abortTransaction(txn.txnId(), txn.txnToken());

    // After abort, resolving the snapshot should fail (released)
    assertThatThrownBy(() -> service.resolveTableMetadataLocation(TABLE_UUID, snapId))
        .isInstanceOf(BadRequestException.class);
  }
}
