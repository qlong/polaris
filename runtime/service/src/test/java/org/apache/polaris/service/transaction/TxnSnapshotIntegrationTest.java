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
}
