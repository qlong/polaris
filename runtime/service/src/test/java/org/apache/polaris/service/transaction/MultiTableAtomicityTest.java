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

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
 * Unit tests for multi-table write atomicity under the {@code commit_scn} model.
 *
 * <p>In the new model a multi-table commit allocates a <b>single</b> {@code commit_scn} inside one
 * JDBC transaction and stamps it on every table's {@code committed_table_snapshots} row (see {@code
 * JdbcBasePersistenceImpl.writeEntitiesWithSnapshotRecords}). These tests exercise that contract at
 * the persistence layer via {@link #batchCommitWithSharedScn} — allocate one scn, insert all rows
 * in one transaction — and verify:
 *
 * <ul>
 *   <li>All rows in a batch commit share a single {@code commit_scn}.
 *   <li>A snapshot taken <em>before</em> the batch commit sees <em>neither</em> table's new state.
 *   <li>A snapshot taken <em>after</em> the batch commit sees <em>both</em> tables.
 *   <li>Contrast: separate single-table commits get distinct {@code commit_scn}s.
 * </ul>
 */
class MultiTableAtomicityTest {

  private static final long CATALOG_ID = 7L;

  private JdbcTransactionPersistence persistence;
  private TransactionService service;
  private DataSource dataSource;
  private String realm;

  @BeforeEach
  void setUp() throws Exception {
    String jdbcUrl =
        "jdbc:h2:mem:multi_atomic_"
            + UUID.randomUUID()
            + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_UPPER=false";
    dataSource = JdbcConnectionPool.create(jdbcUrl, "sa", "");

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

    realm = "realm-" + UUID.randomUUID();
    persistence = new JdbcTransactionPersistence(dataSource, DatabaseType.H2, realm, CATALOG_ID);
    service = new TransactionService(persistence, new LockManager(persistence));
  }

  /**
   * Commits several tables atomically: allocates one {@code commit_scn} and inserts all rows in a
   * single JDBC transaction, mirroring {@code writeEntitiesWithSnapshotRecords}. Returns the shared
   * scn.
   */
  private long batchCommitWithSharedScn(CommittedTableSnapshotRow... rows) throws Exception {
    if (rows.length == 0) {
      return 0L;
    }
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      long scn = persistence.allocateCommitScn(conn);
      for (CommittedTableSnapshotRow row : rows) {
        persistence.insertCommittedTableSnapshot(
            conn,
            new CommittedTableSnapshotRow(
                row.tableUuid(), scn, row.icebergSnapshotId(), row.metadataLocation()));
      }
      conn.commit();
      return scn;
    }
  }

  /** Commits a single table on its own, allocating its own commit_scn. */
  private void singleCommit(String tableUuid, String metadataLocation) throws Exception {
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(
            tableUuid, JdbcTransactionPersistence.UNASSIGNED_SCN, null, metadataLocation));
  }

  // ---------------------------------------------------------------------------
  // Shared commit_scn
  // ---------------------------------------------------------------------------

  @Test
  void batchInsertAllRowsShareSingleCommitScn() throws Exception {
    String table1 = "entity-101";
    String table2 = "entity-102";
    String table3 = "entity-103";

    batchCommitWithSharedScn(
        new CommittedTableSnapshotRow(table1, 0L, null, "s3://t1/v2.json"),
        new CommittedTableSnapshotRow(table2, 0L, null, "s3://t2/v2.json"),
        new CommittedTableSnapshotRow(table3, 0L, null, "s3://t3/v2.json"));

    List<Long> scns = readAllCommitScns(table1, table2, table3);
    assertThat(scns).hasSize(3);
    assertThat(scns.stream().distinct().count())
        .as("all batch rows must share a single commit_scn")
        .isEqualTo(1);
  }

  @Test
  void batchInsertUsesNonZeroCommitScn() throws Exception {
    String table = "entity-200";
    batchCommitWithSharedScn(new CommittedTableSnapshotRow(table, 0L, null, "s3://t/v1.json"));

    List<Long> scns = readAllCommitScns(table);
    assertThat(scns).hasSize(1);
    assertThat(scns.get(0)).as("commit_scn must be a real allocated value, not 0").isNotZero();
  }

  @Test
  void emptyBatchIsNoOp() throws Exception {
    // Should not throw and should allocate nothing.
    assertThat(batchCommitWithSharedScn()).isEqualTo(0L);
  }

  // ---------------------------------------------------------------------------
  // MVCC snapshot isolation: pre-snapshot sees neither table
  // ---------------------------------------------------------------------------

  @Test
  void preSnapshotSeesNeitherTableAfterBatchCommit() throws Exception {
    String orders = "entity-orders-1";
    String customers = "entity-customers-1";

    // Baseline (v1) for both tables — committed before the snapshot.
    singleCommit(orders, "s3://orders/v1.json");
    singleCommit(customers, "s3://customers/v1.json");

    // Snapshot acquired BEFORE the multi-table commit.
    var snap = service.createTransactionSnapshot(3600);

    // Batch commit of new versions — single commit_scn above the snapshot's read_point.
    batchCommitWithSharedScn(
        new CommittedTableSnapshotRow(orders, 0L, null, "s3://orders/v2.json"),
        new CommittedTableSnapshotRow(customers, 0L, null, "s3://customers/v2.json"));

    assertThat(service.resolveTableMetadataLocation(orders, snap.handle()))
        .as("orders: pre-snapshot must see v1, not v2")
        .hasValue("s3://orders/v1.json");
    assertThat(service.resolveTableMetadataLocation(customers, snap.handle()))
        .as("customers: pre-snapshot must see v1, not v2")
        .hasValue("s3://customers/v1.json");
  }

  @Test
  void preSnapshotSeesNothingWhenTablesNotYetCommitted() throws Exception {
    String orders = "entity-orders-fresh-1";
    String customers = "entity-customers-fresh-1";

    // Snapshot BEFORE any commit (read_point = 0).
    var snap = service.createTransactionSnapshot(3600);

    batchCommitWithSharedScn(
        new CommittedTableSnapshotRow(orders, 0L, null, "s3://orders/v1.json"),
        new CommittedTableSnapshotRow(customers, 0L, null, "s3://customers/v1.json"));

    assertThat(service.resolveTableMetadataLocation(orders, snap.handle()))
        .as("orders: no commit visible before snapshot")
        .isEmpty();
    assertThat(service.resolveTableMetadataLocation(customers, snap.handle()))
        .as("customers: no commit visible before snapshot")
        .isEmpty();
  }

  // ---------------------------------------------------------------------------
  // MVCC snapshot isolation: post-snapshot sees BOTH tables
  // ---------------------------------------------------------------------------

  @Test
  void postSnapshotSeesBothTablesAfterBatchCommit() throws Exception {
    String orders = "entity-orders-2";
    String customers = "entity-customers-2";

    batchCommitWithSharedScn(
        new CommittedTableSnapshotRow(orders, 0L, null, "s3://orders/v2.json"),
        new CommittedTableSnapshotRow(customers, 0L, null, "s3://customers/v2.json"));

    // Snapshot AFTER the batch commit.
    var snap = service.createTransactionSnapshot(3600);

    assertThat(service.resolveTableMetadataLocation(orders, snap.handle()))
        .as("orders: post-snapshot must see v2")
        .hasValue("s3://orders/v2.json");
    assertThat(service.resolveTableMetadataLocation(customers, snap.handle()))
        .as("customers: post-snapshot must see v2")
        .hasValue("s3://customers/v2.json");
  }

  // ---------------------------------------------------------------------------
  // Contrast: separate single-table commits get distinct commit_scns
  // ---------------------------------------------------------------------------

  @Test
  void separateSingleCommitsGetDistinctScnsUnlikeBatch() throws Exception {
    String table1 = "entity-separate-1";
    String table2 = "entity-separate-2";

    singleCommit(table1, "s3://t1/v1.json");
    singleCommit(table2, "s3://t2/v1.json");

    List<Long> perRowScns = readAllCommitScns(table1, table2);
    assertThat(perRowScns.stream().distinct().count())
        .as("separate single-table commits get distinct commit_scns")
        .isEqualTo(2);

    // Contrast: a batch commit shares one commit_scn.
    String bTable1 = "entity-batch-1";
    String bTable2 = "entity-batch-2";
    batchCommitWithSharedScn(
        new CommittedTableSnapshotRow(bTable1, 0L, null, "s3://b1/v1.json"),
        new CommittedTableSnapshotRow(bTable2, 0L, null, "s3://b2/v1.json"));

    List<Long> batchScns = readAllCommitScns(bTable1, bTable2);
    assertThat(batchScns.stream().distinct().count())
        .as("batch commit shares a single commit_scn (MVCC atomic)")
        .isEqualTo(1);
  }

  // ---------------------------------------------------------------------------
  // Full atomicity scenario
  // ---------------------------------------------------------------------------

  @Test
  void snapshotTakenBeforeBatchCommitSeesConsistentOldState() throws Exception {
    String orders = "entity-orders-3";
    String customers = "entity-customers-3";

    singleCommit(orders, "s3://orders/v1.json");
    singleCommit(customers, "s3://customers/v1.json");

    // Reader pins a snapshot here.
    var readerSnap = service.createTransactionSnapshot(3600);

    // Writer batch-commits new versions.
    batchCommitWithSharedScn(
        new CommittedTableSnapshotRow(orders, 0L, null, "s3://orders/v2.json"),
        new CommittedTableSnapshotRow(customers, 0L, null, "s3://customers/v2.json"));

    // Reader's snapshot: both tables still at v1.
    assertThat(service.resolveTableMetadataLocation(orders, readerSnap.handle()))
        .as("orders should be at v1 under reader's snapshot")
        .hasValue("s3://orders/v1.json");
    assertThat(service.resolveTableMetadataLocation(customers, readerSnap.handle()))
        .as("customers should be at v1 under reader's snapshot")
        .hasValue("s3://customers/v1.json");

    // Fresh snapshot after commit: both tables at v2.
    var freshSnap = service.createTransactionSnapshot(3600);
    assertThat(service.resolveTableMetadataLocation(orders, freshSnap.handle()))
        .hasValue("s3://orders/v2.json");
    assertThat(service.resolveTableMetadataLocation(customers, freshSnap.handle()))
        .hasValue("s3://customers/v2.json");
  }

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  /** Queries commit_scn for all given table_uuids in this realm/catalog. */
  private List<Long> readAllCommitScns(String... tableUuids) throws Exception {
    List<Long> result = new ArrayList<>();
    String placeholders = "?,".repeat(tableUuids.length);
    placeholders = placeholders.substring(0, placeholders.length() - 1);
    String sql =
        "SELECT commit_scn FROM POLARIS_SCHEMA.committed_table_snapshots "
            + "WHERE realm_id = ? AND catalog_id = ? AND table_uuid IN ("
            + placeholders
            + ") ORDER BY commit_scn";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, realm);
      ps.setLong(2, CATALOG_ID);
      for (int i = 0; i < tableUuids.length; i++) {
        ps.setString(3 + i, tableUuids[i]);
      }
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          result.add(rs.getLong("commit_scn"));
        }
      }
    }
    return result;
  }
}
