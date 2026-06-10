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
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.TxnSnapshotRow;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the multi-table write atomicity fix.
 *
 * <p>Covers:
 *
 * <ul>
 *   <li>All rows in a batch commit share a single {@code committed_txn_id}.
 *   <li>A MVCC snapshot taken <em>before</em> {@link
 *       JdbcTransactionPersistence#insertBatchCommittedTableSnapshots} sees <em>neither</em>
 *       table's new state.
 *   <li>A snapshot taken <em>after</em> the batch insert sees <em>both</em> tables.
 *   <li>Contrast: the legacy per-row insert path gives each table a distinct {@code
 *       committed_txn_id}, which would allow a snapshot taken between two inserts to see a
 *       partial state.
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

  // ---------------------------------------------------------------------------
  // Shared committed_txn_id
  // ---------------------------------------------------------------------------

  /**
   * All rows inserted via {@code insertBatchCommittedTableSnapshots} must share a single
   * committed_txn_id. This is the core MVCC atomicity guarantee: readers see all-or-nothing.
   */
  @Test
  void batchInsertAllRowsShareSingleCommittedTxnId() throws Exception {
    String table1 = "entity-101";
    String table2 = "entity-102";
    String table3 = "entity-103";

    List<CommittedTableSnapshotRow> rows =
        List.of(
            new CommittedTableSnapshotRow(table1, 0L, null, "s3://t1/v2.json"),
            new CommittedTableSnapshotRow(table2, 0L, null, "s3://t2/v2.json"),
            new CommittedTableSnapshotRow(table3, 0L, null, "s3://t3/v2.json"));

    persistence.insertBatchCommittedTableSnapshots(rows);

    // All three rows should share the same committed_txn_id
    List<Long> txnIds = readAllCommittedTxnIds(table1, table2, table3);
    assertThat(txnIds).hasSize(3);
    assertThat(txnIds.stream().distinct().count())
        .as("all batch rows must share a single committed_txn_id")
        .isEqualTo(1);
  }

  @Test
  void batchInsertUsesNewTxnIdNotPlaceholder() throws Exception {
    String table = "entity-200";
    persistence.insertBatchCommittedTableSnapshots(
        List.of(new CommittedTableSnapshotRow(table, 0L, null, "s3://t/v1.json")));

    List<Long> ids = readAllCommittedTxnIds(table);
    assertThat(ids).hasSize(1);
    assertThat(ids.get(0)).as("committed_txn_id must be a real sequence value, not 0").isNotZero();
  }

  @Test
  void emptyBatchIsNoOp() throws Exception {
    // Should not throw
    persistence.insertBatchCommittedTableSnapshots(List.of());
  }

  // ---------------------------------------------------------------------------
  // MVCC snapshot isolation: pre-snapshot sees neither table
  // ---------------------------------------------------------------------------

  /**
   * A TxnSnapshot acquired <em>before</em> the batch insert must see <em>neither</em> table's new
   * state. Without the batch insert, the old per-row path would write rows with separate txn IDs,
   * and a snapshot taken between two inserts would see a partial update.
   */
  @Test
  void preSnapshotSeesNeitherTableAfterBatchCommit() throws Exception {
    String orders = "entity-orders-1";
    String customers = "entity-customers-1";

    // Establish baseline visibility (previous versions)
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(orders, persistence.nextTxnId(), null, "s3://orders/v1.json"));
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(
            customers, persistence.nextTxnId(), null, "s3://customers/v1.json"));

    // Snapshot acquired BEFORE the multi-table commit
    TxnSnapshotRow snap = service.createTransactionSnapshot(3600);

    // Batch commit of new versions for both tables — single committed_txn_id after snap.xmax
    persistence.insertBatchCommittedTableSnapshots(
        List.of(
            new CommittedTableSnapshotRow(orders, 0L, null, "s3://orders/v2.json"),
            new CommittedTableSnapshotRow(customers, 0L, null, "s3://customers/v2.json")));

    // Pre-snapshot must still see v1 for both tables — v2 committed after snap.xmax
    assertThat(service.resolveTableMetadataLocation(orders, snap.txnSnapshotId()))
        .as("orders: pre-snapshot must see v1, not v2")
        .hasValue("s3://orders/v1.json");
    assertThat(service.resolveTableMetadataLocation(customers, snap.txnSnapshotId()))
        .as("customers: pre-snapshot must see v1, not v2")
        .hasValue("s3://customers/v1.json");
  }

  /**
   * A TxnSnapshot acquired <em>before</em> the first commit must see <em>no</em> state for either
   * table (not just the old version — here there is no old version at all).
   */
  @Test
  void preSnapshotSeesNothingWhenTablesNotYetCommitted() throws Exception {
    String orders = "entity-orders-fresh-1";
    String customers = "entity-customers-fresh-1";

    // Snapshot BEFORE any commit
    TxnSnapshotRow snap = service.createTransactionSnapshot(3600);

    persistence.insertBatchCommittedTableSnapshots(
        List.of(
            new CommittedTableSnapshotRow(orders, 0L, null, "s3://orders/v1.json"),
            new CommittedTableSnapshotRow(customers, 0L, null, "s3://customers/v1.json")));

    assertThat(service.resolveTableMetadataLocation(orders, snap.txnSnapshotId()))
        .as("orders: no commit visible before snapshot")
        .isEmpty();
    assertThat(service.resolveTableMetadataLocation(customers, snap.txnSnapshotId()))
        .as("customers: no commit visible before snapshot")
        .isEmpty();
  }

  // ---------------------------------------------------------------------------
  // MVCC snapshot isolation: post-snapshot sees BOTH tables
  // ---------------------------------------------------------------------------

  /**
   * A TxnSnapshot acquired <em>after</em> the batch insert must see <em>both</em> tables' new
   * state. This confirms the batch insert committed successfully and MVCC picks it up.
   */
  @Test
  void postSnapshotSeesBothTablesAfterBatchCommit() throws Exception {
    String orders = "entity-orders-2";
    String customers = "entity-customers-2";

    persistence.insertBatchCommittedTableSnapshots(
        List.of(
            new CommittedTableSnapshotRow(orders, 0L, null, "s3://orders/v2.json"),
            new CommittedTableSnapshotRow(customers, 0L, null, "s3://customers/v2.json")));

    // Snapshot AFTER the batch commit
    TxnSnapshotRow snap = service.createTransactionSnapshot(3600);

    assertThat(service.resolveTableMetadataLocation(orders, snap.txnSnapshotId()))
        .as("orders: post-snapshot must see v2")
        .hasValue("s3://orders/v2.json");
    assertThat(service.resolveTableMetadataLocation(customers, snap.txnSnapshotId()))
        .as("customers: post-snapshot must see v2")
        .hasValue("s3://customers/v2.json");
  }

  // ---------------------------------------------------------------------------
  // Contrast: per-row inserts have separate txn IDs (showing the old gap)
  // ---------------------------------------------------------------------------

  /**
   * Documents the MVCC gap present in the legacy per-row path: two separate {@code
   * insertCommittedTableSnapshotRow} calls produce distinct {@code committed_txn_id} values. A
   * snapshot taken between them would see table 1 but not table 2 — partial state.
   *
   * <p>This test does <em>not</em> assert a bug; it documents the expected behavior difference
   * between per-row and batch inserts to motivate the batch approach.
   */
  @Test
  void perRowInsertGivesSeparateTxnIdsUnlikeBatch() throws Exception {
    String table1 = "entity-separate-1";
    String table2 = "entity-separate-2";

    long id1 = persistence.nextTxnId();
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(table1, id1, null, "s3://t1/v1.json"));
    long id2 = persistence.nextTxnId();
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(table2, id2, null, "s3://t2/v1.json"));

    assertThat(id1).isNotEqualTo(id2);

    // Snapshot taken between id1 and id2 would see table1 but not table2
    // (We simulate this by manually checking the committed_txn_ids)
    List<Long> perRowIds = readAllCommittedTxnIds(table1, table2);
    assertThat(perRowIds.stream().distinct().count())
        .as("per-row inserts give distinct committed_txn_ids (no MVCC atomicity)")
        .isEqualTo(2);

    // Contrast: batch inserts share one txn_id
    String bTable1 = "entity-batch-1";
    String bTable2 = "entity-batch-2";
    persistence.insertBatchCommittedTableSnapshots(
        List.of(
            new CommittedTableSnapshotRow(bTable1, 0L, null, "s3://b1/v1.json"),
            new CommittedTableSnapshotRow(bTable2, 0L, null, "s3://b2/v1.json")));

    List<Long> batchIds = readAllCommittedTxnIds(bTable1, bTable2);
    assertThat(batchIds.stream().distinct().count())
        .as("batch insert gives a single committed_txn_id (MVCC atomic)")
        .isEqualTo(1);
  }

  // ---------------------------------------------------------------------------
  // Multi-table txn snapshot: snapshot taken mid-batch sees consistent old state
  // ---------------------------------------------------------------------------

  /**
   * Simulates the full atomicity scenario: reader holds a snapshot, writer commits two tables via
   * batch insert, reader verifies it sees a consistent (all-old or all-new) view.
   *
   * <p>Since the batch uses a single {@code committed_txn_id > snap.xmax}, the snapshot sees
   * neither table updated.
   */
  @Test
  void snapshotTakenBeforeBatchCommitSeesConsistentOldState() throws Exception {
    String orders = "entity-orders-3";
    String customers = "entity-customers-3";

    // Old state for both tables
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(orders, persistence.nextTxnId(), null, "s3://orders/v1.json"));
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(
            customers, persistence.nextTxnId(), null, "s3://customers/v1.json"));

    // Reader pins a snapshot here
    TxnSnapshotRow readerSnap = service.createTransactionSnapshot(3600);

    // Writer batch-commits new versions
    persistence.insertBatchCommittedTableSnapshots(
        List.of(
            new CommittedTableSnapshotRow(orders, 0L, null, "s3://orders/v2.json"),
            new CommittedTableSnapshotRow(customers, 0L, null, "s3://customers/v2.json")));

    // Reader's snapshot: both tables still at v1
    String ordersAtSnap =
        service.resolveTableMetadataLocation(orders, readerSnap.txnSnapshotId()).orElse(null);
    String customersAtSnap =
        service.resolveTableMetadataLocation(customers, readerSnap.txnSnapshotId()).orElse(null);

    assertThat(ordersAtSnap).as("orders should be at v1 under reader's snapshot").isEqualTo("s3://orders/v1.json");
    assertThat(customersAtSnap).as("customers should be at v1 under reader's snapshot").isEqualTo("s3://customers/v1.json");

    // Fresh snapshot after commit: both tables at v2
    TxnSnapshotRow freshSnap = service.createTransactionSnapshot(3600);
    assertThat(service.resolveTableMetadataLocation(orders, freshSnap.txnSnapshotId()))
        .hasValue("s3://orders/v2.json");
    assertThat(service.resolveTableMetadataLocation(customers, freshSnap.txnSnapshotId()))
        .hasValue("s3://customers/v2.json");
  }

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  /** Queries committed_txn_id for all given table_uuids in this realm/catalog. */
  private List<Long> readAllCommittedTxnIds(String... tableUuids) throws Exception {
    List<Long> result = new ArrayList<>();
    String placeholders = "?,".repeat(tableUuids.length);
    placeholders = placeholders.substring(0, placeholders.length() - 1);
    String sql =
        "SELECT committed_txn_id FROM POLARIS_SCHEMA.committed_table_snapshots "
            + "WHERE realm_id = ? AND catalog_id = ? AND table_uuid IN ("
            + placeholders
            + ") ORDER BY committed_txn_id";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, realm);
      ps.setLong(2, CATALOG_ID);
      for (int i = 0; i < tableUuids.length; i++) {
        ps.setString(3 + i, tableUuids[i]);
      }
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          result.add(rs.getLong("committed_txn_id"));
        }
      }
    }
    return result;
  }
}
