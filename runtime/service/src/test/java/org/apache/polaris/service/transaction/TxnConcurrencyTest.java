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
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.CommittedTableSnapshotRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.TxnSnapshotRow;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * L3 concurrency tests using a real PostgreSQL container (Testcontainers).
 *
 * <p>Covers three scenarios:
 *
 * <ol>
 *   <li><b>Torn-read prevention</b>: a concurrent writer commits to two tables with a delay between
 *       them; a reader that acquires a {@code TxnSnapshot} between the two commits sees a consistent
 *       one-or-the-other view, never a mix.
 *   <li><b>Lock conflict serialization</b>: a ROW_DELTA lock blocks a conflicting second writer;
 *       after the first transaction aborts the second acquires the lock successfully.
 *   <li><b>APPEND non-blocking</b>: concurrent APPEND subtransactions on the same table all
 *       succeed without blocking each other.
 * </ol>
 */
class TxnConcurrencyTest {

  @SuppressWarnings("resource")
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:16-alpine")
          .withDatabaseName("polaris_concurrency_test")
          .withUsername("polaris")
          .withPassword("polaris");

  private static DataSource dataSource;
  private static final long CATALOG_ID = 1L;
  private static final String TABLE_METADATA_DIR = "s3://test/ns/table/metadata";

  // Short lock wait for tests — real conflicts resolve fast in tests
  private static final long LOCK_TIMEOUT_MS = 1_000L;

  private JdbcTransactionPersistence persistence;
  private TransactionService service;

  @BeforeAll
  static void startContainerAndInitSchema() throws Exception {
    POSTGRES.start();
    dataSource = simpleDataSource(POSTGRES.getJdbcUrl(), "polaris", "polaris");
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
                return Optional.of("postgresql");
              }
            });
    InputStream schema =
        DatasourceOperations.class.getClassLoader().getResourceAsStream("postgres/schema-v5.sql");
    ops.executeScript(schema);
  }

  @AfterAll
  static void stopContainer() {
    POSTGRES.stop();
  }

  @BeforeEach
  void setUp() {
    // Unique realm per test — isolates data without dropping/recreating tables
    String realm = "realm-" + UUID.randomUUID();
    persistence = new JdbcTransactionPersistence(dataSource, DatabaseType.POSTGRES, realm, CATALOG_ID);
    service = new TransactionService(persistence, new LockManager(persistence, LOCK_TIMEOUT_MS));
  }

  // ---------------------------------------------------------------------------
  // Test 1: Torn-read prevention
  // ---------------------------------------------------------------------------

  /**
   * Scenario: writer commits orders at T1, then customers at T3. Reader acquires a TxnSnapshot at
   * T2 (between T1 and T3) using a CountDownLatch to control timing precisely. The snapshot at T2
   * must see orders (T1 < T2) but NOT customers (T3 >= T2) — a consistent, tear-free view.
   *
   * <p>Without TxnSnapshot, a reader issuing two successive GETs would see orders at the latest
   * state (including T3's customers commit) — a torn view.
   */
  @Test
  void txnSnapshotPreventsSeingPartialMultiTableCommit() throws Exception {
    String ordersUuid = "orders-" + UUID.randomUUID();
    String customersUuid = "customers-" + UUID.randomUUID();

    CountDownLatch ordersCommitted = new CountDownLatch(1);
    CountDownLatch snapshotAcquired = new CountDownLatch(1);
    List<Throwable> writerErrors = Collections.synchronizedList(new ArrayList<>());

    // Background writer: commits orders, waits for reader's snapshot, then commits customers
    Thread writer =
        new Thread(
            () -> {
              try {
                long t1 = persistence.nextTxnId();
                persistence.insertCommittedTableSnapshotRow(
                    new CommittedTableSnapshotRow(ordersUuid, t1, null, "s3://orders/v2.json"));
                ordersCommitted.countDown(); // signal: orders committed

                assertThat(snapshotAcquired.await(5, TimeUnit.SECONDS))
                    .as("reader should acquire snapshot within 5s")
                    .isTrue();

                long t3 = persistence.nextTxnId();
                persistence.insertCommittedTableSnapshotRow(
                    new CommittedTableSnapshotRow(
                        customersUuid, t3, null, "s3://customers/v2.json"));
              } catch (Throwable e) {
                writerErrors.add(e);
              }
            });
    writer.start();

    // Reader: wait for orders commit, acquire snapshot, signal writer to proceed
    assertThat(ordersCommitted.await(5, TimeUnit.SECONDS)).isTrue();
    TxnSnapshotRow snap = service.createTransactionSnapshot(300);
    snapshotAcquired.countDown();
    writer.join(5_000);
    assertThat(writerErrors).isEmpty();

    // At snap: orders committed before snap (visible), customers committed after (not visible)
    Optional<String> ordersAtSnap =
        service.resolveTableMetadataLocation(ordersUuid, snap.txnSnapshotId());
    Optional<String> customersAtSnap =
        service.resolveTableMetadataLocation(customersUuid, snap.txnSnapshotId());

    assertThat(ordersAtSnap).as("orders should be visible at the pinned snapshot").isPresent();
    assertThat(ordersAtSnap.get()).isEqualTo("s3://orders/v2.json");
    assertThat(customersAtSnap)
        .as("customers committed AFTER the snapshot was acquired — must NOT be visible")
        .isEmpty();

    // A fresh snapshot (acquired after both commits) sees both — confirming the data is there
    TxnSnapshotRow freshSnap = service.createTransactionSnapshot(300);
    assertThat(service.resolveTableMetadataLocation(ordersUuid, freshSnap.txnSnapshotId()))
        .hasValue("s3://orders/v2.json");
    assertThat(service.resolveTableMetadataLocation(customersUuid, freshSnap.txnSnapshotId()))
        .hasValue("s3://customers/v2.json");
  }

  // ---------------------------------------------------------------------------
  // Test 2: Lock conflict serialization
  // ---------------------------------------------------------------------------

  /**
   * Scenario: T1 holds a ROW_DELTA lock on a table. T2 tries to acquire the same lock — it must
   * fail with {@link LockManager.LockWaitTimeoutException} within {@code LOCK_TIMEOUT_MS}. After T1
   * aborts and releases its lock, T2 retries and succeeds.
   */
  @Test
  void rowDeltaLockBlocksConflictingWriterThenReleasesOnAbort() throws Exception {
    String tableUuid = "table-" + UUID.randomUUID();

    // T1 acquires ROW_DELTA lock by creating a subtransaction
    var txn1 = service.beginTransaction("REPEATABLE_READ", 3600, "owner1");
    service.createSubtransaction(
        txn1.txnId(), txn1.txnToken(), tableUuid, "ROW_DELTA", null, null, 3600, TABLE_METADATA_DIR);

    // T2 tries ROW_DELTA on the same resource — must time out
    var txn2 = service.beginTransaction("REPEATABLE_READ", 3600, "owner2");
    assertThatThrownBy(
            () ->
                service.createSubtransaction(
                    txn2.txnId(),
                    txn2.txnToken(),
                    tableUuid,
                    "ROW_DELTA",
                    null,
                    null,
                    3600,
                    TABLE_METADATA_DIR))
        .isInstanceOf(LockManager.LockWaitTimeoutException.class);

    // T1 aborts — releases its lock
    service.abortTransaction(txn1.txnId(), txn1.txnToken());

    // T2 retries with a fresh transaction (its previous attempt cleaned up waiting locks)
    var txn2b = service.beginTransaction("REPEATABLE_READ", 3600, "owner2");
    var subtxn2 =
        service.createSubtransaction(
            txn2b.txnId(),
            txn2b.txnToken(),
            tableUuid,
            "ROW_DELTA",
            null,
            null,
            3600,
            TABLE_METADATA_DIR);

    assertThat(subtxn2.status())
        .as("after T1 releases its lock, T2 must acquire ROW_DELTA successfully")
        .isEqualTo(JdbcTransactionPersistence.SUBTXN_STATUS_IN_PROGRESS);
  }

  /**
   * APPEND is compatible with ROW_DELTA — the high-throughput ingest path must never be blocked by
   * a concurrent destructive write on the same table.
   */
  @Test
  void appendIsCompatibleWithRowDeltaLock() throws Exception {
    String tableUuid = "table-" + UUID.randomUUID();

    // T1 holds ROW_DELTA lock
    var txn1 = service.beginTransaction("REPEATABLE_READ", 3600, "owner1");
    service.createSubtransaction(
        txn1.txnId(), txn1.txnToken(), tableUuid, "ROW_DELTA", null, null, 3600, TABLE_METADATA_DIR);

    // T2 APPEND on same table — must succeed immediately (APPEND never blocks)
    var txn2 = service.beginTransaction("REPEATABLE_READ", 3600, "owner2");
    var appendSubtxn =
        service.createSubtransaction(
            txn2.txnId(), txn2.txnToken(), tableUuid, "APPEND", null, null, 3600, TABLE_METADATA_DIR);

    assertThat(appendSubtxn.status())
        .as("APPEND must not be blocked by a ROW_DELTA lock on the same table")
        .isEqualTo(JdbcTransactionPersistence.SUBTXN_STATUS_IN_PROGRESS);
  }

  // ---------------------------------------------------------------------------
  // Test 3: APPEND non-blocking — N concurrent appenders never conflict
  // ---------------------------------------------------------------------------

  /**
   * Five threads each start their own transaction and create an APPEND subtransaction on the same
   * table. All must succeed without any {@link LockManager.LockWaitTimeoutException} because APPEND
   * is compatible with APPEND.
   */
  @Test
  void concurrentAppendsOnSameTableAllSucceed() throws Exception {
    String tableUuid = "table-" + UUID.randomUUID();
    int N = 5;

    List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
    List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < N; i++) {
      final int idx = i;
      Thread t =
          new Thread(
              () -> {
                try {
                  var txn =
                      service.beginTransaction(
                          "REPEATABLE_READ", 3600, "appender-" + idx);
                  var subtxn =
                      service.createSubtransaction(
                          txn.txnId(),
                          txn.txnToken(),
                          tableUuid,
                          "APPEND",
                          null,
                          null,
                          3600,
                          TABLE_METADATA_DIR);
                  assertThat(subtxn.status())
                      .isEqualTo(JdbcTransactionPersistence.SUBTXN_STATUS_IN_PROGRESS);
                } catch (Throwable e) {
                  errors.add(e);
                }
              });
      threads.add(t);
    }

    threads.forEach(Thread::start);
    for (Thread t : threads) {
      t.join(10_000);
    }

    assertThat(errors)
        .as("all %d concurrent APPEND subtransactions must succeed without any error", N)
        .isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static DataSource simpleDataSource(String url, String user, String password) {
    return new DataSource() {
      @Override
      public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
      }

      @Override
      public Connection getConnection(String u, String p) throws SQLException {
        return DriverManager.getConnection(url, u, p);
      }

      @Override
      public <T> T unwrap(Class<T> iface) {
        return null;
      }

      @Override
      public boolean isWrapperFor(Class<?> iface) {
        return false;
      }

      @Override
      public PrintWriter getLogWriter() {
        return null;
      }

      @Override
      public void setLogWriter(PrintWriter pw) {}

      @Override
      public void setLoginTimeout(int seconds) {}

      @Override
      public int getLoginTimeout() {
        return 0;
      }

      @Override
      public Logger getParentLogger() {
        return null;
      }
    };
  }
}
