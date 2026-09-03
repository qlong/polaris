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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.CommittedTableSnapshotRow;
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
 *       them; a reader that acquires a {@code TxnSnapshot} between the two commits sees a
 *       consistent one-or-the-other view, never a mix.
 *   <li><b>Lock conflict serialization</b>: an EXCLUSIVE lock blocks a conflicting second writer;
 *       after the first transaction aborts the second acquires the lock successfully.
 *   <li><b>ROW_EXCLUSIVE non-blocking</b>: concurrent ROW_EXCLUSIVE operations on the same table
 *       all succeed without blocking each other (OCC path, no lock acquired).
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

  private JdbcTransactionPersistence persistence;
  private TransactionService service;
  private String realm;

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
    realm = "realm-" + UUID.randomUUID();
    persistence =
        new JdbcTransactionPersistence(dataSource, DatabaseType.POSTGRES, realm, CATALOG_ID);
    service = new TransactionService(persistence, new LockManager(persistence));
  }

  /**
   * Builds an independent {@link TransactionService} backed by its own persistence + LockManager on
   * the same DB/realm/catalog — i.e. a second Polaris node sharing the backing database.
   */
  private TransactionService newNodeService() {
    JdbcTransactionPersistence p =
        new JdbcTransactionPersistence(dataSource, DatabaseType.POSTGRES, realm, CATALOG_ID);
    return new TransactionService(p, new LockManager(p));
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
                // commit_scn is allocated by insertCommittedTableSnapshotRow itself (monotonic);
                // the latches enforce commit order: orders (lower scn) before customers (higher).
                persistence.insertCommittedTableSnapshotRow(
                    new CommittedTableSnapshotRow(
                        ordersUuid,
                        JdbcTransactionPersistence.UNASSIGNED_SCN,
                        null,
                        "s3://orders/v2.json"));
                ordersCommitted.countDown(); // signal: orders committed

                assertThat(snapshotAcquired.await(5, TimeUnit.SECONDS))
                    .as("reader should acquire snapshot within 5s")
                    .isTrue();

                persistence.insertCommittedTableSnapshotRow(
                    new CommittedTableSnapshotRow(
                        customersUuid,
                        JdbcTransactionPersistence.UNASSIGNED_SCN,
                        null,
                        "s3://customers/v2.json"));
              } catch (Throwable e) {
                writerErrors.add(e);
              }
            });
    writer.start();

    // Reader: wait for orders commit, acquire snapshot, signal writer to proceed
    assertThat(ordersCommitted.await(5, TimeUnit.SECONDS)).isTrue();
    var snap = service.createTransactionSnapshot(300);
    snapshotAcquired.countDown();
    writer.join(5_000);
    assertThat(writerErrors).isEmpty();

    // At snap: orders committed before snap (visible), customers committed after (not visible)
    Optional<String> ordersAtSnap = service.resolveTableMetadataLocation(ordersUuid, snap.handle());
    Optional<String> customersAtSnap =
        service.resolveTableMetadataLocation(customersUuid, snap.handle());

    assertThat(ordersAtSnap).as("orders should be visible at the pinned snapshot").isPresent();
    assertThat(ordersAtSnap.get()).isEqualTo("s3://orders/v2.json");
    assertThat(customersAtSnap)
        .as("customers committed AFTER the snapshot was acquired — must NOT be visible")
        .isEmpty();

    // A fresh snapshot (acquired after both commits) sees both — confirming the data is there
    var freshSnap = service.createTransactionSnapshot(300);
    assertThat(service.resolveTableMetadataLocation(ordersUuid, freshSnap.handle()))
        .hasValue("s3://orders/v2.json");
    assertThat(service.resolveTableMetadataLocation(customersUuid, freshSnap.handle()))
        .hasValue("s3://customers/v2.json");
  }

  // ---------------------------------------------------------------------------
  // Test 2: Lock conflict serialization
  // ---------------------------------------------------------------------------

  /**
   * Scenario: T1 holds an EXCLUSIVE lock on a table. T2 tries to acquire the same lock — it must
   * fail with {@link LockManager.LockConflictException} immediately (no wait). After T1 aborts and
   * releases its lock, T2 retries and succeeds.
   */
  @Test
  void exclusiveLockBlocksConflictingWriterThenReleasesOnAbort() throws Exception {
    String tableUuid = "table-" + UUID.randomUUID();

    // T1 acquires EXCLUSIVE lock by creating an operation
    var txn1 = service.beginTransaction("REPEATABLE_READ", 3600, "owner1");
    service.createOperation(
        txn1.txnId(),
        txn1.txnToken(),
        tableUuid,
        "EXCLUSIVE",
        null,
        null,
        3600,
        TABLE_METADATA_DIR);

    // T2 tries EXCLUSIVE on the same resource — must throw immediately
    var txn2 = service.beginTransaction("REPEATABLE_READ", 3600, "owner2");
    assertThatThrownBy(
            () ->
                service.createOperation(
                    txn2.txnId(),
                    txn2.txnToken(),
                    tableUuid,
                    "EXCLUSIVE",
                    null,
                    null,
                    3600,
                    TABLE_METADATA_DIR))
        .isInstanceOf(LockManager.LockConflictException.class);

    // T1 aborts — releases its lock
    service.abortTransaction(txn1.txnId(), txn1.txnToken());

    // T2 retries with a fresh transaction and succeeds
    var txn2b = service.beginTransaction("REPEATABLE_READ", 3600, "owner2");
    var op2 =
        service.createOperation(
            txn2b.txnId(),
            txn2b.txnToken(),
            tableUuid,
            "EXCLUSIVE",
            null,
            null,
            3600,
            TABLE_METADATA_DIR);

    assertThat(op2.status())
        .as("after T1 releases its lock, T2 must acquire EXCLUSIVE successfully")
        .isEqualTo(JdbcTransactionPersistence.OP_STATUS_IN_PROGRESS);
  }

  /**
   * Under the 3-mode lock protocol, ROW_EXCLUSIVE records a lock row and is incompatible with a
   * held EXCLUSIVE. Requesting ROW_EXCLUSIVE while another transaction holds EXCLUSIVE fails fast
   * with {@link LockManager.LockConflictException} (no retry hint) rather than blocking or being
   * silently granted.
   */
  @Test
  void rowExclusiveFailsFastWhenExclusiveLockHeld() throws Exception {
    String tableUuid = "table-" + UUID.randomUUID();

    // T1 holds EXCLUSIVE lock
    var txn1 = service.beginTransaction("REPEATABLE_READ", 3600, "owner1");
    service.createOperation(
        txn1.txnId(),
        txn1.txnToken(),
        tableUuid,
        "EXCLUSIVE",
        null,
        null,
        3600,
        TABLE_METADATA_DIR);

    // T2 ROW_EXCLUSIVE on the same table — must fail fast while EXCLUSIVE is held.
    var txn2 = service.beginTransaction("REPEATABLE_READ", 3600, "owner2");
    assertThatThrownBy(
            () ->
                service.createOperation(
                    txn2.txnId(),
                    txn2.txnToken(),
                    tableUuid,
                    "ROW_EXCLUSIVE",
                    null,
                    null,
                    3600,
                    TABLE_METADATA_DIR))
        .isInstanceOf(LockManager.LockConflictException.class);
  }

  // ---------------------------------------------------------------------------
  // Test 3: ROW_EXCLUSIVE non-blocking — N concurrent writers never conflict
  // ---------------------------------------------------------------------------

  /**
   * Five threads each start their own transaction and create a ROW_EXCLUSIVE operation on the same
   * table. All must succeed without any {@link LockManager.LockConflictException} because
   * ROW_EXCLUSIVE never calls {@link LockManager#acquire} (OCC path).
   */
  @Test
  void concurrentRowExclusiveOpsOnSameTableAllSucceed() throws Exception {
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
                  var txn = service.beginTransaction("REPEATABLE_READ", 3600, "writer-" + idx);
                  var op =
                      service.createOperation(
                          txn.txnId(),
                          txn.txnToken(),
                          tableUuid,
                          "ROW_EXCLUSIVE",
                          null,
                          null,
                          3600,
                          TABLE_METADATA_DIR);
                  assertThat(op.status())
                      .isEqualTo(JdbcTransactionPersistence.OP_STATUS_IN_PROGRESS);
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
        .as("all %d concurrent ROW_EXCLUSIVE operations must succeed without any error", N)
        .isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Test 4: commit_scn is a transactional, serialized, monotonic commit clock
  // ---------------------------------------------------------------------------

  /**
   * N threads each open a transaction, allocate a {@code commit_scn}, insert a CTS row, and commit.
   * The per-catalog counter row lock serializes the allocate→commit intervals, so every thread must
   * receive a <b>distinct</b> scn and, after all commit, {@code captureReadPoint()} equals the
   * highest scn — no gaps from aborts, no duplicates.
   */
  @Test
  void commitScnAllocationIsSerializedAndMonotonic() throws Exception {
    int n = 8;
    List<Long> scns = Collections.synchronizedList(new ArrayList<>());
    List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      final int idx = i;
      Thread t =
          new Thread(
              () -> {
                try (Connection conn = dataSource.getConnection()) {
                  conn.setAutoCommit(false);
                  long scn = persistence.allocateCommitScn(conn);
                  persistence.insertCommittedTableSnapshot(
                      conn,
                      new CommittedTableSnapshotRow(
                          "tbl-" + idx, scn, null, "s3://t/" + idx + ".json"));
                  conn.commit();
                  scns.add(scn);
                } catch (Throwable e) {
                  errors.add(e);
                }
              });
      threads.add(t);
    }
    threads.forEach(Thread::start);
    for (Thread t : threads) {
      t.join(20_000);
    }

    assertThat(errors).as("no thread should error allocating commit_scn").isEmpty();
    assertThat(scns).hasSize(n);
    assertThat(scns.stream().distinct().count())
        .as("every commit must receive a distinct commit_scn (serialized on the counter row)")
        .isEqualTo((long) n);
    assertThat(persistence.captureReadPoint())
        .as("read_point after all commits equals the highest allocated commit_scn")
        .isEqualTo(scns.stream().mapToLong(Long::longValue).max().orElse(-1));
  }

  /**
   * The assign-before-visible window is closed: while a committer holds the counter lock (scn
   * allocated but transaction not yet committed), a concurrent reader's {@code captureReadPoint()}
   * must NOT observe that scn — it reads the last <em>committed</em> counter value. Only after the
   * committer commits does a fresh read_point include it.
   */
  @Test
  void readPointNeverExceedsNotYetCommittedScn() throws Exception {
    long before = persistence.captureReadPoint();

    CountDownLatch allocated = new CountDownLatch(1);
    CountDownLatch readerDone = new CountDownLatch(1);
    List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
    long[] committerScn = new long[1];

    Thread committer =
        new Thread(
            () -> {
              try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                committerScn[0] = persistence.allocateCommitScn(conn); // holds counter row lock
                persistence.insertCommittedTableSnapshot(
                    conn,
                    new CommittedTableSnapshotRow(
                        "tbl-window", committerScn[0], null, "s3://t/window.json"));
                allocated.countDown();
                assertThat(readerDone.await(10, TimeUnit.SECONDS)).isTrue();
                conn.commit(); // scn + row become visible together here
              } catch (Throwable e) {
                errors.add(e);
              }
            });
    committer.start();

    assertThat(allocated.await(10, TimeUnit.SECONDS)).isTrue();
    // Committer has allocated (and holds the lock) but has NOT committed. A reader reads the last
    // committed value — never the in-flight scn.
    long midReadPoint = persistence.captureReadPoint();
    readerDone.countDown();
    committer.join(10_000);

    assertThat(errors).isEmpty();
    assertThat(committerScn[0]).isGreaterThan(before);
    assertThat(midReadPoint)
        .as("read_point during an in-flight commit must not include the uncommitted commit_scn")
        .isEqualTo(before);
    assertThat(persistence.captureReadPoint())
        .as("after the committer commits, a fresh read_point includes its commit_scn")
        .isEqualTo(committerScn[0]);
  }

  // ---------------------------------------------------------------------------
  // Test 5: multi-node lock-acquisition serialization (anchor-row FOR UPDATE)
  // ---------------------------------------------------------------------------

  /**
   * Two independent Polaris nodes (separate {@link TransactionService}/{@link LockManager}
   * instances, distinct connections — the old per-JVM mutex cannot help across them) race to
   * acquire an EXCLUSIVE lock on the <b>same</b> table at the same instant. The per-resource {@code
   * lock_resource} anchor (SELECT ... FOR UPDATE) must serialize the grant decision so that
   * <b>exactly one</b> node is granted and the other gets a {@link
   * LockManager.LockConflictException}.
   *
   * <p>Before the anchor fix this test fails: both nodes observe zero conflicts and both insert an
   * EXCLUSIVE lock row (phantom), granting two exclusive locks on one table.
   */
  @Test
  void concurrentExclusiveAcquireAcrossNodesGrantsExactlyOne() throws Exception {
    String tableUuid = "table-" + UUID.randomUUID();
    int nodes = 6;

    CyclicBarrier startLine = new CyclicBarrier(nodes);
    java.util.concurrent.atomic.AtomicInteger granted =
        new java.util.concurrent.atomic.AtomicInteger();
    java.util.concurrent.atomic.AtomicInteger conflicts =
        new java.util.concurrent.atomic.AtomicInteger();
    List<Throwable> unexpected = Collections.synchronizedList(new ArrayList<>());
    List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      final int idx = i;
      Thread t =
          new Thread(
              () -> {
                TransactionService node = newNodeService();
                try {
                  var txn = node.beginTransaction("REPEATABLE_READ", 3600, "node-" + idx);
                  startLine.await(10, TimeUnit.SECONDS); // all nodes fire acquire together
                  node.createOperation(
                      txn.txnId(),
                      txn.txnToken(),
                      tableUuid,
                      "EXCLUSIVE",
                      null,
                      null,
                      3600,
                      TABLE_METADATA_DIR);
                  granted.incrementAndGet();
                } catch (LockManager.LockConflictException e) {
                  conflicts.incrementAndGet();
                } catch (Throwable e) {
                  unexpected.add(e);
                }
              });
      threads.add(t);
    }
    threads.forEach(Thread::start);
    for (Thread t : threads) {
      t.join(30_000);
    }

    assertThat(unexpected).as("no unexpected errors").isEmpty();
    assertThat(granted.get())
        .as("exactly one node may hold EXCLUSIVE on the same table")
        .isEqualTo(1);
    assertThat(conflicts.get())
        .as("every other node must observe a lock conflict")
        .isEqualTo(nodes - 1);
    // The DB agrees: exactly one granted EXCLUSIVE lock row exists for the resource.
    assertThat(persistence.findGrantedLocksOnResource(tableUuid, -1L))
        .as("exactly one granted lock row in the DB")
        .hasSize(1);
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
