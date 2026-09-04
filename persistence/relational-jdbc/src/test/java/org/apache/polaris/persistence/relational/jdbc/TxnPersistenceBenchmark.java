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
package org.apache.polaris.persistence.relational.jdbc;

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.polaris.test.commons.PostgresRelationalJdbcLifeCycleManagement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Direct-DB benchmark for {@link JdbcTransactionPersistence}.
 *
 * <p>Spins up a Postgres container via Testcontainers and exercises each hot path at varying
 * concurrency levels. Measures throughput (ops/s) and latency percentiles (p50/p95/p99). Each
 * scenario is scoped to a single real client→server request boundary, so a row equals a latency the
 * client actually observes — begin-time and commit-time are measured separately because the Spark
 * client fires them from different SQL statements (see {@code TransactionManager}).
 *
 * <ul>
 *   <li>Plain commit baseline — allocateCommitScn + insertCommittedTableSnapshot (1 table)
 *   <li>beginTransaction request — insertTransaction + captureReadPoint + insertTxnSnapshot (fired
 *       lazily on the first loadTable inside a write txn)
 *   <li>commitTransaction request (single table) — allocateCommitScn + insertCTS + commit + 2 tail
 *       updates (fired on COMMIT)
 *   <li>Multi-table commitTransaction — N×(allocate scn + insert CTS) in one DB txn + 2 tail
 *       (1/2/4/8)
 *   <li>Subtxn protocol (parallel workers) — N concurrent staging workers + orchestrator commit
 *   <li>REPEATABLE READ loadTable — captureReadPoint + resolveTableSnapshot
 *   <li>READ COMMITTED loadTable — currentCommitScn only (resolveTableSnapshot skipped)
 *   <li>Sequence contention — allocateCommitScn in isolation
 * </ul>
 *
 * <p>The benchmark runs without the Polaris server, so {@code TransactionReaper} is not active.
 * After each scenario the benchmark calls {@link #runReaper()} to purge accumulated CTS rows and
 * keep table-scan costs stable across scenarios.
 *
 * <p>Run manually:
 *
 * <pre>
 *   # Small smoke run — verify connectivity before committing to full load
 *   POLARIS_BENCH=true POLARIS_BENCH_ITER=100 POLARIS_BENCH_WARMUP=10 \
 *     ./gradlew :polaris-relational-jdbc:test --tests "*.TxnPersistenceBenchmark"
 *
 *   # Full load
 *   POLARIS_BENCH=true POLARIS_BENCH_ITER=2000 POLARIS_BENCH_WARMUP=200 \
 *     ./gradlew :polaris-relational-jdbc:test --tests "*.TxnPersistenceBenchmark"
 * </pre>
 *
 * <p>Skipped in CI — set {@code POLARIS_BENCH=true} to run.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TxnPersistenceBenchmark {

  private static final String REALM = "bench-realm";
  private static final long CATALOG_ID = 9_999_999L;

  // Gradle forks a separate JVM for tests; -D properties on the Gradle command line are
  // not automatically forwarded to the test worker. Use environment variables instead.
  //   POLARIS_BENCH=true POLARIS_BENCH_ITER=2000 POLARIS_BENCH_WARMUP=200 ./gradlew ...
  private static final boolean ENABLED =
      "true".equalsIgnoreCase(System.getenv("POLARIS_BENCH"))
          || "true".equalsIgnoreCase(System.getProperty("polaris.bench.enabled"));
  private static final int ITERATIONS =
      System.getenv("POLARIS_BENCH_ITER") != null
          ? Integer.parseInt(System.getenv("POLARIS_BENCH_ITER"))
          : Integer.getInteger("polaris.bench.iterations", 2000);
  private static final int WARMUP =
      System.getenv("POLARIS_BENCH_WARMUP") != null
          ? Integer.parseInt(System.getenv("POLARIS_BENCH_WARMUP"))
          : Integer.getInteger("polaris.bench.warmup", 200);
  private static final int[] CONCURRENCY_LEVELS = {1, 4, 16, 32};
  private static final int[] MULTI_TABLE_SIZES = {1, 2, 4, 8};

  @Container
  @SuppressWarnings("resource")
  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer(
              containerSpecHelper("postgres", PostgresRelationalJdbcLifeCycleManagement.class)
                  .dockerImageName(null)
                  .asCompatibleSubstituteFor("postgres"))
          // Raise max_connections so the benchmark pool (up to ~256 at c=32) fits.
          .withCommand("postgres", "-c", "max_connections=500");

  private DataSource dataSource;
  private JdbcTransactionPersistence persistence;

  private final List<String> report = new ArrayList<>();

  // ---------------------------------------------------------------------------
  // Functional interface for connection-per-thread ops
  // ---------------------------------------------------------------------------

  @FunctionalInterface
  interface ConnOp {
    void run(Connection conn) throws Exception;
  }

  @FunctionalInterface
  interface NoConnOp {
    void run() throws Exception;
  }

  // ---------------------------------------------------------------------------
  // Setup / teardown
  // ---------------------------------------------------------------------------

  @BeforeAll
  void setUp() throws Exception {
    assumeTrue(ENABLED, "Benchmark skipped — set POLARIS_BENCH=true env var to run");

    PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setURL(POSTGRES.getJdbcUrl());
    ds.setUser(POSTGRES.getUsername());
    ds.setPassword(POSTGRES.getPassword());
    dataSource = ds;

    // Drop and recreate all tables so re-runs start from a clean slate.
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(true);
      try (var stmt = conn.createStatement()) {
        stmt.execute(
            "DROP TABLE IF EXISTS"
                + " committed_table_snapshots,"
                + " txn_snapshots,"
                + " txn_watermarks,"
                + " transaction_operations,"
                + " subtransactions,"
                + " transactions CASCADE");
      }
    }
    DatasourceOperations ops = new DatasourceOperations(dataSource, new BenchJdbcConfig());
    InputStream schema =
        DatasourceOperations.class.getClassLoader().getResourceAsStream("postgres/schema-v5.sql");
    ops.executeScript(schema);

    persistence =
        new JdbcTransactionPersistence(dataSource, DatabaseType.POSTGRES, REALM, CATALOG_ID);

    // Pre-seed 50 committed snapshots so resolveTableSnapshot has rows to scan.
    seedCommittedSnapshots(50);
  }

  @AfterAll
  void printReport() throws Exception {
    if (report.isEmpty()) return;
    var sb = new StringBuilder();
    sb.append("\n");
    sb.append("=".repeat(98)).append("\n");
    sb.append("  Polaris Transaction Persistence Benchmark\n");
    sb.append(String.format("  iterations=%d  warmup=%d%n", ITERATIONS, WARMUP));
    sb.append("=".repeat(98)).append("\n");
    sb.append(
        String.format(
            "%-52s  %6s  %6s  %6s  %6s  %6s  %8s%n",
            "Scenario", "conc", "p50µs", "p95µs", "p99µs", "stdµs", "ops/s"));
    sb.append("-".repeat(98)).append("\n");
    report.forEach(line -> sb.append(line).append("\n"));
    sb.append("=".repeat(98)).append("\n");
    String reportText = sb.toString();
    System.out.println(reportText);
    // Also write to /tmp so it survives Gradle's output buffering.
    java.nio.file.Files.writeString(
        java.nio.file.Path.of("/tmp/polaris-bench-report.txt"), reportText);
  }

  // ---------------------------------------------------------------------------
  // Scenarios
  // ---------------------------------------------------------------------------

  /**
   * Baseline: single allocateCommitScn + insertCommittedTableSnapshot, no surrounding transaction
   * state. This is the minimum overhead added to any write path that goes through CTS.
   */
  @Test
  void bench_01_plainCommit() throws Exception {
    String tableUuid = UUID.randomUUID().toString();
    for (int c : CONCURRENCY_LEVELS) {
      runWithConn(
          "plain commit (1 table)",
          c,
          conn -> {
            conn.setAutoCommit(false);
            long scn = persistence.allocateCommitScn(conn);
            persistence.insertCommittedTableSnapshot(
                conn,
                new JdbcTransactionPersistence.CommittedTableSnapshotRow(
                    tableUuid, scn, null, "s3://bench/meta/" + scn + ".json"));
            conn.commit();
          });
    }
    runReaper();
  }

  /**
   * Sequence contention: just the commit_scn counter increment in isolation. Isolates the
   * single-row serialization point — the bottleneck under high concurrent write load.
   */
  @Test
  void bench_02_allocateCommitScnContention() throws Exception {
    for (int c : CONCURRENCY_LEVELS) {
      runWithConn(
          "allocateCommitScn (sequence contention)",
          c,
          conn -> {
            conn.setAutoCommit(false);
            persistence.allocateCommitScn(conn);
            conn.commit();
          });
    }
    runReaper();
  }

  /**
   * beginTransaction request (REPEATABLE_READ) — the server work behind the client's <em>first</em>
   * {@code loadTable} inside a write transaction. Three autocommit round-trips: insertTransaction +
   * captureReadPoint + insertTxnSnapshot. This is a distinct client-facing latency from the commit;
   * in the Spark flow BEGIN itself makes no server call and this fires lazily on the first table
   * load (see {@code TransactionManager.ensureTransactionStarted}). None of the three rows conflict
   * with any other transaction, so this scales flat with concurrency.
   */
  @Test
  void bench_03a_beginTransaction() throws Exception {
    for (int c : CONCURRENCY_LEVELS) {
      runNoConn(
          "beginTransaction (RR)",
          c,
          () -> {
            long txnId = persistence.nextTxnId();
            long snapId = persistence.nextTxnId();
            Instant now = Instant.now();
            Instant expires = now.plusSeconds(3600);
            persistence.insertTransaction(
                new JdbcTransactionPersistence.TransactionRow(
                    txnId,
                    null, // parentTxnId
                    REALM,
                    CATALOG_ID,
                    "tok-" + txnId,
                    "bench-user",
                    JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
                    "REPEATABLE_READ",
                    snapId,
                    now,
                    expires,
                    null));
            long readPoint = persistence.captureReadPoint();
            persistence.insertTxnSnapshot(
                new JdbcTransactionPersistence.TxnSnapshotRow(
                    snapId, REALM, CATALOG_ID, readPoint, now, expires, null));
          });
    }
    runReaper();
  }

  /**
   * beginTransaction request (REPEATABLE_READ) — <em>collapsed</em> path. Same logical work as
   * {@link #bench_03a_beginTransaction} (recapture read_point + insert snapshot + insert
   * transaction) but folded into a single {@code beginTransactionAtomically} call: one pooled
   * connection, one DB transaction, instead of three autocommit round-trips each borrowing and
   * returning a connection. The pool is what saturates first under write concurrency (disjoint PKs
   * never lock-contend), so this is the throughput lever — compare directly against {@code
   * beginTransaction (RR)} above at each concurrency level.
   */
  @Test
  void bench_03a2_beginTransactionAtomically() throws Exception {
    for (int c : CONCURRENCY_LEVELS) {
      runNoConn(
          "beginTransaction (RR) collapsed",
          c,
          () -> {
            long txnId = persistence.nextTxnId();
            long snapId = persistence.nextTxnId();
            Instant now = Instant.now();
            Instant expires = now.plusSeconds(3600);
            JdbcTransactionPersistence.TxnSnapshotRow snapRow =
                new JdbcTransactionPersistence.TxnSnapshotRow(
                    snapId, REALM, CATALOG_ID, 0L, now, expires, null);
            JdbcTransactionPersistence.TransactionRow txnRow =
                new JdbcTransactionPersistence.TransactionRow(
                    txnId,
                    null, // parentTxnId
                    REALM,
                    CATALOG_ID,
                    "tok-" + txnId,
                    "bench-user",
                    JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
                    "REPEATABLE_READ",
                    snapId,
                    now,
                    expires,
                    null);
            persistence.beginTransactionAtomically(txnRow, snapRow);
          });
    }
    runReaper();
  }

  /**
   * commitTransaction request (single table) — the server work behind the client's {@code COMMIT}.
   * Allocates the commit SCN (the per-catalog serialization point), inserts one CTS row, commits,
   * then updates status + releases the snapshot in two tail round-trips that run <em>after</em> the
   * pointer swap (so they do not extend the SCN-lock window). This is the second distinct
   * client-facing latency; compare against bench_01 (plain commit) — the delta is the SCN lock +
   * the two tail updates.
   */
  @Test
  void bench_03b_commitTransaction() throws Exception {
    String tableUuid = UUID.randomUUID().toString();
    for (int c : CONCURRENCY_LEVELS) {
      runWithConn(
          "commitTransaction (1 table)",
          c,
          conn -> {
            // The transaction + snapshot rows already exist by commit time (bench_03a measures
            // their
            // insert). Allocate ids only so the tail updates target real rows without contending.
            long txnId = persistence.nextTxnId();
            long snapId = persistence.nextTxnId();
            Instant now = Instant.now();
            Instant expires = now.plusSeconds(3600);
            persistence.insertTransaction(
                new JdbcTransactionPersistence.TransactionRow(
                    txnId,
                    null,
                    REALM,
                    CATALOG_ID,
                    "tok-" + txnId,
                    "bench-user",
                    JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
                    "REPEATABLE_READ",
                    snapId,
                    now,
                    expires,
                    null));
            persistence.insertTxnSnapshot(
                new JdbcTransactionPersistence.TxnSnapshotRow(
                    snapId, REALM, CATALOG_ID, 0L, now, expires, null));

            // commitTransaction: allocate scn + insert CTS row + commit (the SCN-locked section)
            conn.setAutoCommit(false);
            long scn = persistence.allocateCommitScn(conn);
            persistence.insertCommittedTableSnapshot(
                conn,
                new JdbcTransactionPersistence.CommittedTableSnapshotRow(
                    tableUuid, scn, null, "s3://bench/meta/" + scn + ".json"));
            conn.commit();

            // tail (after pointer swap, outside the SCN lock): update status + release snapshot
            persistence.updateTransactionStatus(
                txnId, JdbcTransactionPersistence.TXN_STATUS_COMMITTED);
            persistence.releaseTxnSnapshot(snapId);
          });
    }
    runReaper();
  }

  /**
   * Multi-table subtransaction: begin + N×(allocate scn + insert CTS) in one DB transaction +
   * commitTxn. Reveals how cost scales with table count, and whether the single commit_scn counter
   * is the limiting factor at N > 1.
   */
  @Test
  void bench_04_multiTableSubtxn() throws Exception {
    for (int n : MULTI_TABLE_SIZES) {
      List<String> tableUuids = new ArrayList<>();
      for (int i = 0; i < n; i++) tableUuids.add(UUID.randomUUID().toString());

      for (int c : CONCURRENCY_LEVELS) {
        runWithConn(
            "multi-table txn (" + n + " tables)",
            c,
            conn -> {
              long txnId = persistence.nextTxnId();
              long snapId = persistence.nextTxnId();
              Instant now = Instant.now();
              Instant expires = now.plusSeconds(3600);

              persistence.insertTransaction(
                  new JdbcTransactionPersistence.TransactionRow(
                      txnId,
                      null,
                      REALM,
                      CATALOG_ID,
                      "tok-" + txnId,
                      "bench-user",
                      JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
                      "REPEATABLE_READ",
                      snapId,
                      now,
                      expires,
                      null));
              long readPoint = persistence.captureReadPoint();
              persistence.insertTxnSnapshot(
                  new JdbcTransactionPersistence.TxnSnapshotRow(
                      snapId, REALM, CATALOG_ID, readPoint, now, expires, null));

              // Commit all N tables atomically in a single DB transaction
              conn.setAutoCommit(false);
              for (String uuid : tableUuids) {
                long scn = persistence.allocateCommitScn(conn);
                persistence.insertCommittedTableSnapshot(
                    conn,
                    new JdbcTransactionPersistence.CommittedTableSnapshotRow(
                        uuid, scn, null, "s3://bench/meta/" + scn + ".json"));
              }
              conn.commit();

              persistence.updateTransactionStatus(
                  txnId, JdbcTransactionPersistence.TXN_STATUS_COMMITTED);
              persistence.releaseTxnSnapshot(snapId);
            });
      }
      runReaper();
    }
  }

  /**
   * REPEATABLE READ read path: captureReadPoint + resolveTableSnapshot (index scan on CTS). This is
   * what happens on every loadTable inside a transaction — the additional DB cost vs. a plain read.
   */
  @Test
  void bench_05_repeatableReadResolve() throws Exception {
    String tableUuid = seedOneTable();
    for (int c : CONCURRENCY_LEVELS) {
      runNoConn(
          "REPEATABLE READ resolve (1 table)",
          c,
          () -> {
            long readPoint = persistence.captureReadPoint();
            persistence.resolveTableSnapshot(tableUuid, readPoint);
          });
    }
    runReaper();
  }

  /**
   * REPEATABLE READ resolve under concurrent write load: same index scan, but with 8 background
   * writer threads hammering allocateCommitScn + insertCommittedTableSnapshot. Reveals whether the
   * write-side serialization degrades read latency.
   *
   * <p>All writer connections are opened on the main thread before workers start, then handed to
   * writer threads via a pool, to avoid the sandbox restriction on worker-thread TCP connects.
   */
  @Test
  void bench_06_repeatableReadUnderWriteLoad() throws Exception {
    String tableUuid = seedOneTable();
    String writerUuid = UUID.randomUUID().toString();

    // Open writer connections on the main thread.
    int numWriters = 8;
    Connection[] writerConns = new Connection[numWriters];
    for (int i = 0; i < numWriters; i++) writerConns[i] = dataSource.getConnection();

    AtomicInteger keepWriting = new AtomicInteger(1);
    ExecutorService writers = Executors.newFixedThreadPool(numWriters);
    List<Future<?>> writerFutures = new ArrayList<>(numWriters);
    for (int i = 0; i < numWriters; i++) {
      Connection wconn = writerConns[i];
      writerFutures.add(
          writers.submit(
              () -> {
                while (keepWriting.get() == 1) {
                  try {
                    wconn.setAutoCommit(false);
                    long scn = persistence.allocateCommitScn(wconn);
                    persistence.insertCommittedTableSnapshot(
                        wconn,
                        new JdbcTransactionPersistence.CommittedTableSnapshotRow(
                            writerUuid, scn, null, "s3://bench/bg/" + scn + ".json"));
                    wconn.commit();
                  } catch (SQLException ignored) {
                  }
                }
                return null;
              }));
    }

    for (int c : new int[] {1, 4, 16}) {
      runNoConn(
          "REPEATABLE READ resolve (8 bg writers)",
          c,
          () -> {
            long readPoint = persistence.captureReadPoint();
            persistence.resolveTableSnapshot(tableUuid, readPoint);
          });
    }

    keepWriting.set(0);
    writers.shutdown();
    writers.awaitTermination(15, TimeUnit.SECONDS);
    for (Future<?> f : writerFutures) {
      if (f.isDone()) {
        try {
          f.get();
        } catch (Exception ignored) {
        }
      }
    }
    for (Connection c : writerConns) {
      try {
        c.close();
      } catch (SQLException ignored) {
      }
    }
    runReaper();
  }

  /**
   * READ COMMITTED read path: just captureReadPoint. No CTS lookup happens — the client falls
   * through to the live entity read. Measures the marginal cost of even the cheapest consistent
   * read.
   */
  @Test
  void bench_07_readCommittedCapture() throws Exception {
    for (int c : CONCURRENCY_LEVELS) {
      runNoConn("READ COMMITTED (captureReadPoint only)", c, () -> persistence.captureReadPoint());
    }
    runReaper();
  }

  /**
   * Subtransaction protocol — parallel workers model.
   *
   * <p>Models the real deployment: one orchestrator thread begins the transaction, then N staging
   * workers run concurrently (each creates its subtxn, inserts its op, and marks it staged), then
   * the orchestrator commits all N tables atomically. Wall-clock latency per iteration is
   * max(staging workers) + commitTransaction — not N × sequential staging.
   *
   * <p>Outer concurrency (c) = simultaneous orchestrators, driving SCN-lock contention the same way
   * bench_04 does. Inner worker count = N tables. Results are directly comparable to bench_04
   * (inline multi-table commit without subtxn rows).
   */
  @Test
  void bench_08_subtransactionProtocol() throws Exception {
    // Inner staging workers are non-blocking DB calls; a cached pool is fine here
    // because the JVM thread count is bounded by c × N and always small.
    ExecutorService stagingPool = Executors.newCachedThreadPool();
    try {
      for (int n : MULTI_TABLE_SIZES) {
        List<String> tableUuids = new ArrayList<>();
        for (int i = 0; i < n; i++) tableUuids.add(UUID.randomUUID().toString());

        for (int c : CONCURRENCY_LEVELS) {
          runWithConn(
              "subtxn protocol (" + n + " tables)",
              c,
              conn -> {
                long txnId = persistence.nextTxnId();
                long snapId = persistence.nextTxnId();
                Instant now = Instant.now();
                Instant expires = now.plusSeconds(3600);

                // orchestrator: begin transaction
                persistence.insertTransaction(
                    new JdbcTransactionPersistence.TransactionRow(
                        txnId,
                        null,
                        REALM,
                        CATALOG_ID,
                        "tok-" + txnId,
                        "bench-user",
                        JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
                        "REPEATABLE_READ",
                        snapId,
                        now,
                        expires,
                        null));
                long readPoint = persistence.captureReadPoint();
                persistence.insertTxnSnapshot(
                    new JdbcTransactionPersistence.TxnSnapshotRow(
                        snapId, REALM, CATALOG_ID, readPoint, now, expires, null));

                // N staging workers run concurrently — one per table
                List<Future<String>> stagingFutures = new ArrayList<>(n);
                for (String tableUuid : tableUuids) {
                  stagingFutures.add(
                      stagingPool.submit(
                          () -> {
                            long subtxnId = persistence.nextTxnId();
                            long opId = persistence.nextTxnId();
                            persistence.insertSubtxn(
                                new JdbcTransactionPersistence.SubtxnRow(
                                    subtxnId,
                                    txnId,
                                    REALM,
                                    CATALOG_ID,
                                    "subtxn-" + subtxnId,
                                    "stok-" + subtxnId,
                                    JdbcTransactionPersistence.SUBTXN_STATUS_OPEN,
                                    0,
                                    now,
                                    expires,
                                    null));
                            persistence.insertOperation(
                                new JdbcTransactionPersistence.OperationRow(
                                    opId,
                                    txnId,
                                    subtxnId,
                                    REALM,
                                    CATALOG_ID,
                                    tableUuid,
                                    "WRITE",
                                    null,
                                    null,
                                    null,
                                    "s3://bench/staging/" + opId,
                                    null,
                                    "otok-" + opId,
                                    JdbcTransactionPersistence.OP_STATUS_IN_PROGRESS,
                                    null,
                                    now,
                                    expires,
                                    null));
                            persistence.updateSubtxnStatus(
                                subtxnId, JdbcTransactionPersistence.SUBTXN_STATUS_STAGED);
                            String metaLoc = "s3://bench/meta/" + opId + ".json";
                            persistence.markOperationCommitted(opId, metaLoc);
                            return metaLoc;
                          }));
                }

                // wait for all staging workers, collect their metadata locations
                List<String> metaLocs = new ArrayList<>(n);
                for (Future<String> f : stagingFutures) {
                  metaLocs.add(f.get());
                }

                // orchestrator: commitTransaction — allocate scn + N×insertCTS + commit
                conn.setAutoCommit(false);
                for (int i = 0; i < n; i++) {
                  long scn = persistence.allocateCommitScn(conn);
                  persistence.insertCommittedTableSnapshot(
                      conn,
                      new JdbcTransactionPersistence.CommittedTableSnapshotRow(
                          tableUuids.get(i), scn, null, metaLocs.get(i)));
                }
                conn.commit();

                persistence.updateTransactionStatus(
                    txnId, JdbcTransactionPersistence.TXN_STATUS_COMMITTED);
                persistence.releaseTxnSnapshot(snapId);
              });
        }
        runReaper();
      }
    } finally {
      stagingPool.shutdown();
      stagingPool.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  // ---------------------------------------------------------------------------
  // Harness
  // ---------------------------------------------------------------------------

  /**
   * Run scenario where each thread reuses one pre-opened connection for all ops.
   *
   * <p>All connections are opened on the main test thread (before workers start) to work around the
   * test-worker sandbox that blocks TCP connects from spawned threads.
   */
  private void runWithConn(String scenario, int concurrency, ConnOp op) throws Exception {
    runBatch(concurrency, WARMUP, op, null);
    long[] latencies = runBatch(concurrency, ITERATIONS, op, null);
    recordAndPrint(scenario, concurrency, latencies);
  }

  /**
   * Run scenario that calls persistence methods which open their own connections internally.
   *
   * <p>Wraps {@code dataSource} in a {@link PooledDataSource} pre-populated with connections opened
   * on the main thread, so workers' internal {@code dataSource.getConnection()} calls never open
   * new sockets from the sandboxed worker thread.
   */
  private void runNoConn(String scenario, int concurrency, NoConnOp op) throws Exception {
    runBatch(concurrency, WARMUP, null, op);
    long[] latencies = runBatch(concurrency, ITERATIONS, null, op);
    recordAndPrint(scenario, concurrency, latencies);
  }

  /**
   * Core batch runner. Exactly one of {@code connOp} / {@code noConnOp} must be non-null.
   *
   * <p>For {@code connOp}: opens {@code concurrency} connections on the calling thread and passes
   * one to each worker, closing them when done.
   *
   * <p>For {@code noConnOp}: opens a pool of {@code max(32, concurrency*8)} connections on the
   * calling thread, installs a {@link PooledDataSource} wrapper as {@code this.dataSource} during
   * the batch (so internal {@code getConnection()} calls borrow from the pool), then restores the
   * original.
   */
  private long[] runBatch(int concurrency, int totalOps, ConnOp connOp, NoConnOp noConnOp)
      throws Exception {
    int opsPerThread = Math.max(1, totalOps / concurrency);

    // Each worker may hold its explicit ConnOp connection plus up to ~5 internal getConnection()
    // calls (insertTransaction, captureReadPoint, insertTxnSnapshot, updateTransactionStatus,
    // releaseTxnSnapshot) concurrently. Size the pool so workers never queue on pool starvation
    // rather than on the DB. 8× gives headroom; minimum 32 covers low-concurrency scenarios.
    int poolSize = Math.max(32, concurrency * 8);
    Connection[] conns = new Connection[poolSize];
    for (int i = 0; i < poolSize; i++) {
      conns[i] = dataSource.getConnection();
    }

    DataSource savedDs = dataSource;
    PooledDataSource pool = new PooledDataSource(conns);
    dataSource = pool;
    // Rebuild persistence with the pooled DataSource so internal calls use it.
    persistence =
        new JdbcTransactionPersistence(dataSource, DatabaseType.POSTGRES, REALM, CATALOG_ID);

    try {
      ExecutorService executor = Executors.newFixedThreadPool(concurrency);
      List<Future<long[]>> futures = new ArrayList<>();

      for (int t = 0; t < concurrency; t++) {
        futures.add(
            executor.submit(
                () -> {
                  long[] times = new long[opsPerThread];
                  if (connOp != null) {
                    // For connOp, each thread uses one dedicated connection from the pool.
                    Connection conn = pool.borrow();
                    try {
                      for (int i = 0; i < opsPerThread; i++) {
                        long start = System.nanoTime();
                        connOp.run(conn);
                        times[i] = (System.nanoTime() - start) / 1_000;
                      }
                    } finally {
                      pool.returnConn(conn);
                    }
                  } else {
                    for (int i = 0; i < opsPerThread; i++) {
                      long start = System.nanoTime();
                      noConnOp.run();
                      times[i] = (System.nanoTime() - start) / 1_000;
                    }
                  }
                  return times;
                }));
      }

      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.MINUTES);
      return collectLatencies(concurrency, opsPerThread, futures);
    } finally {
      dataSource = savedDs;
      persistence =
          new JdbcTransactionPersistence(dataSource, DatabaseType.POSTGRES, REALM, CATALOG_ID);
      for (Connection conn : conns) {
        try {
          conn.close();
        } catch (SQLException ignored) {
        }
      }
    }
  }

  private long[] collectLatencies(int concurrency, int opsPerThread, List<Future<long[]>> futures)
      throws Exception {
    int total = concurrency * opsPerThread;
    long[] all = new long[total];
    int pos = 0;
    for (Future<long[]> f : futures) {
      for (long v : f.get()) all[pos++] = v;
    }
    return all;
  }

  private void recordAndPrint(String scenario, int concurrency, long[] latencies) {
    Arrays.sort(latencies);
    long p50 = percentile(latencies, 0.50);
    long p95 = percentile(latencies, 0.95);
    long p99 = percentile(latencies, 0.99);

    long sumUs = Arrays.stream(latencies).sum();
    double mean = (double) sumUs / latencies.length;
    double variance = 0;
    for (long v : latencies) {
      double d = v - mean;
      variance += d * d;
    }
    long stddev = Math.round(Math.sqrt(variance / latencies.length));

    double wallClockSec = (double) sumUs / 1_000_000.0 / concurrency;
    double opsPerSec = latencies.length / wallClockSec;

    String line =
        String.format(
            "%-52s  %6d  %6d  %6d  %6d  %6d  %8.0f",
            scenario, concurrency, p50, p95, p99, stddev, opsPerSec);
    report.add(line);
    System.out.println(line);
  }

  private static long percentile(long[] sorted, double p) {
    int idx = (int) Math.min(sorted.length - 1, sorted.length * p);
    return sorted[idx];
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private void seedCommittedSnapshots(int count) throws SQLException {
    String tableUuid = UUID.randomUUID().toString();
    try (Connection conn = dataSource.getConnection()) {
      for (int i = 0; i < count; i++) {
        conn.setAutoCommit(false);
        long scn = persistence.allocateCommitScn(conn);
        persistence.insertCommittedTableSnapshot(
            conn,
            new JdbcTransactionPersistence.CommittedTableSnapshotRow(
                tableUuid, scn, null, "s3://bench/seed/" + scn + ".json"));
        conn.commit();
      }
    }
  }

  private String seedOneTable() throws SQLException {
    String uuid = UUID.randomUUID().toString();
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      long scn = persistence.allocateCommitScn(conn);
      persistence.insertCommittedTableSnapshot(
          conn,
          new JdbcTransactionPersistence.CommittedTableSnapshotRow(
              uuid, scn, null, "s3://bench/table/" + uuid + "/meta.json"));
      conn.commit();
    }
    return uuid;
  }

  /**
   * Mirrors {@code TransactionReaper.advanceWatermarkAndPurge} for our single catalog.
   *
   * <p>The server reaper is not active during benchmark runs. Call this between scenarios so CTS
   * rows accumulated by write-path benchmarks don't accumulate and skew later index-scan costs.
   */
  private void runReaper() throws SQLException {
    // Mirror TransactionReaper constants.
    long leaseSeconds = 3600L;
    long skewPadSeconds = 30L;

    long writeFl = persistence.minLiveReadPointForCatalog(); // -1 when no live snapshots
    long readFl = persistence.readFloor(leaseSeconds, skewPadSeconds); // 0 when none old enough

    // Treat write_floor=-1 (no live readers) as +∞ so it doesn't prevent purge.
    long wf = (writeFl < 0) ? Long.MAX_VALUE : writeFl;
    long watermark = Math.min(wf, readFl);
    if (watermark <= 0) return;

    persistence.upsertWatermark(watermark);
    persistence.purgeCommittedSnapshotsBelowWatermark(watermark);
  }

  // ---------------------------------------------------------------------------
  // Minimal blocking pool — all connections pre-opened on the main thread.
  // Hands out PooledConnection wrappers whose close() returns to the queue
  // instead of closing the underlying connection, so try-with-resources in
  // JdbcTransactionPersistence methods recycles rather than destroys connections.
  // ---------------------------------------------------------------------------

  private static class PooledDataSource implements DataSource {
    private final BlockingQueue<Connection> queue;

    PooledDataSource(Connection[] conns) {
      queue = new ArrayBlockingQueue<>(conns.length);
      for (Connection c : conns) queue.add(c);
    }

    Connection borrow() throws SQLException {
      try {
        Connection real = queue.take();
        return new PooledConnection(real, queue);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("interrupted waiting for connection", e);
      }
    }

    void returnConn(Connection conn) throws SQLException {
      // conn is a PooledConnection wrapper; unwrap and return the real connection.
      conn.close();
    }

    @Override
    public Connection getConnection() throws SQLException {
      return borrow();
    }

    @Override
    public Connection getConnection(String u, String p) throws SQLException {
      return borrow();
    }

    @Override
    public PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      throw new SQLException("not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }

  /** Wraps a real connection; {@code close()} returns it to the pool queue. */
  private static class PooledConnection implements Connection {
    private final Connection real;
    private final BlockingQueue<Connection> queue;

    PooledConnection(Connection real, BlockingQueue<Connection> queue) {
      this.real = real;
      this.queue = queue;
    }

    @Override
    public void close() {
      queue.add(real);
    }

    @Override
    public boolean isClosed() throws SQLException {
      return real.isClosed();
    }

    @Override
    public void commit() throws SQLException {
      real.commit();
    }

    @Override
    public void rollback() throws SQLException {
      real.rollback();
    }

    @Override
    public void rollback(java.sql.Savepoint sp) throws SQLException {
      real.rollback(sp);
    }

    @Override
    public void setAutoCommit(boolean ac) throws SQLException {
      real.setAutoCommit(ac);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
      return real.getAutoCommit();
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException {
      return real.prepareStatement(sql);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
        throws SQLException {
      return real.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int[] columnIndexes)
        throws SQLException {
      return real.prepareStatement(sql, columnIndexes);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, String[] columnNames)
        throws SQLException {
      return real.prepareStatement(sql, columnNames);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(
        String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return real.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(
        String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
      return real.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public java.sql.Statement createStatement() throws SQLException {
      return real.createStatement();
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency)
        throws SQLException {
      return real.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public java.sql.Statement createStatement(
        int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return real.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public java.sql.CallableStatement prepareCall(String sql) throws SQLException {
      return real.prepareCall(sql);
    }

    @Override
    public java.sql.CallableStatement prepareCall(
        String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return real.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public java.sql.CallableStatement prepareCall(
        String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
      return real.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public java.sql.DatabaseMetaData getMetaData() throws SQLException {
      return real.getMetaData();
    }

    @Override
    public String getCatalog() throws SQLException {
      return real.getCatalog();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
      real.setCatalog(catalog);
    }

    @Override
    public String getSchema() throws SQLException {
      return real.getSchema();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
      real.setSchema(schema);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
      return real.getTransactionIsolation();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
      real.setTransactionIsolation(level);
    }

    @Override
    public java.sql.SQLWarning getWarnings() throws SQLException {
      return real.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
      real.clearWarnings();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
      return real.isReadOnly();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
      real.setReadOnly(readOnly);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
      return real.nativeSQL(sql);
    }

    @Override
    public java.util.Map<String, Class<?>> getTypeMap() throws SQLException {
      return real.getTypeMap();
    }

    @Override
    public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException {
      real.setTypeMap(map);
    }

    @Override
    public int getHoldability() throws SQLException {
      return real.getHoldability();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
      real.setHoldability(holdability);
    }

    @Override
    public java.sql.Savepoint setSavepoint() throws SQLException {
      return real.setSavepoint();
    }

    @Override
    public java.sql.Savepoint setSavepoint(String name) throws SQLException {
      return real.setSavepoint(name);
    }

    @Override
    public void releaseSavepoint(java.sql.Savepoint savepoint) throws SQLException {
      real.releaseSavepoint(savepoint);
    }

    @Override
    public java.sql.Clob createClob() throws SQLException {
      return real.createClob();
    }

    @Override
    public java.sql.Blob createBlob() throws SQLException {
      return real.createBlob();
    }

    @Override
    public java.sql.NClob createNClob() throws SQLException {
      return real.createNClob();
    }

    @Override
    public java.sql.SQLXML createSQLXML() throws SQLException {
      return real.createSQLXML();
    }

    @Override
    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
      return real.createArrayOf(typeName, elements);
    }

    @Override
    public java.sql.Struct createStruct(String typeName, Object[] attributes) throws SQLException {
      return real.createStruct(typeName, attributes);
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
      return real.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws java.sql.SQLClientInfoException {
      real.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(java.util.Properties properties)
        throws java.sql.SQLClientInfoException {
      real.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
      return real.getClientInfo(name);
    }

    @Override
    public java.util.Properties getClientInfo() throws SQLException {
      return real.getClientInfo();
    }

    @Override
    public void abort(java.util.concurrent.Executor executor) throws SQLException {
      real.abort(executor);
    }

    @Override
    public void setNetworkTimeout(java.util.concurrent.Executor executor, int milliseconds)
        throws SQLException {
      real.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
      return real.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return real.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return real.isWrapperFor(iface);
    }
  }

  // ---------------------------------------------------------------------------
  // Config stub (mirrors the private SimpleJdbcConfig in JdbcTransactionPersistenceTest)
  // ---------------------------------------------------------------------------

  private static class BenchJdbcConfig implements RelationalJdbcConfiguration {
    @Override
    public java.util.Optional<Integer> maxRetries() {
      return java.util.Optional.of(3);
    }

    @Override
    public java.util.Optional<Long> maxDurationInMs() {
      return java.util.Optional.of(5000L);
    }

    @Override
    public java.util.Optional<Long> initialDelayInMs() {
      return java.util.Optional.of(50L);
    }

    @Override
    public java.util.Optional<String> databaseType() {
      return java.util.Optional.empty();
    }
  }
}
