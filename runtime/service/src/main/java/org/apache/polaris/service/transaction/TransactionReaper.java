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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Startup;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background task that cleans up expired transactions.
 *
 * <p>Runs on a configurable schedule (default every 60 seconds). For each expired IN_PROGRESS
 * transaction it:
 *
 * <ol>
 *   <li>Marks the transaction ABORTED.
 *   <li>Deletes all locks held by the transaction, unblocking any waiting writers.
 * </ol>
 *
 * <p>Also periodically advances the {@code txn_watermark} and purges stale {@code
 * committed_table_snapshots} rows that are no longer referenced by any live {@code TxnSnapshot}.
 *
 * <h2>Safety</h2>
 *
 * Safety does not depend on timely reaping. The CAS check in {@code IcebergCatalog.doCommit}
 * (metadataLocation comparison) catches data conflicts independently. The worst effect of a delayed
 * reaper is that locks are held longer, increasing contention — not data corruption.
 *
 * <h2>Scheduling</h2>
 *
 * Uses a plain {@link ScheduledExecutorService} to avoid adding the {@code quarkus-scheduler}
 * extension as a dependency. The scheduler is started lazily on first {@link #start()} call. When
 * the {@code quarkus-scheduler} extension is added to the project, this class can be converted to
 * use {@code @Scheduled(every = "60s")} and the executor replaced.
 */
@ApplicationScoped
public class TransactionReaper {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionReaper.class);

  /** Default reaper interval in seconds. */
  public static final long DEFAULT_INTERVAL_SECONDS = 60L;

  /**
   * Retention lease for standalone read handles, in seconds. A handle whose read point falls below
   * {@code read_floor(SNAPSHOT_LEASE_SECONDS, SKEW_PAD_SECONDS)} was minted longer ago than this
   * and is treated as expired for purge purposes. Standalone reads carry no row, so this time bound
   * is what keeps a live reader's {@code committed_table_snapshots} rows from being purged out from
   * under it. Set to the maximum lease any client uses (the Trino resolver renews at 3600s; the
   * server's own default snapshot lease is 300s) so the reaper is conservative for every client.
   */
  public static final long SNAPSHOT_LEASE_SECONDS = 3600L;

  /**
   * Clock-skew / in-flight padding added to the lease before treating a read handle as expired.
   * Guards against a handle minted moments before the reaper runs on a node whose clock trails the
   * DB, and against a request still in flight at the lease boundary.
   */
  public static final long SKEW_PAD_SECONDS = 30L;

  private final Instance<DataSource> dataSources;
  private final DatasourceOperations datasourceOperations;
  private final String realmsCsv;
  private final boolean enabled;
  private volatile ScheduledExecutorService scheduler;

  @Inject
  public TransactionReaper(
      Instance<DataSource> dataSources,
      DatasourceOperations datasourceOperations,
      @ConfigProperty(name = "polaris.realm-context.realms", defaultValue = "POLARIS")
          String realmsCsv,
      @ConfigProperty(name = "polaris.transaction.reaper.enabled", defaultValue = "true")
          boolean enabled) {
    this.dataSources = dataSources;
    this.datasourceOperations = datasourceOperations;
    this.realmsCsv = realmsCsv;
    this.enabled = enabled;
  }

  /**
   * Starts the reaper on server startup — on <b>every</b> Polaris node. This is safe: every action
   * {@link #runOnce()} performs is idempotent (aborting an already-aborted txn is a no-op; lock
   * deletes and CTS purges converge), the watermark only moves forward, and correctness never
   * depends on the reaper (the CAS/commit_scn checks in {@code IcebergCatalog.doCommit} stand
   * alone). Running on all nodes therefore costs redundant scans, not correctness. Each node adds
   * random jitter to its first tick (see {@link #start(long)}) so a large fleet does not stampede
   * the DB in lock-step. Disable per-node with {@code polaris.transaction.reaper.enabled=false}.
   *
   * <p>Future scaling lever (documented, not implemented): a DB advisory-lock / leased-leader gate
   * so only one node reaps per interval — see multi-table_transactions.plan.md.
   */
  void onStartup(@Observes Startup event) {
    if (!enabled) {
      LOGGER.info("Transaction reaper disabled (polaris.transaction.reaper.enabled=false)");
      return;
    }
    start();
  }

  /** Starts the background reaper thread. Idempotent — safe to call multiple times. */
  public synchronized void start() {
    start(DEFAULT_INTERVAL_SECONDS);
  }

  public synchronized void start(long intervalSeconds) {
    if (scheduler != null && !scheduler.isShutdown()) return;
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "polaris-txn-reaper");
              t.setDaemon(true);
              return t;
            });
    // Per-node jitter on the first tick so N nodes don't all fire at the same wall-clock instant
    // and hammer the shared DB in lock-step. Subsequent ticks are fixed-delay from completion.
    long initialDelay = intervalSeconds + ThreadLocalRandom.current().nextLong(intervalSeconds + 1);
    var unused =
        scheduler.scheduleWithFixedDelay(
            () -> {
              try {
                runOnce();
              } catch (Exception e) {
                LOGGER.error("Transaction reaper failed", e);
              }
            },
            initialDelay,
            intervalSeconds,
            TimeUnit.SECONDS);
    LOGGER.info(
        "Transaction reaper started with interval={}s (first tick in {}s)",
        intervalSeconds,
        initialDelay);
  }

  /** Stops the background reaper. */
  public synchronized void stop() {
    if (scheduler != null) {
      scheduler.shutdownNow();
      scheduler = null;
    }
  }

  /**
   * Performs one reaping pass. Exposed for testing.
   *
   * @return number of transactions reaped
   */
  public int runOnce() throws SQLException {
    if (dataSources.isUnsatisfied()) {
      return 0;
    }
    DataSource ds = dataSources.get();
    int total = 0;
    for (String realm : realmsCsv.split(",")) {
      realm = realm.trim();
      if (realm.isEmpty()) {
        continue;
      }
      JdbcTransactionPersistence persistence =
          new JdbcTransactionPersistence(ds, datasourceOperations.getDatabaseType(), realm, 0L);
      List<Long> expired = persistence.findExpiredTransactionIdsForRealm();
      if (expired.isEmpty()) {
        advanceWatermarkAndPurge(ds, realm);
        continue;
      }

      LOGGER.info(
          "Reaper: found {} expired transaction(s) to abort in realm {}", expired.size(), realm);
      LockManager lockManager = new LockManager(persistence);
      for (long txnId : expired) {
        try {
          reapTransaction(persistence, lockManager, txnId);
          total++;
        } catch (Exception e) {
          LOGGER.warn("Failed to reap transaction {}: {}", txnId, e.getMessage());
        }
      }
      advanceWatermarkAndPurge(ds, realm);
    }
    return total;
  }

  private static void reapTransaction(
      JdbcTransactionPersistence persistence, LockManager lockManager, long txnId)
      throws SQLException {
    var txnRow = persistence.findTransaction(txnId).orElse(null);
    Long snapId = txnRow != null ? txnRow.txnSnapshotId() : null;
    persistence.updateTransactionStatus(txnId, JdbcTransactionPersistence.TXN_STATUS_ABORTED);
    lockManager.releaseAll(txnId);
    if (snapId != null) {
      try {
        persistence.releaseTxnSnapshot(snapId);
      } catch (Exception e) {
        LOGGER.warn(
            "Reaper: failed to release txn_snapshot {} for txn {}: {}",
            snapId,
            txnId,
            e.getMessage());
      }
    }
    LOGGER.info("Reaper: aborted expired transaction txn_id={}", txnId);
  }

  private void advanceWatermarkAndPurge(DataSource ds, String realm) {
    try {
      // commit_scn is per-catalog, so the purge horizon and CTS purge are per-catalog too.
      JdbcTransactionPersistence realmScoped =
          new JdbcTransactionPersistence(ds, datasourceOperations.getDatabaseType(), realm, 0L);
      List<Long> catalogIds = realmScoped.findCatalogIdsForRealm();
      for (long catalogId : catalogIds) {
        JdbcTransactionPersistence perCatalog =
            new JdbcTransactionPersistence(
                ds, datasourceOperations.getDatabaseType(), realm, catalogId);
        // Purge horizon W = MIN(write_floor, read_floor):
        //  - write_floor = MIN(read_point) over live WRITE snapshots (in-progress REPEATABLE_READ
        //    writers keep a linked live txn_snapshots row). Exact — we own those rows. -1 (no live
        //    write) ⟹ +infinity (writes impose no floor).
        //  - read_floor = MAX(commit_scn) among committed_table_snapshots old enough that any
        //    standalone read handle referencing them must have exceeded its lease. Standalone reads
        //    no longer persist a row, so this time-based floor is what protects a live reader.
        // read_point / commit_scn share the per-catalog commit clock, so they compare directly.
        long writeFloor = perCatalog.minLiveReadPointForCatalog();
        long writeFloorOrInf = writeFloor >= 0 ? writeFloor : Long.MAX_VALUE;
        long readFloor = perCatalog.readFloor(SNAPSHOT_LEASE_SECONDS, SKEW_PAD_SECONDS);
        long watermark = Math.min(writeFloorOrInf, readFloor);
        if (watermark <= 0) {
          continue;
        }
        perCatalog.upsertWatermark(watermark);
        perCatalog.purgeCommittedSnapshotsBelowWatermark(watermark);
      }
      LOGGER.debug(
          "Watermark advanced for realm {} across {} catalog(s)", realm, catalogIds.size());
    } catch (SQLException e) {
      LOGGER.warn("Watermark advance failed for realm {}: {}", realm, e.getMessage());
    }
  }
}
