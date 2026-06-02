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
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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

  private final Instance<DataSource> dataSources;
  private final DatasourceOperations datasourceOperations;
  private final String realmsCsv;
  private volatile ScheduledExecutorService scheduler;

  @Inject
  public TransactionReaper(
      Instance<DataSource> dataSources,
      DatasourceOperations datasourceOperations,
      @ConfigProperty(name = "polaris.realm-context.realms", defaultValue = "POLARIS")
          String realmsCsv) {
    this.dataSources = dataSources;
    this.datasourceOperations = datasourceOperations;
    this.realmsCsv = realmsCsv;
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
    var unused =
        scheduler.scheduleWithFixedDelay(
            () -> {
              try {
                runOnce();
              } catch (Exception e) {
                LOGGER.error("Transaction reaper failed", e);
              }
            },
            intervalSeconds,
            intervalSeconds,
            TimeUnit.SECONDS);
    LOGGER.info("Transaction reaper started with interval={}s", intervalSeconds);
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
        advanceWatermarkAndPurge(persistence);
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
      advanceWatermarkAndPurge(persistence);
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
        LOGGER.warn("Reaper: failed to release txn_snapshot {} for txn {}: {}", snapId, txnId, e.getMessage());
      }
    }
    LOGGER.info("Reaper: aborted expired transaction txn_id={}", txnId);
  }

  private void advanceWatermarkAndPurge(JdbcTransactionPersistence persistence) {
    try {
      long[] xs = persistence.captureXminXmaxForRealm();
      long newWatermark = xs[0];
      if (newWatermark <= 0) {
        return;
      }
      persistence.raiseWatermarkAcrossRealm(newWatermark);
      persistence.purgeCommittedSnapshotsRealmWideBelow(newWatermark);
      LOGGER.debug("Watermark advanced for realm {}", persistence.getRealmId());
    } catch (SQLException e) {
      LOGGER.warn("Watermark advance failed: {}", e.getMessage());
    }
  }
}
