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
package org.apache.polaris.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.spark.rest.CommitTransactionRESTRequest;
import org.apache.polaris.spark.rest.StagedTableCommitRESTRequest;
import org.apache.polaris.spark.rest.SubtransactionRESTResponse;
import org.apache.polaris.spark.rest.TransactionRESTResponse;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages Polaris multi-table transaction state for a single Spark catalog session.
 *
 * <p>State is stored in SparkSession conf so it survives across multiple loadTable calls and is
 * automatically per-session. Three states:
 *
 * <ul>
 *   <li>NONE — no active transaction (pending_txn_isolation absent)
 *   <li>PENDING — BEGIN issued, no query has run yet (pending_txn_isolation set, txn_id absent)
 *   <li>ACTIVE — first query ran, snapshot pinned (both pending_txn_isolation and txn_id set)
 * </ul>
 *
 * <p>Worker mode is a special ACTIVE variant where the session acts as a subtransaction delegate:
 * it writes only to the assigned table's server-allocated staging path and calls
 * commitSubtransaction rather than commitTransaction at the end.
 */
public class TransactionManager {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

  // Basic txn conf keys — formatted with catalogName only
  static final String CONF_PENDING_ISOLATION = "spark.sql.catalog.%s.pending_txn_isolation";
  static final String CONF_TXN_ID = "spark.sql.catalog.%s.active_txn_id";
  static final String CONF_TXN_TOKEN = "spark.sql.catalog.%s.active_txn_token";
  static final String CONF_TXN_SNAPSHOT_ID = "spark.sql.catalog.%s.active_txn_snapshot_id";
  static final String CONF_SESSION_ISOLATION = "spark.sql.catalog.%s.isolation_level";

  // Staged write tracking — CONF_STAGED_TABLES holds comma-separated table keys;
  // per-table path/op use stagedPathConfKey/stagedOpConfKey helpers (not sprintf templates)
  static final String CONF_STAGED_TABLES = "spark.sql.catalog.%s.active_staged_tables";

  // Worker mode conf keys — formatted with catalogName only
  static final String CONF_WORKER_SUBTXN_ID = "spark.sql.catalog.%s.active_worker_subtxn_id";
  static final String CONF_WORKER_SUBTXN_TOKEN = "spark.sql.catalog.%s.active_worker_subtxn_token";
  static final String CONF_WORKER_TABLE = "spark.sql.catalog.%s.active_worker_table";

  static final String DEFAULT_ISOLATION = "REPEATABLE_READ";
  static final int DEFAULT_LEASE_SECONDS = 3600;

  private final PolarisRESTCatalog restCatalog;
  private final String catalogName;

  // In-memory savepoints (not persisted to conf — per-JVM, per-TransactionManager instance).
  // name → { tableKey → [stagingPath, operation] }
  private final Map<String, Map<String, String[]>> savepoints = new HashMap<>();

  public TransactionManager(PolarisRESTCatalog restCatalog, String catalogName) {
    this.restCatalog = restCatalog;
    this.catalogName = catalogName;
  }

  public String catalogName() {
    return catalogName;
  }

  // -------------------------------------------------------------------------
  // Methods called by SQL command objects
  // -------------------------------------------------------------------------

  /** Called by BeginTransactionCommand. Stores pending isolation; does NOT call REST yet. */
  public void begin(SparkSession spark, String isolation) {
    String pendingIsolation = getConf(spark, CONF_PENDING_ISOLATION);
    if (pendingIsolation != null) {
      throw new IllegalStateException(
          "Transaction already in progress. COMMIT or ROLLBACK first.");
    }
    setConf(spark, CONF_PENDING_ISOLATION, isolation);
    System.out.println(
        "Transaction pending — isolation: "
            + isolation
            + "  (transaction starts at first query)");
  }

  /**
   * Called by SetTransactionIsolationCommand. Updates pending isolation if still PENDING; throws if
   * ACTIVE (isolation cannot be changed after first query — PostgreSQL rule).
   */
  public void setIsolationLevel(SparkSession spark, String isolation) {
    String txnId = getConf(spark, CONF_TXN_ID);
    if (txnId != null) {
      throw new IllegalStateException(
          "Cannot set isolation level after the first query — transaction is already ACTIVE.");
    }
    String pending = getConf(spark, CONF_PENDING_ISOLATION);
    if (pending == null) {
      throw new IllegalStateException("No transaction in progress. Use BEGIN first.");
    }
    setConf(spark, CONF_PENDING_ISOLATION, isolation);
    System.out.println("Isolation level set to: " + isolation);
  }

  /** Called by CommitTransactionCommand. */
  public void commit(SparkSession spark) {
    if (isWorkerMode(spark)) {
      throw new IllegalStateException(
          "Worker session: use COMMIT SUBTRANSACTION instead of COMMIT.");
    }

    String txnId = getConf(spark, CONF_TXN_ID);
    String txnToken = getConf(spark, CONF_TXN_TOKEN);
    String pending = getConf(spark, CONF_PENDING_ISOLATION);

    if (pending == null) {
      System.out.println("Nothing to commit — no active transaction.");
      return;
    }

    try {
      if (txnId != null && txnToken != null) {
        List<StagedTableCommitRESTRequest> stagedTables = collectStagedTables(spark);
        restCatalog.commitTransaction(Long.parseLong(txnId), txnToken, stagedTables);
        System.out.println("Transaction committed  (txn_id=" + txnId + ")");
      } else {
        System.out.println("Nothing to commit — no query ran in this transaction.");
      }
    } finally {
      clearConf(spark);
    }
  }

  /** Called by RollbackTransactionCommand. */
  public void rollback(SparkSession spark) {
    if (isWorkerMode(spark)) {
      throw new IllegalStateException(
          "Worker session: use ROLLBACK SUBTRANSACTION instead of ROLLBACK.");
    }

    String txnId = getConf(spark, CONF_TXN_ID);
    String txnToken = getConf(spark, CONF_TXN_TOKEN);
    String pending = getConf(spark, CONF_PENDING_ISOLATION);

    if (pending == null) {
      System.out.println("Nothing to roll back — no active transaction.");
      return;
    }

    try {
      if (txnId != null && txnToken != null) {
        restCatalog.abortTransaction(Long.parseLong(txnId), txnToken);
        System.out.println("Transaction rolled back  (txn_id=" + txnId + ")");
      } else {
        System.out.println("Transaction rolled back  (no query had run).");
      }
    } finally {
      clearConf(spark);
    }
  }

  // -------------------------------------------------------------------------
  // Method called by SparkCatalog.loadTable — lazy-BEGIN trigger + RYOW
  // -------------------------------------------------------------------------

  /**
   * Resolves the pinned metadata location for REPEATABLE_READ transactions.
   *
   * <ul>
   *   <li>NONE → returns empty (no active transaction)
   *   <li>ACTIVE (RYOW) → if this table was already written in this transaction, return the local
   *       staged path directly; no REST call
   *   <li>PENDING → calls beginTransaction REST, transitions to ACTIVE, then proceeds
   *   <li>ACTIVE + REPEATABLE_READ → calls resolveTableMetadataLocation, returns pinned path
   *   <li>ACTIVE + READ_COMMITTED → returns empty (use normal loadTable)
   * </ul>
   */
  public Optional<String> resolveMetadataLocation(SparkSession spark, TableIdentifier ident) {
    String pendingIsolation = getConf(spark, CONF_PENDING_ISOLATION);
    if (pendingIsolation == null) {
      return Optional.empty();
    }

    // RYOW: if already written in this transaction, return the staged path directly
    Optional<String> staged = getStagedPath(spark, ident);
    if (staged.isPresent()) {
      LOG.debug("RYOW: returning staged path for {} in active transaction", ident);
      return staged;
    }

    String txnId = getConf(spark, CONF_TXN_ID);
    if (txnId == null) {
      // PENDING → ACTIVE: call beginTransaction now
      TransactionRESTResponse txn =
          restCatalog.beginTransaction(pendingIsolation, DEFAULT_LEASE_SECONDS);
      setConf(spark, CONF_TXN_ID, String.valueOf(txn.txnId()));
      setConf(spark, CONF_TXN_TOKEN, txn.txnToken());
      Long snapId = txn.txnSnapshotId();
      if (snapId != null) {
        setConf(spark, CONF_TXN_SNAPSHOT_ID, String.valueOf(snapId));
      }
      System.out.println(
          "Transaction started  txn_id="
              + txn.txnId()
              + (snapId != null ? "  txn_snapshot_id=" + snapId : "  (READ_COMMITTED)"));
      txnId = getConf(spark, CONF_TXN_ID);
    }

    String snapIdStr = getConf(spark, CONF_TXN_SNAPSHOT_ID);
    if (snapIdStr == null) {
      // ACTIVE but READ_COMMITTED — load the latest version normally
      return Optional.empty();
    }

    // ACTIVE + REPEATABLE_READ — resolve via pinned snapshot
    long snapId = Long.parseLong(snapIdStr);
    String metadataLocation = restCatalog.resolveTableMetadataLocation(ident, snapId);
    LOG.debug(
        "Resolved pinned metadata location for {} at snapshot {}: {}",
        ident,
        snapId,
        metadataLocation);
    return Optional.of(metadataLocation);
  }

  // -------------------------------------------------------------------------
  // Staging write methods — called by PinnedTableOperations
  // -------------------------------------------------------------------------

  /**
   * Records a staged write for the given table. Upgrades the stored operation from APPEND to
   * ROW_DELTA if needed; never downgrades. Must be called after writing the staged metadata file.
   */
  public void stageWrite(
      SparkSession spark, TableIdentifier ident, String stagingPath, String operation) {
    String tableKey = identToKey(ident);
    spark.conf().set(stagedPathConfKey(tableKey), stagingPath);
    // Upgrade operation: ROW_DELTA takes precedence over APPEND
    String currentOp =
        spark.conf().getOption(stagedOpConfKey(tableKey)).getOrElse(() -> null);
    String finalOp =
        ("ROW_DELTA".equals(currentOp) || "ROW_DELTA".equals(operation)) ? "ROW_DELTA" : operation;
    spark.conf().set(stagedOpConfKey(tableKey), finalOp);
    // Track in the staged tables list
    String tablesStr = getConf(spark, CONF_STAGED_TABLES);
    if (tablesStr == null || tablesStr.isEmpty()) {
      setConf(spark, CONF_STAGED_TABLES, tableKey);
    } else {
      boolean alreadyPresent = Arrays.asList(tablesStr.split(",")).contains(tableKey);
      if (!alreadyPresent) {
        setConf(spark, CONF_STAGED_TABLES, tablesStr + "," + tableKey);
      }
    }
  }

  /** Returns the current staged metadata path for the given table, if any. Used for RYOW. */
  public Optional<String> getStagedPath(SparkSession spark, TableIdentifier ident) {
    String val =
        spark.conf().getOption(stagedPathConfKey(identToKey(ident))).getOrElse(() -> null);
    return Optional.ofNullable(val);
  }

  // -------------------------------------------------------------------------
  // Worker mode — Use Case 2 (multi-app parallel ingest, worker cluster)
  // -------------------------------------------------------------------------

  /**
   * Enters worker mode directly into ACTIVE state using credentials provided by the orchestrator.
   * Skips beginTransaction REST call. Pre-populates the staged path with the server-allocated path.
   */
  public void beginWorkerSubtransaction(
      SparkSession spark,
      long txnId,
      long txnSnapshotId,
      long subtxnId,
      String subtxnToken,
      TableIdentifier table,
      String stagingPath,
      String operation) {
    String pending = getConf(spark, CONF_PENDING_ISOLATION);
    if (pending != null) {
      throw new IllegalStateException(
          "Transaction already in progress. COMMIT or ROLLBACK first.");
    }
    setConf(spark, CONF_PENDING_ISOLATION, DEFAULT_ISOLATION);
    setConf(spark, CONF_TXN_ID, String.valueOf(txnId));
    setConf(spark, CONF_TXN_SNAPSHOT_ID, String.valueOf(txnSnapshotId));
    setConf(spark, CONF_WORKER_SUBTXN_ID, String.valueOf(subtxnId));
    setConf(spark, CONF_WORKER_SUBTXN_TOKEN, subtxnToken);
    setConf(spark, CONF_WORKER_TABLE, identToKey(table));
    // Pre-populate staged path so RYOW works and PinnedTableOperations writes to it
    stageWrite(spark, table, stagingPath, operation);
    System.out.println(
        "Worker subtransaction started  txn_id=" + txnId + "  subtxn_id=" + subtxnId);
  }

  /**
   * Commits the worker's staged write by calling commitSubtransaction REST. Should be called only
   * after all write statements have completed.
   */
  public void commitWorkerSubtransaction(SparkSession spark) {
    String subtxnIdStr = getConf(spark, CONF_WORKER_SUBTXN_ID);
    if (subtxnIdStr == null) {
      throw new IllegalStateException(
          "Not in worker mode. Use BEGIN WORKER SUBTRANSACTION first.");
    }
    String subtxnToken = getConf(spark, CONF_WORKER_SUBTXN_TOKEN);
    String txnId = getConf(spark, CONF_TXN_ID);
    String workerTable = getConf(spark, CONF_WORKER_TABLE);
    String stagedPath =
        spark.conf().getOption(stagedPathConfKey(workerTable)).getOrElse(() -> null);
    if (stagedPath == null) {
      throw new IllegalStateException(
          "No staged data for worker table: " + workerTable + ". Write at least one row first.");
    }
    try {
      restCatalog.commitSubtransaction(
          Long.parseLong(txnId), Long.parseLong(subtxnIdStr), subtxnToken, stagedPath);
      System.out.println("Subtransaction committed  (subtxn_id=" + subtxnIdStr + ")");
    } finally {
      clearConf(spark);
    }
  }

  /** Abandons the worker subtransaction. Staged file remains on storage for GC. */
  public void rollbackWorkerSubtransaction(SparkSession spark) {
    String subtxnIdStr = getConf(spark, CONF_WORKER_SUBTXN_ID);
    if (subtxnIdStr == null) {
      throw new IllegalStateException("Not in worker mode.");
    }
    System.out.println("Worker subtransaction rolled back  (subtxn_id=" + subtxnIdStr + ")");
    clearConf(spark);
  }

  /**
   * Called by orchestrator SQL: {@code CREATE SUBTRANSACTION FOR <table>}. Allocates a
   * server-side staging path and returns the credentials to be passed to the worker cluster.
   */
  public SubtransactionRESTResponse createSubtransactionForOrchestrator(
      SparkSession spark, TableIdentifier table, String operation) {
    String txnId = getConf(spark, CONF_TXN_ID);
    String txnToken = getConf(spark, CONF_TXN_TOKEN);
    if (txnId == null || txnToken == null) {
      throw new IllegalStateException("No active transaction. Use BEGIN first.");
    }
    return restCatalog.createSubtransaction(
        Long.parseLong(txnId), txnToken, identToKey(table), operation, DEFAULT_LEASE_SECONDS);
  }

  /** Returns true if this session is in worker subtransaction mode. */
  public boolean isWorkerMode(SparkSession spark) {
    return getConf(spark, CONF_WORKER_SUBTXN_ID) != null;
  }

  /** Returns the worker's assigned table key (used by PinnedTableOperations for lock guard). */
  public String getWorkerTable(SparkSession spark) {
    return getConf(spark, CONF_WORKER_TABLE);
  }

  // -------------------------------------------------------------------------
  // SAVEPOINT — Use Case 1, Phase 5 (client-side only, no REST calls)
  // -------------------------------------------------------------------------

  /** Captures a named checkpoint of the current staged write state. */
  public void savepointCreate(SparkSession spark, String name) {
    if (getConf(spark, CONF_PENDING_ISOLATION) == null) {
      throw new IllegalStateException("No active transaction.");
    }
    Map<String, String[]> snapshot = new HashMap<>();
    String tablesStr = getConf(spark, CONF_STAGED_TABLES);
    if (tablesStr != null && !tablesStr.isEmpty()) {
      for (String tableKey : tablesStr.split(",")) {
        String path =
            spark.conf().getOption(stagedPathConfKey(tableKey)).getOrElse(() -> null);
        String op = spark.conf().getOption(stagedOpConfKey(tableKey)).getOrElse(() -> null);
        if (path != null) {
          snapshot.put(tableKey, new String[] {path, op != null ? op : "APPEND"});
        }
      }
    }
    savepoints.put(name, snapshot);
    System.out.println("Savepoint created: " + name);
  }

  /** Restores staged write state to a previously created savepoint. */
  public void savepointRollback(SparkSession spark, String name) {
    Map<String, String[]> snapshot = savepoints.get(name);
    if (snapshot == null) {
      throw new IllegalStateException("Savepoint not found: " + name);
    }
    clearStagedConf(spark);
    for (Map.Entry<String, String[]> entry : snapshot.entrySet()) {
      String tableKey = entry.getKey();
      String[] state = entry.getValue();
      spark.conf().set(stagedPathConfKey(tableKey), state[0]);
      spark.conf().set(stagedOpConfKey(tableKey), state[1]);
    }
    if (!snapshot.isEmpty()) {
      setConf(spark, CONF_STAGED_TABLES, String.join(",", snapshot.keySet()));
    }
    System.out.println("Rolled back to savepoint: " + name);
  }

  /** Discards a savepoint. */
  public void savepointRelease(String name) {
    if (savepoints.remove(name) == null) {
      throw new IllegalStateException("Savepoint not found: " + name);
    }
    System.out.println("Savepoint released: " + name);
  }

  // -------------------------------------------------------------------------
  // Session conf helpers
  // -------------------------------------------------------------------------

  private List<StagedTableCommitRESTRequest> collectStagedTables(SparkSession spark) {
    String tablesStr = getConf(spark, CONF_STAGED_TABLES);
    if (tablesStr == null || tablesStr.isEmpty()) {
      return Collections.emptyList();
    }
    List<StagedTableCommitRESTRequest> result = new ArrayList<>();
    for (String tableKey : tablesStr.split(",")) {
      String path =
          spark.conf().getOption(stagedPathConfKey(tableKey)).getOrElse(() -> null);
      String op = spark.conf().getOption(stagedOpConfKey(tableKey)).getOrElse(() -> null);
      if (path != null) {
        result.add(
            new StagedTableCommitRESTRequest(tableKey, path, op != null ? op : "APPEND"));
      }
    }
    return result;
  }

  private void clearStagedConf(SparkSession spark) {
    String tablesStr = getConf(spark, CONF_STAGED_TABLES);
    if (tablesStr != null && !tablesStr.isEmpty()) {
      for (String tableKey : tablesStr.split(",")) {
        spark.conf().unset(stagedPathConfKey(tableKey));
        spark.conf().unset(stagedOpConfKey(tableKey));
      }
    }
    spark.conf().unset(confKey(CONF_STAGED_TABLES));
  }

  /** Converts a TableIdentifier to a stable string key for use in conf entries. */
  static String identToKey(TableIdentifier ident) {
    String[] levels = ident.namespace().levels();
    if (levels.length == 0) {
      return ident.name();
    }
    return String.join(".", levels) + "." + ident.name();
  }

  private String stagedPathConfKey(String tableKey) {
    return "spark.sql.catalog." + catalogName + ".active_staged." + tableKey;
  }

  private String stagedOpConfKey(String tableKey) {
    return "spark.sql.catalog." + catalogName + ".active_staged_op." + tableKey;
  }

  private String confKey(String template) {
    return String.format(template, catalogName);
  }

  // Package-private for tests and SparkCatalog
  String getConf(SparkSession spark, String template) {
    return spark.conf().getOption(confKey(template)).getOrElse(() -> null);
  }

  private void setConf(SparkSession spark, String template, String value) {
    spark.conf().set(confKey(template), value);
  }

  private void clearConf(SparkSession spark) {
    clearStagedConf(spark);
    spark.conf().unset(confKey(CONF_PENDING_ISOLATION));
    spark.conf().unset(confKey(CONF_TXN_ID));
    spark.conf().unset(confKey(CONF_TXN_TOKEN));
    spark.conf().unset(confKey(CONF_TXN_SNAPSHOT_ID));
    spark.conf().unset(confKey(CONF_WORKER_SUBTXN_ID));
    spark.conf().unset(confKey(CONF_WORKER_SUBTXN_TOKEN));
    spark.conf().unset(confKey(CONF_WORKER_TABLE));
    savepoints.clear();
  }
}
