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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.LockRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages pessimistic locks for multi-table transactions.
 *
 * <h2>Lock modes (from least to most restrictive)</h2>
 *
 * <pre>
 * APPEND           — additive writes; compatible with everything except ACCESS_EXCLUSIVE
 * ROW_DELTA        — row-level deletes / updates targeting a specific partition
 * OVERWRITE        — partition overwrite (replaces all data in a partition)
 * EXCLUSIVE        — full-table exclusive write; blocks all other writes
 * ACCESS_EXCLUSIVE — blocks all reads and writes (e.g. schema migration, DROP)
 * </pre>
 *
 * <h2>Deadlock prevention</h2>
 *
 * Locks for a multi-table transaction are always acquired in {@link #canonicalOrder} (sorted by
 * {@code tableUuid} lexicographically). This guarantees that two concurrent orchestrators acquiring
 * the same set of tables will do so in the same order, making lock-ordering deadlocks impossible. A
 * configurable {@code lockWaitTimeoutMs} aborts waiting transactions that exceed the timeout,
 * providing a safety net for any unexpected cycle.
 *
 * <h2>Old vs. new clients</h2>
 *
 * Old (optimistic) clients never acquire locks. They are subject to a conflict check in {@code
 * IcebergCatalog.doCommit} via {@link #checkConflictAtCommit}, which rejects their commit with
 * {@link CommitFailedException} if a conflicting lock is held — the same error they already handle
 * via retry today. APPEND commits from old clients are never blocked except by ACCESS_EXCLUSIVE.
 */
public class LockManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LockManager.class);

  /** Default wait before aborting a lock acquisition attempt. */
  public static final long DEFAULT_LOCK_WAIT_TIMEOUT_MS = 30_000L;

  /** Poll interval when waiting for a conflicting lock to be released. */
  private static final long LOCK_POLL_INTERVAL_MS = 500L;

  // Lock mode constants — must match the lock_mode column in the locks table.
  public static final String MODE_APPEND = "APPEND";
  public static final String MODE_ROW_DELTA = "ROW_DELTA";
  public static final String MODE_OVERWRITE = "OVERWRITE";
  public static final String MODE_EXCLUSIVE = "EXCLUSIVE";
  public static final String MODE_ACCESS_EXCLUSIVE = "ACCESS_EXCLUSIVE";

  /**
   * Compatibility matrix. {@code compatible[held][requested]} is {@code true} when a new lock of
   * mode {@code requested} can be granted while {@code held} is already granted.
   *
   * <p>Rows/columns indexed by: 0=APPEND, 1=ROW_DELTA, 2=OVERWRITE, 3=EXCLUSIVE,
   * 4=ACCESS_EXCLUSIVE.
   */
  private static final boolean[][] COMPAT =
      new boolean[][] {
        //                    APP     RD      OW      EX      AX
        /* APPEND          */ {true, true, true, true, false},
        /* ROW_DELTA       */ {true, false, false, false, false},
        /* OVERWRITE       */ {true, false, false, false, false},
        /* EXCLUSIVE       */ {true, false, false, false, false},
        /* ACCESS_EXCLUSIVE*/ {false, false, false, false, false},
      };

  private static final List<String> MODE_ORDER =
      List.of(MODE_APPEND, MODE_ROW_DELTA, MODE_OVERWRITE, MODE_EXCLUSIVE, MODE_ACCESS_EXCLUSIVE);

  private final JdbcTransactionPersistence persistence;
  private final long lockWaitTimeoutMs;

  public LockManager(JdbcTransactionPersistence persistence) {
    this(persistence, DEFAULT_LOCK_WAIT_TIMEOUT_MS);
  }

  public LockManager(JdbcTransactionPersistence persistence, long lockWaitTimeoutMs) {
    this.persistence = persistence;
    this.lockWaitTimeoutMs = lockWaitTimeoutMs;
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} if a new lock of mode {@code requested} is compatible with an existing
   * lock of mode {@code held}.
   */
  public static boolean compatible(String held, String requested) {
    int h = modeIndex(held);
    int r = modeIndex(requested);
    return COMPAT[h][r];
  }

  /**
   * Sorts table UUIDs into a deterministic canonical order. Acquiring locks in this order for every
   * multi-table transaction prevents deadlocks.
   */
  public static List<String> canonicalOrder(Collection<String> tableUuids) {
    List<String> sorted = new ArrayList<>(tableUuids);
    sorted.sort(Comparator.naturalOrder());
    return sorted;
  }

  /**
   * Attempts to acquire a lock for {@code txnId} on {@code resource} with the given {@code
   * lockMode}. Waits up to {@code lockWaitTimeoutMs} for conflicting locks to clear, polling every
   * {@link #LOCK_POLL_INTERVAL_MS}.
   *
   * @throws LockWaitTimeoutException if the lock cannot be acquired within the timeout
   */
  public long acquire(long txnId, String resource, String lockMode)
      throws SQLException, LockWaitTimeoutException {
    long deadline = System.currentTimeMillis() + lockWaitTimeoutMs;
    long lockId = persistence.insertLock(txnId, resource, lockMode);

    while (true) {
      List<LockRow> conflicts = persistence.findGrantedLocksOnResource(resource, txnId);
      boolean hasConflict = conflicts.stream().anyMatch(l -> !compatible(l.lockMode(), lockMode));

      if (!hasConflict) {
        persistence.grantLock(lockId);
        LOGGER.debug("Lock granted: txn={} resource={} mode={}", txnId, resource, lockMode);
        return lockId;
      }

      if (System.currentTimeMillis() >= deadline) {
        // Clean up the waiting lock before aborting
        persistence.deleteLocksForTransaction(txnId);
        throw new LockWaitTimeoutException(
            String.format(
                "Lock wait timeout after %d ms: txn=%d resource=%s mode=%s",
                lockWaitTimeoutMs, txnId, resource, lockMode));
      }

      LOGGER.debug(
          "Lock waiting: txn={} resource={} mode={} conflicts={}",
          txnId,
          resource,
          lockMode,
          conflicts.size());
      try {
        Thread.sleep(LOCK_POLL_INTERVAL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new LockWaitTimeoutException("Lock wait interrupted");
      }
    }
  }

  /** Releases all locks held by {@code txnId}. Called on COMMIT or ABORT. */
  public void releaseAll(long txnId) throws SQLException {
    persistence.deleteLocksForTransaction(txnId);
    LOGGER.debug("Released all locks for txn={}", txnId);
  }

  /**
   * Checks whether an incoming commit (from any client — old or new API) conflicts with a granted
   * lock on {@code resource}. Called from {@code IcebergCatalog.doCommit} for every table commit.
   *
   * <p>Old APPEND clients are never blocked unless an ACCESS_EXCLUSIVE lock is held.
   *
   * @param resource the lock resource string (e.g. tableUuid or tableUuid:partitionKey)
   * @param incomingOperation the operation the committing client is performing
   * @param callerTxnId the txn_id of the caller (use {@code -1} for old clients with no txn_id)
   * @throws CommitFailedException if a conflicting lock is held; the caller (old client) will retry
   */
  public void checkConflictAtCommit(String resource, String incomingOperation, long callerTxnId)
      throws SQLException {
    List<LockRow> grantedLocks = persistence.findGrantedLocksOnResource(resource, callerTxnId);
    for (LockRow lock : grantedLocks) {
      if (!compatible(lock.lockMode(), incomingOperation)) {
        throw new CommitFailedException(
            "Cannot commit %s on resource %s: conflicting %s lock held by txn=%d",
            incomingOperation, resource, lock.lockMode(), lock.txnId());
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static int modeIndex(String mode) {
    int idx = MODE_ORDER.indexOf(mode);
    if (idx < 0) throw new IllegalArgumentException("Unknown lock mode: " + mode);
    return idx;
  }

  // ---------------------------------------------------------------------------
  // Exception types
  // ---------------------------------------------------------------------------

  public static class LockWaitTimeoutException extends Exception {
    public LockWaitTimeoutException(String message) {
      super(message);
    }
  }
}
