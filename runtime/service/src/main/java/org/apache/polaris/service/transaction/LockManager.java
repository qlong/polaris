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
import java.util.concurrent.ConcurrentHashMap;
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
 * ROW_EXCLUSIVE    — default OCC mode; self-compatible; conflicts detected at commit via OCC
 * EXCLUSIVE        — full-table pessimistic write; blocks all other writers
 * ACCESS_EXCLUSIVE — DDL / admin; blocks all readers and writers
 * </pre>
 *
 * <h2>Lock wait mechanism (Option A — client-side retry)</h2>
 *
 * {@link #acquire} returns immediately: grants on no conflict, throws {@link LockConflictException}
 * (HTTP 409) on conflict. The caller retries with backoff up to its own timeout, then gives up with
 * HTTP 423. No server-side polling — avoids proxy/LB timeout issues for long waits.
 *
 * <h2>Deadlock prevention</h2>
 *
 * Locks for a multi-table transaction are always acquired in {@link #canonicalOrder} (sorted by
 * {@code tableUuid} lexicographically), making lock-ordering deadlocks impossible.
 *
 * <h2>Old vs. new clients</h2>
 *
 * Old (optimistic) clients never acquire locks. They are subject to a conflict check in {@code
 * IcebergCatalog.doCommit} via {@link #checkConflictAtCommit}, which rejects their commit with
 * {@link CommitFailedException} if a conflicting EXCLUSIVE or ACCESS_EXCLUSIVE lock is held.
 */
public class LockManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LockManager.class);

  /**
   * Per-resource mutex used to serialize concurrent {@link #acquire} calls within the same JVM.
   * Keyed by "realmId/catalogId/resource". Static so that it outlives per-request bean instances.
   */
  private static final ConcurrentHashMap<String, Object> ACQUIRE_MUTEXES =
      new ConcurrentHashMap<>();

  // Lock mode constants — must match the lock_mode column in the locks table.
  public static final String MODE_ROW_EXCLUSIVE = "ROW_EXCLUSIVE";
  public static final String MODE_EXCLUSIVE = "EXCLUSIVE";
  public static final String MODE_ACCESS_EXCLUSIVE = "ACCESS_EXCLUSIVE";

  /**
   * Compatibility matrix. {@code compatible[held][requested]} is {@code true} when a new lock of
   * mode {@code requested} can be granted while {@code held} is already granted.
   *
   * <p>Rows/columns indexed by: 0=ROW_EXCLUSIVE, 1=EXCLUSIVE, 2=ACCESS_EXCLUSIVE.
   */
  private static final boolean[][] COMPAT =
      new boolean[][] {
        //                       RE      EX      AX
        /* ROW_EXCLUSIVE    */ {true, false, false},
        /* EXCLUSIVE        */ {false, false, false},
        /* ACCESS_EXCLUSIVE */ {false, false, false},
      };

  static final long DEFAULT_RETRY_AFTER_MS = 1_000L;

  private static final List<String> MODE_ORDER =
      List.of(MODE_ROW_EXCLUSIVE, MODE_EXCLUSIVE, MODE_ACCESS_EXCLUSIVE);

  private final JdbcTransactionPersistence persistence;

  public LockManager(JdbcTransactionPersistence persistence) {
    this.persistence = persistence;
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
   * lockMode}. Grants immediately if no conflict exists; throws {@link LockConflictException}
   * (HTTP 409) on conflict.
   *
   * <p>Conflict semantics by mode:
   * <ul>
   *   <li>ROW_EXCLUSIVE: inserts a lock row so EXCLUSIVE can detect it. If EXCLUSIVE or
   *       ACCESS_EXCLUSIVE is already held, throws immediately (no retry hint — client should fail).
   *   <li>EXCLUSIVE / ACCESS_EXCLUSIVE: if any conflicting lock is held, throws with a
   *       {@link LockConflictException#retryAfterMs()} hint so the client can retry with backoff.
   * </ul>
   *
   * @throws LockConflictException if a conflicting lock is held; check {@code retryAfterMs()} to
   *     decide whether to retry
   */
  public long acquire(long txnId, String resource, String lockMode)
      throws SQLException, LockConflictException {
    // Serialize concurrent acquire calls for the same resource within this JVM.
    // Each LockManager is per-request, so the mutex map must be static.
    String mutexKey = persistence.getRealmId() + "/" + persistence.getCatalogId() + "/" + resource;
    Object mutex = ACQUIRE_MUTEXES.computeIfAbsent(mutexKey, k -> new Object());
    synchronized (mutex) {
      List<LockRow> conflicts = persistence.findGrantedLocksOnResource(resource, txnId);
      for (LockRow held : conflicts) {
        if (!compatible(held.lockMode(), lockMode)) {
          String msg =
              String.format(
                  "Lock conflict: txn=%d resource=%s mode=%s blocked by txn=%d mode=%s",
                  txnId, resource, lockMode, held.txnId(), held.lockMode());
          if (MODE_ROW_EXCLUSIVE.equals(lockMode)) {
            // ROW_EXCLUSIVE blocked by an EXCLUSIVE/ACCESS_EXCLUSIVE: fail immediately.
            throw new LockConflictException(msg);
          } else {
            // EXCLUSIVE/ACCESS_EXCLUSIVE: signal client to retry with backoff.
            throw new LockConflictException(msg, DEFAULT_RETRY_AFTER_MS);
          }
        }
      }
      long lockId = persistence.insertLock(txnId, resource, lockMode);
      persistence.grantLock(lockId);
      LOGGER.debug("Lock granted: txn={} resource={} mode={}", txnId, resource, lockMode);
      return lockId;
    }
  }

  /** Releases all locks held by {@code txnId}. Called on COMMIT or ABORT of the transaction. */
  public void releaseAll(long txnId) throws SQLException {
    persistence.deleteLocksForTransaction(txnId);
    LOGGER.debug("Released all locks for txn={}", txnId);
  }

  /**
   * Releases the lock held by {@code txnId} on {@code resource}. Used to release ROW_EXCLUSIVE
   * locks at subtransaction commit/abort time (before the parent transaction ends), so that a
   * waiting EXCLUSIVE acquirer can proceed.
   */
  public void releaseForResource(long txnId, String resource) throws SQLException {
    persistence.deleteLockForResource(txnId, resource);
    LOGGER.debug("Released lock: txn={} resource={}", txnId, resource);
  }

  /**
   * Checks whether an incoming commit (from any client — old or new API) conflicts with a granted
   * EXCLUSIVE or ACCESS_EXCLUSIVE lock on {@code resource}. Called from {@code
   * IcebergCatalog.doCommit} for every table commit.
   *
   * @param resource the lock resource string (tableUuid)
   * @param callerTxnId the txn_id of the caller ({@code -1} for legacy clients with no txn_id)
   * @throws CommitFailedException if a conflicting lock is held; the caller will retry via OCC
   */
  public void checkConflictAtCommit(String resource, long callerTxnId) throws SQLException {
    List<LockRow> grantedLocks = persistence.findGrantedLocksOnResource(resource, callerTxnId);
    for (LockRow lock : grantedLocks) {
      // ROW_EXCLUSIVE locks are compatible with new optimistic commits; only block on EX / AX.
      if (!compatible(lock.lockMode(), MODE_ROW_EXCLUSIVE)) {
        throw new CommitFailedException(
            "Cannot commit on resource %s: conflicting %s lock held by txn=%d",
            resource, lock.lockMode(), lock.txnId());
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

  /**
   * Thrown when a lock cannot be granted due to a conflicting held lock (HTTP 409).
   *
   * <p>If {@link #retryAfterMs()} returns a positive value, the client should sleep that many
   * milliseconds and retry. A value of {@code -1} means the conflict is permanent for this request
   * (e.g. ROW_EXCLUSIVE blocked by EXCLUSIVE) and the client should not retry.
   */
  public static class LockConflictException extends Exception {
    private final long retryAfterMs;

    /** Fail-fast conflict: no retry hint. */
    public LockConflictException(String message) {
      super(message);
      this.retryAfterMs = -1;
    }

    /** Retryable conflict: client should sleep {@code retryAfterMs} ms before retrying. */
    public LockConflictException(String message, long retryAfterMs) {
      super(message);
      this.retryAfterMs = retryAfterMs;
    }

    /** Returns the suggested retry delay in milliseconds, or {@code -1} for fail-fast. */
    public long retryAfterMs() {
      return retryAfterMs;
    }
  }

}
