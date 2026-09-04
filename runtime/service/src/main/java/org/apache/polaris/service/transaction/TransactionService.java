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

import jakarta.annotation.Nullable;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotFoundException;
import java.security.SecureRandom;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.util.LocationUtil;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.CommittedTableSnapshotRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.OperationRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.TransactionRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.TxnSnapshotRow;
import org.apache.polaris.service.transaction.LockManager.LockConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core business logic for the multi-table transaction extension.
 *
 * <p>Manages the full transaction lifecycle:
 *
 * <ol>
 *   <li>{@link #createTransactionSnapshot} — acquire a read-only consistency handle
 *   <li>{@link #beginTransaction} — start a write transaction (also acquires a TxnSnapshot for
 *       REPEATABLE_READ)
 *   <li>{@link #createSubtransaction} — allocate a per-table subtransaction; acquire lock if needed
 *   <li>{@link #commitSubtransaction} — stage metadata for one table
 *   <li>{@link #commitTransaction} — finalize: atomically publish all tables + record snapshots
 *   <li>{@link #abortTransaction} — mark ABORTED + release locks
 * </ol>
 */
public class TransactionService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionService.class);

  private static final int DEFAULT_TXN_LEASE_SECONDS = 3600;
  private static final int DEFAULT_SNAPSHOT_LEASE_SECONDS = 300;
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  private final JdbcTransactionPersistence persistence;
  private final LockManager lockManager;

  public TransactionService(JdbcTransactionPersistence persistence, LockManager lockManager) {
    this.persistence = persistence;
    this.lockManager = lockManager;
  }

  // ---------------------------------------------------------------------------
  // TxnSnapshot (read-only consistency handles)
  // ---------------------------------------------------------------------------

  /** A newly minted standalone read snapshot: an opaque handle plus its captured read point. */
  public record MintedSnapshot(String handle, long readPoint) {}

  /**
   * Acquires a new read-consistent snapshot for standalone (non-transactional) reads. Captures a
   * single {@code read_point} (a commit-order cutoff) atomically and mints an opaque handle
   * carrying it, giving the caller a pinned view across all subsequent {@code
   * loadTable?txn_snapshot=X} calls.
   *
   * <p>No {@code txn_snapshots} row is inserted: the handle is self-describing and is reclaimed by
   * the reaper's time-based {@code read_floor} once its lease elapses. {@code leaseSeconds} is
   * accepted for API symmetry but is not persisted — retention is governed globally by the reaper's
   * configured lease.
   */
  public MintedSnapshot createTransactionSnapshot(int leaseSeconds) throws SQLException {
    long readPoint = persistence.captureReadPoint();
    String handle = TxnSnapshotHandle.encode(readPoint);
    LOGGER.info(
        "TxnSnapshot minted (handle-only): read_point={} requested_lease_seconds={}",
        readPoint,
        leaseSeconds);
    return new MintedSnapshot(handle, readPoint);
  }

  public TxnSnapshotRow getTransactionSnapshot(long txnSnapshotId) throws SQLException {
    return persistence
        .findTxnSnapshot(txnSnapshotId)
        .orElseThrow(
            () -> new NotFoundException("Transaction snapshot not found: " + txnSnapshotId));
  }

  /**
   * Derives the opaque read-point handle for a write-linked snapshot row so a response can carry
   * the uniform string read wire ({@code txn_snapshot}) alongside the numeric {@code
   * txn_snapshot_id}. The write path keeps its live row; this only re-expresses that row's read
   * point as a handle a client can echo on {@code loadTable?txn_snapshot=X}. Returns {@code null}
   * when {@code snapId} is {@code null} (e.g. a READ_COMMITTED transaction has no pinned snapshot).
   */
  public String encodeHandleForSnapshot(Long snapId) throws SQLException {
    if (snapId == null) {
      return null;
    }
    TxnSnapshotRow row = getTransactionSnapshot(snapId);
    return TxnSnapshotHandle.encode(row.readPoint());
  }

  /** Encodes a handle directly from a snapshot row, without a lookup. */
  public String encodeHandleForSnapshot(TxnSnapshotRow row) {
    return TxnSnapshotHandle.encode(row.readPoint());
  }

  public void releaseTransactionSnapshot(long txnSnapshotId) throws SQLException {
    TxnSnapshotRow existing =
        persistence
            .findTxnSnapshot(txnSnapshotId)
            .orElseThrow(
                () -> new NotFoundException("Transaction snapshot not found: " + txnSnapshotId));
    persistence.releaseTxnSnapshot(txnSnapshotId);
    LOGGER.debug("TxnSnapshot released: id={}", existing.txnSnapshotId());
  }

  public TxnSnapshotRow renewTransactionSnapshot(long txnSnapshotId, int leaseSeconds)
      throws SQLException {
    TxnSnapshotRow snap = getTransactionSnapshot(txnSnapshotId);
    if (snap.releasedAt() != null) {
      throw new BadRequestException("Cannot renew a released snapshot: " + txnSnapshotId);
    }
    int effective = leaseSeconds > 0 ? leaseSeconds : DEFAULT_SNAPSHOT_LEASE_SECONDS;
    persistence.renewTxnSnapshot(txnSnapshotId, Instant.now().plus(effective, ChronoUnit.SECONDS));
    return persistence
        .findTxnSnapshot(txnSnapshotId)
        .orElseThrow(() -> new IllegalStateException("TxnSnapshot missing after renew"));
  }

  // ---------------------------------------------------------------------------
  // Transactions (write lifecycle)
  // ---------------------------------------------------------------------------

  /**
   * Begins a new multi-table write transaction. For REPEATABLE_READ, a TxnSnapshot is captured
   * atomically at BEGIN time (analogous to PostgreSQL's snapshot at {@code BEGIN TRANSACTION
   * ISOLATION LEVEL REPEATABLE READ}).
   *
   * @return the newly created {@link TransactionRow} with {@code txnToken} populated
   */
  public TransactionRow beginTransaction(String isolation, int leaseSeconds, String ownerPrincipal)
      throws SQLException {
    long txnId = persistence.nextTxnId();
    String txnToken = generateToken();
    int effective = leaseSeconds > 0 ? leaseSeconds : DEFAULT_TXN_LEASE_SECONDS;
    Instant now = Instant.now();
    Instant expiresAt = now.plus(effective, ChronoUnit.SECONDS);

    // A linked txn_snapshots row is created for REPEATABLE_READ so the orchestrator can use
    // txn_snapshot_id for consistent reads (loadTable?txn_snapshot_id=X) with the same pinned view.
    // Its read_point is captured inside beginTransactionAtomically on the same connection.
    Long linkedTxnSnapshotId = null;
    TxnSnapshotRow snapRow = null;
    if ("REPEATABLE_READ".equalsIgnoreCase(isolation)) {
      long snapId = persistence.nextTxnId();
      snapRow =
          new TxnSnapshotRow(
              snapId,
              persistence.getRealmId(),
              persistence.getCatalogId(),
              // read_point is (re)captured atomically on the insert connection; this value is
              // unused.
              0L,
              now,
              expiresAt,
              null);
      linkedTxnSnapshotId = snapId;
    }

    TransactionRow row =
        new TransactionRow(
            txnId,
            null,
            persistence.getRealmId(),
            persistence.getCatalogId(),
            txnToken,
            ownerPrincipal,
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            isolation != null ? isolation : "REPEATABLE_READ",
            linkedTxnSnapshotId,
            now,
            expiresAt,
            (Instant) null);

    // Persist the transaction and its optional linked snapshot in ONE database transaction on one
    // pooled connection, instead of three autocommit round-trips (captureReadPoint +
    // insertTxnSnapshot + insertTransaction). Keeps BEGIN from tripling its draw on the connection
    // pool under write concurrency. Read-only transactions never reach this path (the Spark client
    // mints a stateless read handle instead), so this optimizes the write BEGIN specifically.
    persistence.beginTransactionAtomically(row, snapRow);
    LOGGER.info(
        "Transaction started: txn_id={} isolation={} expires={}", txnId, isolation, expiresAt);
    return row;
  }

  public TransactionRow getTransaction(long txnId) throws SQLException {
    return persistence
        .findTransaction(txnId)
        .orElseThrow(() -> new NotFoundException("Transaction not found: " + txnId));
  }

  public void abortTransaction(long txnId, String txnToken) throws SQLException {
    TransactionRow txn = validateTxnToken(txnId, txnToken);
    if (txn.status() == JdbcTransactionPersistence.TXN_STATUS_COMMITTED) {
      throw new BadRequestException("Transaction already committed: " + txnId);
    }
    persistence.updateTransactionStatus(txnId, JdbcTransactionPersistence.TXN_STATUS_ABORTED);
    lockManager.releaseAll(txnId);
    releaseTxnSnapshot(txn);
    LOGGER.info("Transaction aborted: txn_id={}", txnId);
  }

  public TransactionRow renewTransaction(long txnId, String txnToken, int leaseSeconds)
      throws SQLException {
    validateTxnToken(txnId, txnToken);
    int effective = leaseSeconds > 0 ? leaseSeconds : DEFAULT_TXN_LEASE_SECONDS;
    persistence.renewTransaction(txnId, Instant.now().plus(effective, ChronoUnit.SECONDS));
    return persistence.findTransaction(txnId).orElseThrow();
  }

  // ---------------------------------------------------------------------------
  // Subtransactions
  // ---------------------------------------------------------------------------

  /**
   * Creates a per-table subtransaction. Acquires a pessimistic lock for destructive operations
   * (ROW_DELTA, OVERWRITE, EXCLUSIVE, ACCESS_EXCLUSIVE). APPEND operations skip locking; they rely
   * on merge-and-retry at finalize time.
   *
   * @param tableUuid stable identifier of the target table (identifier string, e.g. "ns.table")
   * @param tableMetadataDir the table's canonical metadata directory (e.g.
   *     "s3://warehouse/ns/table/metadata"). The assigned staging path is a specific file inside
   *     this directory, keeping staged metadata within the table's own storage tree so that
   *     credential vending and location validation require no special-casing.
   * @return the new subtransaction with {@code subtxnToken} and {@code assignedStagingPath}
   */
  public OperationRow createOperation(
      long txnId,
      String txnToken,
      String tableUuid,
      String lockMode,
      Long baseSnapshotId,
      Long retryOfOpId,
      int leaseSeconds,
      String tableMetadataDir)
      throws SQLException, LockConflictException {
    return createOperation(
        txnId,
        txnToken,
        null,
        tableUuid,
        lockMode,
        null,
        baseSnapshotId,
        retryOfOpId,
        leaseSeconds,
        tableMetadataDir);
  }

  /**
   * Overload that binds the new operation to a subtransaction ({@code subtxnId}). When {@code
   * subtxnId} is null the operation is a bare inline op (the Problem-1 single-session path); when
   * non-null the op is a child of a {@code subtransactions} row and shares that subtransaction's
   * succeed-or-fail / retry-by-different-worker fate.
   */
  public OperationRow createOperation(
      long txnId,
      String txnToken,
      Long subtxnId,
      String tableUuid,
      String lockMode,
      String tableEntityId,
      Long baseSnapshotId,
      Long retryOfOpId,
      int leaseSeconds,
      String tableMetadataDir)
      throws SQLException, LockConflictException {

    TransactionRow txn = validateTxnToken(txnId, txnToken);
    requireInProgress(txn);

    // Always acquire a lock for every mode.
    // ROW_EXCLUSIVE: inserts a lock row so in-flight EXCLUSIVE requests can detect it and wait;
    //   fails immediately if EXCLUSIVE/ACCESS_EXCLUSIVE is already held.
    // EXCLUSIVE/ACCESS_EXCLUSIVE: returns a retry hint (409) if any conflict is found.
    String normalizedMode =
        lockMode == null ? LockManager.MODE_ROW_EXCLUSIVE : lockMode.toUpperCase(Locale.ROOT);
    lockManager.acquire(txnId, tableUuid, normalizedMode);

    long opId = persistence.nextTxnId();
    String opToken = generateToken();
    int effective = leaseSeconds > 0 ? leaseSeconds : DEFAULT_TXN_LEASE_SECONDS;
    Instant now = Instant.now();
    String stagingPath =
        LocationUtil.stripTrailingSlash(tableMetadataDir)
            + "/"
            + txnId
            + "-"
            + opId
            + "-"
            + UUID.randomUUID()
            + ".metadata.json";

    // Capture the table's current committed metadata-location so we can detect concurrent commits
    // at finalization time (conflict detection for merge-and-retry / serialization failure).
    // committed_table_snapshots is keyed by the numeric entity id, so look up by tableEntityId when
    // it is known; fall back to tableUuid (the qualified name) only for legacy/unresolved callers.
    String ctsKey = tableEntityId != null ? tableEntityId : tableUuid;
    String serverBase = persistence.findLatestCommittedMetadataLocation(ctsKey).orElse(null);

    OperationRow row =
        new OperationRow(
            opId,
            txnId,
            subtxnId,
            persistence.getRealmId(),
            persistence.getCatalogId(),
            tableUuid,
            normalizedMode,
            tableEntityId,
            baseSnapshotId,
            serverBase,
            stagingPath,
            null,
            opToken,
            JdbcTransactionPersistence.OP_STATUS_IN_PROGRESS,
            retryOfOpId,
            now,
            now.plus(effective, ChronoUnit.SECONDS),
            null);
    persistence.insertOperation(row);
    LOGGER.info(
        "Operation created: op_id={} txn_id={} table={} lock_mode={}",
        opId,
        txnId,
        tableUuid,
        normalizedMode);
    return row;
  }

  public void commitOperation(long txnId, long opId, String opToken, String stagedMetadataLocation)
      throws SQLException {
    OperationRow op = validateOpToken(opId, opToken);
    if (op.txnId() != txnId) {
      throw new BadRequestException(
          "Operation " + opId + " does not belong to transaction " + txnId);
    }
    if (op.status() != JdbcTransactionPersistence.OP_STATUS_IN_PROGRESS) {
      throw new BadRequestException("Operation already finalized: " + opId);
    }
    if (!stagedMetadataLocation.equals(op.assignedStagingPath())) {
      throw new BadRequestException(
          "staged_metadata_location '"
              + stagedMetadataLocation
              + "' does not match the assigned staging path '"
              + op.assignedStagingPath()
              + "'");
    }
    persistence.markOperationCommitted(opId, stagedMetadataLocation);
    // Release ROW_EXCLUSIVE lock immediately after staging so a waiting EXCLUSIVE can proceed.
    // EXCLUSIVE/ACCESS_EXCLUSIVE locks are held until the parent transaction ends.
    if (LockManager.MODE_ROW_EXCLUSIVE.equals(op.lockMode())) {
      lockManager.releaseForResource(txnId, op.tableUuid());
    }
    LOGGER.info("Operation staged: op_id={} metadata={}", opId, stagedMetadataLocation);
  }

  public void abortOperation(long txnId, long opId, String opToken, String txnToken)
      throws SQLException {
    // Accept either the op token or the parent txn token (orchestrator abort)
    OperationRow op;
    if (opToken != null && !opToken.isBlank()) {
      op = validateOpToken(opId, opToken);
    } else {
      validateTxnToken(txnId, txnToken);
      op =
          persistence
              .findOperation(opId)
              .orElseThrow(() -> new NotFoundException("Operation not found: " + opId));
    }
    if (op.txnId() != txnId) {
      throw new BadRequestException(
          "Operation " + opId + " does not belong to transaction " + txnId);
    }
    persistence.markOperationAborted(opId);
    // Release ROW_EXCLUSIVE lock on abort so waiting EXCLUSIVE requests can proceed.
    if (LockManager.MODE_ROW_EXCLUSIVE.equals(op.lockMode())) {
      lockManager.releaseForResource(op.txnId(), op.tableUuid());
    }
  }

  /**
   * Looks up a subtransaction by ID + token. Returns the parent transaction's ID, snapshot ID
   * (REPEATABLE_READ only), the assigned staging path, and the table UUID. Used by the worker's
   * {@code BEGIN SUBTRANSACTION} command to avoid passing those values through the orchestrator's
   * SQL output.
   */
  public record OperationInfo(
      long txnId, Long txnSnapshotId, String assignedStagingPath, String tableUuid) {}

  public OperationInfo getOperation(long opId, String opToken) throws SQLException {
    OperationRow op = validateOpToken(opId, opToken);
    TransactionRow txn = getTransaction(op.txnId());
    return new OperationInfo(
        txn.txnId(), txn.txnSnapshotId(), op.assignedStagingPath(), op.tableUuid());
  }

  public OperationRow renewOperation(long txnId, long opId, String opToken, int leaseSeconds)
      throws SQLException {
    validateOpToken(opId, opToken);
    int effective = leaseSeconds > 0 ? leaseSeconds : DEFAULT_TXN_LEASE_SECONDS;
    persistence.renewTransaction(txnId, Instant.now().plus(effective, ChronoUnit.SECONDS));
    return persistence.findOperation(opId).orElseThrow();
  }

  // ---------------------------------------------------------------------------
  // Subtransactions (savepoints — Use Case 2: worker grouping)
  // ---------------------------------------------------------------------------

  /**
   * One requested table within a subtransaction: the table's stable identifier, its metadata
   * directory (where the assigned staging path is placed), the lock mode, and an optional base
   * snapshot id.
   *
   * @param tableUuid stable identifier of the target table (identifier string, e.g. "ns.orders")
   * @param tableMetadataDir the table's canonical metadata directory
   * @param lockMode {@code ROW_EXCLUSIVE} / {@code EXCLUSIVE} / {@code ACCESS_EXCLUSIVE} (defaults
   *     to {@code ROW_EXCLUSIVE} when null)
   * @param baseSnapshotId optional Iceberg base snapshot the worker read from
   */
  public record SubtxnTableSpec(
      String tableUuid,
      String tableMetadataDir,
      String lockMode,
      Long baseSnapshotId,
      @Nullable String tableEntityId) {

    /** Convenience constructor for callers that cannot resolve the numeric entity id. */
    public SubtxnTableSpec(
        String tableUuid, String tableMetadataDir, String lockMode, Long baseSnapshotId) {
      this(tableUuid, tableMetadataDir, lockMode, baseSnapshotId, null);
    }
  }

  /**
   * A subtransaction plus its per-table child operations. Returned by {@link
   * #createSubtransaction}; the client (or a retrying worker) uses {@code subtxnToken} to drive the
   * whole group and each {@link OperationRow}'s {@code opToken} + {@code assignedStagingPath} to
   * stage a single table.
   */
  public record SubtxnInfo(
      JdbcTransactionPersistence.SubtxnRow subtxn,
      List<OperationRow> operations,
      @Nullable Long parentTxnSnapshotId) {}

  /**
   * Creates a subtransaction that groups {@code tables.size()} per-table operations under a single
   * succeed-or-fail boundary. Locks for every table are acquired up front; if any acquisition fails
   * the whole subtransaction is rolled back (locks released) before the error propagates, so a
   * partial subtransaction never lingers holding locks.
   *
   * @param name optional stable name for the subtransaction (savepoint name / worker task key).
   *     Reusing a non-aborted name within the same transaction is rejected by the unique index; a
   *     name freed by rollback (ABORTED) can be reused.
   */
  public SubtxnInfo createSubtransaction(
      long txnId, String txnToken, String name, int leaseSeconds, List<SubtxnTableSpec> tables)
      throws SQLException, LockConflictException {
    TransactionRow txn = validateTxnToken(txnId, txnToken);
    requireInProgress(txn);
    if (tables == null || tables.isEmpty()) {
      throw new BadRequestException("A subtransaction must reference at least one table");
    }

    long subtxnId = persistence.nextTxnId();
    String subtxnToken = generateToken();
    int effective = leaseSeconds > 0 ? leaseSeconds : DEFAULT_TXN_LEASE_SECONDS;
    Instant now = Instant.now();
    Instant expiresAt = now.plus(effective, ChronoUnit.SECONDS);

    persistence.insertSubtxn(
        new JdbcTransactionPersistence.SubtxnRow(
            subtxnId,
            txnId,
            persistence.getRealmId(),
            persistence.getCatalogId(),
            name,
            subtxnToken,
            JdbcTransactionPersistence.SUBTXN_STATUS_OPEN,
            1,
            now,
            expiresAt,
            null));

    List<OperationRow> ops = new ArrayList<>(tables.size());
    try {
      for (SubtxnTableSpec t : tables) {
        ops.add(
            createOperation(
                txnId,
                txnToken,
                subtxnId,
                t.tableUuid(),
                t.lockMode(),
                t.tableEntityId(),
                t.baseSnapshotId(),
                null,
                leaseSeconds,
                t.tableMetadataDir()));
      }
    } catch (RuntimeException | SQLException | LockConflictException e) {
      // Roll the partially-built subtransaction back so no lock is orphaned. Mark ABORTED (not
      // reopened) —
      // creation failed outright; the caller decides whether to retry with a fresh subtxn.
      try {
        List<OperationRow> aborted = persistence.abortSubtxn(subtxnId);
        releaseSubtxnLocks(txnId, aborted);
      } catch (SQLException cleanup) {
        LOGGER.warn(
            "Failed to clean up partial subtransaction {}: {}", subtxnId, cleanup.getMessage());
      }
      throw e;
    }

    LOGGER.info(
        "Subtransaction created: subtxn_id={} txn_id={} tables={}", subtxnId, txnId, tables.size());
    return new SubtxnInfo(persistence.findSubtxn(subtxnId).orElseThrow(), ops, txn.txnSnapshotId());
  }

  /**
   * Stages an entire subtransaction atomically. A subtransaction is executed by a single worker, so
   * staging is all-or-nothing: the server already assigned each operation's staging path at create
   * time, so there is no per-table body — the worker has written each table's metadata to its
   * {@code assigned_staging_path}, and this call marks every operation COMMITTED at that path, then
   * marks the subtransaction STAGED. The data is not yet visible; visibility happens only when the
   * parent transaction commits.
   *
   * <p>ROW_EXCLUSIVE locks are released as each op is staged (so waiting EXCLUSIVE requests can
   * proceed); EXCLUSIVE/ACCESS_EXCLUSIVE locks are held until the parent transaction ends.
   */
  public void commitSubtransaction(long subtxnId, String subtxnToken) throws SQLException {
    JdbcTransactionPersistence.SubtxnRow subtxn = validateSubtxnToken(subtxnId, subtxnToken);
    if (subtxn.status() != JdbcTransactionPersistence.SUBTXN_STATUS_OPEN) {
      throw new BadRequestException(
          "Subtransaction " + subtxnId + " is not OPEN (status=" + subtxn.status() + ")");
    }
    List<OperationRow> ops = persistence.findOperationsForSubtxn(subtxnId);
    for (OperationRow op : ops) {
      if (op.status() != JdbcTransactionPersistence.OP_STATUS_IN_PROGRESS) {
        continue;
      }
      persistence.markOperationCommitted(op.opId(), op.assignedStagingPath());
      if (LockManager.MODE_ROW_EXCLUSIVE.equals(op.lockMode())) {
        lockManager.releaseForResource(subtxn.txnId(), op.tableUuid());
      }
    }
    persistence.updateSubtxnStatus(subtxnId, JdbcTransactionPersistence.SUBTXN_STATUS_STAGED);
    LOGGER.info("Subtransaction staged: subtxn_id={} ops={}", subtxnId, ops.size());
  }

  /**
   * Rolls a subtransaction back: aborts every one of its operations, releases their locks, and —
   * when {@code reopenForRetry} is true — re-opens the subtransaction (attempt++) so the same
   * subtransaction can be re-staged, possibly by a different worker. When false the subtransaction
   * stays ABORTED (the transaction skips it at commit). Other subtransactions' staged work is
   * untouched.
   *
   * <p>Accepts either the subtransaction token or the parent txn token (orchestrator-driven
   * rollback).
   */
  public void rollbackSubtransaction(
      long txnId, long subtxnId, String subtxnToken, String txnToken, boolean reopenForRetry)
      throws SQLException {
    JdbcTransactionPersistence.SubtxnRow subtxn;
    if (subtxnToken != null && !subtxnToken.isBlank()) {
      subtxn = validateSubtxnToken(subtxnId, subtxnToken);
    } else {
      validateTxnToken(txnId, txnToken);
      subtxn =
          persistence
              .findSubtxn(subtxnId)
              .orElseThrow(() -> new NotFoundException("Subtransaction not found: " + subtxnId));
    }
    if (subtxn.txnId() != txnId) {
      throw new BadRequestException(
          "Subtransaction " + subtxnId + " does not belong to transaction " + txnId);
    }
    // Only an OPEN subtransaction can be rolled back or aborted. A STAGED subtransaction has
    // already
    // been committed by its (winning) worker and is waiting for the parent's atomic pointer swap;
    // aborting it here would discard that worker's committed operations and orphan its staged files
    // (abortSubtxn discards COMMITTED ops too). This is exactly the concurrent-worker case: a loser
    // that reacts to losing by issuing ROLLBACK/ABORT would silently revert the winner. Reject up
    // front — before abortSubtxn mutates anything — so the winner's staged work is never touched.
    // The loser's client still resets its own session (clearConf runs in its finally), so it
    // becomes
    // usable again without damaging the subtransaction. An ABORTED subtransaction is already
    // terminal.
    if (subtxn.status() != JdbcTransactionPersistence.SUBTXN_STATUS_OPEN) {
      throw new BadRequestException(
          "Subtransaction "
              + subtxnId
              + " is not OPEN (status="
              + subtxn.status()
              + "); it cannot be rolled back or aborted. A STAGED subtransaction was already committed"
              + " by another worker and will be applied when the parent transaction commits.");
    }

    List<OperationRow> aborted = persistence.abortSubtxn(subtxnId);
    releaseSubtxnLocks(txnId, aborted);

    if (reopenForRetry) {
      Instant expiresAt = Instant.now().plus(DEFAULT_TXN_LEASE_SECONDS, ChronoUnit.SECONDS);
      persistence.reopenSubtxn(subtxnId, expiresAt);
      // Re-create fresh operation rows (new staging paths) for each aborted op so the retry
      // worker gets a new assignedStagingPath from getSubtransaction — the old file may still
      // exist on storage, so reusing it would cause FileAlreadyExistsException on write.
      for (OperationRow old : aborted) {
        String tableMetadataDir =
            old.assignedStagingPath().substring(0, old.assignedStagingPath().lastIndexOf('/'));
        long newOpId = persistence.nextTxnId();
        String newOpToken = generateToken();
        String newStagingPath =
            tableMetadataDir
                + "/"
                + txnId
                + "-"
                + newOpId
                + "-"
                + UUID.randomUUID()
                + ".metadata.json";
        String ctsKey = old.tableEntityId() != null ? old.tableEntityId() : old.tableUuid();
        String serverBase = persistence.findLatestCommittedMetadataLocation(ctsKey).orElse(null);
        OperationRow retry =
            new OperationRow(
                newOpId,
                txnId,
                subtxnId,
                old.realmId(),
                old.catalogId(),
                old.tableUuid(),
                old.lockMode(),
                old.tableEntityId(),
                old.baseSnapshotId(),
                serverBase,
                newStagingPath,
                null,
                newOpToken,
                JdbcTransactionPersistence.OP_STATUS_IN_PROGRESS,
                old.opId(),
                Instant.now(),
                expiresAt,
                null);
        persistence.insertOperation(retry);
        try {
          lockManager.acquire(txnId, old.tableUuid(), old.lockMode());
        } catch (LockManager.LockConflictException e) {
          throw new jakarta.ws.rs.BadRequestException(
              "Lock conflict re-acquiring lock for retry: " + e.getMessage());
        }
      }
      LOGGER.info("Subtransaction rolled back and reopened for retry: subtxn_id={}", subtxnId);
    } else {
      LOGGER.info("Subtransaction rolled back (aborted): subtxn_id={}", subtxnId);
    }
  }

  /** Releases every lock held by the given (now-aborted) operations. */
  private void releaseSubtxnLocks(long txnId, List<OperationRow> ops) throws SQLException {
    for (OperationRow op : ops) {
      // Only ROW_EXCLUSIVE locks are released eagerly elsewhere; here we release whatever this op
      // still holds so a reopened/retried subtransaction re-acquires cleanly and EXCLUSIVE waiters
      // proceed.
      lockManager.releaseForResource(txnId, op.tableUuid());
    }
  }

  private JdbcTransactionPersistence.SubtxnRow validateSubtxnToken(
      long subtxnId, String subtxnToken) throws SQLException {
    JdbcTransactionPersistence.SubtxnRow subtxn =
        persistence
            .findSubtxn(subtxnId)
            .orElseThrow(() -> new NotFoundException("Subtransaction not found: " + subtxnId));
    if (!subtxn.subtxnToken().equals(subtxnToken)) {
      throw new ForbiddenException("Invalid subtxn_token for subtransaction " + subtxnId);
    }
    return subtxn;
  }

  /**
   * Looks up a subtransaction by id + token, returning the subtransaction row and its per-table
   * operations. Used by {@code getSubtransaction} so a resuming worker can see every operation's
   * assigned staging path.
   */
  public SubtxnInfo getSubtransactionInfo(long subtxnId, String subtxnToken) throws SQLException {
    JdbcTransactionPersistence.SubtxnRow subtxn = validateSubtxnToken(subtxnId, subtxnToken);
    // Only an OPEN subtransaction is resumable. Re-beginning a STAGED (already committed) or
    // ABORTED
    // subtransaction is a client mistake: commitSubtransaction would reject the eventual write
    // anyway (it guards status == OPEN), but by then beginWorkerSubtransaction has already set the
    // worker's full session state (txn_id, pinned snapshot id, worker keys). That state then
    // outlives the failed write and poisons every subsequent read against a released snapshot. Fail
    // the resume up front, before any session state is set, so the worker session stays clean.
    if (subtxn.status() != JdbcTransactionPersistence.SUBTXN_STATUS_OPEN) {
      throw new BadRequestException(
          "Subtransaction "
              + subtxnId
              + " is not OPEN (status="
              + subtxn.status()
              + "); it cannot be resumed. ROLLBACK SUBTRANSACTION to retry it, or start a new one.");
    }
    Long parentSnapshotId =
        persistence.findTransaction(subtxn.txnId()).map(TransactionRow::txnSnapshotId).orElse(null);
    return new SubtxnInfo(subtxn, persistence.findOperationsForSubtxn(subtxnId), parentSnapshotId);
  }

  /**
   * Extends a subtransaction's lease. The lease is tracked on the subtransaction row; renewing
   * bumps its {@code expires_at} (and the parent transaction's, so the whole group stays live).
   */
  public SubtxnInfo renewSubtransaction(
      long txnId, long subtxnId, String subtxnToken, int leaseSeconds) throws SQLException {
    JdbcTransactionPersistence.SubtxnRow subtxn = validateSubtxnToken(subtxnId, subtxnToken);
    if (subtxn.txnId() != txnId) {
      throw new BadRequestException(
          "Subtransaction " + subtxnId + " does not belong to transaction " + txnId);
    }
    int effective = leaseSeconds > 0 ? leaseSeconds : DEFAULT_TXN_LEASE_SECONDS;
    Instant expiresAt = Instant.now().plus(effective, ChronoUnit.SECONDS);
    persistence.renewSubtxn(subtxnId, expiresAt);
    persistence.renewTransaction(txnId, expiresAt);
    Long parentSnapshotId =
        persistence.findTransaction(txnId).map(TransactionRow::txnSnapshotId).orElse(null);
    return new SubtxnInfo(
        persistence.findSubtxn(subtxnId).orElseThrow(),
        persistence.findOperationsForSubtxn(subtxnId),
        parentSnapshotId);
  }

  // ---------------------------------------------------------------------------
  // Inline commit (Use Case 1 — single-session staged writes)
  // ---------------------------------------------------------------------------

  /**
   * One table's contribution to an inline commit. Built by {@link #prepareInlineCommit}; consumed
   * by {@code IcebergCatalogHandler.finalizeInlineTables}.
   *
   * @param tableIdentifier dot-separated name, e.g. {@code "ns.orders"}
   * @param stagedMetadataLocation path to the client-written staged metadata JSON
   * @param serverBaseMetadataLocation the latest committed metadata location at prepare time;
   *     {@code null} when no prior commit exists or this entry is on the merge-retry path
   * @param lockMode {@code "ROW_EXCLUSIVE"}, {@code "EXCLUSIVE"}, or {@code "ACCESS_EXCLUSIVE"}
   */
  public record StagedTableInfo(
      String tableIdentifier,
      String stagedMetadataLocation,
      String serverBaseMetadataLocation,
      String lockMode,
      // Numeric entity ID string matching committed_table_snapshots.table_uuid
      @Nullable String tableEntityId) {

    /** Backward-compatible constructor without tableEntityId. */
    public StagedTableInfo(
        String tableIdentifier,
        String stagedMetadataLocation,
        String serverBaseMetadataLocation,
        String lockMode) {
      this(tableIdentifier, stagedMetadataLocation, serverBaseMetadataLocation, lockMode, null);
    }
  }

  /**
   * Result of {@link #prepareInlineCommit}. All validated tables go to {@code direct}; conflicts
   * abort the transaction and throw {@link SerializationFailureException} before this returns.
   * {@code mergeRetry} is always empty for inline transactions (kept for API symmetry with {@link
   * PrepareCommitResult}).
   */
  public record PrepareInlineResult(
      List<StagedTableInfo> direct, List<StagedTableInfo> mergeRetry) {}

  /**
   * Validates and classifies the inline staged tables provided by the client (Use Case 1).
   *
   * <p>Conflict detection mirrors {@link #prepareCommit}: for REPEATABLE_READ transactions the
   * snapshot-time metadata location is compared against the current committed location. A
   * difference means a concurrent commit occurred:
   *
   * <ul>
   *   <li>Any concurrent write → first-committer-wins; aborts the transaction and throws {@link
   *       SerializationFailureException}. Merge-and-retry is not used for inline transactions
   *       because there is no server-held staged file to re-parent onto the new table state.
   * </ul>
   *
   * For READ_COMMITTED all tables are routed directly (no snapshot baseline).
   */
  public PrepareInlineResult prepareInlineCommit(
      long txnId, String txnToken, List<StagedTableInfo> inputTables) throws SQLException {
    TransactionRow txn = validateTxnToken(txnId, txnToken);
    requireInProgress(txn);

    List<StagedTableInfo> direct = new ArrayList<>();
    List<StagedTableInfo> mergeRetry = new ArrayList<>();

    for (StagedTableInfo info : inputTables) {
      // Use the resolved numeric entity ID for committed_table_snapshots lookups when available;
      // fall back to tableIdentifier for tests / callers that don't supply it.
      String snapshotKey =
          info.tableEntityId() != null ? info.tableEntityId() : info.tableIdentifier();

      String currentMeta =
          persistence.findLatestCommittedMetadataLocation(snapshotKey).orElse(null);

      // Determine the baseline visible at transaction-begin time (REPEATABLE_READ only).
      String baselineMeta = null;
      if (txn.txnSnapshotId() != null) {
        baselineMeta =
            resolveTableMetadataLocationByRow(snapshotKey, txn.txnSnapshotId()).orElse(null);
      }

      boolean conflict = baselineMeta != null && !baselineMeta.equals(currentMeta);
      if (!conflict) {
        direct.add(
            new StagedTableInfo(
                info.tableIdentifier(),
                info.stagedMetadataLocation(),
                currentMeta,
                info.lockMode(),
                snapshotKey));
      } else {
        // Inline transactions always use first-committer-wins under REPEATABLE_READ.
        // Unlike the subtransaction path, there is no server-held staged file to re-parent,
        // so merge-and-retry is not applicable here regardless of lock mode.
        persistence.updateTransactionStatus(txnId, JdbcTransactionPersistence.TXN_STATUS_ABORTED);
        lockManager.releaseAll(txnId);
        LOGGER.info(
            "Serialization failure (inline): txn={} table={} lock_mode={} base={} current={}",
            txnId,
            info.tableIdentifier(),
            info.lockMode(),
            baselineMeta,
            currentMeta);
        throw new SerializationFailureException(
            "could not serialize access due to concurrent update on table "
                + info.tableIdentifier());
      }
    }
    return new PrepareInlineResult(direct, mergeRetry);
  }

  /**
   * Thrown by {@link #prepareCommit} when a non-APPEND subtransaction detects a concurrent write.
   */
  public static class SerializationFailureException extends RuntimeException {
    public SerializationFailureException(String message) {
      super(message);
    }
  }

  /**
   * Result of {@link #prepareCommit}. Subtransactions are split by required finalization action:
   *
   * <ul>
   *   <li>{@code direct} — no concurrent write detected; do a straightforward pointer swap.
   *   <li>{@code mergeRetry} — APPEND with a concurrent commit; server must re-parent data files.
   * </ul>
   */
  public record PrepareCommitResult(List<OperationRow> direct, List<OperationRow> mergeRetry) {}

  /**
   * Validates and classifies all SUB_COMMITTED subtransactions for finalization.
   *
   * <p>For each subtransaction, compares {@code server_base_metadata_location} (captured at
   * creation time) against the latest entry in {@code committed_table_snapshots}. A difference
   * means another writer committed to the same table since this subtransaction started:
   *
   * <ul>
   *   <li>APPEND: routed to {@link PrepareCommitResult#mergeRetry()} for server-side re-parenting.
   *   <li>ROW_DELTA / OVERWRITE / EXCLUSIVE: transaction is aborted immediately and a {@link
   *       SerializationFailureException} is thrown (HTTP 409, first-committer-wins).
   * </ul>
   */
  public PrepareCommitResult prepareCommit(long txnId, String txnToken) throws SQLException {
    TransactionRow txn = validateTxnToken(txnId, txnToken);
    requireInProgress(txn);

    List<OperationRow> subtxns = persistence.findOperationsForTransaction(txnId);

    // Subtransaction-granularity guard: ops of an ABORTED subtransaction are skipped entirely
    // (their staged work is
    // discarded); ops that belong to a still-OPEN subtransaction — or a bare inline op — must be
    // finalized
    // before the parent can commit. A non-null subtxn_id whose subtransaction is ABORTED is
    // invisible here.
    java.util.Set<Long> abortedSubtxnIds =
        persistence.findSubtxnsForTransaction(txnId).stream()
            .filter(u -> u.status() == JdbcTransactionPersistence.SUBTXN_STATUS_ABORTED)
            .map(JdbcTransactionPersistence.SubtxnRow::subtxnId)
            .collect(java.util.stream.Collectors.toSet());

    List<OperationRow> live =
        subtxns.stream()
            .filter(s -> s.subtxnId() == null || !abortedSubtxnIds.contains(s.subtxnId()))
            .toList();

    long uncommitted =
        live.stream()
            .filter(s -> s.status() == JdbcTransactionPersistence.OP_STATUS_IN_PROGRESS)
            .count();
    if (uncommitted > 0) {
      throw new BadRequestException(
          uncommitted
              + " operation(s) are still IN_PROGRESS for txn "
              + txnId
              + " (open subtransaction or un-staged inline write)."
              + " Stage/commit or roll them back before committing the transaction.");
    }

    List<OperationRow> direct = new ArrayList<>();
    List<OperationRow> mergeRetry = new ArrayList<>();
    for (OperationRow s :
        live.stream()
            .filter(s -> s.status() == JdbcTransactionPersistence.OP_STATUS_COMMITTED)
            .toList()) {
      // committed_table_snapshots is keyed by the numeric entity id; look up by tableEntityId when
      // known (fall back to tableUuid only for legacy ops created before the id was recorded).
      String ctsKey = s.tableEntityId() != null ? s.tableEntityId() : s.tableUuid();
      String currentMeta = persistence.findLatestCommittedMetadataLocation(ctsKey).orElse(null);
      // Conflict when the live committed state diverges from our recorded baseline.
      // Two cases require mergeRetry:
      // 1. serverBase is set and differs from currentMeta — a concurrent transaction committed
      //    between lock-acquire and parent COMMIT.
      // 2. serverBase is null (lock acquired before any transaction committed to committed_table_
      //    snapshots) but currentMeta is not null — a concurrent transaction committed in the
      //    window between lock-acquire and parent COMMIT. Without this check, the direct path
      //    would do ops.commit(ops.current(), staged_on_old_base), replacing the concurrent
      //    commit's snapshot chain and losing that writer's data files.
      boolean conflict =
          currentMeta != null
              && (s.serverBaseMetadataLocation() == null
                  || !s.serverBaseMetadataLocation().equals(currentMeta));
      if (!conflict) {
        direct.add(s);
      } else if (LockManager.MODE_ROW_EXCLUSIVE.equals(s.lockMode())) {
        // ROW_EXCLUSIVE (additive/OCC): a concurrent writer committed between this subtransaction's
        // creation and the parent COMMIT. Merge-and-retry — extract this transaction's added data
        // files from the staged metadata and re-parent them onto the current table state.
        LOGGER.info(
            "ROW_EXCLUSIVE conflict detected for table {} in txn {}: scheduling merge-and-retry",
            s.tableUuid(),
            txnId);
        mergeRetry.add(s);
      } else {
        // EXCLUSIVE / ACCESS_EXCLUSIVE (pessimistic, potentially destructive): a concurrent writer
        // advanced the table since this op was created. First-committer-wins — abort the whole
        // transaction and surface a serialization failure (PostgreSQL 40001 semantics, documented
        // in multi-table-transaction-design.md). Unlike the inline path — which cannot re-parent
        // and so aborts on ANY conflict — the subtransaction path merges ROW_EXCLUSIVE above and
        // only reaches this abort branch for the pessimistic lock modes.
        persistence.updateTransactionStatus(txnId, JdbcTransactionPersistence.TXN_STATUS_ABORTED);
        lockManager.releaseAll(txnId);
        LOGGER.info(
            "Serialization failure: txn={} table={} lock_mode={} base={} current={}",
            txnId,
            s.tableUuid(),
            s.lockMode(),
            s.serverBaseMetadataLocation(),
            currentMeta);
        throw new SerializationFailureException(
            "could not serialize access due to concurrent update on table " + s.tableUuid());
      }
    }
    return new PrepareCommitResult(direct, mergeRetry);
  }

  /**
   * Marks the transaction COMMITTED and releases all locks. Called by the adapter after it has
   * successfully persisted entity updates and inserted {@code committed_table_snapshots} rows.
   */
  public void finalizeCommit(long txnId) throws SQLException {
    TransactionRow txn = getTransaction(txnId);
    persistence.updateTransactionStatus(txnId, JdbcTransactionPersistence.TXN_STATUS_COMMITTED);
    lockManager.releaseAll(txnId);
    releaseTxnSnapshot(txn);
    LOGGER.info("Transaction committed: txn_id={}", txnId);
  }

  /** Releases the linked {@code txn_snapshots} row when a transaction terminates. */
  private void releaseTxnSnapshot(TransactionRow txn) {
    if (txn.txnSnapshotId() == null) {
      return;
    }
    try {
      persistence.releaseTxnSnapshot(txn.txnSnapshotId());
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to release txn_snapshots row {} for transaction {}: {}",
          txn.txnSnapshotId(),
          txn.txnId(),
          e.getMessage());
    }
  }

  // ---------------------------------------------------------------------------
  // Snapshot resolution (loadTable with txn_snapshot_id)
  // ---------------------------------------------------------------------------

  /**
   * Resolves the metadata location for {@code tableUuid} as visible under the opaque read-point
   * {@code handle} (standalone read path). The handle is decoded fail-closed: a malformed handle
   * throws {@link BadRequestException} rather than falling back to the latest state. Returns {@link
   * Optional#empty()} if the table had no commits visible at the handle's read point.
   *
   * <p>The handle's read point is authoritative; there is no server row to look up, and the lease
   * is enforced globally by the reaper's {@code read_floor} GC (a handle whose read point has been
   * purged resolves to {@link Optional#empty()} → the caller surfaces a fail-closed 404).
   */
  public Optional<String> resolveTableMetadataLocation(String tableUuid, String handle)
      throws SQLException {
    TxnSnapshotHandle.Payload payload = TxnSnapshotHandle.decode(handle);
    return persistence
        .resolveTableSnapshot(tableUuid, payload.readPoint())
        .map(CommittedTableSnapshotRow::metadataLocation);
  }

  /**
   * Resolves the metadata location for {@code tableUuid} as visible under a write-linked snapshot
   * row (REPEATABLE_READ transaction baseline). Unlike the standalone read path this consults the
   * live {@code txn_snapshots} row: the write path owns that row (it is lockable, revocable, and
   * abortable), and its expiry/release is meaningful. Returns {@link Optional#empty()} if the table
   * had no commits visible under the row's read point.
   */
  public Optional<String> resolveTableMetadataLocationByRow(String tableUuid, long txnSnapshotId)
      throws SQLException {
    TxnSnapshotRow snap =
        persistence
            .findTxnSnapshot(txnSnapshotId)
            .orElseThrow(
                () -> new NotFoundException("Transaction snapshot not found: " + txnSnapshotId));
    if (snap.releasedAt() != null || snap.expiresAt().isBefore(Instant.now())) {
      throw new BadRequestException(
          "Transaction snapshot " + txnSnapshotId + " has expired or been released");
    }
    return persistence
        .resolveTableSnapshot(tableUuid, snap)
        .map(CommittedTableSnapshotRow::metadataLocation);
  }

  /**
   * Returns the staged metadata location for {@code tableUuid} within an in-progress transaction
   * (read-your-own-writes). Returns {@link Optional#empty()} if no SUB_COMMITTED subtransaction
   * exists for this table.
   */
  public Optional<String> resolveRYOWMetadataLocation(String tableUuid, long txnId)
      throws SQLException {
    return persistence.findOperationsForTransaction(txnId).stream()
        .filter(
            s ->
                s.tableUuid().equals(tableUuid)
                    && s.status() == JdbcTransactionPersistence.OP_STATUS_COMMITTED)
        .map(OperationRow::stagedMetadataLocation)
        .filter(loc -> loc != null && !loc.isBlank())
        .findFirst();
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  private TransactionRow validateTxnToken(long txnId, String txnToken) throws SQLException {
    TransactionRow txn = getTransaction(txnId);
    if (!txn.txnToken().equals(txnToken)) {
      throw new ForbiddenException("Invalid txn_token for transaction " + txnId);
    }
    return txn;
  }

  private OperationRow validateOpToken(long opId, String opToken) throws SQLException {
    OperationRow op =
        persistence
            .findOperation(opId)
            .orElseThrow(() -> new NotFoundException("Operation not found: " + opId));
    if (!op.opToken().equals(opToken)) {
      throw new ForbiddenException("Invalid op_token for operation " + opId);
    }
    return op;
  }

  private static void requireInProgress(TransactionRow txn) {
    if (txn.status() != JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS) {
      throw new BadRequestException(
          "Transaction " + txn.txnId() + " is not IN_PROGRESS (status=" + txn.status() + ")");
    }
    if (txn.expiresAt().isBefore(Instant.now())) {
      throw new BadRequestException("Transaction " + txn.txnId() + " has expired");
    }
  }

  private static String generateToken() {
    byte[] bytes = new byte[32];
    SECURE_RANDOM.nextBytes(bytes);
    return HexFormat.of().formatHex(bytes);
  }
}
