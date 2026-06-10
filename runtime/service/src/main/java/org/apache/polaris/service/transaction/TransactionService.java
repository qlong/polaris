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

import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotFoundException;
import java.security.SecureRandom;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.iceberg.util.LocationUtil;
import java.util.stream.Collectors;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.CommittedTableSnapshotRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.SubtransactionRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.TransactionRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.TxnSnapshotRow;
import org.apache.polaris.service.transaction.LockManager.LockWaitTimeoutException;
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
 *   <li>{@link #createSubtransaction} — allocate a per-table work unit; acquire lock if needed
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

  /**
   * Acquires a new read-consistent snapshot. The returned {@link TxnSnapshotRow} contains (xmin,
   * xmax, xip_list) captured atomically, giving the caller a pinned view across all subsequent
   * {@code loadTable?txn_snapshot_id=X} calls.
   */
  public TxnSnapshotRow createTransactionSnapshot(int leaseSeconds) throws SQLException {
    long id = persistence.nextTxnId();
    long[] xState = persistence.captureXminXmax();
    long xmin = xState[0];
    long xmax = xState[1];
    long[] xipArr = Arrays.copyOfRange(xState, 2, xState.length);
    String xipList =
        Arrays.stream(xipArr).mapToObj(Long::toString).collect(Collectors.joining(","));

    int effectiveLease = leaseSeconds > 0 ? leaseSeconds : DEFAULT_SNAPSHOT_LEASE_SECONDS;
    Instant now = Instant.now();
    TxnSnapshotRow row =
        new TxnSnapshotRow(
            id,
            persistence.getRealmId(),
            persistence.getCatalogId(),
            xmin,
            xmax,
            xipList,
            now,
            now.plus(effectiveLease, ChronoUnit.SECONDS),
            null);
    persistence.insertTxnSnapshot(row);
    LOGGER.info("TxnSnapshot created: id={} xmin={} xmax={} xip=[{}]", id, xmin, xmax, xipList);
    return row;
  }

  public TxnSnapshotRow getTransactionSnapshot(long txnSnapshotId) throws SQLException {
    return persistence
        .findTxnSnapshot(txnSnapshotId)
        .orElseThrow(
            () -> new NotFoundException("Transaction snapshot not found: " + txnSnapshotId));
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

    Long snapXmin = null, snapXmax = null;
    String snapXipList = null;
    Long linkedTxnSnapshotId = null;
    if ("REPEATABLE_READ".equalsIgnoreCase(isolation)) {
      long[] xs = persistence.captureXminXmax();
      snapXmin = xs[0];
      snapXmax = xs[1];
      long[] xipArr = Arrays.copyOfRange(xs, 2, xs.length);
      snapXipList = Arrays.stream(xipArr).mapToObj(Long::toString).collect(Collectors.joining(","));

      // Create a real txn_snapshots row so the orchestrator can use txn_snapshot_id for
      // consistent reads (loadTable?txn_snapshot_id=X) with the same pinned view.
      long snapId = persistence.nextTxnId();
      String xipListForSnap = snapXipList;
      TxnSnapshotRow snapRow =
          new TxnSnapshotRow(
              snapId,
              persistence.getRealmId(),
              persistence.getCatalogId(),
              snapXmin,
              snapXmax,
              xipListForSnap != null ? xipListForSnap : "",
              now,
              expiresAt,
              null);
      persistence.insertTxnSnapshot(snapRow);
      linkedTxnSnapshotId = snapId;
    }

    TransactionRow row =
        new TransactionRow(
            txnId,
            persistence.getRealmId(),
            persistence.getCatalogId(),
            txnToken,
            ownerPrincipal,
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            isolation != null ? isolation : "REPEATABLE_READ",
            snapXmin,
            snapXmax,
            snapXipList,
            linkedTxnSnapshotId,
            now,
            expiresAt,
            (Instant) null);
    persistence.insertTransaction(row);
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
  public SubtransactionRow createSubtransaction(
      long txnId,
      String txnToken,
      String tableUuid,
      String operation,
      Long baseSnapshotId,
      Long retryOfSubtxnId,
      int leaseSeconds,
      String tableMetadataDir)
      throws SQLException, LockWaitTimeoutException {

    TransactionRow txn = validateTxnToken(txnId, txnToken);
    requireInProgress(txn);

    // Acquire lock for destructive operations (APPEND skips this)
    if (!LockManager.MODE_APPEND.equalsIgnoreCase(operation)) {
      String resource = tableUuid;
      String normalizedOp = operation.toUpperCase(Locale.ROOT);
      lockManager.acquire(txnId, resource, normalizedOp);
    }

    long subtxnId = persistence.nextTxnId();
    String subtxnToken = generateToken();
    int effective = leaseSeconds > 0 ? leaseSeconds : DEFAULT_TXN_LEASE_SECONDS;
    Instant now = Instant.now();
    String stagingPath =
        LocationUtil.stripTrailingSlash(tableMetadataDir)
            + "/"
            + txnId
            + "-"
            + subtxnId
            + "-"
            + UUID.randomUUID()
            + ".metadata.json";

    // Capture the table's current committed metadata-location so we can detect concurrent commits
    // at finalization time (conflict detection for merge-and-retry / serialization failure).
    String serverBase =
        persistence.findLatestCommittedMetadataLocation(tableUuid).orElse(null);

    SubtransactionRow row =
        new SubtransactionRow(
            subtxnId,
            txnId,
            persistence.getRealmId(),
            persistence.getCatalogId(),
            tableUuid,
            operation.toUpperCase(Locale.ROOT),
            baseSnapshotId,
            serverBase,
            stagingPath,
            null,
            subtxnToken,
            JdbcTransactionPersistence.SUBTXN_STATUS_IN_PROGRESS,
            retryOfSubtxnId,
            now,
            now.plus(effective, ChronoUnit.SECONDS),
            null);
    persistence.insertSubtransaction(row);
    LOGGER.info(
        "Subtransaction created: subtxn_id={} txn_id={} table={} op={}",
        subtxnId,
        txnId,
        tableUuid,
        operation);
    return row;
  }

  public void commitSubtransaction(
      long txnId, long subtxnId, String subtxnToken, String stagedMetadataLocation)
      throws SQLException {
    SubtransactionRow subtxn = validateSubtxnToken(subtxnId, subtxnToken);
    if (subtxn.parentTxnId() != txnId) {
      throw new BadRequestException(
          "Subtransaction " + subtxnId + " does not belong to transaction " + txnId);
    }
    if (subtxn.status() != JdbcTransactionPersistence.SUBTXN_STATUS_IN_PROGRESS) {
      throw new BadRequestException("Subtransaction already finalized: " + subtxnId);
    }
    if (!stagedMetadataLocation.equals(subtxn.assignedStagingPath())) {
      throw new BadRequestException(
          "staged_metadata_location '"
              + stagedMetadataLocation
              + "' does not match the assigned staging path '"
              + subtxn.assignedStagingPath()
              + "'");
    }
    persistence.markSubtransactionCommitted(subtxnId, stagedMetadataLocation);
    LOGGER.info(
        "Subtransaction staged: subtxn_id={} metadata={}", subtxnId, stagedMetadataLocation);
  }

  public void abortSubtransaction(long txnId, long subtxnId, String subtxnToken, String txnToken)
      throws SQLException {
    // Accept either the subtxn token or the parent txn token (orchestrator abort)
    SubtransactionRow subtxn;
    if (subtxnToken != null && !subtxnToken.isBlank()) {
      subtxn = validateSubtxnToken(subtxnId, subtxnToken);
    } else {
      validateTxnToken(txnId, txnToken);
      subtxn =
          persistence
              .findSubtransaction(subtxnId)
              .orElseThrow(() -> new NotFoundException("Subtransaction not found: " + subtxnId));
    }
    if (subtxn.parentTxnId() != txnId) {
      throw new BadRequestException(
          "Subtransaction " + subtxnId + " does not belong to transaction " + txnId);
    }
    persistence.markSubtransactionAborted(subtxnId);
  }

  /**
   * Looks up a subtransaction by ID + token. Returns the parent transaction's ID,
   * snapshot ID (REPEATABLE_READ only), the assigned staging path, and the table UUID.
   * Used by the worker's {@code BEGIN SUBTRANSACTION} command to avoid passing those
   * values through the orchestrator's SQL output.
   */
  public record SubtransactionInfo(
      long txnId, Long txnSnapshotId, String assignedStagingPath, String tableUuid) {}

  public SubtransactionInfo getSubtransaction(long subtxnId, String subtxnToken)
      throws SQLException {
    SubtransactionRow subtxn = validateSubtxnToken(subtxnId, subtxnToken);
    TransactionRow txn = getTransaction(subtxn.parentTxnId());
    return new SubtransactionInfo(
        txn.txnId(), txn.txnSnapshotId(), subtxn.assignedStagingPath(), subtxn.tableUuid());
  }

  public SubtransactionRow renewSubtransaction(
      long txnId, long subtxnId, String subtxnToken, int leaseSeconds) throws SQLException {
    validateSubtxnToken(subtxnId, subtxnToken);
    int effective = leaseSeconds > 0 ? leaseSeconds : DEFAULT_TXN_LEASE_SECONDS;
    // Reuse transaction renew since subtransactions share the same lease semantics
    persistence.renewTransaction(txnId, Instant.now().plus(effective, ChronoUnit.SECONDS));
    return persistence.findSubtransaction(subtxnId).orElseThrow();
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
   * @param operation {@code "APPEND"} or {@code "ROW_DELTA"}
   */
  public record StagedTableInfo(
      String tableIdentifier,
      String stagedMetadataLocation,
      String serverBaseMetadataLocation,
      String operation) {}

  /** Parallel to {@link PrepareCommitResult} but for the inline path. */
  public record PrepareInlineResult(
      List<StagedTableInfo> direct, List<StagedTableInfo> mergeRetry) {}

  /**
   * Validates and classifies the inline staged tables provided by the client (Use Case 1).
   *
   * <p>Conflict detection mirrors {@link #prepareCommit}: for REPEATABLE_READ transactions the
   * snapshot-time metadata location is compared against the current committed location. A
   * difference means a concurrent commit occurred:
   * <ul>
   *   <li>APPEND → merge-and-retry
   *   <li>ROW_DELTA → first-committer-wins; aborts the transaction and throws
   *       {@link SerializationFailureException}
   * </ul>
   * For READ_COMMITTED all tables are routed directly (no snapshot baseline).
   */
  public PrepareInlineResult prepareInlineCommit(
      long txnId, String txnToken, List<StagedTableInfo> inputTables) throws SQLException {
    TransactionRow txn = validateTxnToken(txnId, txnToken);
    requireInProgress(txn);

    List<StagedTableInfo> direct = new ArrayList<>();
    List<StagedTableInfo> mergeRetry = new ArrayList<>();

    for (StagedTableInfo info : inputTables) {
      String currentMeta =
          persistence.findLatestCommittedMetadataLocation(info.tableIdentifier()).orElse(null);

      // Determine the baseline visible at transaction-begin time (REPEATABLE_READ only).
      String baselineMeta = null;
      if (txn.txnSnapshotId() != null) {
        baselineMeta =
            resolveTableMetadataLocation(info.tableIdentifier(), txn.txnSnapshotId()).orElse(null);
      }

      boolean conflict = baselineMeta != null && !baselineMeta.equals(currentMeta);
      if (!conflict) {
        direct.add(
            new StagedTableInfo(
                info.tableIdentifier(),
                info.stagedMetadataLocation(),
                currentMeta,
                info.operation()));
      } else if (LockManager.MODE_APPEND.equalsIgnoreCase(info.operation())) {
        LOGGER.info(
            "APPEND conflict detected for table {} in inline txn {}: scheduling merge-and-retry",
            info.tableIdentifier(), txnId);
        mergeRetry.add(
            new StagedTableInfo(
                info.tableIdentifier(), info.stagedMetadataLocation(), null, info.operation()));
      } else {
        persistence.updateTransactionStatus(txnId, JdbcTransactionPersistence.TXN_STATUS_ABORTED);
        lockManager.releaseAll(txnId);
        LOGGER.info(
            "Serialization failure (inline): txn={} table={} op={} base={} current={}",
            txnId, info.tableIdentifier(), info.operation(), baselineMeta, currentMeta);
        throw new SerializationFailureException(
            "could not serialize access due to concurrent update on table "
                + info.tableIdentifier());
      }
    }
    return new PrepareInlineResult(direct, mergeRetry);
  }

  /** Thrown by {@link #prepareCommit} when a non-APPEND subtransaction detects a concurrent write. */
  public static class SerializationFailureException extends RuntimeException {
    public SerializationFailureException(String message) {
      super(message);
    }
  }

  /**
   * Result of {@link #prepareCommit}. Subtransactions are split by required finalization action:
   * <ul>
   *   <li>{@code direct} — no concurrent write detected; do a straightforward pointer swap.
   *   <li>{@code mergeRetry} — APPEND with a concurrent commit; server must re-parent data files.
   * </ul>
   */
  public record PrepareCommitResult(
      List<SubtransactionRow> direct, List<SubtransactionRow> mergeRetry) {}

  /**
   * Validates and classifies all SUB_COMMITTED subtransactions for finalization.
   *
   * <p>For each subtransaction, compares {@code server_base_metadata_location} (captured at
   * creation time) against the latest entry in {@code committed_table_snapshots}. A difference
   * means another writer committed to the same table since this subtransaction started:
   * <ul>
   *   <li>APPEND: routed to {@link PrepareCommitResult#mergeRetry()} for server-side re-parenting.
   *   <li>ROW_DELTA / OVERWRITE / EXCLUSIVE: transaction is aborted immediately and a
   *       {@link SerializationFailureException} is thrown (HTTP 409, first-committer-wins).
   * </ul>
   */
  public PrepareCommitResult prepareCommit(long txnId, String txnToken) throws SQLException {
    TransactionRow txn = validateTxnToken(txnId, txnToken);
    requireInProgress(txn);

    List<SubtransactionRow> subtxns = persistence.findSubtransactionsForTransaction(txnId);
    long uncommitted =
        subtxns.stream()
            .filter(s -> s.status() == JdbcTransactionPersistence.SUBTXN_STATUS_IN_PROGRESS)
            .count();
    if (uncommitted > 0) {
      throw new BadRequestException(
          uncommitted
              + " subtransaction(s) are still IN_PROGRESS for txn "
              + txnId
              + ". Commit or abort them before committing the transaction.");
    }

    List<SubtransactionRow> direct = new ArrayList<>();
    List<SubtransactionRow> mergeRetry = new ArrayList<>();
    for (SubtransactionRow s :
        subtxns.stream()
            .filter(s -> s.status() == JdbcTransactionPersistence.SUBTXN_STATUS_SUB_COMMITTED)
            .toList()) {
      String currentMeta =
          persistence.findLatestCommittedMetadataLocation(s.tableUuid()).orElse(null);
      boolean conflict =
          s.serverBaseMetadataLocation() != null
              && !s.serverBaseMetadataLocation().equals(currentMeta);
      if (!conflict) {
        direct.add(s);
      } else if (LockManager.MODE_APPEND.equalsIgnoreCase(s.operation())) {
        LOGGER.info(
            "APPEND conflict detected for table {} in txn {}: scheduling merge-and-retry",
            s.tableUuid(), txnId);
        mergeRetry.add(s);
      } else {
        // First-committer-wins: abort transaction and signal 409.
        persistence.updateTransactionStatus(txnId, JdbcTransactionPersistence.TXN_STATUS_ABORTED);
        lockManager.releaseAll(txnId);
        LOGGER.info(
            "Serialization failure: txn={} table={} op={} base={} current={}",
            txnId, s.tableUuid(), s.operation(), s.serverBaseMetadataLocation(), currentMeta);
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
   * Resolves the metadata location for {@code tableUuid} as visible under {@code txnSnapshotId}.
   * Returns {@link Optional#empty()} if the table had no commits visible under this snapshot.
   */
  public Optional<String> resolveTableMetadataLocation(String tableUuid, long txnSnapshotId)
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
    return persistence.findSubtransactionsForTransaction(txnId).stream()
        .filter(
            s ->
                s.tableUuid().equals(tableUuid)
                    && s.status() == JdbcTransactionPersistence.SUBTXN_STATUS_SUB_COMMITTED)
        .map(SubtransactionRow::stagedMetadataLocation)
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

  private SubtransactionRow validateSubtxnToken(long subtxnId, String subtxnToken)
      throws SQLException {
    SubtransactionRow subtxn =
        persistence
            .findSubtransaction(subtxnId)
            .orElseThrow(() -> new NotFoundException("Subtransaction not found: " + subtxnId));
    if (!subtxn.subtxnToken().equals(subtxnToken)) {
      throw new ForbiddenException("Invalid subtxn_token for subtransaction " + subtxnId);
    }
    return subtxn;
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
