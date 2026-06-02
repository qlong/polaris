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
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
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
   * @param tableUuid stable UUID of the target Iceberg table entity
   * @param stagingBase base S3 path for the catalog's staging area
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
      String stagingBase)
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
    String stagingPath = stagingBase + "/" + txnId + "/" + subtxnId + "/";

    SubtransactionRow row =
        new SubtransactionRow(
            subtxnId,
            txnId,
            persistence.getRealmId(),
            persistence.getCatalogId(),
            tableUuid,
            operation.toUpperCase(Locale.ROOT),
            baseSnapshotId,
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
    if (!stagedMetadataLocation.startsWith(subtxn.assignedStagingPath())) {
      throw new BadRequestException(
          "staged_metadata_location '"
              + stagedMetadataLocation
              + "' is not under the assigned staging path '"
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

  public SubtransactionRow renewSubtransaction(
      long txnId, long subtxnId, String subtxnToken, int leaseSeconds) throws SQLException {
    validateSubtxnToken(subtxnId, subtxnToken);
    int effective = leaseSeconds > 0 ? leaseSeconds : DEFAULT_TXN_LEASE_SECONDS;
    // Reuse transaction renew since subtransactions share the same lease semantics
    persistence.renewTransaction(txnId, Instant.now().plus(effective, ChronoUnit.SECONDS));
    return persistence.findSubtransaction(subtxnId).orElseThrow();
  }

  /**
   * Commits a transaction. All SUB_COMMITTED subtransactions are finalized atomically — the
   * metadata pointer for each table is swapped and a {@code committed_table_snapshots} record is
   * inserted in the same DB transaction. Locks are released on success.
   *
   * <p>The actual entity persistence (swapping {@code metadata-location} in the {@code entities}
   * table) is delegated to the caller (the REST adapter / {@code IcebergCatalog}) which already
   * holds the correct entity handles. This method validates state and records finalization in the
   * transaction tables; the adapter drives the entity update loop.
   *
   * @return list of SUB_COMMITTED subtransactions to finalize
   */
  public List<SubtransactionRow> prepareCommit(long txnId, String txnToken) throws SQLException {
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
    return subtxns.stream()
        .filter(s -> s.status() == JdbcTransactionPersistence.SUBTXN_STATUS_SUB_COMMITTED)
        .toList();
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
