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
package org.apache.polaris.service.catalog.transaction;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Date;
import java.util.Locale;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.util.LocationUtil;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.OperationRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.TransactionRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.TxnSnapshotRow;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.api.PolarisCatalogTransactionApiService;
import org.apache.polaris.service.catalog.common.CatalogAdapter;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogHandler;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogHandlerFactory;
import org.apache.polaris.service.transaction.LockManager.LockConflictException;
import org.apache.polaris.service.transaction.TransactionService;
import org.apache.polaris.service.types.BeginTransactionRequest;
import org.apache.polaris.service.types.GetSubtransactionResponse;
import org.apache.polaris.service.types.CommitSubtransactionRequest;
import org.apache.polaris.service.types.CommitTransactionRequest;
import org.apache.polaris.service.types.CreateSubtransactionRequest;
import org.apache.polaris.service.types.StagedTableCommit;
import org.apache.polaris.service.types.CreateTransactionSnapshotRequest;
import org.apache.polaris.service.types.LockMode;
import org.apache.polaris.service.types.RenewLeaseRequest;
import org.apache.polaris.service.types.Subtransaction;
import org.apache.polaris.service.types.SubtransactionStatus;
import org.apache.polaris.service.types.Transaction;
import org.apache.polaris.service.types.TransactionIsolation;
import org.apache.polaris.service.types.TransactionSnapshot;
import org.apache.polaris.service.types.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST adapter implementing the Transaction API endpoints.
 *
 * <p>Builds a per-request {@link TransactionService} after authorizing catalog access via {@link
 * IcebergCatalogHandler}.
 */
@RequestScoped
public class TransactionCatalogAdapter
    implements PolarisCatalogTransactionApiService, CatalogAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionCatalogAdapter.class);

  private final CatalogPrefixParser prefixParser;
  private final IcebergCatalogHandlerFactory handlerFactory;

  @Inject
  public TransactionCatalogAdapter(
      CatalogPrefixParser prefixParser, IcebergCatalogHandlerFactory handlerFactory) {
    this.prefixParser = prefixParser;
    this.handlerFactory = handlerFactory;
  }

  private TransactionService openTxnService(SecurityContext securityContext, String prefix) {
    PolarisPrincipal principal = validatePrincipal(securityContext);
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    IcebergCatalogHandler handler = handlerFactory.createHandler(catalogName, principal);
    try {
      handler.listNamespaces(Namespace.of());
      return handler.newTransactionServiceForCatalog();
    } finally {
      try {
        handler.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Resolves the canonical metadata directory for the given table by loading it through a fresh
   * handler instance. Returns e.g. {@code s3://warehouse/ns/table/metadata}.
   *
   * <p>A fresh handler is required because each handler's resolution manifest is scoped to the
   * specific operation that initialized it; reusing the existing {@code listNamespaces} handler
   * would fail to resolve the table path.
   */
  private String resolveTableMetadataDir(
      IcebergCatalogHandlerFactory factory,
      String catalogName,
      PolarisPrincipal principal,
      String tableIdentifierStr) {
    String[] parts = tableIdentifierStr.split("\\.");
    if (parts.length < 2) {
      throw new jakarta.ws.rs.BadRequestException(
          "Invalid table_identifier '" + tableIdentifierStr + "': expected 'namespace.table'");
    }
    TableIdentifier tableId =
        TableIdentifier.of(
            Namespace.of(Arrays.copyOf(parts, parts.length - 1)), parts[parts.length - 1]);
    IcebergCatalogHandler tableHandler = factory.createHandler(catalogName, principal);
    try {
      String metadataLocation = tableHandler.loadTable(tableId, null).metadataLocation();
      int lastSlash = metadataLocation.lastIndexOf('/');
      return LocationUtil.stripTrailingSlash(metadataLocation.substring(0, lastSlash));
    } finally {
      try {
        tableHandler.close();
      } catch (Exception ignored) {
      }
    }
  }

  @Override
  public Response createTransactionSnapshot(
      String prefix,
      CreateTransactionSnapshotRequest createTransactionSnapshotRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    int lease = createTransactionSnapshotRequest.getLeaseSeconds();
    try {
      TransactionService txnService = openTxnService(securityContext, prefix);
      TxnSnapshotRow snap = txnService.createTransactionSnapshot(lease);
      return Response.ok(toSnapshot(snap)).build();
    } catch (SQLException e) {
      return serverError("createTransactionSnapshot failed", e);
    }
  }

  @Override
  public Response getTransactionSnapshot(
      String prefix,
      Long txnSnapshotId,
      RealmContext realmContext,
      SecurityContext securityContext) {
    try {
      TransactionService txnService = openTxnService(securityContext, prefix);
      TxnSnapshotRow snap = txnService.getTransactionSnapshot(txnSnapshotId);
      return Response.ok(toSnapshot(snap)).build();
    } catch (jakarta.ws.rs.NotFoundException e) {
      return notFound(e.getMessage());
    } catch (SQLException e) {
      return serverError("getTransactionSnapshot failed", e);
    }
  }

  @Override
  public Response releaseTransactionSnapshot(
      String prefix,
      Long txnSnapshotId,
      RealmContext realmContext,
      SecurityContext securityContext) {
    try {
      TransactionService txnService = openTxnService(securityContext, prefix);
      txnService.releaseTransactionSnapshot(txnSnapshotId);
      return Response.noContent().build();
    } catch (jakarta.ws.rs.NotFoundException e) {
      return notFound(e.getMessage());
    } catch (SQLException e) {
      return serverError("releaseTransactionSnapshot failed", e);
    }
  }

  @Override
  public Response renewTransactionSnapshot(
      String prefix,
      Long txnSnapshotId,
      RenewLeaseRequest renewLeaseRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    int leaseSeconds = renewLeaseRequest.getLeaseSeconds();
    try {
      TransactionService txnService = openTxnService(securityContext, prefix);
      TxnSnapshotRow snap = txnService.renewTransactionSnapshot(txnSnapshotId, leaseSeconds);
      return Response.ok(toSnapshot(snap)).build();
    } catch (jakarta.ws.rs.NotFoundException e) {
      return notFound(e.getMessage());
    } catch (SQLException e) {
      return serverError("renewTransactionSnapshot failed", e);
    }
  }

  @Override
  public Response beginTransaction(
      String prefix,
      BeginTransactionRequest beginTransactionRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    TransactionIsolation isolation = beginTransactionRequest.getIsolation();
    int leaseSeconds = beginTransactionRequest.getLeaseSeconds();
    String principalName =
        securityContext.getUserPrincipal() != null
            ? securityContext.getUserPrincipal().getName()
            : "unknown";
    try {
      TransactionService txnService = openTxnService(securityContext, prefix);
      TransactionRow txn =
          txnService.beginTransaction(isolation.name(), leaseSeconds, principalName);
      return Response.ok(toTransaction(txn, true)).build();
    } catch (SQLException e) {
      return serverError("beginTransaction failed", e);
    }
  }

  @Override
  public Response getTransaction(
      String prefix, Long txnId, RealmContext realmContext, SecurityContext securityContext) {
    try {
      TransactionService txnService = openTxnService(securityContext, prefix);
      TransactionRow txn = txnService.getTransaction(txnId);
      return Response.ok(toTransaction(txn, false)).build();
    } catch (jakarta.ws.rs.NotFoundException e) {
      return notFound(e.getMessage());
    } catch (SQLException e) {
      return serverError("getTransaction failed", e);
    }
  }

  @Override
  public Response commitTransaction(
      String prefix,
      Long txnId,
      String xTxnToken,
      CommitTransactionRequest commitTransactionRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    if (xTxnToken == null || xTxnToken.isBlank()) {
      return Response.status(401)
          .entity(
              ErrorResponse.builder()
                  .responseCode(401)
                  .withType("Unauthorized")
                  .withMessage("X-Txn-Token header is required")
                  .build())
          .build();
    }
    PolarisPrincipal principal = validatePrincipal(securityContext);
    String catalogName = prefixParser.prefixToCatalogName(prefix);

    boolean inlinePath =
        commitTransactionRequest != null
            && commitTransactionRequest.getTables() != null
            && !commitTransactionRequest.getTables().isEmpty();

    try {
      TransactionService txnService = openTxnService(securityContext, prefix);
      IcebergCatalogHandler handler = handlerFactory.createHandler(catalogName, principal);
      try {
        handler.listNamespaces(org.apache.iceberg.catalog.Namespace.of());
        if (inlinePath) {
          // Use Case 1: single-session inline staged writes
          List<TransactionService.StagedTableInfo> inputTables =
              commitTransactionRequest.getTables().stream()
                  .map(
                      t ->
                          new TransactionService.StagedTableInfo(
                              t.getTableIdentifier(),
                              t.getStagedMetadataLocation(),
                              null,
                              t.getOperation() != null ? t.getOperation().name() : "ROW_EXCLUSIVE"))
                  .toList();
          TransactionService.PrepareInlineResult result =
              txnService.prepareInlineCommit(txnId, xTxnToken, inputTables);
          LOGGER.info(
              "Committing txn={} (inline): {} direct, {} merge-and-retry",
              txnId, result.direct().size(), result.mergeRetry().size());
          handler.finalizeInlineTables(
              result.direct(), result.mergeRetry(), handlerFactory, catalogName, principal);
        } else {
          // Use Case 2: multi-app subtransaction path
          TransactionService.PrepareCommitResult result =
              txnService.prepareCommit(txnId, xTxnToken);
          LOGGER.info(
              "Committing txn={} (subtxn): {} direct, {} merge-and-retry",
              txnId, result.direct().size(), result.mergeRetry().size());
          handler.finalizeExtensionTransaction(
              result.direct(), result.mergeRetry(), handlerFactory, catalogName, principal);
        }
      } finally {
        closeQuietly(handler);
      }

      txnService.finalizeCommit(txnId);
      return Response.noContent().build();
    } catch (TransactionService.SerializationFailureException e) {
      LOGGER.info("Serialization failure for txn={}: {}", txnId, e.getMessage());
      return Response.status(409)
          .entity(
              ErrorResponse.builder()
                  .responseCode(409)
                  .withType("SerializationFailure")
                  .withMessage(e.getMessage())
                  .build())
          .build();
    } catch (jakarta.ws.rs.BadRequestException e) {
      return Response.status(400).entity(simpleError(e.getMessage())).build();
    } catch (jakarta.ws.rs.ForbiddenException e) {
      return Response.status(403).entity(simpleError(e.getMessage())).build();
    } catch (jakarta.ws.rs.NotFoundException e) {
      return notFound(e.getMessage());
    } catch (SQLException e) {
      return serverError("commitTransaction failed", e);
    }
  }

  private static void closeQuietly(IcebergCatalogHandler handler) {
    try {
      handler.close();
    } catch (Exception ignored) {
    }
  }

  @Override
  public Response getSubtransaction(
      String prefix,
      Long subtxnId,
      String xSubtxnToken,
      RealmContext realmContext,
      SecurityContext securityContext) {
    if (xSubtxnToken == null || xSubtxnToken.isBlank()) {
      return Response.status(401).entity(simpleError("X-Subtxn-Token header is required")).build();
    }
    try {
      TransactionService txnService = openTxnService(securityContext, prefix);
      TransactionService.OperationInfo info =
          txnService.getOperation(subtxnId, xSubtxnToken);
      return Response.ok(
              new GetSubtransactionResponse(
                  info.txnId(), info.txnSnapshotId(),
                  info.assignedStagingPath(), info.tableUuid()))
          .build();
    } catch (jakarta.ws.rs.ForbiddenException e) {
      return Response.status(403).entity(simpleError(e.getMessage())).build();
    } catch (jakarta.ws.rs.NotFoundException e) {
      return notFound(e.getMessage());
    } catch (java.sql.SQLException e) {
      return serverError("getOperation failed", e);
    }
  }

  @Override
  public Response abortTransaction(
      String prefix,
      Long txnId,
      String xTxnToken,
      RealmContext realmContext,
      SecurityContext securityContext) {
    if (xTxnToken == null || xTxnToken.isBlank()) {
      return Response.status(401).entity(simpleError("X-Txn-Token header is required")).build();
    }
    try {
      TransactionService txnService = openTxnService(securityContext, prefix);
      txnService.abortTransaction(txnId, xTxnToken);
      return Response.noContent().build();
    } catch (jakarta.ws.rs.ForbiddenException e) {
      return Response.status(403).entity(simpleError(e.getMessage())).build();
    } catch (jakarta.ws.rs.NotFoundException e) {
      return notFound(e.getMessage());
    } catch (SQLException e) {
      return serverError("abortTransaction failed", e);
    }
  }

  @Override
  public Response renewTransaction(
      String prefix,
      Long txnId,
      String xTxnToken,
      RenewLeaseRequest renewLeaseRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    int leaseSeconds = renewLeaseRequest.getLeaseSeconds();
    try {
      TransactionService txnService = openTxnService(securityContext, prefix);
      TransactionRow txn = txnService.renewTransaction(txnId, xTxnToken, leaseSeconds);
      return Response.ok(toTransaction(txn, false)).build();
    } catch (jakarta.ws.rs.ForbiddenException e) {
      return Response.status(403).entity(simpleError(e.getMessage())).build();
    } catch (jakarta.ws.rs.NotFoundException e) {
      return notFound(e.getMessage());
    } catch (SQLException e) {
      return serverError("renewTransaction failed", e);
    }
  }

  @Override
  public Response createSubtransaction(
      String prefix,
      Long txnId,
      String xTxnToken,
      CreateSubtransactionRequest createSubtransactionRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    if (xTxnToken == null || xTxnToken.isBlank()) {
      return Response.status(401).entity(simpleError("X-Txn-Token header is required")).build();
    }
    PolarisPrincipal principal = validatePrincipal(securityContext);
    String catalogName = prefixParser.prefixToCatalogName(prefix);
    IcebergCatalogHandler handler = handlerFactory.createHandler(catalogName, principal);
    try {
      handler.listNamespaces(Namespace.of());
      TransactionService txnService = handler.newTransactionServiceForCatalog();
      String tableMetadataDir = resolveTableMetadataDir(
          handlerFactory, catalogName, principal,
          createSubtransactionRequest.getTableIdentifier());
      LockMode lockModeEnum = createSubtransactionRequest.getOperation();
      String lockMode = (lockModeEnum != null ? lockModeEnum : LockMode.ROW_EXCLUSIVE).name();
      OperationRow op =
          txnService.createOperation(
              txnId,
              xTxnToken,
              createSubtransactionRequest.getTableIdentifier(),
              lockMode,
              null,
              createSubtransactionRequest.getRetryOfSubtxnId(),
              createSubtransactionRequest.getLeaseSeconds(),
              tableMetadataDir);
      return Response.ok(toSubtransaction(op, true)).build();
    } catch (jakarta.ws.rs.BadRequestException | jakarta.ws.rs.ForbiddenException e) {
      return Response.status(e instanceof jakarta.ws.rs.ForbiddenException ? 403 : 400)
          .entity(simpleError(e.getMessage()))
          .build();
    } catch (LockConflictException e) {
      long retryAfterMs = e.retryAfterMs();
      if (retryAfterMs > 0) {
        // EXCLUSIVE/ACCESS_EXCLUSIVE conflict: tell the client to retry with backoff.
        return Response.status(409)
            .entity(
                ErrorResponse.builder()
                    .responseCode(409)
                    .withType("LockConflictError")
                    .withMessage(String.valueOf(retryAfterMs))
                    .build())
            .build();
      }
      // ROW_EXCLUSIVE blocked by held EXCLUSIVE/ACCESS_EXCLUSIVE: fail immediately.
      return Response.status(409).entity(simpleError(e.getMessage())).build();
    } catch (SQLException e) {
      return serverError("createSubtransaction failed", e);
    } finally {
      try {
        handler.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Response commitSubtransaction(
      String prefix,
      Long txnId,
      Long subtxnId,
      String xSubtxnToken,
      CommitSubtransactionRequest commitSubtransactionRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    if (xSubtxnToken == null || xSubtxnToken.isBlank()) {
      return Response.status(401).entity(simpleError("X-Subtxn-Token header is required")).build();
    }
    String staged = commitSubtransactionRequest.getStagedMetadataLocation();
    if (staged == null || staged.isBlank()) {
      return Response.status(400)
          .entity(simpleError("staged_metadata_location is required"))
          .build();
    }
    try {
      TransactionService txnService = openTxnService(securityContext, prefix);
      txnService.commitOperation(txnId, subtxnId, xSubtxnToken, staged);
      return Response.noContent().build();
    } catch (jakarta.ws.rs.BadRequestException e) {
      return Response.status(400).entity(simpleError(e.getMessage())).build();
    } catch (jakarta.ws.rs.ForbiddenException e) {
      return Response.status(403).entity(simpleError(e.getMessage())).build();
    } catch (jakarta.ws.rs.NotFoundException e) {
      return notFound(e.getMessage());
    } catch (SQLException e) {
      return serverError("commitSubtransaction failed", e);
    }
  }

  @Override
  public Response abortSubtransaction(
      String prefix,
      Long txnId,
      Long subtxnId,
      String xSubtxnToken,
      String xTxnToken,
      RealmContext realmContext,
      SecurityContext securityContext) {
    try {
      TransactionService txnService = openTxnService(securityContext, prefix);
      txnService.abortOperation(txnId, subtxnId, xSubtxnToken, xTxnToken);
      return Response.noContent().build();
    } catch (jakarta.ws.rs.BadRequestException e) {
      return Response.status(400).entity(simpleError(e.getMessage())).build();
    } catch (jakarta.ws.rs.ForbiddenException e) {
      return Response.status(403).entity(simpleError(e.getMessage())).build();
    } catch (jakarta.ws.rs.NotFoundException e) {
      return notFound(e.getMessage());
    } catch (SQLException e) {
      return serverError("abortSubtransaction failed", e);
    }
  }

  @Override
  public Response renewSubtransaction(
      String prefix,
      Long txnId,
      Long subtxnId,
      String xSubtxnToken,
      RenewLeaseRequest renewLeaseRequest,
      RealmContext realmContext,
      SecurityContext securityContext) {
    try {
      TransactionService txnService = openTxnService(securityContext, prefix);
      OperationRow row =
          txnService.renewOperation(
              txnId, subtxnId, xSubtxnToken, renewLeaseRequest.getLeaseSeconds());
      return Response.ok(toSubtransaction(row, false)).build();
    } catch (jakarta.ws.rs.ForbiddenException e) {
      return Response.status(403).entity(simpleError(e.getMessage())).build();
    } catch (jakarta.ws.rs.NotFoundException e) {
      return notFound(e.getMessage());
    } catch (SQLException e) {
      return serverError("renewSubtransaction failed", e);
    }
  }

  private static TransactionSnapshot toSnapshot(TxnSnapshotRow snap) {
    return new TransactionSnapshot(
        snap.txnSnapshotId(),
        snap.xmin(),
        snap.xmax(),
        JdbcTransactionPersistence.parseXipList(snap.xipList()),
        Date.from(snap.expiresAt()),
        snap.releasedAt() != null ? Date.from(snap.releasedAt()) : null);
  }

  private static Transaction toTransaction(TransactionRow txn, boolean includeToken) {
    return new Transaction(
        txn.txnId(),
        includeToken ? txn.txnToken() : null,
        toTransactionStatus(txn.status()),
        parseIsolation(txn.isolation()),
        txn.txnSnapshotId(),
        Date.from(txn.expiresAt()),
        Date.from(txn.startedAt()),
        txn.endedAt() != null ? Date.from(txn.endedAt()) : null);
  }

  private static TransactionIsolation parseIsolation(String raw) {
    if (raw == null || raw.isBlank()) {
      return TransactionIsolation.REPEATABLE_READ;
    }
    try {
      return TransactionIsolation.valueOf(raw.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      return TransactionIsolation.REPEATABLE_READ;
    }
  }

  private static Subtransaction toSubtransaction(OperationRow op, boolean includeToken) {
    return new Subtransaction(
        op.opId(),
        includeToken ? op.opToken() : null,
        op.txnId(),
        op.tableUuid(),
        LockMode.valueOf(op.lockMode()),
        op.baseSnapshotId(),
        op.assignedStagingPath(),
        op.stagedMetadataLocation(),
        toSubtransactionStatus(op.status()),
        Date.from(op.expiresAt()));
  }

  private static TransactionStatus toTransactionStatus(int status) {
    return switch (status) {
      case JdbcTransactionPersistence.TXN_STATUS_COMMITTED -> TransactionStatus.COMMITTED;
      case JdbcTransactionPersistence.TXN_STATUS_ABORTED -> TransactionStatus.ABORTED;
      default -> TransactionStatus.IN_PROGRESS;
    };
  }

  private static SubtransactionStatus toSubtransactionStatus(int status) {
    return switch (status) {
      case JdbcTransactionPersistence.OP_STATUS_COMMITTED -> SubtransactionStatus.COMMITTED;
      case JdbcTransactionPersistence.OP_STATUS_ABORTED -> SubtransactionStatus.ABORTED;
      default -> SubtransactionStatus.IN_PROGRESS;
    };
  }

  private static ErrorResponse simpleError(String message) {
    return ErrorResponse.builder()
        .responseCode(400)
        .withType("BadRequest")
        .withMessage(message)
        .build();
  }

  private static Response notFound(String message) {
    return Response.status(404)
        .entity(
            ErrorResponse.builder()
                .responseCode(404)
                .withType("NotFound")
                .withMessage(message)
                .build())
        .build();
  }

  private static Response serverError(String message, SQLException e) {
    LOGGER.error(message, e);
    return Response.serverError()
        .entity(
            ErrorResponse.builder()
                .responseCode(500)
                .withType("InternalServerError")
                .withMessage(message + ": " + e.getMessage())
                .build())
        .build();
  }
}
