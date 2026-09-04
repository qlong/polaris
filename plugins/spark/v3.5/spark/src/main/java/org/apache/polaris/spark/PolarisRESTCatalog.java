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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.ErrorHandler;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.util.EnvironmentUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.polaris.core.rest.PolarisEndpoints;
import org.apache.polaris.core.rest.PolarisResourcePaths;
import org.apache.polaris.spark.rest.BeginTransactionRESTRequest;
import org.apache.polaris.spark.rest.CommitTransactionRESTRequest;
import org.apache.polaris.spark.rest.CreateGenericTableRESTRequest;
import org.apache.polaris.spark.rest.CreateGenericTableRequest;
import org.apache.polaris.spark.rest.CreateSubtransactionRESTRequest;
import org.apache.polaris.spark.rest.GenericTable;
import org.apache.polaris.spark.rest.GetSubtransactionRESTResponse;
import org.apache.polaris.spark.rest.ListGenericTablesRESTResponse;
import org.apache.polaris.spark.rest.LoadGenericTableRESTResponse;
import org.apache.polaris.spark.rest.RollbackSubtransactionRESTRequest;
import org.apache.polaris.spark.rest.StagedTableCommitRESTRequest;
import org.apache.polaris.spark.rest.SubtransactionRESTResponse;
import org.apache.polaris.spark.rest.SubtransactionTableRESTRequest;
import org.apache.polaris.spark.rest.TransactionRESTResponse;
import org.apache.polaris.spark.rest.TransactionSnapshotRESTResponse;

/**
 * [[PolarisRESTCatalog]] talks to Polaris REST APIs, and implements the PolarisCatalog interfaces,
 * which are generic table related APIs at this moment. This class doesn't interact with any Spark
 * objects.
 */
public class PolarisRESTCatalog implements PolarisCatalog, Closeable {
  public static final String REST_PAGE_SIZE = "rest-page-size";

  private final Function<Map<String, String>, RESTClient> clientBuilder;

  private RESTClient restClient = null;
  private CloseableGroup closeables = null;
  private Set<Endpoint> endpoints;
  private OAuth2Util.AuthSession catalogAuth = null;
  private PolarisResourcePaths pathGenerator = null;
  private Integer pageSize = null;

  // the default endpoints to config if server doesn't specify the 'endpoints' configuration.
  private static final Set<Endpoint> DEFAULT_ENDPOINTS = PolarisEndpoints.GENERIC_TABLE_ENDPOINTS;

  /**
   * Thrown when the server signals a retryable lock conflict (HTTP 409, type=LockConflictError).
   */
  static class LockConflictRetryException extends RuntimeException {
    final long retryAfterMs;

    LockConflictRetryException(long retryAfterMs) {
      super("Lock conflict; retry after " + retryAfterMs + " ms");
      this.retryAfterMs = retryAfterMs;
    }
  }

  /** Thrown when lock wait retries are exhausted. */
  public static class LockWaitTimeoutException extends RuntimeException {
    public LockWaitTimeoutException(String message) {
      super(message);
    }
  }

  /**
   * Error handler that intercepts 409 LockConflictError responses and converts them to {@link
   * LockConflictRetryException} so callers can implement retry-with-backoff. All other errors fall
   * through to the default handler.
   */
  private static final ErrorHandler LOCK_AWARE_ERROR_HANDLER =
      new ErrorHandler() {
        private final ErrorHandler delegate = (ErrorHandler) ErrorHandlers.defaultErrorHandler();

        @Override
        public org.apache.iceberg.rest.responses.ErrorResponse parseResponse(
            int code, String json) {
          return delegate.parseResponse(code, json);
        }

        @Override
        public void accept(org.apache.iceberg.rest.responses.ErrorResponse error) {
          if (error.code() == 409 && "LockConflictError".equals(error.type())) {
            long ms = 1_000L;
            try {
              ms = Long.parseLong(error.message().trim());
            } catch (NumberFormatException ignored) {
            }
            throw new LockConflictRetryException(ms);
          }
          delegate.accept(error);
        }
      };

  public PolarisRESTCatalog() {
    this(config -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build());
  }

  public PolarisRESTCatalog(Function<Map<String, String>, RESTClient> clientBuilder) {
    this.clientBuilder = clientBuilder;
  }

  public void initialize(Map<String, String> unresolved, OAuth2Util.AuthSession catalogAuth) {
    Preconditions.checkArgument(unresolved != null, "Invalid configuration: null");

    // Resolve any configuration that is supplied by environment variables.
    // For example: if we have an entity ("key", "env:envVar") in the unresolved,
    // and envVar is configured to envValue in system env. After resolve, we got
    // entity ("key", "envValue").
    Map<String, String> props = EnvironmentUtil.resolveAll(unresolved);

    // TODO: switch to use authManager once iceberg dependency is updated to 1.9.0
    this.catalogAuth = catalogAuth;

    ConfigResponse config;
    try (RESTClient initClient = clientBuilder.apply(props).withAuthSession(catalogAuth)) {
      config = fetchConfig(initClient, catalogAuth.headers(), props);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close HTTP client", e);
    }

    // call getConfig to get the server configurations
    Map<String, String> mergedProps = config.merge(props);
    if (config.endpoints().isEmpty()) {
      this.endpoints = DEFAULT_ENDPOINTS;
    } else {
      this.endpoints = ImmutableSet.copyOf(config.endpoints());
    }

    this.pathGenerator = PolarisResourcePaths.forCatalogProperties(mergedProps);
    this.restClient = clientBuilder.apply(mergedProps).withAuthSession(catalogAuth);

    this.pageSize = PropertyUtil.propertyAsNullableInt(mergedProps, REST_PAGE_SIZE);
    if (pageSize != null) {
      Preconditions.checkArgument(
          pageSize > 0, "Invalid value for %s, must be a positive integer", REST_PAGE_SIZE);
    }

    this.closeables = new CloseableGroup();
    this.closeables.addCloseable(this.restClient);
    this.closeables.setSuppressCloseFailure(true);
  }

  protected static ConfigResponse fetchConfig(
      RESTClient client, Map<String, String> headers, Map<String, String> properties) {
    // send the client's warehouse location to the service to keep in sync
    // this is needed for cases where the warehouse is configured at client side,
    // and used by Polaris server as catalog name.
    ImmutableMap.Builder<String, String> queryParams = ImmutableMap.builder();
    if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
      queryParams.put(
          CatalogProperties.WAREHOUSE_LOCATION,
          properties.get(CatalogProperties.WAREHOUSE_LOCATION));
    }

    ConfigResponse configResponse =
        client.get(
            ResourcePaths.config(),
            queryParams.build(),
            ConfigResponse.class,
            headers,
            ErrorHandlers.defaultErrorHandler());
    configResponse.validate();
    return configResponse;
  }

  @Override
  public void close() throws IOException {
    if (closeables != null) {
      closeables.close();
    }
  }

  @Override
  public List<TableIdentifier> listGenericTables(Namespace ns) {
    Endpoint.check(endpoints, PolarisEndpoints.V1_LIST_GENERIC_TABLES);

    Map<String, String> queryParams = Maps.newHashMap();
    ImmutableList.Builder<TableIdentifier> tables = ImmutableList.builder();
    String pageToken = "";
    if (pageSize != null) {
      queryParams.put("pageSize", String.valueOf(pageSize));
    }

    do {
      queryParams.put("pageToken", pageToken);
      ListGenericTablesRESTResponse response =
          restClient
              .withAuthSession(this.catalogAuth)
              .get(
                  pathGenerator.genericTables(ns),
                  queryParams,
                  ListGenericTablesRESTResponse.class,
                  Map.of(),
                  ErrorHandlers.namespaceErrorHandler());
      pageToken = response.getNextPageToken();
      tables.addAll(response.getIdentifiers());
    } while (pageToken != null);

    return tables.build();
  }

  @Override
  public boolean dropGenericTable(TableIdentifier identifier) {
    Endpoint.check(endpoints, PolarisEndpoints.V1_DELETE_GENERIC_TABLE);

    try {
      restClient
          .withAuthSession(this.catalogAuth)
          .delete(
              pathGenerator.genericTable(identifier),
              null,
              Map.of(),
              ErrorHandlers.tableErrorHandler());
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public GenericTable createGenericTable(
      TableIdentifier identifier,
      String format,
      String baseLocation,
      String doc,
      Map<String, String> props) {
    Endpoint.check(endpoints, PolarisEndpoints.V1_CREATE_GENERIC_TABLE);
    CreateGenericTableRESTRequest request =
        new CreateGenericTableRESTRequest(
            CreateGenericTableRequest.builder()
                .setName(identifier.name())
                .setFormat(format)
                .setBaseLocation(baseLocation)
                .setDoc(doc)
                .setProperties(props)
                .build());

    LoadGenericTableRESTResponse response =
        restClient
            .withAuthSession(this.catalogAuth)
            .post(
                pathGenerator.genericTables(identifier.namespace()),
                request,
                LoadGenericTableRESTResponse.class,
                Map.of(),
                ErrorHandlers.tableErrorHandler());

    return response.getTable();
  }

  @Override
  public GenericTable loadGenericTable(TableIdentifier identifier) {
    Endpoint.check(endpoints, PolarisEndpoints.V1_LOAD_GENERIC_TABLE);
    LoadGenericTableRESTResponse response =
        restClient
            .withAuthSession(this.catalogAuth)
            .get(
                pathGenerator.genericTable(identifier),
                null,
                LoadGenericTableRESTResponse.class,
                Map.of(),
                ErrorHandlers.tableErrorHandler());

    return response.getTable();
  }

  /**
   * POST /polaris/v1/{prefix}/transaction-snapshots — mints a stateless read-only snapshot handle.
   *
   * <p>No row is inserted; the returned opaque handle encodes the current read point and is
   * reclaimed by the reaper after the TTL. Used by READ ONLY REPEATABLE READ transactions.
   *
   * @param leaseSeconds desired TTL for the handle
   * @return opaque {@code txn_snapshot} handle (base64url-encoded read point)
   */
  public String createTransactionSnapshot(int leaseSeconds) {
    TransactionSnapshotRESTResponse resp =
        restClient
            .withAuthSession(this.catalogAuth)
            .post(
                pathGenerator.transactionSnapshots(),
                new BeginTransactionRESTRequest(null, leaseSeconds),
                TransactionSnapshotRESTResponse.class,
                Map.of(),
                ErrorHandlers.defaultErrorHandler());
    return resp.txnSnapshot();
  }

  /**
   * POST /polaris/v1/{prefix}/transactions — begins a new transaction.
   *
   * @param isolation "REPEATABLE_READ" or "READ_COMMITTED"
   * @param leaseSeconds transaction lease duration
   * @return parsed Transaction response containing txn_id, txn_token, txn_snapshot_id
   */
  public TransactionRESTResponse beginTransaction(String isolation, int leaseSeconds) {
    return restClient
        .withAuthSession(this.catalogAuth)
        .post(
            pathGenerator.transactions(),
            new BeginTransactionRESTRequest(isolation, leaseSeconds),
            TransactionRESTResponse.class,
            Map.of(),
            ErrorHandlers.defaultErrorHandler());
  }

  /**
   * POST /polaris/v1/{prefix}/transactions/{txnId}/commit — commits the transaction.
   *
   * <p>When {@code tables} is non-empty the server uses the inline single-session path; when empty
   * the server reads pre-registered subtransactions (multi-app path).
   */
  public void commitTransaction(
      long txnId, String txnToken, List<StagedTableCommitRESTRequest> tables) {
    CommitTransactionRESTRequest body =
        tables.isEmpty() ? null : new CommitTransactionRESTRequest(tables);
    restClient
        .withAuthSession(this.catalogAuth)
        .post(
            pathGenerator.transactionCommit(txnId),
            body,
            TransactionRESTResponse.class,
            Map.of("X-Txn-Token", txnToken),
            ErrorHandlers.defaultErrorHandler());
  }

  /**
   * POST /polaris/v1/{prefix}/transactions/{txnId}/subtransactions — allocates a staging path and
   * lock for one table. Returns subtxn_id, subtxn_token, and assigned_staging_path.
   */
  public SubtransactionRESTResponse createSubtransaction(
      long txnId, String txnToken, String tableIdentifier, String operation, int leaseSeconds) {
    return postCreateSubtransaction(
        txnId,
        txnToken,
        new CreateSubtransactionRESTRequest(tableIdentifier, operation, leaseSeconds));
  }

  /**
   * Multi-table form of {@link #createSubtransaction}: allocates one subtransaction spanning every
   * table in {@code tables}, each with its own assigned staging path and lock, as one atomic unit.
   * The response's {@code operations[]} carries the per-table staging paths.
   */
  public SubtransactionRESTResponse createSubtransaction(
      long txnId, String txnToken, List<SubtransactionTableRESTRequest> tables, int leaseSeconds) {
    return postCreateSubtransaction(
        txnId, txnToken, new CreateSubtransactionRESTRequest(tables, leaseSeconds));
  }

  private SubtransactionRESTResponse postCreateSubtransaction(
      long txnId, String txnToken, CreateSubtransactionRESTRequest body) {
    return restClient
        .withAuthSession(this.catalogAuth)
        .post(
            pathGenerator.subtransactions(txnId),
            body,
            SubtransactionRESTResponse.class,
            Map.of("X-Txn-Token", txnToken),
            LOCK_AWARE_ERROR_HANDLER);
  }

  /**
   * POST /polaris/v1/{prefix}/transactions/{txnId}/subtransactions/{subtxnId}/commit — atomically
   * stages the whole subtransaction for commit. Body-less: the server assigned each operation's
   * staging path at create time, so the worker only proves ownership via the X-Subtxn-Token header.
   * Marks every operation COMMITTED at its assigned path and the subtransaction STAGED.
   */
  public void commitSubtransaction(long txnId, long subtxnId, String subtxnToken) {
    restClient
        .withAuthSession(this.catalogAuth)
        .post(
            pathGenerator.subtransactionCommit(txnId, subtxnId),
            null,
            TransactionRESTResponse.class,
            Map.of("X-Subtxn-Token", subtxnToken),
            ErrorHandlers.defaultErrorHandler());
  }

  /**
   * POST /polaris/v1/{prefix}/transactions/{txnId}/subtransactions/{subtxnId}/abort — rolls the
   * subtransaction back permanently (not reopened): its operations are aborted and the parent
   * transaction skips it at commit. Requires X-Subtxn-Token.
   */
  public void abortSubtransaction(long txnId, long subtxnId, String subtxnToken) {
    restClient
        .withAuthSession(this.catalogAuth)
        .post(
            pathGenerator.subtransactionAbort(txnId, subtxnId),
            null,
            TransactionRESTResponse.class,
            Map.of("X-Subtxn-Token", subtxnToken),
            ErrorHandlers.defaultErrorHandler());
  }

  /**
   * POST /polaris/v1/{prefix}/transactions/{txnId}/subtransactions/{subtxnId}/rollback — rolls the
   * subtransaction back and, when {@code reopenForRetry} is true (default), reopens it (attempt++)
   * so the same subtransaction can be re-staged, possibly by a different worker. Accepts the
   * X-Subtxn-Token (worker) or X-Txn-Token (orchestrator) — pass whichever is non-null.
   */
  public void rollbackSubtransaction(
      long txnId, long subtxnId, String subtxnToken, String txnToken, boolean reopenForRetry) {
    Map<String, String> headers = new java.util.HashMap<>();
    if (subtxnToken != null && !subtxnToken.isBlank()) {
      headers.put("X-Subtxn-Token", subtxnToken);
    }
    if (txnToken != null && !txnToken.isBlank()) {
      headers.put("X-Txn-Token", txnToken);
    }
    restClient
        .withAuthSession(this.catalogAuth)
        .post(
            pathGenerator.subtransactionRollback(txnId, subtxnId),
            new RollbackSubtransactionRESTRequest(reopenForRetry),
            TransactionRESTResponse.class,
            headers,
            ErrorHandlers.defaultErrorHandler());
  }

  /**
   * GET /polaris/v1/{prefix}/transactions/subtransactions/{subtxnId} — looks up a subtransaction by
   * ID and validates ownership via the subtxn_token. Returns the parent txn_id, txn_snapshot_id,
   * assigned_staging_path, and table_uuid needed by the worker to enter worker mode.
   */
  public GetSubtransactionRESTResponse getSubtransaction(long subtxnId, String subtxnToken) {
    return restClient
        .withAuthSession(this.catalogAuth)
        .get(
            pathGenerator.subtransactionGet(subtxnId),
            Map.of(),
            GetSubtransactionRESTResponse.class,
            Map.of("X-Subtxn-Token", subtxnToken),
            ErrorHandlers.defaultErrorHandler());
  }

  /**
   * POST /polaris/v1/{prefix}/transactions/{txnId}/abort — aborts the transaction. Requires
   * X-Txn-Token header to prove ownership.
   */
  public void abortTransaction(long txnId, String txnToken) {
    restClient
        .withAuthSession(this.catalogAuth)
        .post(
            pathGenerator.transactionAbort(txnId),
            null,
            TransactionRESTResponse.class,
            Map.of("X-Txn-Token", txnToken),
            ErrorHandlers.defaultErrorHandler());
  }

  /**
   * GET /v1/{prefix}/namespaces/{ns}/tables/{table}?txn_snapshot={handle} Calls the standard
   * Iceberg loadTable endpoint with a Polaris extension query param. Returns the metadata-location
   * pinned to the given transaction snapshot. {@code txnSnapshot} is an opaque, server-minted
   * handle; the client echoes it verbatim and never parses it.
   */
  public String resolveTableMetadataLocation(TableIdentifier identifier, String txnSnapshot) {
    LoadTableResponse response =
        restClient
            .withAuthSession(this.catalogAuth)
            .get(
                pathGenerator.table(identifier),
                Map.of("txn_snapshot", txnSnapshot),
                LoadTableResponse.class,
                Map.of(),
                ErrorHandlers.tableErrorHandler());
    return response.metadataLocation();
  }
}
