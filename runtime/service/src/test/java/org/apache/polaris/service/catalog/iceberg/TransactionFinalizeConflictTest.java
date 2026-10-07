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
package org.apache.polaris.service.catalog.iceberg;

import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.transaction.LockManager;
import org.apache.polaris.service.transaction.TransactionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Exercises the multi-table finalize paths ({@link IcebergCatalogHandler#finalizeInlineTables} and
 * {@link IcebergCatalogHandler#finalizeExtensionTransaction}) against real Iceberg metadata, for
 * the case where the table moves after the prepare-time conflict check ran.
 *
 * <p>Each test stages a transaction's change the way a client does — a full metadata file built on
 * the state it read, written without moving the table pointer — then optionally lets a concurrent
 * writer commit, then finalizes. A concurrent writer's committed work is never discarded: an
 * additive change is re-applied on top of it where the isolation level allows, and anything else
 * fails with a serialization failure that publishes nothing.
 */
public class TransactionFinalizeConflictTest {
  private static final String NAMESPACE = "ns";
  private static final String CATALOG = "test-catalog";
  private static final TableIdentifier ORDERS = TableIdentifier.of(NAMESPACE, "orders");
  private static final TableIdentifier CUSTOMERS = TableIdentifier.of(NAMESPACE, "customers");

  private String catalogLocation;
  private TestServices services;
  private IcebergCatalogHandlerFactory handlerFactory;

  @BeforeEach
  void setUp(@TempDir Path tempDir) {
    catalogLocation = tempDir.toAbsolutePath().toUri().toString();
    if (catalogLocation.endsWith("/")) {
      catalogLocation = catalogLocation.substring(0, catalogLocation.length() - 1);
    }
    services =
        TestServices.builder()
            .config(
                Map.of(
                    "ALLOW_INSECURE_STORAGE_TYPES",
                    "true",
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("FILE")))
            .build();
    handlerFactory =
        new IcebergCatalogHandlerFactory() {
          @Override
          public IcebergCatalogHandler createHandler(
              String catalogName, PolarisPrincipal principal) {
            return services.catalogAdapter().newHandler(services.securityContext(), catalogName);
          }
        };
    createCatalogAndNamespace();
    createTable(ORDERS);
    createTable(CUSTOMERS);
    plainCommit(ORDERS, t -> t.newAppend().appendFile(dataFile(t, "orders-0")).commit());
    plainCommit(CUSTOMERS, t -> t.newAppend().appendFile(dataFile(t, "customers-0")).commit());
  }

  // ---------------------------------------------------------------------------
  // Controls: no concurrent writer
  // ---------------------------------------------------------------------------

  @Test
  void inlineAppendWithoutConcurrentWriterCommits() {
    String staged = stage(ORDERS, t -> t.newAppend().appendFile(dataFile(t, "t1")).commit());

    assertThat(finalizeInline(false, inline(ORDERS, staged))).isTrue();

    assertThat(dataFileNames(ORDERS)).containsExactlyInAnyOrder("orders-0", "t1");
  }

  @Test
  void inlineSchemaChangeWithoutConcurrentWriterCommits() {
    String staged =
        stage(ORDERS, t -> t.updateSchema().addColumn("t1_col", Types.StringType.get()).commit());

    assertThat(finalizeInline(false, inline(ORDERS, staged))).isTrue();

    assertThat(columnNames(ORDERS)).contains("t1_col");
    assertThat(dataFileNames(ORDERS)).containsExactly("orders-0");
  }

  // ---------------------------------------------------------------------------
  // Inline path: the table moves between prepareInlineCommit and finalize
  // ---------------------------------------------------------------------------

  @ParameterizedTest(name = "mergeOnConflict={0}")
  @ValueSource(booleans = {true, false})
  void inlineConcurrentAppend(boolean mergeOnConflict) {
    String stagedOrders = stage(ORDERS, t -> t.newAppend().appendFile(dataFile(t, "t1")).commit());
    String stagedCustomers =
        stage(CUSTOMERS, t -> t.newAppend().appendFile(dataFile(t, "t1-c")).commit());
    plainCommit(ORDERS, t -> t.newAppend().appendFile(dataFile(t, "w2")).commit());

    boolean committed =
        finalizeInline(
            mergeOnConflict, inline(ORDERS, stagedOrders), inline(CUSTOMERS, stagedCustomers));

    // READ_COMMITTED re-applies the append onto the concurrent commit; REPEATABLE_READ fails the
    // whole transaction, so neither table changes.
    assertThat(committed).isEqualTo(mergeOnConflict);
    if (mergeOnConflict) {
      assertThat(dataFileNames(ORDERS)).containsExactlyInAnyOrder("orders-0", "w2", "t1");
      assertThat(dataFileNames(CUSTOMERS)).containsExactlyInAnyOrder("customers-0", "t1-c");
    } else {
      assertThat(dataFileNames(ORDERS)).containsExactlyInAnyOrder("orders-0", "w2");
      assertThat(dataFileNames(CUSTOMERS)).containsExactly("customers-0");
    }
  }

  @ParameterizedTest(name = "mergeOnConflict={0}")
  @ValueSource(booleans = {true, false})
  void inlineConcurrentSchemaChange(boolean mergeOnConflict) {
    String staged = stage(ORDERS, t -> t.newAppend().appendFile(dataFile(t, "t1")).commit());
    plainCommit(ORDERS, t -> t.updateSchema().addColumn("w2_col", Types.StringType.get()).commit());

    boolean committed = finalizeInline(mergeOnConflict, inline(ORDERS, staged));

    assertThat(committed).isEqualTo(mergeOnConflict);
    assertThat(columnNames(ORDERS)).contains("w2_col");
    if (mergeOnConflict) {
      assertThat(dataFileNames(ORDERS)).containsExactlyInAnyOrder("orders-0", "t1");
    } else {
      assertThat(dataFileNames(ORDERS)).containsExactly("orders-0");
    }
  }

  @Test
  void inlineReadCommittedReappliesEveryStagedAppend() {
    // Two writes to one table in one transaction: the second is staged on top of the first
    // (read-your-own-writes), so the staged metadata adds two snapshots.
    String staged =
        stage(
            ORDERS,
            t -> {
              t.newAppend().appendFile(dataFile(t, "t1")).commit();
              t.newAppend().appendFile(dataFile(t, "t2")).commit();
            });
    plainCommit(ORDERS, t -> t.newAppend().appendFile(dataFile(t, "w2")).commit());

    assertThat(finalizeInline(true, inline(ORDERS, staged))).isTrue();

    assertThat(dataFileNames(ORDERS)).containsExactlyInAnyOrder("orders-0", "w2", "t1", "t2");
  }

  @Test
  void inlineReadCommittedConcurrentAppendFailsDestructiveChange() {
    String staged =
        stage(ORDERS, t -> t.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit());
    plainCommit(ORDERS, t -> t.newAppend().appendFile(dataFile(t, "w2")).commit());

    assertThat(finalizeInline(true, inline(ORDERS, staged))).isFalse();

    assertThat(dataFileNames(ORDERS)).containsExactlyInAnyOrder("orders-0", "w2");
  }

  @Test
  void inlineReadCommittedConcurrentAppendFailsMetadataOnlyChange() {
    String staged =
        stage(ORDERS, t -> t.updateSchema().addColumn("t1_col", Types.StringType.get()).commit());
    plainCommit(ORDERS, t -> t.newAppend().appendFile(dataFile(t, "w2")).commit());

    assertThat(finalizeInline(true, inline(ORDERS, staged))).isFalse();

    assertThat(dataFileNames(ORDERS)).containsExactlyInAnyOrder("orders-0", "w2");
    assertThat(columnNames(ORDERS)).doesNotContain("t1_col");
  }

  // ---------------------------------------------------------------------------
  // Subtransaction path
  // ---------------------------------------------------------------------------

  @Test
  void subtxnSchemaChangeWithoutConcurrentWriterCommits() {
    String staged =
        stage(ORDERS, t -> t.updateSchema().addColumn("t1_col", Types.StringType.get()).commit());

    assertThat(finalizeSubtxn(operation(ORDERS, staged, LockManager.MODE_ROW_EXCLUSIVE))).isTrue();

    assertThat(dataFileNames(ORDERS))
        .as("a metadata-only change must not re-append existing data files")
        .containsExactly("orders-0");
    assertThat(columnNames(ORDERS)).contains("t1_col");
  }

  @Test
  void subtxnConcurrentAppendIsMerged() {
    String staged = stage(ORDERS, t -> t.newAppend().appendFile(dataFile(t, "t1")).commit());
    plainCommit(ORDERS, t -> t.newAppend().appendFile(dataFile(t, "w2")).commit());

    assertThat(finalizeSubtxn(operation(ORDERS, staged, LockManager.MODE_ROW_EXCLUSIVE))).isTrue();

    assertThat(dataFileNames(ORDERS)).containsExactlyInAnyOrder("orders-0", "w2", "t1");
  }

  @Test
  void subtxnConcurrentSchemaChangeIsNotLost() {
    String staged = stage(ORDERS, t -> t.newAppend().appendFile(dataFile(t, "t1")).commit());
    plainCommit(ORDERS, t -> t.updateSchema().addColumn("w2_col", Types.StringType.get()).commit());

    assertThat(finalizeSubtxn(operation(ORDERS, staged, LockManager.MODE_ROW_EXCLUSIVE))).isTrue();

    assertThat(columnNames(ORDERS)).contains("w2_col");
    assertThat(dataFileNames(ORDERS)).containsExactlyInAnyOrder("orders-0", "t1");
  }

  // ---------------------------------------------------------------------------
  // Staged metadata must belong to the table being committed
  // ---------------------------------------------------------------------------

  @Test
  void inlineStagedMetadataOfAnotherTableIsRejected() throws Exception {
    String stagedForCustomers =
        stage(CUSTOMERS, t -> t.newAppend().appendFile(dataFile(t, "t1-c")).commit());

    try (IcebergCatalogHandler handler = newHandler()) {
      assertThatThrownBy(
              () ->
                  handler.finalizeInlineTables(
                      new ArrayList<>(List.of(inline(ORDERS, stagedForCustomers))),
                      new ArrayList<>(),
                      false,
                      handlerFactory,
                      CATALOG,
                      principal(),
                      -1L))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("does not belong to table " + ORDERS);
    }

    assertThat(dataFileNames(ORDERS)).containsExactly("orders-0");
    assertThat(dataFileNames(CUSTOMERS)).containsExactly("customers-0");
  }

  @Test
  void subtxnMergeRetryOfAnotherTablesMetadataIsRejected() throws Exception {
    String stagedForCustomers =
        stage(CUSTOMERS, t -> t.newAppend().appendFile(dataFile(t, "t1-c")).commit());

    try (IcebergCatalogHandler handler = newHandler()) {
      assertThatThrownBy(
              () ->
                  handler.finalizeExtensionTransaction(
                      new ArrayList<>(),
                      new ArrayList<>(
                          List.of(
                              operation(
                                  ORDERS, stagedForCustomers, LockManager.MODE_ROW_EXCLUSIVE))),
                      handlerFactory,
                      CATALOG,
                      principal()))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("does not belong to table " + ORDERS);
    }

    assertThat(dataFileNames(ORDERS))
        .as("another table's data files must not be appended here")
        .containsExactly("orders-0");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Runs the inline finalize; returns false if it failed with a serialization failure. */
  private boolean finalizeInline(
      boolean mergeOnConflict, TransactionService.StagedTableInfo... direct) {
    try (IcebergCatalogHandler handler = newHandler()) {
      handler.finalizeInlineTables(
          new ArrayList<>(List.of(direct)),
          new ArrayList<>(),
          mergeOnConflict,
          handlerFactory,
          CATALOG,
          principal(),
          -1L);
      return true;
    } catch (TransactionService.SerializationFailureException e) {
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Runs the subtransaction finalize; returns false if it failed with a serialization failure. */
  private boolean finalizeSubtxn(JdbcTransactionPersistence.OperationRow... direct) {
    try (IcebergCatalogHandler handler = newHandler()) {
      handler.finalizeExtensionTransaction(
          new ArrayList<>(List.of(direct)),
          new ArrayList<>(),
          handlerFactory,
          CATALOG,
          principal());
      return true;
    } catch (TransactionService.SerializationFailureException e) {
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static TransactionService.StagedTableInfo inline(TableIdentifier id, String staged) {
    return new TransactionService.StagedTableInfo(
        id.toString(), staged, null, LockManager.MODE_ROW_EXCLUSIVE);
  }

  private static JdbcTransactionPersistence.OperationRow operation(
      TableIdentifier id, String staged, String lockMode) {
    Instant now = Instant.now();
    return new JdbcTransactionPersistence.OperationRow(
        1L,
        1L,
        1L,
        "test-realm",
        0L,
        id.toString(),
        lockMode,
        null,
        null,
        null,
        staged,
        staged,
        "op-token",
        JdbcTransactionPersistence.OP_STATUS_COMMITTED,
        null,
        now,
        now.plusSeconds(3600),
        null);
  }

  /**
   * Stages a change the way a transaction client does: the update is built on the table's current
   * state and its metadata file is written, but the table pointer does not move. Returns the staged
   * metadata location.
   */
  private String stage(TableIdentifier id, Consumer<Table> change) {
    try (IcebergCatalogHandler handler = newHandler()) {
      handler.loadTable(id, null);
      TableOperations live = ((BaseTable) handler.loadRawTable(id)).operations();
      StagingOperations staging = new StagingOperations(live);
      change.accept(new BaseTable(staging, id.toString()));
      return staging.stagedLocation();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Commits a change directly through the catalog, as a non-transactional writer would. */
  private void plainCommit(TableIdentifier id, Consumer<Table> change) {
    try (IcebergCatalogHandler handler = newHandler()) {
      handler.loadTable(id, null);
      change.accept(handler.loadRawTable(id));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Table liveTable(IcebergCatalogHandler handler, TableIdentifier id) {
    handler.loadTable(id, null);
    return handler.loadRawTable(id);
  }

  private List<String> dataFileNames(TableIdentifier id) {
    try (IcebergCatalogHandler handler = newHandler();
        CloseableIterable<FileScanTask> tasks = liveTable(handler, id).newScan().planFiles()) {
      List<String> names = new ArrayList<>();
      for (FileScanTask task : tasks) {
        String location = task.file().location();
        names.add(location.substring(location.lastIndexOf('/') + 1, location.lastIndexOf('.')));
      }
      return names;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> columnNames(TableIdentifier id) {
    try (IcebergCatalogHandler handler = newHandler()) {
      return liveTable(handler, id).schema().columns().stream()
          .map(Types.NestedField::name)
          .toList();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static DataFile dataFile(Table table, String name) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(table.location() + "/data/" + name + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private IcebergCatalogHandler newHandler() {
    return services.catalogAdapter().newHandler(services.securityContext(), CATALOG);
  }

  private PolarisPrincipal principal() {
    return services.principal();
  }

  private void createCatalogAndNamespace() {
    CatalogProperties properties =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s/%s", catalogLocation, CATALOG))
            .build();
    StorageConfigInfo storage =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    Catalog catalog =
        new Catalog(Catalog.TypeEnum.INTERNAL, CATALOG, properties, 0L, 0L, 1, storage);
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
    try (Response response =
        services
            .restApi()
            .createNamespace(
                CATALOG,
                CreateNamespaceRequest.builder().withNamespace(Namespace.of(NAMESPACE)).build(),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private void createTable(TableIdentifier id) {
    CreateTableRequest request =
        CreateTableRequest.builder()
            .withName(id.name())
            .withLocation(
                String.format("%s/%s/%s/%s", catalogLocation, CATALOG, NAMESPACE, id.name()))
            .withSchema(SCHEMA)
            .build();
    try (Response response =
        services
            .restApi()
            .createTable(
                CATALOG,
                NAMESPACE,
                request,
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  /**
   * Table operations that read the live table but write commits to a staged metadata file instead
   * of moving the table pointer — the client-side half of a transactional write.
   */
  private static final class StagingOperations implements TableOperations {
    private final TableOperations live;
    private TableMetadata current;
    private String stagedLocation;

    StagingOperations(TableOperations live) {
      this.live = live;
      this.current = live.current();
    }

    String stagedLocation() {
      return stagedLocation;
    }

    @Override
    public TableMetadata current() {
      return current;
    }

    @Override
    public TableMetadata refresh() {
      return current;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      String location = live.metadataFileLocation("staged-" + UUID.randomUUID() + ".metadata.json");
      TableMetadataParser.write(metadata, io().newOutputFile(location));
      stagedLocation = location;
      current = TableMetadataParser.read(io(), location);
    }

    @Override
    public FileIO io() {
      return live.io();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return live.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return live.locationProvider();
    }
  }
}
