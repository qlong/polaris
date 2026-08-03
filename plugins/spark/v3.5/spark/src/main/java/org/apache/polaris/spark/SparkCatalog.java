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
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SupportsReplaceView;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.polaris.spark.utils.DeltaHelper;
import org.apache.polaris.spark.utils.HudiHelper;
import org.apache.polaris.spark.utils.PaimonHelper;
import org.apache.polaris.spark.utils.PolarisCatalogUtils;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.catalog.ViewChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * SparkCatalog Implementation that is able to interact with both Iceberg SparkCatalog and Polaris
 * SparkCatalog. All namespaces and view related operations continue goes through the Iceberg
 * SparkCatalog. For table operations, depends on the table format, the operation can be achieved
 * with interaction with both Iceberg and Polaris SparkCatalog.
 */
public class SparkCatalog
    implements StagingTableCatalog,
        TableCatalog,
        SupportsNamespaces,
        ViewCatalog,
        SupportsReplaceView {

  @VisibleForTesting protected String catalogName = null;
  @VisibleForTesting protected org.apache.iceberg.spark.SparkCatalog icebergsSparkCatalog = null;
  @VisibleForTesting protected PolarisSparkCatalog polarisSparkCatalog = null;
  @VisibleForTesting protected DeltaHelper deltaHelper = null;
  @VisibleForTesting protected HudiHelper hudiHelper = null;
  @VisibleForTesting protected PaimonHelper paimonHelper = null;
  @VisibleForTesting TransactionManager txnManager = null;

  @Override
  public String name() {
    return catalogName;
  }

  /**
   * Check whether invalid catalog configuration is provided, and return an option map with catalog
   * type configured correctly. This function mainly validates two parts: 1) No customized catalog
   * implementation is provided. 2) No non-rest catalog type is configured.
   */
  @VisibleForTesting
  public CaseInsensitiveStringMap validateAndResolveCatalogOptions(
      CaseInsensitiveStringMap options) {
    Preconditions.checkArgument(
        options.get(CatalogProperties.CATALOG_IMPL) == null,
        "Customized catalog implementation is not supported and not needed, please remove the configuration!");

    String catalogType =
        PropertyUtil.propertyAsString(
            options, CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    Preconditions.checkArgument(
        catalogType.equals(CatalogUtil.ICEBERG_CATALOG_TYPE_REST),
        "Only rest catalog type is allowed, but got catalog type: "
            + catalogType
            + ". Either configure the type to rest or remove the config");

    Map<String, String> resolvedOptions = Maps.newHashMap();
    resolvedOptions.putAll(options);
    // when no catalog type is configured, iceberg uses hive by default. Here, we make sure the
    // type is set to rest since we only support rest catalog.
    resolvedOptions.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);

    return new CaseInsensitiveStringMap(resolvedOptions);
  }

  /**
   * Initialize REST Catalog for Iceberg and Polaris, this is the only catalog type supported by
   * Polaris at this moment.
   */
  private void initRESTCatalog(String name, CaseInsensitiveStringMap options) {
    CaseInsensitiveStringMap resolvedOptions = validateAndResolveCatalogOptions(options);

    // initialize the icebergSparkCatalog
    this.icebergsSparkCatalog = new org.apache.iceberg.spark.SparkCatalog();
    this.icebergsSparkCatalog.initialize(name, resolvedOptions);

    // initialize the polaris spark catalog
    OAuth2Util.AuthSession catalogAuth =
        PolarisCatalogUtils.getAuthSession(this.icebergsSparkCatalog);
    PolarisRESTCatalog restCatalog = new PolarisRESTCatalog();
    restCatalog.initialize(options, catalogAuth);
    this.txnManager = new TransactionManager(restCatalog, name);
    this.polarisSparkCatalog = new PolarisSparkCatalog(restCatalog);
    this.polarisSparkCatalog.initialize(name, resolvedOptions);
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    initRESTCatalog(name, options);
    this.deltaHelper = new DeltaHelper(options);
    this.hudiHelper = new HudiHelper(options);
    this.paimonHelper = new PaimonHelper(options);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.active();
    TableIdentifier icebergIdent = toIcebergIdentifier(ident);
    java.util.Optional<String> metadataLocation =
        txnManager.resolveMetadataLocation(spark, icebergIdent);

    if (metadataLocation.isPresent()) {
      // REPEATABLE_READ (or read-your-own-writes): base is a pinned snapshot metadata file.
      Configuration conf = SparkUtil.hadoopConfCatalogOverrides(spark, catalogName);
      HadoopFileIO fileIO = new HadoopFileIO(conf);
      TableMetadata pinnedMetadata = TableMetadataParser.read(fileIO, metadataLocation.get());

      Table wrapped = tryWrapPinned(spark, ident, icebergIdent, pinnedMetadata, fileIO);
      if (wrapped != null) {
        return wrapped;
      }
      // Read-only fallback when the live table cannot be loaded.
      return newStaticPinnedTable(ident, pinnedMetadata, fileIO);
    }

    // Worker mode with no pinned snapshot (READ_COMMITTED parent): the worker must still stage
    // its write to the server-allocated path, but its read base is the table's LATEST committed
    // metadata (READ_COMMITTED = no pinning). Wrap the live table in PinnedTableOperations using
    // the live metadata as the base so writes route to the staged path just like REPEATABLE_READ.
    if (txnManager.isWorkerMode(spark) && txnManager.isWorkerTable(spark, icebergIdent)) {
      Configuration conf = SparkUtil.hadoopConfCatalogOverrides(spark, catalogName);
      HadoopFileIO fileIO = new HadoopFileIO(conf);
      Table wrapped = tryWrapPinned(spark, ident, icebergIdent, null, fileIO);
      if (wrapped != null) {
        return wrapped;
      }
      // If the live table cannot be loaded, fall through to the normal load path below.
    }

    try {
      return this.icebergsSparkCatalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      return this.polarisSparkCatalog.loadTable(ident);
    }
  }

  /**
   * Wraps the live Iceberg table in {@link PinnedTableOperations} so reads use {@code baseMetadata}
   * and writes are staged. When {@code baseMetadata} is null the live table's current metadata is
   * used as the base (READ_COMMITTED worker path — read latest, stage writes). Returns null if the
   * live table is not a {@link BaseTable} (caller falls back).
   */
  /**
   * Builds a self-contained, read-only Spark table pinned to {@code pinnedMetadata}. Backed by a
   * {@link StaticTableOperations}, it holds no handle to the live table and never re-fetches server
   * state. Used as the read-only fallback when the live table cannot be loaded for wrapping.
   */
  private Table newStaticPinnedTable(
      Identifier ident, TableMetadata pinnedMetadata, HadoopFileIO fileIO) {
    LocationProvider locationProvider =
        LocationProviders.locationsFor(pinnedMetadata.location(), pinnedMetadata.properties());
    StaticTableOperations ops = new StaticTableOperations(pinnedMetadata, fileIO, locationProvider);
    org.apache.iceberg.Table icebergTable =
        new BaseTable(ops, ident.namespace()[ident.namespace().length - 1] + "." + ident.name());
    return newSparkTable(icebergTable);
  }

  private Table tryWrapPinned(
      org.apache.spark.sql.SparkSession spark,
      Identifier ident,
      TableIdentifier icebergIdent,
      TableMetadata baseMetadata,
      HadoopFileIO fileIO) {
    String txnIdStr = txnManager.getConf(spark, TransactionManager.CONF_TXN_ID);
    long txnId = txnIdStr != null ? Long.parseLong(txnIdStr) : 0L;
    try {
      Table liveSparkTable = this.icebergsSparkCatalog.loadTable(ident);
      if (liveSparkTable instanceof SparkTable sparkTable) {
        org.apache.iceberg.Table liveIceberg = sparkTable.table();
        if (liveIceberg instanceof BaseTable baseTable) {
          TableOperations liveOps = baseTable.operations();
          TableMetadata effectiveBase = baseMetadata != null ? baseMetadata : liveOps.current();
          LocationProvider locationProvider =
              LocationProviders.locationsFor(effectiveBase.location(), effectiveBase.properties());
          TableOperations ops =
              new PinnedTableOperations(
                  effectiveBase,
                  fileIO,
                  locationProvider,
                  liveOps,
                  txnManager,
                  spark,
                  icebergIdent,
                  txnId);
          org.apache.iceberg.Table icebergTable =
              new BaseTable(
                  ops, ident.namespace()[ident.namespace().length - 1] + "." + ident.name());
          return newSparkTable(icebergTable);
        }
      }
    } catch (Exception ignored) {
      // caller handles the null (static fallback or normal load)
    }
    return null;
  }

  /**
   * Constructs a SparkTable from an Iceberg Table, handling both Iceberg 1.10 (compile-time,
   * SparkTable(Table, boolean)) and 1.12+ (runtime, SparkTable(Table)).
   */
  private static Table newSparkTable(org.apache.iceberg.Table icebergTable) {
    try {
      // Iceberg 1.12+: single-arg constructor
      return SparkTable.class
          .getConstructor(org.apache.iceberg.Table.class)
          .newInstance(icebergTable);
    } catch (java.lang.NoSuchMethodException e) {
      // Iceberg 1.10: SparkTable(Table, boolean refreshEagerly)
      try {
        return SparkTable.class
            .getConstructor(org.apache.iceberg.Table.class, boolean.class)
            .newInstance(icebergTable, true);
      } catch (ReflectiveOperationException ex) {
        throw new RuntimeException("Cannot instantiate SparkTable — no compatible constructor", ex);
      }
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Cannot instantiate SparkTable", e);
    }
  }

  private static TableIdentifier toIcebergIdentifier(Identifier ident) {
    return TableIdentifier.of(
        org.apache.iceberg.catalog.Namespace.of(ident.namespace()), ident.name());
  }

  /**
   * TableOperations that pins reads to a specific metadata snapshot (REPEATABLE_READ consistency)
   * while staging writes locally — so they are invisible to other sessions until COMMIT. Writes are
   * NOT forwarded to liveOps; instead the staged metadata file is sent inline to the server at
   * commitTransaction time.
   */
  private static final class PinnedTableOperations implements TableOperations {
    private final TableMetadata pinnedMetadata;
    private final FileIO fileIO;
    private final LocationProvider locationProvider;
    private final TableOperations liveOps;
    private final TransactionManager txnManager;
    private final org.apache.spark.sql.SparkSession spark;
    private final TableIdentifier tableIdent;
    private final long txnId;

    PinnedTableOperations(
        TableMetadata pinnedMetadata,
        FileIO fileIO,
        LocationProvider locationProvider,
        TableOperations liveOps,
        TransactionManager txnManager,
        org.apache.spark.sql.SparkSession spark,
        TableIdentifier tableIdent,
        long txnId) {
      this.pinnedMetadata = pinnedMetadata;
      this.fileIO = fileIO;
      this.locationProvider = locationProvider;
      this.liveOps = liveOps;
      this.txnManager = txnManager;
      this.spark = spark;
      this.tableIdent = tableIdent;
      this.txnId = txnId;
    }

    @Override
    public TableMetadata current() {
      return pinnedMetadata;
    }

    @Override
    public TableMetadata refresh() {
      return pinnedMetadata;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      // Worker mode guard: prevent writing to a table outside the worker's subtransaction. A
      // subtransaction may span multiple tables, so the check is set membership, not equality.
      if (txnManager.isWorkerMode(spark) && !txnManager.isWorkerTable(spark, tableIdent)) {
        throw new IllegalStateException(
            "Worker subtransaction owns tables "
                + txnManager.getWorkerTables(spark)
                + " — cannot write to '"
                + TransactionManager.identToKey(tableIdent)
                + "'");
      }

      // In worker mode, use this table's server-allocated path from beginWorkerSubtransaction;
      // otherwise allocate a new local path (new UUID per commit keeps savepoint history intact).
      String stagingPath =
          txnManager
              .getWorkerStagingPath(spark, tableIdent)
              .orElseGet(
                  () ->
                      liveOps.metadataFileLocation(
                          txnId
                              + "-"
                              + UUID.randomUUID().toString().replace("-", "").substring(0, 12)
                              + ".metadata.json"));

      // Write the staged metadata file
      TableMetadataParser.write(metadata, fileIO.newOutputFile(stagingPath));

      // Map the Iceberg snapshot operation to a server LockMode. The server keys its
      // conflict-resolution policy off the lock mode (not the raw Iceberg op): an additive
      // append is ROW_EXCLUSIVE (merge-and-retry on a concurrent commit), while a delta /
      // overwrite / replace is EXCLUSIVE (first-committer-wins). The inline commit request
      // deserializes this field into the LockMode enum, so it MUST be one of
      // ROW_EXCLUSIVE / EXCLUSIVE / ACCESS_EXCLUSIVE.
      String lockMode = "ROW_EXCLUSIVE";
      if (metadata.currentSnapshot() != null) {
        String snapOp = metadata.currentSnapshot().operation();
        if (!"append".equalsIgnoreCase(snapOp)) {
          lockMode = "EXCLUSIVE";
        }
      }

      txnManager.stageWrite(spark, tableIdent, stagingPath, lockMode);
    }

    @Override
    public FileIO io() {
      return fileIO;
    }

    @Override
    public EncryptionManager encryption() {
      return liveOps.encryption();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return liveOps.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return locationProvider;
    }
  }

  @Override
  @SuppressWarnings({"deprecation"})
  public Table createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String provider = properties.get(PolarisCatalogUtils.TABLE_PROVIDER_KEY);
    if (PolarisCatalogUtils.useIceberg(provider)) {
      return this.icebergsSparkCatalog.createTable(ident, schema, transforms, properties);
    } else {
      if (PolarisCatalogUtils.isTableWithSparkManagedLocation(properties)) {
        throw new UnsupportedOperationException(
            "Create table without location key is not supported by Polaris. Please provide location or path on table creation.");
      }
      if (PolarisCatalogUtils.useDelta(provider)) {
        // For delta table, we load the delta catalog to help dealing with the
        // delta log creation.
        TableCatalog deltaCatalog = deltaHelper.loadDeltaCatalog(this.polarisSparkCatalog);
        return deltaCatalog.createTable(ident, schema, transforms, properties);
      } else if (PolarisCatalogUtils.useHudi(provider)) {
        // For creating the hudi table, we load HoodieCatalog
        // to create the .hoodie folder in cloud storage
        TableCatalog hudiCatalog = hudiHelper.loadHudiCatalog(this.polarisSparkCatalog);
        return hudiCatalog.createTable(ident, schema, transforms, properties);
      } else if (PolarisCatalogUtils.usePaimon(provider)) {
        // For creating the paimon table, we load PaimonCatalog
        // to create the paimon metadata in cloud storage
        TableCatalog paimonCatalog = paimonHelper.loadPaimonCatalog(this.polarisSparkCatalog);
        return paimonCatalog.createTable(ident, schema, transforms, properties);
      } else {
        return this.polarisSparkCatalog.createTable(ident, schema, transforms, properties);
      }
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    try {
      return this.icebergsSparkCatalog.alterTable(ident, changes);
    } catch (NoSuchTableException e) {
      Table table = this.polarisSparkCatalog.loadTable(ident);
      String provider = table.properties().get(PolarisCatalogUtils.TABLE_PROVIDER_KEY);
      if (PolarisCatalogUtils.useDelta(provider)) {
        // For delta table, most of the alter operations is a delta log manipulation,
        // we load the delta catalog to help handling the alter table operation.
        // NOTE: This currently doesn't work for changing file location and file format
        //     using ALTER TABLE ...SET LOCATION, and ALTER TABLE ... SET FILEFORMAT.
        TableCatalog deltaCatalog = deltaHelper.loadDeltaCatalog(this.polarisSparkCatalog);
        return deltaCatalog.alterTable(ident, changes);
      } else if (PolarisCatalogUtils.useHudi(provider)) {
        TableCatalog hudiCatalog = hudiHelper.loadHudiCatalog(this.polarisSparkCatalog);
        return hudiCatalog.alterTable(ident, changes);
      } else if (PolarisCatalogUtils.usePaimon(provider)) {
        TableCatalog paimonCatalog = paimonHelper.loadPaimonCatalog(this.polarisSparkCatalog);
        return paimonCatalog.alterTable(ident, changes);
      } else {
        return this.polarisSparkCatalog.alterTable(ident);
      }
    }
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return this.icebergsSparkCatalog.dropTable(ident) || this.polarisSparkCatalog.dropTable(ident);
  }

  @Override
  public void renameTable(Identifier from, Identifier to)
      throws NoSuchTableException, TableAlreadyExistsException {
    try {
      this.icebergsSparkCatalog.renameTable(from, to);
    } catch (NoSuchTableException e) {
      this.polarisSparkCatalog.renameTable(from, to);
    }
  }

  @Override
  public void invalidateTable(Identifier ident) {
    this.icebergsSparkCatalog.invalidateTable(ident);
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    if (this.icebergsSparkCatalog.purgeTable(ident)) {
      return true;
    } else {
      return this.polarisSparkCatalog.purgeTable(ident);
    }
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    Identifier[] icebergIdents = this.icebergsSparkCatalog.listTables(namespace);
    Identifier[] genericTableIdents = this.polarisSparkCatalog.listTables(namespace);

    return Stream.concat(Arrays.stream(icebergIdents), Arrays.stream(genericTableIdents))
        .toArray(Identifier[]::new);
  }

  @Override
  @SuppressWarnings({"deprecation", "RedundantSuppression"})
  public StagedTable stageCreate(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
    return this.icebergsSparkCatalog.stageCreate(ident, schema, transforms, properties);
  }

  @Override
  public StagedTable stageReplace(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws NoSuchTableException {
    return this.icebergsSparkCatalog.stageReplace(ident, schema, transforms, properties);
  }

  @Override
  public StagedTable stageCreateOrReplace(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties) {
    return this.icebergsSparkCatalog.stageCreateOrReplace(ident, schema, transforms, properties);
  }

  @Override
  public String[] defaultNamespace() {
    return this.icebergsSparkCatalog.defaultNamespace();
  }

  @Override
  public String[][] listNamespaces() {
    return this.icebergsSparkCatalog.listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return this.icebergsSparkCatalog.listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    return this.icebergsSparkCatalog.loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    this.icebergsSparkCatalog.createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    this.icebergsSparkCatalog.alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException {
    return this.icebergsSparkCatalog.dropNamespace(namespace, cascade);
  }

  @Override
  public Identifier[] listViews(String... namespace) {
    return this.icebergsSparkCatalog.listViews(namespace);
  }

  @Override
  public View loadView(Identifier ident) throws NoSuchViewException {
    return this.icebergsSparkCatalog.loadView(ident);
  }

  @Override
  public View createView(
      Identifier ident,
      String sql,
      String currentCatalog,
      String[] currentNamespace,
      StructType schema,
      String[] queryColumnNames,
      String[] columnAliases,
      String[] columnComments,
      Map<String, String> properties)
      throws ViewAlreadyExistsException, NoSuchNamespaceException {
    return this.icebergsSparkCatalog.createView(
        ident,
        sql,
        currentCatalog,
        currentNamespace,
        schema,
        queryColumnNames,
        columnAliases,
        columnComments,
        properties);
  }

  @Override
  public View alterView(Identifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    return this.icebergsSparkCatalog.alterView(ident, changes);
  }

  @Override
  public boolean dropView(Identifier ident) {
    return this.icebergsSparkCatalog.dropView(ident);
  }

  @Override
  public void renameView(Identifier fromIdentifier, Identifier toIdentifier)
      throws NoSuchViewException, ViewAlreadyExistsException {
    this.icebergsSparkCatalog.renameView(fromIdentifier, toIdentifier);
  }

  @Override
  public View replaceView(
      Identifier ident,
      String sql,
      String currentCatalog,
      String[] currentNamespace,
      StructType schema,
      String[] queryColumnNames,
      String[] columnAliases,
      String[] columnComments,
      Map<String, String> properties)
      throws NoSuchNamespaceException, NoSuchViewException {
    return this.icebergsSparkCatalog.replaceView(
        ident,
        sql,
        currentCatalog,
        currentNamespace,
        schema,
        queryColumnNames,
        columnAliases,
        columnComments,
        properties);
  }
}
