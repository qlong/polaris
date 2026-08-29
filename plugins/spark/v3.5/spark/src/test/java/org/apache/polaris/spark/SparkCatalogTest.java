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

import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Maps;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.DeleteReachableFiles;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.actions.DeleteReachableFilesSparkAction;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.types.Types;
import org.apache.polaris.spark.utils.DeltaHelper;
import org.apache.polaris.spark.utils.HudiHelper;
import org.apache.polaris.spark.utils.PaimonHelper;
import org.apache.polaris.spark.utils.PolarisCatalogUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.SessionState;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import scala.Option;

public class SparkCatalogTest {
  private static MockedStatic<SparkSession> mockedStaticSparkSession;
  private static SparkSession mockedSession;

  private static class InMemoryIcebergSparkCatalog extends org.apache.iceberg.spark.SparkCatalog {
    private PolarisInMemoryCatalog inMemoryCatalog = null;

    @Override
    protected Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
      PolarisInMemoryCatalog inMemoryCatalog = new PolarisInMemoryCatalog();
      inMemoryCatalog.initialize(name, options);

      this.inMemoryCatalog = inMemoryCatalog;

      return inMemoryCatalog;
    }

    public PolarisInMemoryCatalog getInMemoryCatalog() {
      return this.inMemoryCatalog;
    }
  }

  /**
   * And SparkCatalog implementation that uses InMemory catalog implementation for both Iceberg and
   * Polaris
   */
  private static class InMemorySparkCatalog extends SparkCatalog {
    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
      this.catalogName = name;
      // initialize the InMemory icebergSparkCatalog
      this.icebergsSparkCatalog = new InMemoryIcebergSparkCatalog();
      this.icebergsSparkCatalog.initialize(name, options);

      // initialize the polarisSparkCatalog with PolarisSparkCatalog
      this.polarisSparkCatalog =
          new PolarisSparkCatalog(
              ((InMemoryIcebergSparkCatalog) this.icebergsSparkCatalog).getInMemoryCatalog());
      this.polarisSparkCatalog.initialize(name, options);

      this.deltaHelper = new DeltaHelper(options);
      this.hudiHelper = new HudiHelper(options);
      this.paimonHelper = new PaimonHelper(options);
    }
  }

  private InMemorySparkCatalog catalog;
  private String catalogName;

  private static final String[] defaultNS = new String[] {"ns"};
  private static StructType defaultSchema =
      new StructType().add("id", "long").add("name", "string");

  @BeforeEach
  public void setup() throws Exception {
    catalogName = "test_" + UUID.randomUUID();
    Map<String, String> catalogConfig = Maps.newHashMap();
    catalogConfig.put(CATALOG_IMPL, "org.apache.iceberg.inmemory.InMemoryCatalog");
    catalogConfig.put("cache-enabled", "false");
    catalogConfig.put(
        DeltaHelper.DELTA_CATALOG_IMPL_KEY, "org.apache.polaris.spark.NoopDeltaCatalog");
    catalogConfig.put(HudiHelper.HUDI_CATALOG_IMPL_KEY, "org.apache.polaris.spark.NoopHudiCatalog");
    catalogConfig.put(
        PaimonHelper.PAIMON_CATALOG_IMPL_KEY, "org.apache.polaris.spark.NoopPaimonCatalog");
    catalog = new InMemorySparkCatalog();
    Configuration conf = new Configuration();

    // Setup persistent SparkSession mock
    mockedStaticSparkSession = Mockito.mockStatic(SparkSession.class);
    mockedSession = Mockito.mock(SparkSession.class);
    org.apache.spark.sql.RuntimeConfig mockedConfig =
        Mockito.mock(org.apache.spark.sql.RuntimeConfig.class);
    SparkContext mockedContext = Mockito.mock(SparkContext.class);
    SessionState mockedSessionState = Mockito.mock(SessionState.class);
    SQLConf mockedSQLConf = Mockito.mock(SQLConf.class);

    mockedStaticSparkSession.when(SparkSession::active).thenReturn(mockedSession);
    Mockito.when(mockedSession.conf()).thenReturn(mockedConfig);
    Mockito.when(mockedSession.sessionState()).thenReturn(mockedSessionState);
    Mockito.when(mockedSessionState.conf()).thenReturn(mockedSQLConf);
    Mockito.when(mockedConfig.get("spark.sql.extensions", null))
        .thenReturn(
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                + "io.delta.sql.DeltaSparkSessionExtension"
                + "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
    Mockito.when(mockedConfig.get("spark.sql.warehouse.dir", "spark-warehouse"))
        .thenReturn("/tmp/test-warehouse");
    Mockito.when(mockedSession.sparkContext()).thenReturn(mockedContext);
    Mockito.when(mockedContext.applicationId()).thenReturn("appId");
    Mockito.when(mockedContext.sparkUser()).thenReturn("test-user");
    Mockito.when(mockedContext.version()).thenReturn("3.5");

    try (MockedStatic<SparkUtil> mockedSparkUtil = Mockito.mockStatic(SparkUtil.class)) {
      mockedSparkUtil
          .when(() -> SparkUtil.hadoopConfCatalogOverrides(mockedSession, catalogName))
          .thenReturn(conf);

      catalog.initialize(catalogName, new CaseInsensitiveStringMap(catalogConfig));
      catalog.createNamespace(defaultNS, Maps.newHashMap());
    }

    // loadTable calls txnManager.resolveMetadataLocation; initialize to a no-op mock so
    // tests that don't exercise the transaction path don't NPE. Individual tests override this.
    catalog.txnManager = Mockito.mock(TransactionManager.class);
    Mockito.when(catalog.txnManager.resolveMetadataLocation(Mockito.any(), Mockito.any()))
        .thenReturn(Optional.empty());
  }

  @AfterEach
  public void tearDown() {
    if (mockedStaticSparkSession != null) {
      mockedStaticSparkSession.close();
    }
  }

  @Test
  void testCatalogValidation() {
    Map<String, String> catalogConfigWithImpl = Maps.newHashMap();
    catalogConfigWithImpl.put(CATALOG_IMPL, "org.apache.iceberg.inmemory.InMemoryCatalog");
    catalogConfigWithImpl.put("cache-enabled", "false");
    SparkCatalog testCatalog = new SparkCatalog();
    assertThatThrownBy(
            () ->
                testCatalog.validateAndResolveCatalogOptions(
                    new CaseInsensitiveStringMap(catalogConfigWithImpl)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Customized catalog implementation is not supported and not needed");

    Map<String, String> catalogConfigInvalidType = Maps.newHashMap();
    catalogConfigInvalidType.put(CatalogUtil.ICEBERG_CATALOG_TYPE, "hive");
    assertThatThrownBy(
            () ->
                testCatalog.validateAndResolveCatalogOptions(
                    new CaseInsensitiveStringMap(catalogConfigInvalidType)))
        .isInstanceOf(IllegalArgumentException.class);

    CaseInsensitiveStringMap resolvedMap =
        testCatalog.validateAndResolveCatalogOptions(
            new CaseInsensitiveStringMap(Maps.newHashMap()));
    assertThat(resolvedMap.get(CatalogUtil.ICEBERG_CATALOG_TYPE))
        .isEqualTo(CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
  }

  @Test
  void testCreateAndLoadNamespace() throws Exception {
    String[] namespace = new String[] {"ns1"};
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put("key1", "value1");

    // no namespace can be found
    assertThatThrownBy(() -> catalog.loadNamespaceMetadata(namespace))
        .isInstanceOf(NoSuchNamespaceException.class);

    // create the namespace
    catalog.createNamespace(namespace, metadata);

    Map<String, String> nsMetadata = catalog.loadNamespaceMetadata(namespace);
    assertThat(nsMetadata).contains(Map.entry("key1", "value1"));
  }

  @Test
  void testDropAndListNamespaces() throws Exception {
    String[][] lv1ns = new String[][] {{"l1ns1"}, {"l1ns2"}};
    String[][] lv2ns1 = new String[][] {{"l1ns1", "l2ns1"}, {"l1ns1", "l2ns2"}};
    String[][] lv2ns2 = new String[][] {{"l1ns2", "l2ns3"}};

    // create the namespaces
    for (String[] namespace : lv1ns) {
      catalog.createNamespace(namespace, Maps.newHashMap());
    }
    for (String[] namespace : lv2ns1) {
      catalog.createNamespace(namespace, Maps.newHashMap());
    }
    for (String[] namespace : lv2ns2) {
      catalog.createNamespace(namespace, Maps.newHashMap());
    }

    // list namespaces under root
    String[][] lv1nsResult = catalog.listNamespaces();
    assertThat(lv1nsResult.length).isEqualTo(lv1ns.length + 1);
    assertThat(Arrays.asList(lv1nsResult)).contains(defaultNS);
    for (String[] namespace : lv1ns) {
      assertThat(Arrays.asList(lv1nsResult)).contains(namespace);
    }
    // list namespace under l1ns1
    String[][] lv2ns1Result = catalog.listNamespaces(lv1ns[0]);
    assertThat(lv2ns1Result.length).isEqualTo(lv2ns1.length);
    for (String[] namespace : lv2ns1) {
      assertThat(Arrays.asList(lv2ns1Result)).contains(namespace);
    }
    // list namespace under l1ns2
    String[][] lv2ns2Result = catalog.listNamespaces(lv1ns[1]);
    assertThat(lv2ns2Result.length).isEqualTo(lv2ns2.length);
    for (String[] namespace : lv2ns2) {
      assertThat(Arrays.asList(lv2ns2Result)).contains(namespace);
    }
    // no namespace under l1ns2.l2ns3
    assertThat(catalog.listNamespaces(lv2ns2[0]).length).isEqualTo(0);

    // drop l1ns2
    catalog.dropNamespace(lv2ns2[0], true);
    assertThat(catalog.listNamespaces(lv1ns[1]).length).isEqualTo(0);

    catalog.dropNamespace(lv1ns[1], true);
    assertThatThrownBy(() -> catalog.listNamespaces(lv1ns[1]))
        .isInstanceOf(NoSuchNamespaceException.class);
  }

  @Test
  void testAlterNamespace() throws Exception {
    String[] namespace = new String[] {"ns1"};
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put("orig_key1", "orig_value1");

    catalog.createNamespace(namespace, metadata);
    assertThat(catalog.loadNamespaceMetadata(namespace))
        .contains(Map.entry("orig_key1", "orig_value1"));

    catalog.alterNamespace(namespace, NamespaceChange.setProperty("new_key", "new_value"));
    assertThat(catalog.loadNamespaceMetadata(namespace))
        .contains(Map.entry("new_key", "new_value"));
  }

  @Test
  void testStageOperations() throws Exception {
    Identifier createId = Identifier.of(defaultNS, "iceberg-table-create");
    Map<String, String> icebergProperties = Maps.newHashMap();
    icebergProperties.put("provider", "iceberg");
    icebergProperties.put(TableCatalog.PROP_LOCATION, "file:///tmp/path/to/iceberg-table/");
    StructType iceberg_schema = new StructType().add("boolType", "boolean");

    catalog.stageCreate(createId, iceberg_schema, new Transform[0], icebergProperties);

    catalog.stageCreateOrReplace(createId, iceberg_schema, new Transform[0], icebergProperties);
  }

  @Test
  void testBasicViewOperations() throws Exception {
    Identifier viewIdentifier = Identifier.of(defaultNS, "test-view");
    String viewSql = "select id from test-table where id < 3";
    StructType schema = new StructType().add("id", "long");
    catalog.createView(
        viewIdentifier,
        viewSql,
        catalogName,
        defaultNS,
        schema,
        new String[0],
        new String[0],
        new String[0],
        Maps.newHashMap());

    // load the view
    View view = catalog.loadView(viewIdentifier);
    assertThat(view.query()).isEqualTo(viewSql);
    assertThat(view.schema()).isEqualTo(schema);

    // alter the view properties
    catalog.alterView(viewIdentifier, ViewChange.setProperty("view_key1", "view_value1"));
    view = catalog.loadView(viewIdentifier);
    assertThat(view.properties()).contains(Map.entry("view_key1", "view_value1"));

    // rename the view
    Identifier newIdentifier = Identifier.of(defaultNS, "new-view");
    catalog.renameView(viewIdentifier, newIdentifier);
    assertThatThrownBy(() -> catalog.loadView(viewIdentifier))
        .isInstanceOf(NoSuchViewException.class);
    view = catalog.loadView(newIdentifier);
    assertThat(view.query()).isEqualTo(viewSql);
    assertThat(view.schema()).isEqualTo(schema);

    // replace the view
    String newSql = "select id from test-table where id == 3";
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "value1");
    catalog.replaceView(
        newIdentifier,
        newSql,
        catalogName,
        defaultNS,
        schema,
        new String[0],
        new String[0],
        new String[0],
        properties);
    view = catalog.loadView(newIdentifier);
    assertThat(view.query()).isEqualTo(newSql);
    assertThat(view.properties()).contains(Map.entry("key1", "value1"));

    // drop the view
    catalog.dropView(newIdentifier);
    assertThatThrownBy(() -> catalog.loadView(newIdentifier))
        .isInstanceOf(NoSuchViewException.class);
  }

  @Test
  void testListViews() throws Exception {
    // create a new namespace under the default NS
    String[] namespace = new String[] {"ns", "nsl2"};
    catalog.createNamespace(namespace, Maps.newHashMap());
    // create  under defaultNS
    String view1Name = "test-view1";
    String view1SQL = "select id from test-table where id >= 3";
    catalog.createView(
        Identifier.of(defaultNS, view1Name),
        view1SQL,
        catalogName,
        defaultNS,
        defaultSchema,
        new String[0],
        new String[0],
        new String[0],
        Maps.newHashMap());
    // create two views under ns.nsl2
    String[] nsl2ViewNames = new String[] {"test-view2", "test-view3"};
    String[] nsl2ViewSQLs =
        new String[] {
          "select id from test-table where id == 3", "select id from test-table where id < 3"
        };
    for (int i = 0; i < nsl2ViewNames.length; i++) {
      catalog.createView(
          Identifier.of(namespace, nsl2ViewNames[i]),
          nsl2ViewSQLs[i],
          catalogName,
          namespace,
          defaultSchema,
          new String[0],
          new String[0],
          new String[0],
          Maps.newHashMap());
    }
    // list views under defaultNS
    Identifier[] l1Views = catalog.listViews(defaultNS);
    assertThat(l1Views.length).isEqualTo(1);
    assertThat(l1Views[0].name()).isEqualTo(view1Name);

    // list views under ns1.nsl2
    Identifier[] l2Views = catalog.listViews(namespace);
    assertThat(l2Views.length).isEqualTo(nsl2ViewSQLs.length);
    for (String name : nsl2ViewNames) {
      assertThat(Arrays.asList(l2Views)).contains(Identifier.of(namespace, name));
    }
  }

  @Test
  void testIcebergTableOperations() throws Exception {
    Identifier identifier = Identifier.of(defaultNS, "iceberg-table");
    createAndValidateGenericTableWithLoad(catalog, identifier, defaultSchema, "iceberg");

    // load the table
    Table table = catalog.loadTable(identifier);
    // verify iceberg SparkTable is loaded
    assertThat(table).isInstanceOf(SparkTable.class);

    Identifier[] icebergTables = catalog.listTables(defaultNS);
    assertThat(icebergTables.length).isEqualTo(1);
    assertThat(icebergTables[0]).isEqualTo(Identifier.of(defaultNS, "iceberg-table"));

    // verify create table with the same identifier fails with spark TableAlreadyExistsException
    Map<String, String> newProperties = Maps.newHashMap();
    newProperties.put(PolarisCatalogUtils.TABLE_PROVIDER_KEY, "iceberg");
    newProperties.put(TableCatalog.PROP_LOCATION, "file:///tmp/path/to/table/");
    assertThatThrownBy(
            () -> catalog.createTable(identifier, defaultSchema, new Transform[0], newProperties))
        .isInstanceOf(TableAlreadyExistsException.class);

    // drop the iceberg table
    catalog.dropTable(identifier);
    assertThatThrownBy(() -> catalog.loadTable(identifier))
        .isInstanceOf(NoSuchTableException.class);
    assertThat(catalog.listTables(defaultNS)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"delta", "hudi", "csv", "paimon"})
  void testCreateAndLoadGenericTable(String format) throws Exception {
    Identifier identifier = Identifier.of(defaultNS, "generic-test-table");
    createAndValidateGenericTableWithLoad(catalog, identifier, defaultSchema, format);

    Identifier[] icebergTables = catalog.listTables(defaultNS);
    assertThat(icebergTables.length).isEqualTo(1);
    assertThat(icebergTables[0]).isEqualTo(Identifier.of(defaultNS, "generic-test-table"));

    Map<String, String> newProperties = Maps.newHashMap();
    newProperties.put(PolarisCatalogUtils.TABLE_PROVIDER_KEY, "parquet");
    newProperties.put(TableCatalog.PROP_LOCATION, "file:///tmp/path/to/table/");
    assertThatThrownBy(
            () -> catalog.createTable(identifier, defaultSchema, new Transform[0], newProperties))
        .isInstanceOf(TableAlreadyExistsException.class);

    catalog.dropTable(identifier);
    assertThatThrownBy(() -> catalog.loadTable(identifier))
        .isInstanceOf(NoSuchTableException.class);
    assertThat(catalog.listTables(defaultNS)).isEmpty();
  }

  @Test
  void testMixedTables() throws Exception {
    // create two iceberg tables, and three non-iceberg tables
    String[] tableNames =
        new String[] {"iceberg1", "iceberg2", "delta1", "csv1", "delta2", "hudi1", "hudi2"};
    String[] tableFormats = new String[] {"iceberg", null, "delta", "csv", "delta", "hudi", "hudi"};
    for (int i = 0; i < tableNames.length; i++) {
      Identifier identifier = Identifier.of(defaultNS, tableNames[i]);
      createAndValidateGenericTableWithLoad(catalog, identifier, defaultSchema, tableFormats[i]);
    }

    // list all tables
    Identifier[] tableIdents = catalog.listTables(defaultNS);
    assertThat(tableIdents.length).isEqualTo(tableNames.length);
    for (String name : tableNames) {
      assertThat(tableIdents).contains(Identifier.of(defaultNS, name));
    }

    // drop iceberg2 and delta1 table
    catalog.dropTable(Identifier.of(defaultNS, "iceberg2"));
    catalog.dropTable(Identifier.of(defaultNS, "delta2"));
    catalog.dropTable(Identifier.of(defaultNS, "hudi2"));

    String[] remainingTableNames = new String[] {"iceberg1", "delta1", "csv1", "hudi1"};
    Identifier[] remainingTableIndents = catalog.listTables(defaultNS);
    assertThat(remainingTableIndents.length).isEqualTo(remainingTableNames.length);
    for (String name : remainingTableNames) {
      assertThat(tableIdents).contains(Identifier.of(defaultNS, name));
    }

    // drop the remaining tables
    for (String name : remainingTableNames) {
      catalog.dropTable(Identifier.of(defaultNS, name));
    }
    assertThat(catalog.listTables(defaultNS)).isEmpty();
  }

  @Test
  void testAlterAndRenameTable() throws Exception {
    String icebergTableName = "iceberg-table";
    String deltaTableName = "delta-table";
    String csvTableName = "csv-table";
    String hudiTableName = "hudi-table";
    Identifier icebergIdent = Identifier.of(defaultNS, icebergTableName);
    Identifier deltaIdent = Identifier.of(defaultNS, deltaTableName);
    Identifier csvIdent = Identifier.of(defaultNS, csvTableName);
    Identifier hudiIdent = Identifier.of(defaultNS, hudiTableName);
    createAndValidateGenericTableWithLoad(catalog, icebergIdent, defaultSchema, "iceberg");
    createAndValidateGenericTableWithLoad(catalog, deltaIdent, defaultSchema, "delta");
    createAndValidateGenericTableWithLoad(catalog, csvIdent, defaultSchema, "csv");
    createAndValidateGenericTableWithLoad(catalog, hudiIdent, defaultSchema, "hudi");

    // verify alter iceberg table
    Table newIcebergTable =
        catalog.alterTable(icebergIdent, TableChange.setProperty("iceberg_key", "iceberg_value"));
    assertThat(newIcebergTable).isInstanceOf(SparkTable.class);
    assertThat(newIcebergTable.properties()).contains(Map.entry("iceberg_key", "iceberg_value"));

    // verify rename iceberg table works
    Identifier newIcebergIdent = Identifier.of(defaultNS, "new-iceberg-table");
    catalog.renameTable(icebergIdent, newIcebergIdent);
    assertThatThrownBy(() -> catalog.loadTable(icebergIdent))
        .isInstanceOf(NoSuchTableException.class);
    Table icebergTable = catalog.loadTable(newIcebergIdent);
    assertThat(icebergTable).isInstanceOf(SparkTable.class);

    // verify alter delta table is a no-op, and alter csv table throws an exception
    SQLConf conf = new SQLConf();
    try (MockedStatic<DataSource> mockedStaticDS = Mockito.mockStatic(DataSource.class);
        MockedStatic<DataSourceV2Utils> mockedStaticDSV2 =
            Mockito.mockStatic(DataSourceV2Utils.class)) {
      SessionState mockedState = Mockito.mock(SessionState.class);
      Mockito.when(mockedSession.sessionState()).thenReturn(mockedState);
      Mockito.when(mockedState.conf()).thenReturn(conf);

      // Mock SessionCatalog for Hudi support
      org.apache.spark.sql.catalyst.catalog.SessionCatalog mockedSessionCatalog =
          Mockito.mock(org.apache.spark.sql.catalyst.catalog.SessionCatalog.class);
      Mockito.when(mockedState.catalog()).thenReturn(mockedSessionCatalog);

      TableProvider deltaProvider = Mockito.mock(TableProvider.class);
      mockedStaticDS
          .when(() -> DataSource.lookupDataSourceV2(Mockito.eq("delta"), Mockito.any()))
          .thenReturn(Option.apply(deltaProvider));
      V1Table deltaTable = Mockito.mock(V1Table.class);
      Map<String, String> deltaProps = Maps.newHashMap();
      deltaProps.put(PolarisCatalogUtils.TABLE_PROVIDER_KEY, "delta");
      deltaProps.put(TableCatalog.PROP_LOCATION, "file:///tmp/delta/path/to/table/test-delta/");
      Mockito.when(deltaTable.properties()).thenReturn(deltaProps);
      mockedStaticDSV2
          .when(
              () ->
                  DataSourceV2Utils.getTableFromProvider(
                      Mockito.eq(deltaProvider), Mockito.any(), Mockito.any()))
          .thenReturn(deltaTable);

      Table delta =
          catalog.alterTable(deltaIdent, TableChange.setProperty("delta_key", "delta_value"));
      assertThat(delta).isInstanceOf(V1Table.class);

      TableProvider csvProvider = Mockito.mock(TableProvider.class);
      mockedStaticDS
          .when(() -> DataSource.lookupDataSourceV2(Mockito.eq("csv"), Mockito.any()))
          .thenReturn(Option.apply(csvProvider));
      Map<String, String> csvProps = Maps.newHashMap();
      csvProps.put(PolarisCatalogUtils.TABLE_PROVIDER_KEY, "csv");
      V1Table csvTable = Mockito.mock(V1Table.class);
      Mockito.when(csvTable.properties()).thenReturn(csvProps);
      mockedStaticDSV2
          .when(
              () ->
                  DataSourceV2Utils.getTableFromProvider(
                      Mockito.eq(csvProvider), Mockito.any(), Mockito.any()))
          .thenReturn(csvTable);
      assertThatThrownBy(
              () -> catalog.alterTable(csvIdent, TableChange.setProperty("csv_key", "scv_value")))
          .isInstanceOf(UnsupportedOperationException.class);
    }

    // verify rename non-iceberg table is not supported
    assertThatThrownBy(
            () -> catalog.renameTable(deltaIdent, Identifier.of(defaultNS, "new-delta-table")))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(
            () -> catalog.renameTable(csvIdent, Identifier.of(defaultNS, "new-csv-table")))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void testPurgeInvalidateTable() throws Exception {
    Identifier icebergIdent = Identifier.of(defaultNS, "iceberg-table");
    Identifier deltaIdent = Identifier.of(defaultNS, "delta-table");
    Identifier hudiIdent = Identifier.of(defaultNS, "hudi-table");
    createAndValidateGenericTableWithLoad(catalog, icebergIdent, defaultSchema, "iceberg");
    createAndValidateGenericTableWithLoad(catalog, deltaIdent, defaultSchema, "delta");
    createAndValidateGenericTableWithLoad(catalog, hudiIdent, defaultSchema, "hudi");
    // test invalidate table is a no op today
    catalog.invalidateTable(icebergIdent);
    catalog.invalidateTable(deltaIdent);
    catalog.invalidateTable(hudiIdent);

    Identifier[] tableIdents = catalog.listTables(defaultNS);
    assertThat(tableIdents.length).isEqualTo(3);

    // verify purge tables drops the table
    catalog.purgeTable(deltaIdent);
    catalog.purgeTable(hudiIdent);
    assertThat(catalog.listTables(defaultNS).length).isEqualTo(1);

    // purge iceberg table triggers file deletion
    try (MockedStatic<SparkActions> mockedStaticActions = Mockito.mockStatic(SparkActions.class)) {
      SparkActions actions = Mockito.mock(SparkActions.class);
      DeleteReachableFilesSparkAction deleteAction =
          Mockito.mock(DeleteReachableFilesSparkAction.class);
      mockedStaticActions.when(SparkActions::get).thenReturn(actions);
      Mockito.when(actions.deleteReachableFiles(Mockito.any())).thenReturn(deleteAction);
      Mockito.when(deleteAction.io(Mockito.any())).thenReturn(deleteAction);
      Mockito.when(deleteAction.execute())
          .thenReturn(Mockito.mock(DeleteReachableFiles.Result.class));

      catalog.purgeTable(icebergIdent);
    }
    assertThat(catalog.listTables(defaultNS).length).isEqualTo(0);
  }

  // ---------------------------------------------------------------------------
  // PinnedTableOperations tests (transaction path in loadTable)
  // ---------------------------------------------------------------------------

  /**
   * Regression test for the "Cannot modify a static table" bug. When a REPEATABLE_READ transaction
   * is active, loadTable must return a table backed by PinnedTableOperations (which delegates
   * writes to the live OCC ops), not StaticTableOperations (which throws on any write attempt).
   */
  @Test
  void testLoadTableInTransactionUsesPinnedOpsAllowingWrites() throws Exception {
    // Create a live table in the in-memory catalog so the live-ops lookup succeeds.
    Identifier ident = Identifier.of(defaultNS, "txn-write-test");
    Map<String, String> props = Maps.newHashMap();
    props.put(TableCatalog.PROP_LOCATION, "file:///tmp/polaris-txn-write-test/");
    catalog.createTable(ident, defaultSchema, new Transform[0], props);

    // Write a standalone metadata file to disk — this is the "pinned" snapshot the txn sees.
    Path tempDir = Files.createTempDirectory("polaris-pinned-ops-");
    String pinnedMetaPath = tempDir.resolve("v1.metadata.json").toUri().toString();
    Schema pinnedSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    TableMetadata pinnedMeta =
        TableMetadata.newTableMetadata(
            pinnedSchema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            "file:///tmp/polaris-txn-write-test/",
            Maps.newHashMap());
    HadoopFileIO fio = new HadoopFileIO(new Configuration());
    TableMetadataParser.write(pinnedMeta, fio.newOutputFile(pinnedMetaPath));

    // Simulate an active REPEATABLE_READ transaction.
    Mockito.when(catalog.txnManager.resolveMetadataLocation(Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(pinnedMetaPath));

    try (MockedStatic<SparkUtil> su = Mockito.mockStatic(SparkUtil.class)) {
      su.when(() -> SparkUtil.hadoopConfCatalogOverrides(Mockito.any(), Mockito.any()))
          .thenReturn(new Configuration());

      Table txnTable = catalog.loadTable(ident);

      assertThat(txnTable).isInstanceOf(SparkTable.class);
      TableOperations ops = ((BaseTable) ((SparkTable) txnTable).table()).operations();

      // PinnedTableOperations delegates metadataFileLocation to liveOps — no throw.
      // StaticTableOperations always throws UnsupportedOperationException here.
      assertThatCode(() -> ops.metadataFileLocation("00001.metadata.json"))
          .as("metadataFileLocation must not throw; StaticTableOperations would throw here")
          .doesNotThrowAnyException();
    }
  }

  /**
   * When inside a transaction, current() and refresh() must return the PINNED snapshot's metadata —
   * not the live table's current state. This guarantees REPEATABLE_READ semantics.
   */
  @Test
  void testLoadTableInTransactionPinsReadsToSpecifiedSnapshot() throws Exception {
    Identifier ident = Identifier.of(defaultNS, "txn-pin-read-test");
    Map<String, String> props = Maps.newHashMap();
    props.put(TableCatalog.PROP_LOCATION, "file:///tmp/polaris-txn-pin-read-test/");
    // Live table uses defaultSchema (id: long, name: string).
    catalog.createTable(ident, defaultSchema, new Transform[0], props);

    // Pinned metadata has a completely different schema with a sentinel column.
    Path tempDir = Files.createTempDirectory("polaris-pinned-read-");
    String pinnedMetaPath = tempDir.resolve("v1.metadata.json").toUri().toString();
    Schema pinnedSchema =
        new Schema(Types.NestedField.required(99, "txn_pinned_sentinel", Types.IntegerType.get()));
    TableMetadata pinnedMeta =
        TableMetadata.newTableMetadata(
            pinnedSchema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            "file:///tmp/polaris-txn-pin-read-test/",
            Maps.newHashMap());
    HadoopFileIO fio = new HadoopFileIO(new Configuration());
    TableMetadataParser.write(pinnedMeta, fio.newOutputFile(pinnedMetaPath));

    Mockito.when(catalog.txnManager.resolveMetadataLocation(Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(pinnedMetaPath));

    try (MockedStatic<SparkUtil> su = Mockito.mockStatic(SparkUtil.class)) {
      su.when(() -> SparkUtil.hadoopConfCatalogOverrides(Mockito.any(), Mockito.any()))
          .thenReturn(new Configuration());

      Table txnTable = catalog.loadTable(ident);
      TableOperations ops = ((BaseTable) ((SparkTable) txnTable).table()).operations();

      // Both current() and refresh() must return the PINNED metadata, not the live table's.
      assertThat(ops.current().schema().findField("txn_pinned_sentinel"))
          .as("current() must return pinned metadata with sentinel column")
          .isNotNull();
      assertThat(ops.refresh().schema().findField("txn_pinned_sentinel"))
          .as("refresh() must also return pinned metadata — no live re-fetch")
          .isNotNull();
      // Live table column must NOT be visible through the pinned view.
      assertThat(ops.current().schema().findField("name"))
          .as("live table column 'name' must not bleed into pinned view")
          .isNull();
    }
  }

  /**
   * When the live table cannot be loaded (e.g., catalog is temporarily unavailable), loadTable
   * falls back to StaticTableOperations. The fallback is read-only: metadataFileLocation() throws,
   * matching the original StaticTableOperations contract.
   */
  @Test
  void testLoadTableInTransactionFallsBackToStaticOpsWhenLiveUnreachable() throws Exception {
    // No table created in the catalog — icebergsSparkCatalog.loadTable will throw
    // NoSuchTableException.
    Identifier ident = Identifier.of(defaultNS, "txn-fallback-test");

    Path tempDir = Files.createTempDirectory("polaris-fallback-");
    String pinnedMetaPath = tempDir.resolve("v1.metadata.json").toUri().toString();
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    TableMetadata pinnedMeta =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            "file:///tmp/polaris-txn-fallback/",
            Maps.newHashMap());
    HadoopFileIO fio = new HadoopFileIO(new Configuration());
    TableMetadataParser.write(pinnedMeta, fio.newOutputFile(pinnedMetaPath));

    Mockito.when(catalog.txnManager.resolveMetadataLocation(Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(pinnedMetaPath));

    try (MockedStatic<SparkUtil> su = Mockito.mockStatic(SparkUtil.class)) {
      su.when(() -> SparkUtil.hadoopConfCatalogOverrides(Mockito.any(), Mockito.any()))
          .thenReturn(new Configuration());

      // loadTable: live lookup throws → catch → fall back to StaticTableOperations.
      Table txnTable = catalog.loadTable(ident);

      assertThat(txnTable).isInstanceOf(SparkTable.class);
      TableOperations ops = ((BaseTable) ((SparkTable) txnTable).table()).operations();

      // StaticTableOperations throws on write helpers — confirms the fallback path is taken.
      assertThatThrownBy(() -> ops.metadataFileLocation("00001.metadata.json"))
          .as("Fallback StaticTableOperations must throw UnsupportedOperationException on writes")
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("static table");
    }
  }

  /**
   * Worker-mode re-stage regression test. A worker operation gets ONE fixed, server-allocated
   * staging path (validated at commitSubtransaction). A re-attach or a repeated INSERT into an
   * already-created-but-not-yet-committed worker operation calls commit() again on that same fixed
   * path. The staging write must be create-or-replace (TableMetadataParser.overwrite) so the second
   * commit does NOT throw AlreadyExistsException — the previous fix for the distributable-subtxn
   * "Path already exists" bug.
   */
  @Test
  void testWorkerCommitOverwritesFixedStagingPath() throws Exception {
    Identifier ident = Identifier.of(defaultNS, "worker-overwrite-test");
    Map<String, String> props = Maps.newHashMap();
    props.put(TableCatalog.PROP_LOCATION, "file:///tmp/polaris-worker-overwrite/");
    catalog.createTable(ident, defaultSchema, new Transform[0], props);

    // The single server-allocated staging path this worker operation writes to on every commit().
    Path tempDir = Files.createTempDirectory("polaris-worker-staging-");
    String workerStagingPath = tempDir.resolve("15-18-fixedstaging.metadata.json").toUri().toString();

    // Simulate an active worker subtransaction for this table (READ_COMMITTED parent → no pin,
    // resolveMetadataLocation stays empty; worker path is taken because isWorkerMode/isWorkerTable).
    Mockito.when(catalog.txnManager.getConf(Mockito.any(), Mockito.eq(TransactionManager.CONF_TXN_ID)))
        .thenReturn("15");
    Mockito.when(catalog.txnManager.isWorkerMode(Mockito.any())).thenReturn(true);
    Mockito.when(catalog.txnManager.isWorkerTable(Mockito.any(), Mockito.any())).thenReturn(true);
    Mockito.when(catalog.txnManager.getWorkerStagingPath(Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(workerStagingPath));

    try (MockedStatic<SparkUtil> su = Mockito.mockStatic(SparkUtil.class)) {
      su.when(() -> SparkUtil.hadoopConfCatalogOverrides(Mockito.any(), Mockito.any()))
          .thenReturn(new Configuration());

      Table txnTable = catalog.loadTable(ident);
      TableOperations ops = ((BaseTable) ((SparkTable) txnTable).table()).operations();
      TableMetadata base = ops.current();

      // First commit stages to the fixed path.
      TableMetadata firstUpdate =
          base.replaceProperties(Collections.singletonMap("attempt", "1"));
      assertThatCode(() -> ops.commit(base, firstUpdate))
          .as("first worker commit stages to the fixed path")
          .doesNotThrowAnyException();

      // Second commit to the SAME fixed path (re-attach / repeated INSERT). With overwrite this
      // must succeed; with strict create it would throw AlreadyExistsException.
      TableMetadata secondUpdate =
          base.replaceProperties(Collections.singletonMap("attempt", "2"));
      assertThatCode(() -> ops.commit(base, secondUpdate))
          .as("re-staging the same worker operation must overwrite, not throw AlreadyExistsException")
          .doesNotThrowAnyException();

      // The staged metadata file exists at the fixed worker path, and the write was staged there.
      assertThat(new HadoopFileIO(new Configuration()).newInputFile(workerStagingPath).exists())
          .as("staged metadata.json must exist at the fixed worker path")
          .isTrue();
      Mockito.verify(catalog.txnManager, Mockito.atLeast(2))
          .stageWrite(Mockito.any(), Mockito.any(), Mockito.eq(workerStagingPath), Mockito.any());
    }
  }

  /**
   * Inline / orchestrator-direct writes (NOT worker mode) must keep strict-create staging: each
   * commit() allocates a fresh UUID metadata path via liveOps.metadataFileLocation, so no two
   * commits collide and TableMetadataParser.write's create-if-absent contract is unchanged. This
   * pins the "scoped to worker commit only" guarantee of the overwrite fix.
   */
  @Test
  void testInlineCommitUsesFreshPathPerCommitAndDoesNotOverwrite() throws Exception {
    Identifier ident = Identifier.of(defaultNS, "inline-freshpath-test");
    Map<String, String> props = Maps.newHashMap();
    props.put(TableCatalog.PROP_LOCATION, "file:///tmp/polaris-inline-freshpath/");
    catalog.createTable(ident, defaultSchema, new Transform[0], props);

    // Pin reads so loadTable wraps in PinnedTableOperations, but NOT worker mode: the inline branch.
    Path tempDir = Files.createTempDirectory("polaris-inline-pin-");
    String pinnedMetaPath = tempDir.resolve("v1.metadata.json").toUri().toString();
    TableMetadata pinnedMeta =
        TableMetadata.newTableMetadata(
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            "file:///tmp/polaris-inline-freshpath/",
            Maps.newHashMap());
    TableMetadataParser.write(
        pinnedMeta, new HadoopFileIO(new Configuration()).newOutputFile(pinnedMetaPath));

    Mockito.when(catalog.txnManager.getConf(Mockito.any(), Mockito.eq(TransactionManager.CONF_TXN_ID)))
        .thenReturn("42");
    Mockito.when(catalog.txnManager.resolveMetadataLocation(Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(pinnedMetaPath));
    // Inline path: not worker mode, no worker staging path.
    Mockito.when(catalog.txnManager.isWorkerMode(Mockito.any())).thenReturn(false);
    Mockito.when(catalog.txnManager.getWorkerStagingPath(Mockito.any(), Mockito.any()))
        .thenReturn(Optional.empty());

    // Capture the staging paths passed to stageWrite across two commits.
    java.util.List<String> stagedPaths = new java.util.ArrayList<>();
    Mockito.doAnswer(
            inv -> {
              stagedPaths.add(inv.getArgument(2));
              return null;
            })
        .when(catalog.txnManager)
        .stageWrite(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

    try (MockedStatic<SparkUtil> su = Mockito.mockStatic(SparkUtil.class)) {
      su.when(() -> SparkUtil.hadoopConfCatalogOverrides(Mockito.any(), Mockito.any()))
          .thenReturn(new Configuration());

      Table txnTable = catalog.loadTable(ident);
      TableOperations ops = ((BaseTable) ((SparkTable) txnTable).table()).operations();
      TableMetadata base = ops.current();

      ops.commit(base, base.replaceProperties(Collections.singletonMap("attempt", "1")));
      ops.commit(base, base.replaceProperties(Collections.singletonMap("attempt", "2")));

      // Two commits → two DISTINCT fresh UUID paths (never the worker's fixed path), so strict
      // create never collides. This is what keeps inline multi-table transactions unaffected.
      assertThat(stagedPaths).as("two commits stage to two paths").hasSize(2);
      assertThat(stagedPaths.get(0))
          .as("inline commits allocate distinct fresh paths per commit")
          .isNotEqualTo(stagedPaths.get(1));
      assertThat(stagedPaths.get(0)).contains("42-");
    }
  }

  private void createAndValidateGenericTableWithLoad(
      InMemorySparkCatalog sparkCatalog, Identifier identifier, StructType schema, String format)
      throws Exception {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(PolarisCatalogUtils.TABLE_PROVIDER_KEY, format);
    properties.put(
        TableCatalog.PROP_LOCATION,
        String.format("file:///tmp/%s/path/to/table/%s/", format, identifier.name()));

    if (PolarisCatalogUtils.useIceberg(format)) {
      Table createdTable =
          sparkCatalog.createTable(identifier, schema, new Transform[0], properties);
      Table loadedTable = sparkCatalog.loadTable(identifier);

      // verify iceberg SparkTable is returned for iceberg tables
      assertThat(createdTable).isInstanceOf(SparkTable.class);
      assertThat(loadedTable).isInstanceOf(SparkTable.class);
    } else {
      // For non-Iceberg tables, use mocking
      try (MockedStatic<DataSource> mockedStaticDS = Mockito.mockStatic(DataSource.class);
          MockedStatic<DataSourceV2Utils> mockedStaticDSV2 =
              Mockito.mockStatic(DataSourceV2Utils.class);
          MockedStatic<PolarisCatalogUtils> mockedStaticUtils =
              Mockito.mockStatic(PolarisCatalogUtils.class)) {

        V1Table table = Mockito.mock(V1Table.class);

        // Mock the routing utility methods
        mockedStaticUtils
            .when(() -> PolarisCatalogUtils.useHudi(Mockito.eq(format)))
            .thenCallRealMethod();
        mockedStaticUtils
            .when(() -> PolarisCatalogUtils.usePaimon(Mockito.eq(format)))
            .thenCallRealMethod();

        if ("hudi".equalsIgnoreCase(format)) {
          // For Hudi tables, mock the loadV1SparkHudiTable method to return the mock table
          mockedStaticUtils
              .when(
                  () ->
                      PolarisCatalogUtils.loadV1SparkTable(
                          Mockito.any(), Mockito.any(), Mockito.any()))
              .thenReturn(table);
        } else {
          TableProvider provider = Mockito.mock(TableProvider.class);
          mockedStaticDS
              .when(() -> DataSource.lookupDataSourceV2(Mockito.eq(format), Mockito.any()))
              .thenReturn(Option.apply(provider));
          mockedStaticDSV2
              .when(
                  () ->
                      DataSourceV2Utils.getTableFromProvider(
                          Mockito.eq(provider), Mockito.any(), Mockito.any()))
              .thenReturn(table);
          mockedStaticUtils
              .when(() -> PolarisCatalogUtils.loadV2SparkTable(Mockito.any()))
              .thenCallRealMethod();
        }

        Table createdTable =
            sparkCatalog.createTable(identifier, schema, new Transform[0], properties);
        Table loadedTable = sparkCatalog.loadTable(identifier);

        // verify Spark V1 table is returned for non-iceberg tables
        assertThat(createdTable).isInstanceOf(V1Table.class);
        assertThat(loadedTable).isInstanceOf(V1Table.class);
      }
    }
  }
}
