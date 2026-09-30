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
package org.apache.polaris.service.context.catalog;

import io.quarkus.arc.InactiveBeanException;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.LocalCatalogFactory;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalog;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.events.PolarisEventDispatcher;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.task.TaskExecutor;
import org.apache.polaris.service.transaction.LockManager;
import org.apache.polaris.service.transaction.TransactionSchemaSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
public class PolarisLocalCatalogFactory implements LocalCatalogFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisLocalCatalogFactory.class);

  private final PolarisDiagnostics diagnostics;
  private final TaskExecutor taskExecutor;
  private final StorageAccessConfigProvider storageAccessConfigProvider;
  private final FileIOFactory fileIOFactory;
  private final ResolverFactory resolverFactory;
  private final PolarisEventDispatcher polarisEventDispatcher;
  private final PolarisEventMetadataFactory eventMetadataFactory;
  private final PolarisMetaStoreManager metaStoreManager;
  private final CallContext callContext;
  private final PolarisPrincipal principal;
  private final Instance<DataSource> jdbcDataSources;
  private final Instance<DatasourceOperations> datasourceOperations;

  @Inject
  public PolarisLocalCatalogFactory(
      PolarisDiagnostics diagnostics,
      ResolverFactory resolverFactory,
      TaskExecutor taskExecutor,
      StorageAccessConfigProvider storageAccessConfigProvider,
      FileIOFactory fileIOFactory,
      PolarisEventDispatcher polarisEventDispatcher,
      PolarisEventMetadataFactory eventMetadataFactory,
      PolarisMetaStoreManager metaStoreManager,
      CallContext callContext,
      PolarisPrincipal principal,
      Instance<DataSource> jdbcDataSources,
      Instance<DatasourceOperations> datasourceOperations) {
    this.diagnostics = diagnostics;
    this.resolverFactory = resolverFactory;
    this.taskExecutor = taskExecutor;
    this.storageAccessConfigProvider = storageAccessConfigProvider;
    this.fileIOFactory = fileIOFactory;
    this.polarisEventDispatcher = polarisEventDispatcher;
    this.eventMetadataFactory = eventMetadataFactory;
    this.metaStoreManager = metaStoreManager;
    this.callContext = callContext;
    this.principal = principal;
    this.jdbcDataSources = jdbcDataSources;
    this.datasourceOperations = datasourceOperations;
  }

  @Override
  public Catalog createCatalog(final PolarisResolutionManifest resolvedManifest) {
    CatalogEntity catalog = resolvedManifest.getResolvedCatalogEntity();
    String catalogName = catalog.getName();

    String realm = callContext.getRealmContext().getRealmIdentifier();
    String catalogKey = realm + "/" + catalogName;
    LOGGER.debug("Initializing new BasePolarisCatalog for key: {}", catalogKey);

    IcebergCatalog catalogInstance =
        new IcebergCatalog(
            diagnostics,
            resolverFactory,
            metaStoreManager,
            callContext,
            resolvedManifest,
            principal,
            taskExecutor,
            storageAccessConfigProvider,
            fileIOFactory,
            polarisEventDispatcher,
            eventMetadataFactory);

    Map<String, String> catalogProperties = new HashMap<>(catalog.getPropertiesAsMap());
    String defaultBaseLocation = catalog.getBaseLocation();
    LOGGER.debug(
        "Looked up defaultBaseLocation {} for catalog {}", defaultBaseLocation, catalogKey);

    if (defaultBaseLocation == null) {
      throw new IllegalStateException(
          String.format(
              "Catalog '%s' does not have a configured warehouse location. "
                  + "Please configure a default base location for this catalog.",
              catalogKey));
    }

    catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, defaultBaseLocation);

    // TODO: The initialize properties might need to take more from CallContext and the
    // CatalogEntity.
    catalogInstance.initialize(catalogName, catalogProperties);

    // Only wire transaction persistence when the LIVE schema actually has the v5 transaction
    // tables. Presence of a JDBC datasource is not sufficient: a dialect without a v5 script
    // (or a deployment bootstrapped at an older version) is JDBC-backed but has no
    // committed_table_snapshots / commit_scn_counter, and writing to them would fail every commit.
    // Leaving jdbcTransactionPersistence null makes IcebergCatalog take its non-transactional
    // path, which is the pre-existing upstream behaviour.
    if (jdbcDataSources.isResolvable()
        && datasourceOperations.isResolvable()
        && TransactionSchemaSupport.isSupported(callContext)) {
      try {
        JdbcTransactionPersistence jdbcTx =
            new JdbcTransactionPersistence(
                jdbcDataSources.get(),
                datasourceOperations.get().getDatabaseType(),
                realm,
                catalog.getId());
        catalogInstance.setLockManager(new LockManager(jdbcTx));
        catalogInstance.setJdbcTransactionPersistence(jdbcTx);
      } catch (InactiveBeanException e) {
        LOGGER
            .atDebug()
            .addKeyValue("catalogKey", catalogKey)
            .log(
                "Skipping JDBC transaction persistence wiring (datasource inactive): {}",
                e.getMessage());
      }
    }

    return catalogInstance;
  }
}
