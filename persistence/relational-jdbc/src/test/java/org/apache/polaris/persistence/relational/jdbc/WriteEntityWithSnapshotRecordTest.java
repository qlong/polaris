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
package org.apache.polaris.persistence.relational.jdbc;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.CommittedTableSnapshotRow;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests that {@link JdbcBasePersistenceImpl#writeEntityWithSnapshotRecord} propagates failures from
 * the snapshot insert and that the transaction is rolled back on failure.
 *
 * <p>Uses standard H2 mode (uppercase table names) for the entity persistence layer, and a separate
 * in-memory H2 instance in PostgreSQL mode for the transaction persistence layer. The rollback
 * invariant is tested by injecting a broken {@link JdbcTransactionPersistence} that throws on
 * {@code insertCommittedTableSnapshot} and verifying that the entity row is not present.
 */
class WriteEntityWithSnapshotRecordTest {

  private static final String REALM = "wr-realm";

  /** Standard H2 for entity tables (QueryGenerator uses uppercase names like ENTITIES). */
  private DataSource entityDataSource;

  /** PostgreSQL-mode H2 for transaction tables (nextval, lowercase schema names). */
  private DataSource txnDataSource;

  private JdbcBasePersistenceImpl persistence;
  private JdbcTransactionPersistence txnPersistence;
  private PolarisCallContext callCtx;

  @BeforeEach
  void setUp() throws Exception {
    // Entity DB: standard H2 (no mode) — table names uppercased by default, matches ModelEntity
    String entityDbId = "wr_entity_" + UUID.randomUUID().toString().replace("-", "");
    entityDataSource =
        JdbcConnectionPool.create("jdbc:h2:mem:" + entityDbId + ";DB_CLOSE_DELAY=-1", "sa", "");
    DatasourceOperations entityOps = new DatasourceOperations(entityDataSource, new H2JdbcConfig());
    InputStream entitySchema =
        DatasourceOperations.class.getClassLoader().getResourceAsStream("h2/schema-v5.sql");
    entityOps.executeScript(entitySchema);

    PolarisDiagnostics diag = new PolarisDefaultDiagServiceImpl();
    RealmContext realmContext = () -> REALM;
    persistence =
        new JdbcBasePersistenceImpl(diag, entityOps, RANDOM_SECRETS, Mockito.mock(), REALM, 5);
    callCtx = new PolarisCallContext(realmContext, persistence);

    // Transaction DB: H2 in PostgreSQL mode (enables nextval; case-insensitive for txn tables)
    String txnDbId = "wr_txn_" + UUID.randomUUID().toString().replace("-", "");
    txnDataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:" + txnDbId + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_UPPER=false",
            "sa",
            "");
    DatasourceOperations txnOps = new DatasourceOperations(txnDataSource, new H2JdbcConfig());
    InputStream txnSchema =
        DatasourceOperations.class.getClassLoader().getResourceAsStream("h2/schema-v5.sql");
    txnOps.executeScript(txnSchema);
    txnPersistence = new JdbcTransactionPersistence(txnDataSource, DatabaseType.H2, REALM, 42L);
  }

  private static class H2JdbcConfig implements RelationalJdbcConfiguration {
    @Override
    public java.util.Optional<Integer> maxRetries() {
      return java.util.Optional.of(1);
    }

    @Override
    public java.util.Optional<Long> maxDurationInMs() {
      return java.util.Optional.of(5000L);
    }

    @Override
    public java.util.Optional<Long> initialDelayInMs() {
      return java.util.Optional.empty();
    }

    @Override
    public java.util.Optional<String> databaseType() {
      return java.util.Optional.of("h2");
    }
  }

  private PolarisBaseEntity buildEntity() {
    long id = IdGenerator.getIdGenerator().nextId();
    long now = System.currentTimeMillis();
    return new PolarisBaseEntity.Builder()
        .id(id)
        .catalogId(0L)
        .parentId(0L)
        .typeCode(PolarisEntityType.PRINCIPAL.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name("entity-" + id)
        .entityVersion(1)
        .createTimestamp(now)
        .lastUpdateTimestamp(now)
        .build();
  }

  @Test
  void failedSnapshotInsertRollsBackEntityWrite() throws Exception {
    // A broken JdbcTransactionPersistence that uses the entity datasource (same DB)
    // and throws on insertCommittedTableSnapshot so we can verify rollback.
    JdbcTransactionPersistence brokenTxnPersistence =
        new JdbcTransactionPersistence(entityDataSource, DatabaseType.H2, REALM, 42L) {
          @Override
          public void insertCommittedTableSnapshot(Connection conn, CommittedTableSnapshotRow row)
              throws SQLException {
            throw new SQLException("Simulated snapshot insert failure");
          }
        };

    PolarisBaseEntity entity = buildEntity();
    CommittedTableSnapshotRow snapshotRow =
        new CommittedTableSnapshotRow("tbl-rollback", 999L, null, "s3://bucket/v-fail.json");

    assertThatThrownBy(
            () ->
                persistence.writeEntityWithSnapshotRecord(
                    callCtx, entity, null, snapshotRow, brokenTxnPersistence))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Simulated snapshot insert failure");

    // Entity write must have been rolled back
    PolarisBaseEntity found =
        persistence.lookupEntity(callCtx, 0L, entity.getId(), entity.getTypeCode());
    assertThat(found).isNull();
  }

  @Test
  void successfulWriteInsertsEntityRow() throws Exception {
    // Use a no-op snapshot persistence (same entity DB) for the success path.
    // The cross-DB atomicity of the snapshot row is verified in production Postgres tests.
    JdbcTransactionPersistence noOpTxnPersistence =
        new JdbcTransactionPersistence(entityDataSource, DatabaseType.H2, REALM, 42L) {
          @Override
          public void insertCommittedTableSnapshot(Connection conn, CommittedTableSnapshotRow row) {
            // no-op for this unit test
          }
        };

    PolarisBaseEntity entity = buildEntity();
    CommittedTableSnapshotRow snapshotRow =
        new CommittedTableSnapshotRow("tbl-success", 1L, null, "s3://bucket/v-ok.json");

    persistence.writeEntityWithSnapshotRecord(
        callCtx, entity, null, snapshotRow, noOpTxnPersistence);

    PolarisBaseEntity found =
        persistence.lookupEntity(callCtx, 0L, entity.getId(), entity.getTypeCode());
    assertThat(found).isNotNull();
    assertThat(found.getId()).isEqualTo(entity.getId());
  }
}
