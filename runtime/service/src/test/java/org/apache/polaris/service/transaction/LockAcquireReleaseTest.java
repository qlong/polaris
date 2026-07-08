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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.LockRow;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link LockManager} lock lifecycle using an in-memory H2 database.
 *
 * <p>Verifies: acquire → grant, conflicting acquire → immediate {@link
 * LockManager.LockConflictException}, release → grant on retry.
 */
class LockAcquireReleaseTest {

  private static final String REALM = "lock-realm";
  private static final long CATALOG_ID = 66L;
  private static final String RESOURCE = "table-uuid-lock-test";

  private JdbcTransactionPersistence persistence;
  private LockManager lockManager;

  @BeforeEach
  void setUp() throws Exception {
    String jdbcUrl =
        "jdbc:h2:mem:lock_integ_"
            + UUID.randomUUID()
            + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_UPPER=false";
    DataSource dataSource = JdbcConnectionPool.create(jdbcUrl, "sa", "");

    DatasourceOperations ops =
        new DatasourceOperations(
            dataSource,
            new RelationalJdbcConfiguration() {
              @Override
              public Optional<Integer> maxRetries() {
                return Optional.of(1);
              }

              @Override
              public Optional<Long> maxDurationInMs() {
                return Optional.of(5000L);
              }

              @Override
              public Optional<Long> initialDelayInMs() {
                return Optional.empty();
              }

              @Override
              public Optional<String> databaseType() {
                return Optional.of("h2");
              }
            });
    InputStream schema =
        DatasourceOperations.class.getClassLoader().getResourceAsStream("h2/schema-v5.sql");
    ops.executeScript(schema);

    persistence = new JdbcTransactionPersistence(dataSource, DatabaseType.H2, REALM, CATALOG_ID);
    lockManager = new LockManager(persistence);
  }

  @Test
  void acquireGrantsLockWhenNoConflict() throws Exception {
    long txnId = persistence.nextTxnId();

    long lockId = lockManager.acquire(txnId, RESOURCE, LockManager.MODE_ROW_EXCLUSIVE);

    assertThat(lockId).isGreaterThan(0);
    List<LockRow> granted = persistence.findGrantedLocksOnResource(RESOURCE, -1L);
    assertThat(granted).hasSize(1);
    assertThat(granted.get(0).txnId()).isEqualTo(txnId);
    assertThat(granted.get(0).lockMode()).isEqualTo(LockManager.MODE_ROW_EXCLUSIVE);
  }

  @Test
  void conflictingAcquireThrowsImmediately() throws Exception {
    long txn1 = persistence.nextTxnId();
    long txn2 = persistence.nextTxnId();

    // txn1 holds EXCLUSIVE lock
    lockManager.acquire(txn1, RESOURCE, LockManager.MODE_EXCLUSIVE);

    // txn2 tries to acquire conflicting EXCLUSIVE — must throw immediately (no wait)
    assertThatThrownBy(() -> lockManager.acquire(txn2, RESOURCE, LockManager.MODE_EXCLUSIVE))
        .isInstanceOf(LockManager.LockConflictException.class);
  }

  @Test
  void rowExclusiveIsCompatibleWithRowExclusive() throws Exception {
    long txn1 = persistence.nextTxnId();
    long txn2 = persistence.nextTxnId();

    lockManager.acquire(txn1, RESOURCE, LockManager.MODE_ROW_EXCLUSIVE);

    // ROW_EXCLUSIVE on same resource by another txn — compatible (OCC path)
    long lockId2 = lockManager.acquire(txn2, RESOURCE, LockManager.MODE_ROW_EXCLUSIVE);
    assertThat(lockId2).isGreaterThan(0);
  }

  @Test
  void releaseAllClearsGrantedLocks() throws Exception {
    long txnId = persistence.nextTxnId();
    lockManager.acquire(txnId, RESOURCE, LockManager.MODE_EXCLUSIVE);

    lockManager.releaseAll(txnId);

    List<LockRow> remaining = persistence.findGrantedLocksOnResource(RESOURCE, -1L);
    assertThat(remaining).isEmpty();
  }

  @Test
  void newTransactionCanAcquireAfterConflictingTxnReleases() throws Exception {
    long txn1 = persistence.nextTxnId();
    long txn2 = persistence.nextTxnId();

    // txn1 holds EXCLUSIVE — txn2 fails immediately
    lockManager.acquire(txn1, RESOURCE, LockManager.MODE_EXCLUSIVE);
    assertThatThrownBy(() -> lockManager.acquire(txn2, RESOURCE, LockManager.MODE_EXCLUSIVE))
        .isInstanceOf(LockManager.LockConflictException.class);

    // txn1 releases — txn2 retries (new attempt) and succeeds
    lockManager.releaseAll(txn1);
    long lockId = lockManager.acquire(txn2, RESOURCE, LockManager.MODE_EXCLUSIVE);

    assertThat(lockId).isGreaterThan(0);
    List<LockRow> granted = persistence.findGrantedLocksOnResource(RESOURCE, -1L);
    assertThat(granted).hasSize(1);
    assertThat(granted.get(0).txnId()).isEqualTo(txn2);
  }

  @Test
  void exclusiveBlocksAccessExclusive() throws Exception {
    long txn1 = persistence.nextTxnId();
    long txn2 = persistence.nextTxnId();

    lockManager.acquire(txn1, RESOURCE, LockManager.MODE_EXCLUSIVE);

    assertThatThrownBy(() -> lockManager.acquire(txn2, RESOURCE, LockManager.MODE_ACCESS_EXCLUSIVE))
        .isInstanceOf(LockManager.LockConflictException.class);
  }

  @Test
  void accessExclusiveBlocksRowExclusive() throws Exception {
    long txn1 = persistence.nextTxnId();
    long txn2 = persistence.nextTxnId();

    lockManager.acquire(txn1, RESOURCE, LockManager.MODE_ACCESS_EXCLUSIVE);

    assertThatThrownBy(() -> lockManager.acquire(txn2, RESOURCE, LockManager.MODE_ROW_EXCLUSIVE))
        .isInstanceOf(LockManager.LockConflictException.class);
  }
}
