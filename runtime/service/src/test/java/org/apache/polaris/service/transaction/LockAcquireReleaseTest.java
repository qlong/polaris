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
 * <p>Verifies: acquire → grant, conflicting acquire → wait (timeout), release → grant.
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
    // Use a very short timeout so conflict tests don't block for long
    lockManager = new LockManager(persistence, 200L);
  }

  @Test
  void acquireGrantsLockWhenNoConflict() throws Exception {
    long txnId = persistence.nextTxnId();

    long lockId = lockManager.acquire(txnId, RESOURCE, LockManager.MODE_APPEND);

    assertThat(lockId).isGreaterThan(0);
    List<LockRow> granted = persistence.findGrantedLocksOnResource(RESOURCE, -1L);
    assertThat(granted).hasSize(1);
    assertThat(granted.get(0).txnId()).isEqualTo(txnId);
    assertThat(granted.get(0).lockMode()).isEqualTo(LockManager.MODE_APPEND);
  }

  @Test
  void conflictingAcquireTimesOut() throws Exception {
    long txn1 = persistence.nextTxnId();
    long txn2 = persistence.nextTxnId();

    // txn1 holds ROW_DELTA lock
    lockManager.acquire(txn1, RESOURCE, LockManager.MODE_ROW_DELTA);

    // txn2 tries to acquire conflicting ROW_DELTA — should timeout
    assertThatThrownBy(() -> lockManager.acquire(txn2, RESOURCE, LockManager.MODE_ROW_DELTA))
        .isInstanceOf(LockManager.LockWaitTimeoutException.class);
  }

  @Test
  void appendDoesNotConflictWithAppend() throws Exception {
    long txn1 = persistence.nextTxnId();
    long txn2 = persistence.nextTxnId();

    lockManager.acquire(txn1, RESOURCE, LockManager.MODE_APPEND);

    // APPEND on same resource by another txn — compatible
    long lockId2 = lockManager.acquire(txn2, RESOURCE, LockManager.MODE_APPEND);
    assertThat(lockId2).isGreaterThan(0);
  }

  @Test
  void releaseAllClearsGrantedLocks() throws Exception {
    long txnId = persistence.nextTxnId();
    lockManager.acquire(txnId, RESOURCE, LockManager.MODE_ROW_DELTA);

    lockManager.releaseAll(txnId);

    List<LockRow> remaining = persistence.findGrantedLocksOnResource(RESOURCE, -1L);
    assertThat(remaining).isEmpty();
  }

  @Test
  void lockGrantedAfterConflictingLockIsReleased() throws Exception {
    long txn1 = persistence.nextTxnId();
    long txn2 = persistence.nextTxnId();
    long txn3 = persistence.nextTxnId();

    // txn1 holds ROW_DELTA
    lockManager.acquire(txn1, RESOURCE, LockManager.MODE_ROW_DELTA);

    // Release txn1 lock asynchronously so txn3 can get it
    Thread releaser =
        new Thread(
            () -> {
              try {
                Thread.sleep(50);
                lockManager.releaseAll(txn1);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    releaser.start();

    // txn3 with a long enough timeout — should get the lock after txn1 releases
    LockManager longTimeoutLM = new LockManager(persistence, 5000L);
    long lockId = longTimeoutLM.acquire(txn3, RESOURCE, LockManager.MODE_ROW_DELTA);
    releaser.join();

    assertThat(lockId).isGreaterThan(0);
    List<LockRow> granted = persistence.findGrantedLocksOnResource(RESOURCE, -1L);
    assertThat(granted).hasSize(1);
    assertThat(granted.get(0).txnId()).isEqualTo(txn3);
  }
}
