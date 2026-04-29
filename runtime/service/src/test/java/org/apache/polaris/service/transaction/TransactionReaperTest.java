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

import java.io.InputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.LockRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.TransactionRow;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the reaper logic: expired transactions are aborted and their locks
 * released.
 *
 * <p>These tests exercise the same paths that {@link TransactionReaper} calls ( {@link
 * JdbcTransactionPersistence#findExpiredTransactionIdsForRealm}, {@link
 * JdbcTransactionPersistence#updateTransactionStatus}, {@link LockManager#releaseAll}) directly,
 * without CDI wiring.
 */
class TransactionReaperTest {

  private static final String REALM = "reaper-realm";
  private static final long CATALOG_ID = 88L;
  private static final String RESOURCE = "table-uuid-reaper";
  private static final String OWNER = "reaper-principal";

  private JdbcTransactionPersistence persistence;
  private LockManager lockManager;

  @BeforeEach
  void setUp() throws Exception {
    String jdbcUrl =
        "jdbc:h2:mem:reaper_integ_"
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

  /** Inserts a transaction that has already expired (expiresAt in the past). */
  private TransactionRow insertExpiredTransaction() throws Exception {
    long txnId = persistence.nextTxnId();
    Instant now = Instant.now();
    TransactionRow row =
        new TransactionRow(
            txnId,
            REALM,
            CATALOG_ID,
            "token-" + txnId,
            OWNER,
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            "REPEATABLE_READ",
            null,
            null,
            null,
            now,
            now.minus(1, ChronoUnit.MINUTES), // already expired
            null);
    persistence.insertTransaction(row);
    return row;
  }

  @Test
  void expiredTransactionIsFoundByReaper() throws Exception {
    TransactionRow txn = insertExpiredTransaction();

    List<Long> expired = persistence.findExpiredTransactionIdsForRealm();

    assertThat(expired).contains(txn.txnId());
  }

  @Test
  void nonExpiredTransactionIsNotFoundByReaper() throws Exception {
    long txnId = persistence.nextTxnId();
    Instant now = Instant.now();
    TransactionRow row =
        new TransactionRow(
            txnId,
            REALM,
            CATALOG_ID,
            "token-" + txnId,
            OWNER,
            JdbcTransactionPersistence.TXN_STATUS_IN_PROGRESS,
            "REPEATABLE_READ",
            null,
            null,
            null,
            now,
            now.plus(1, ChronoUnit.HOURS), // not yet expired
            null);
    persistence.insertTransaction(row);

    List<Long> expired = persistence.findExpiredTransactionIdsForRealm();

    assertThat(expired).doesNotContain(txnId);
  }

  @Test
  void reaperAbortsExpiredTransactionAndReleasesLocks() throws Exception {
    TransactionRow txn = insertExpiredTransaction();
    // Give the expired transaction a lock
    lockManager.acquire(txn.txnId(), RESOURCE, LockManager.MODE_ROW_DELTA);

    // Simulate what TransactionReaper.reapTransaction does
    List<Long> expired = persistence.findExpiredTransactionIdsForRealm();
    assertThat(expired).contains(txn.txnId());

    for (long id : expired) {
      persistence.updateTransactionStatus(id, JdbcTransactionPersistence.TXN_STATUS_ABORTED);
      lockManager.releaseAll(id);
    }

    // Transaction is now ABORTED
    TransactionRow aborted = persistence.findTransaction(txn.txnId()).orElseThrow();
    assertThat(aborted.status()).isEqualTo(JdbcTransactionPersistence.TXN_STATUS_ABORTED);

    // Lock is released
    List<LockRow> remaining = persistence.findGrantedLocksOnResource(RESOURCE, -1L);
    assertThat(remaining).isEmpty();
  }

  @Test
  void reaperSkipsAlreadyAbortedTransaction() throws Exception {
    TransactionRow txn = insertExpiredTransaction();
    persistence.updateTransactionStatus(txn.txnId(), JdbcTransactionPersistence.TXN_STATUS_ABORTED);

    // Already aborted — reaper should not find it in the expired list again
    List<Long> expired = persistence.findExpiredTransactionIdsForRealm();
    assertThat(expired).doesNotContain(txn.txnId());
  }

  @Test
  void reaperReleasesLocksForExpiredTransactionEnablingNewAcquire() throws Exception {
    TransactionRow expiredTxn = insertExpiredTransaction();
    lockManager.acquire(expiredTxn.txnId(), RESOURCE, LockManager.MODE_ROW_DELTA);

    // Reap the expired transaction
    persistence.updateTransactionStatus(
        expiredTxn.txnId(), JdbcTransactionPersistence.TXN_STATUS_ABORTED);
    lockManager.releaseAll(expiredTxn.txnId());

    // A new transaction should now be able to acquire the same resource
    long newTxnId = persistence.nextTxnId();
    long lockId = lockManager.acquire(newTxnId, RESOURCE, LockManager.MODE_ROW_DELTA);
    assertThat(lockId).isGreaterThan(0);
  }
}
