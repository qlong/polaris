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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.CommittedTableSnapshotRow;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.TxnSnapshotRow;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the MVCC visibility logic in {@link JdbcTransactionPersistence#resolveTableSnapshot}.
 *
 * <p>The visibility rule is: a commit is visible under snapshot S if {@code committed_txn_id <
 * S.xmax AND committed_txn_id NOT IN S.xip_list}. xmin is intentionally NOT used as a lower bound:
 * commits older than any active transaction are definitively committed and must remain visible.
 */
class TxnSnapshotVisibilityTest {

  private static final String REALM = "vis-realm";
  private static final long CATALOG_ID = 77L;
  private static final String TABLE_UUID = "vis-table-uuid";

  private JdbcTransactionPersistence persistence;

  @BeforeEach
  void setUp() throws Exception {
    String jdbcUrl =
        "jdbc:h2:mem:vis_test_"
            + UUID.randomUUID()
            + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_UPPER=false";
    DataSource dataSource = JdbcConnectionPool.create(jdbcUrl, "sa", "");

    DatasourceOperations ops =
        new DatasourceOperations(
            dataSource,
            new RelationalJdbcConfiguration() {
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
            });
    InputStream schema =
        DatasourceOperations.class.getClassLoader().getResourceAsStream("h2/schema-v5.sql");
    ops.executeScript(schema);

    persistence = new JdbcTransactionPersistence(dataSource, DatabaseType.H2, REALM, CATALOG_ID);
  }

  /** Inserts a committed_table_snapshots row at the given txn_id. */
  private void insertSnapshot(long committedTxnId, String metadataLocation) throws Exception {
    persistence.insertCommittedTableSnapshotRow(
        new CommittedTableSnapshotRow(TABLE_UUID, committedTxnId, null, metadataLocation));
  }

  /** Builds a synthetic TxnSnapshotRow for visibility queries. */
  private TxnSnapshotRow snapshot(long xmin, long xmax, String xipList) {
    Instant now = Instant.now();
    return new TxnSnapshotRow(
        1L, REALM, CATALOG_ID, xmin, xmax, xipList, now, now.plus(1, ChronoUnit.HOURS), null);
  }

  @Test
  void visibleCommitReturnedWhenWithinBounds() throws Exception {
    insertSnapshot(100, "s3://bucket/v100.metadata.json");

    Optional<CommittedTableSnapshotRow> result =
        persistence.resolveTableSnapshot(TABLE_UUID, snapshot(50, 200, ""));

    assertThat(result).isPresent();
    assertThat(result.get().metadataLocation()).isEqualTo("s3://bucket/v100.metadata.json");
  }

  @Test
  void commitBeforeXminIsStillVisible() throws Exception {
    // Commits older than xmin are definitively committed before any active transaction started;
    // xmin is NOT a lower visibility bound — they must be visible.
    insertSnapshot(10, "s3://bucket/v10.metadata.json");

    Optional<CommittedTableSnapshotRow> result =
        persistence.resolveTableSnapshot(TABLE_UUID, snapshot(50, 200, ""));

    assertThat(result).isPresent();
    assertThat(result.get().metadataLocation()).isEqualTo("s3://bucket/v10.metadata.json");
  }

  @Test
  void commitAtOrAfterXmaxIsNotVisible() throws Exception {
    insertSnapshot(200, "s3://bucket/v200.metadata.json");

    Optional<CommittedTableSnapshotRow> result =
        persistence.resolveTableSnapshot(TABLE_UUID, snapshot(50, 200, ""));

    assertThat(result).isEmpty();
  }

  @Test
  void commitInXipListIsNotVisible() throws Exception {
    insertSnapshot(100, "s3://bucket/v100.metadata.json");

    // 100 is within [50, 200) but is in the in-progress list
    Optional<CommittedTableSnapshotRow> result =
        persistence.resolveTableSnapshot(TABLE_UUID, snapshot(50, 200, "100"));

    assertThat(result).isEmpty();
  }

  @Test
  void latestVisibleCommitReturnedWhenMultipleExist() throws Exception {
    insertSnapshot(50, "s3://bucket/v50.metadata.json");
    insertSnapshot(80, "s3://bucket/v80.metadata.json");
    insertSnapshot(150, "s3://bucket/v150.metadata.json"); // beyond xmax

    Optional<CommittedTableSnapshotRow> result =
        persistence.resolveTableSnapshot(TABLE_UUID, snapshot(40, 100, ""));

    assertThat(result).isPresent();
    assertThat(result.get().metadataLocation()).isEqualTo("s3://bucket/v80.metadata.json");
  }

  @Test
  void xipListExcludesOnlyTheListedTxnId() throws Exception {
    insertSnapshot(80, "s3://bucket/v80.metadata.json"); // visible
    insertSnapshot(90, "s3://bucket/v90.metadata.json"); // in xip — not visible
    insertSnapshot(95, "s3://bucket/v95.metadata.json"); // visible

    Optional<CommittedTableSnapshotRow> result =
        persistence.resolveTableSnapshot(TABLE_UUID, snapshot(50, 200, "90"));

    // Latest visible (not in xip) is v95
    assertThat(result).isPresent();
    assertThat(result.get().metadataLocation()).isEqualTo("s3://bucket/v95.metadata.json");
  }

  @Test
  void noVisibleCommitReturnsEmpty() throws Exception {
    Optional<CommittedTableSnapshotRow> result =
        persistence.resolveTableSnapshot(TABLE_UUID, snapshot(50, 200, ""));

    assertThat(result).isEmpty();
  }
}
