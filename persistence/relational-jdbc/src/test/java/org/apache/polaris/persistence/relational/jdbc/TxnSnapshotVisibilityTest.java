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
import java.sql.Connection;
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
 * Tests the visibility logic in {@link JdbcTransactionPersistence#resolveTableSnapshot}.
 *
 * <p>The visibility rule is a single commit-order cutoff: a commit is visible under snapshot S iff
 * {@code commit_scn <= S.read_point}. A {@code committed_table_snapshots} row exists only for
 * committed transactions, so no in-flight/aborted exclusion is needed.
 */
class TxnSnapshotVisibilityTest {

  private static final String REALM = "vis-realm";
  private static final long CATALOG_ID = 77L;
  private static final String TABLE_UUID = "vis-table-uuid";

  private JdbcTransactionPersistence persistence;
  private DataSource dataSource;

  @BeforeEach
  void setUp() throws Exception {
    String jdbcUrl =
        "jdbc:h2:mem:vis_test_"
            + UUID.randomUUID()
            + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_UPPER=false";
    dataSource = JdbcConnectionPool.create(jdbcUrl, "sa", "");

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

  /**
   * Inserts a committed_table_snapshots row with an explicit {@code commit_scn}. Uses the
   * connection-scoped primitive (which honors the row's scn) rather than {@code
   * insertCommittedTableSnapshotRow} (which auto-allocates), so the visibility-boundary tests can
   * pin specific scn values.
   */
  private void insertSnapshot(long commitScn, String metadataLocation) throws Exception {
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      persistence.insertCommittedTableSnapshot(
          conn, new CommittedTableSnapshotRow(TABLE_UUID, commitScn, null, metadataLocation));
      conn.commit();
    }
  }

  /** Builds a synthetic TxnSnapshotRow pinned at the given read_point. */
  private TxnSnapshotRow snapshot(long readPoint) {
    Instant now = Instant.now();
    return new TxnSnapshotRow(
        1L, REALM, CATALOG_ID, readPoint, now, now.plus(1, ChronoUnit.HOURS), null);
  }

  @Test
  void visibleCommitReturnedWhenAtOrBelowReadPoint() throws Exception {
    insertSnapshot(100, "s3://bucket/v100.metadata.json");

    Optional<CommittedTableSnapshotRow> result =
        persistence.resolveTableSnapshot(TABLE_UUID, snapshot(200));

    assertThat(result).isPresent();
    assertThat(result.get().metadataLocation()).isEqualTo("s3://bucket/v100.metadata.json");
  }

  @Test
  void commitEqualToReadPointIsVisible() throws Exception {
    // The cutoff is inclusive: a commit whose id equals read_point is visible.
    insertSnapshot(200, "s3://bucket/v200.metadata.json");

    Optional<CommittedTableSnapshotRow> result =
        persistence.resolveTableSnapshot(TABLE_UUID, snapshot(200));

    assertThat(result).isPresent();
    assertThat(result.get().metadataLocation()).isEqualTo("s3://bucket/v200.metadata.json");
  }

  @Test
  void commitAboveReadPointIsNotVisible() throws Exception {
    insertSnapshot(201, "s3://bucket/v201.metadata.json");

    Optional<CommittedTableSnapshotRow> result =
        persistence.resolveTableSnapshot(TABLE_UUID, snapshot(200));

    assertThat(result).isEmpty();
  }

  @Test
  void latestVisibleCommitReturnedWhenMultipleExist() throws Exception {
    insertSnapshot(50, "s3://bucket/v50.metadata.json");
    insertSnapshot(80, "s3://bucket/v80.metadata.json");
    insertSnapshot(150, "s3://bucket/v150.metadata.json"); // above read_point

    Optional<CommittedTableSnapshotRow> result =
        persistence.resolveTableSnapshot(TABLE_UUID, snapshot(100));

    assertThat(result).isPresent();
    assertThat(result.get().metadataLocation()).isEqualTo("s3://bucket/v80.metadata.json");
  }

  @Test
  void noVisibleCommitReturnsEmpty() throws Exception {
    Optional<CommittedTableSnapshotRow> result =
        persistence.resolveTableSnapshot(TABLE_UUID, snapshot(200));

    assertThat(result).isEmpty();
  }
}
