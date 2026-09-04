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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcTransactionPersistence.CommittedTableSnapshotRow;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@code read_floor} time-based retention horizon and the reaper's watermark composition
 * {@code W = MIN(write_floor, read_floor)}, plus the per-table survivor rule of the purge.
 *
 * <p>Standalone read snapshots no longer persist a {@code txn_snapshots} row (they are opaque
 * handles reclaimed purely by lease-TTL GC), so a live reader's protection comes from {@code
 * read_floor}: the highest {@code commit_scn} whose {@code committed_at} is old enough that any
 * handle referencing it must already have exceeded its lease. This test seeds {@code
 * committed_table_snapshots} rows with controlled {@code committed_at} ages (by direct UPDATE,
 * since the insert path stamps {@code now()}) and asserts:
 *
 * <ul>
 *   <li>{@code readFloor(lease, skew)} = {@code MAX(commit_scn)} over rows older than {@code
 *       now-(lease+skew)}, and {@code 0} when no row is old enough;
 *   <li>the reaper composition {@code W = MIN(writeFloor, readFloor)} where {@code writeFloor = -1}
 *       (no live write snapshot) is treated as {@code +infinity};
 *   <li>{@code purgeCommittedSnapshotsBelowWatermark(W)} keeps, for every table, the newest row
 *       with {@code commit_scn <= W} (the per-table survivor) plus everything above {@code W}.
 * </ul>
 */
class ReadFloorWatermarkTest {

  private static final String REALM = "test-realm";
  private static final long CATALOG_ID = 99L;
  private static final long LEASE_SECONDS = 3600L;
  private static final long SKEW_PAD_SECONDS = 30L;

  private DataSource dataSource;
  private JdbcTransactionPersistence persistence;

  @BeforeEach
  void setUp() throws Exception {
    String jdbcUrl =
        "jdbc:h2:mem:readfloor_test_"
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

  // ---------------------------------------------------------------------------
  // read_floor
  // ---------------------------------------------------------------------------

  @Test
  void readFloorIsZeroWhenNoRowIsOldEnough() throws Exception {
    // Two fresh commits (committed_at defaults to now()); neither is older than lease+skew.
    persistence.insertCommittedTableSnapshotRow(row("t1", 100L));
    persistence.insertCommittedTableSnapshotRow(row("t2", 200L));

    assertThat(persistence.readFloor(LEASE_SECONDS, SKEW_PAD_SECONDS))
        .as("no CTS row older than now-(lease+skew) ⟹ read_floor is 0")
        .isZero();
  }

  @Test
  void readFloorIsMaxScnAmongRowsOlderThanLease() throws Exception {
    // Two "old" rows (well past the lease+skew cutoff) and one "fresh" row (within the lease).
    // read_floor must be the MAX(commit_scn) over the OLD rows only — the fresh row's scn is still
    // protected because a handle minted against it may not yet have expired.
    long oldScnA = insertScn(row("t1", 100L));
    long oldScnB = insertScn(row("t2", 200L));
    long freshScn = insertScn(row("t1", 300L));

    // Backdate the two "old" rows past the lease+skew cutoff; leave the fresh row at now().
    ageRow(oldScnA, LEASE_SECONDS + SKEW_PAD_SECONDS + 60);
    ageRow(oldScnB, LEASE_SECONDS + SKEW_PAD_SECONDS + 60);

    assertThat(persistence.readFloor(LEASE_SECONDS, SKEW_PAD_SECONDS))
        .as("read_floor is the highest commit_scn among rows past the lease horizon")
        .isEqualTo(Math.max(oldScnA, oldScnB));
    assertThat(freshScn).isGreaterThan(Math.max(oldScnA, oldScnB));
  }

  // ---------------------------------------------------------------------------
  // watermark composition W = MIN(write_floor, read_floor)
  // ---------------------------------------------------------------------------

  @Test
  void watermarkIsReadFloorWhenNoLiveWriteSnapshot() throws Exception {
    long oldScn = insertScn(row("t1", 100L));
    ageRow(oldScn, LEASE_SECONDS + SKEW_PAD_SECONDS + 60);

    long writeFloor = persistence.minLiveReadPointForCatalog();
    long writeFloorOrInf = writeFloor >= 0 ? writeFloor : Long.MAX_VALUE;
    long readFloor = persistence.readFloor(LEASE_SECONDS, SKEW_PAD_SECONDS);
    long watermark = Math.min(writeFloorOrInf, readFloor);

    assertThat(writeFloor).as("no live write snapshot ⟹ minLiveReadPoint == -1").isEqualTo(-1L);
    assertThat(watermark)
        .as("with no write floor, the watermark is the read_floor")
        .isEqualTo(readFloor)
        .isEqualTo(oldScn);
  }

  // ---------------------------------------------------------------------------
  // purge honoring the per-table survivor rule
  // ---------------------------------------------------------------------------

  @Test
  void purgeKeepsPerTableSurvivorAtOrBelowWatermarkAndEverythingAbove() throws Exception {
    // t1: three commits; t2: one commit. All aged past the lease so read_floor covers them.
    long t1a = insertScn(row("t1", 10L)); // oldest t1 — should be purged
    long t1b = insertScn(row("t1", 20L)); // middle t1 — survivor if <= watermark
    long t2a = insertScn(row("t2", 30L)); // only t2 — survivor, never purged
    long t1c = insertScn(row("t1", 40L)); // newest t1 — above watermark, kept

    for (long scn : new long[] {t1a, t1b, t2a, t1c}) {
      ageRow(scn, LEASE_SECONDS + SKEW_PAD_SECONDS + 60);
    }

    // Choose a watermark strictly between t1b and t1c so t1c is "above" and the t1 survivor is t1b.
    long watermark = t1b;
    persistence.purgeCommittedSnapshotsBelowWatermark(watermark);

    // t1a (oldest, strictly below the t1 survivor t1b) is gone.
    assertThat(scnExists(t1a)).as("t1a is below the per-table survivor and is purged").isFalse();
    // t1b survives as the newest t1 row with commit_scn <= watermark.
    assertThat(scnExists(t1b)).as("t1b is the t1 survivor at the watermark").isTrue();
    // t2a survives: it is t2's only (and therefore newest <= watermark) row.
    assertThat(scnExists(t2a)).as("t2 survivor is kept even though it is old").isTrue();
    // t1c survives: strictly above the watermark.
    assertThat(scnExists(t1c)).as("rows above the watermark are always kept").isTrue();
  }

  @Test
  void purgeIsNoOpForNonPositiveWatermark() throws Exception {
    long scn = insertScn(row("t1", 10L));
    ageRow(scn, LEASE_SECONDS + SKEW_PAD_SECONDS + 60);

    persistence.purgeCommittedSnapshotsBelowWatermark(0L);

    assertThat(scnExists(scn)).as("watermark <= 0 purges nothing").isTrue();
  }

  // ---------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------

  private static CommittedTableSnapshotRow row(String tableUuid, long icebergSnapshotId) {
    // commitScn on the template is ignored by insertCommittedTableSnapshotRow (a fresh scn is
    // allocated inside the insert transaction); metadata_location must be non-null.
    return new CommittedTableSnapshotRow(
        tableUuid,
        JdbcTransactionPersistence.UNASSIGNED_SCN,
        icebergSnapshotId,
        "s3://meta/" + tableUuid);
  }

  /** Inserts a CTS row and returns the {@code commit_scn} the insert allocated. */
  private long insertScn(CommittedTableSnapshotRow template) throws Exception {
    // insertCommittedTableSnapshotRow allocates commit_scn internally and does not return it, so
    // record the counter before/after via resolveTableSnapshot at Long.MAX_VALUE (newest for
    // table).
    persistence.insertCommittedTableSnapshotRow(template);
    return persistence
        .resolveTableSnapshot(template.tableUuid(), Long.MAX_VALUE)
        .orElseThrow()
        .commitScn();
  }

  /**
   * Backdates a row's {@code committed_at} by {@code seconds} so it falls past the lease horizon.
   */
  private void ageRow(long commitScn, long seconds) throws Exception {
    String sql =
        "UPDATE POLARIS_SCHEMA.committed_table_snapshots "
            + "SET committed_at = DATEADD('SECOND', -?, now()) "
            + "WHERE realm_id = ? AND catalog_id = ? AND commit_scn = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, seconds);
      ps.setString(2, REALM);
      ps.setLong(3, CATALOG_ID);
      ps.setLong(4, commitScn);
      ps.executeUpdate();
    }
  }

  private boolean scnExists(long commitScn) throws Exception {
    String sql =
        "SELECT 1 FROM POLARIS_SCHEMA.committed_table_snapshots "
            + "WHERE realm_id = ? AND catalog_id = ? AND commit_scn = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, REALM);
      ps.setLong(2, CATALOG_ID);
      ps.setLong(3, commitScn);
      try (var rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }
}
