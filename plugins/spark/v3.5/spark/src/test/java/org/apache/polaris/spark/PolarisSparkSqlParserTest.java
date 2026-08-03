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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.spark.rest.SubtransactionRESTResponse;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Option;

/**
 * Tests for PolarisSparkSqlParser SQL routing.
 *
 * <p>Uses a test subclass that injects a mocked TransactionManager directly, bypassing the catalog
 * lookup that would require a running SparkSession.
 */
public class PolarisSparkSqlParserTest {

  private ParserInterface mockDelegate;
  private TransactionManager mockTxnManager;
  private SparkSession mockSpark;
  private LogicalPlan mockPlan;
  private PolarisSparkSqlParser parser;

  /** Test subclass that bypasses catalog lookup and returns the injected TransactionManager. */
  private class TestParser extends PolarisSparkSqlParser {
    TestParser(ParserInterface delegate) {
      super(delegate, "polaris");
    }

    @Override
    TransactionManager txnManager(SparkSession spark) {
      return mockTxnManager;
    }
  }

  @BeforeEach
  public void setup() throws Exception {
    mockDelegate = mock(ParserInterface.class);
    mockTxnManager = mock(TransactionManager.class);
    mockSpark = mock(SparkSession.class);
    mockPlan = mock(LogicalPlan.class);

    org.apache.spark.sql.RuntimeConfig mockConf = mock(org.apache.spark.sql.RuntimeConfig.class);
    when(mockSpark.conf()).thenReturn(mockConf);
    when(mockConf.getOption(anyString())).thenReturn(Option.empty());

    // delegate returns mockPlan for any SELECT statement
    when(mockDelegate.parsePlan(anyString())).thenReturn(mockPlan);

    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);
      parser = new TestParser(mockDelegate);
    }
  }

  // -------------------------------------------------------------------------
  // BEGIN
  // -------------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(strings = {"BEGIN", "begin", "BEGIN;", "BEGIN TRANSACTION", "begin transaction"})
  public void testBeginCallsTxnManagerBegin(String sql) throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan(sql);

      verify(mockTxnManager).begin(any(), anyString());
      verify(mockDelegate).parsePlan(anyString()); // returns status SELECT
    }
  }

  @Test
  public void testBeginWithRepeatableReadIsolation() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ");

      verify(mockTxnManager).begin(mockSpark, "REPEATABLE_READ");
    }
  }

  @Test
  public void testBeginWithReadCommittedIsolation() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");

      verify(mockTxnManager).begin(mockSpark, "READ_COMMITTED");
    }
  }

  // -------------------------------------------------------------------------
  // SET TRANSACTION ISOLATION LEVEL
  // -------------------------------------------------------------------------

  @Test
  public void testSetTransactionIsolationCallsSetIsolationLevel() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

      verify(mockTxnManager).setIsolationLevel(mockSpark, "REPEATABLE_READ");
    }
  }

  @Test
  public void testSetTransactionIsolationReadCommitted() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");

      verify(mockTxnManager).setIsolationLevel(mockSpark, "READ_COMMITTED");
    }
  }

  @Test
  public void testSetTransactionUnknownIsolationThrows() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      assertThatThrownBy(() -> parser.parsePlan("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Unknown isolation level");
    }
  }

  // -------------------------------------------------------------------------
  // COMMIT
  // -------------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(strings = {"COMMIT", "commit", "COMMIT;", "COMMIT TRANSACTION"})
  public void testCommitCallsTxnManagerCommit(String sql) throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan(sql);

      verify(mockTxnManager).commit(mockSpark);
    }
  }

  // -------------------------------------------------------------------------
  // ROLLBACK
  // -------------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(strings = {"ROLLBACK", "rollback", "ROLLBACK;", "ROLLBACK TRANSACTION"})
  public void testRollbackCallsTxnManagerRollback(String sql) throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan(sql);

      verify(mockTxnManager).rollback(mockSpark);
    }
  }

  // -------------------------------------------------------------------------
  // Non-transaction SQL — must be delegated unchanged
  // -------------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT count(1) FROM polaris.demo_ns.orders",
        "INSERT INTO orders VALUES (1, 2, 3.0, 'test')",
        "SHOW TABLES",
        "BEGIN DISTRIBUTED TRANSACTION" // not a supported form — should delegate
      })
  public void testNonTransactionSqlDelegatesToDelegate(String sql) throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan(sql);

      verify(mockDelegate).parsePlan(sql);
      verify(mockTxnManager, never()).begin(any(), anyString());
      verify(mockTxnManager, never()).commit(any());
      verify(mockTxnManager, never()).rollback(any());
    }
  }

  // -------------------------------------------------------------------------
  // COMMIT SUBTRANSACTION / ROLLBACK SUBTRANSACTION
  // -------------------------------------------------------------------------

  @Test
  public void testCommitSubtransactionCallsCommitWorker() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("COMMIT SUBTRANSACTION");

      verify(mockTxnManager).commitWorkerSubtransaction(mockSpark);
    }
  }

  @Test
  public void testRollbackSubtransactionCallsRollbackWorker() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("ROLLBACK SUBTRANSACTION");

      verify(mockTxnManager).rollbackWorkerSubtransaction(mockSpark);
    }
  }

  // -------------------------------------------------------------------------
  // CREATE SUBTRANSACTION FOR <table> [WITH operation]
  // -------------------------------------------------------------------------

  @Test
  public void testCreateSubtransactionDefaultAppend() throws Exception {
    SubtransactionRESTResponse mockResp = mock(SubtransactionRESTResponse.class);
    when(mockResp.subtxnId()).thenReturn(10L);
    when(mockResp.subtxnToken()).thenReturn("tok");
    when(mockResp.assignedStagingPath()).thenReturn("s3://bucket/path");
    when(mockTxnManager.createSubtransactionForOrchestrator(any(), anyList(), anyList()))
        .thenReturn(mockResp);

    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("CREATE SUBTRANSACTION FOR ns.orders");

      verify(mockTxnManager)
          .createSubtransactionForOrchestrator(
              eq(mockSpark),
              eq(java.util.List.of(TableIdentifier.of(Namespace.of("ns"), "orders"))),
              eq(java.util.List.of("ROW_EXCLUSIVE")));
    }
  }

  @Test
  public void testCreateSubtransactionWithRowDelta() throws Exception {
    SubtransactionRESTResponse mockResp = mock(SubtransactionRESTResponse.class);
    when(mockResp.subtxnId()).thenReturn(11L);
    when(mockResp.subtxnToken()).thenReturn("tok2");
    when(mockResp.assignedStagingPath()).thenReturn("s3://bucket/path2");
    when(mockTxnManager.createSubtransactionForOrchestrator(any(), anyList(), anyList()))
        .thenReturn(mockResp);

    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("CREATE SUBTRANSACTION FOR ns.orders WITH EXCLUSIVE");

      verify(mockTxnManager)
          .createSubtransactionForOrchestrator(
              eq(mockSpark),
              eq(java.util.List.of(TableIdentifier.of(Namespace.of("ns"), "orders"))),
              eq(java.util.List.of("EXCLUSIVE")));
    }
  }

  @Test
  public void testCreateSubtransactionForMultipleTablesAppliesLockToAll() throws Exception {
    SubtransactionRESTResponse mockResp = mock(SubtransactionRESTResponse.class);
    when(mockResp.subtxnId()).thenReturn(12L);
    when(mockResp.subtxnToken()).thenReturn("tok3");
    when(mockResp.assignedStagingPath()).thenReturn("s3://bucket/path3");
    when(mockTxnManager.createSubtransactionForOrchestrator(any(), anyList(), anyList()))
        .thenReturn(mockResp);

    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      // One WITH <lock> applies to every table in the list.
      parser.parsePlan("CREATE SUBTRANSACTION FOR ns.orders, ns.customers WITH EXCLUSIVE");

      verify(mockTxnManager)
          .createSubtransactionForOrchestrator(
              eq(mockSpark),
              eq(
                  java.util.List.of(
                      TableIdentifier.of(Namespace.of("ns"), "orders"),
                      TableIdentifier.of(Namespace.of("ns"), "customers"))),
              eq(java.util.List.of("EXCLUSIVE", "EXCLUSIVE")));
    }
  }

  @Test
  public void testCreateSubtransactionForMultipleTablesDefaultsRowExclusive() throws Exception {
    SubtransactionRESTResponse mockResp = mock(SubtransactionRESTResponse.class);
    when(mockResp.subtxnId()).thenReturn(13L);
    when(mockResp.subtxnToken()).thenReturn("tok4");
    when(mockResp.assignedStagingPath()).thenReturn("s3://bucket/path4");
    when(mockTxnManager.createSubtransactionForOrchestrator(any(), anyList(), anyList()))
        .thenReturn(mockResp);

    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("CREATE SUBTRANSACTION FOR ns.orders, ns.customers");

      verify(mockTxnManager)
          .createSubtransactionForOrchestrator(
              eq(mockSpark),
              eq(
                  java.util.List.of(
                      TableIdentifier.of(Namespace.of("ns"), "orders"),
                      TableIdentifier.of(Namespace.of("ns"), "customers"))),
              eq(java.util.List.of("ROW_EXCLUSIVE", "ROW_EXCLUSIVE")));
    }
  }

  @Test
  public void testCreateSubtransactionStrayCommaThrows() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      assertThatThrownBy(() -> parser.parsePlan("CREATE SUBTRANSACTION FOR ns.orders,, ns.customers"))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  // -------------------------------------------------------------------------
  // BEGIN SUBTRANSACTION
  // -------------------------------------------------------------------------

  @Test
  public void testBeginSubtransactionParsesMinimalParams() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("BEGIN SUBTRANSACTION ns.orders SUBTXN_ID=100, SUBTXN_TOKEN='secret-tok'");

      // The worker self-hydrates every table it owns from the server, so only subtxn_id and
      // subtxn_token are passed through (the table list is informational).
      verify(mockTxnManager).beginWorkerSubtransaction(eq(mockSpark), eq(100L), eq("secret-tok"));
    }
  }

  @Test
  public void testBeginSubtransactionNewlinesAndMixedCase() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      // newlines between params, lowercase param names
      parser.parsePlan(
          "BEGIN SUBTRANSACTION ns.orders\n"
              + "  subtxn_id=100,\n"
              + "  subtxn_token='secret-tok'");

      verify(mockTxnManager).beginWorkerSubtransaction(eq(mockSpark), eq(100L), eq("secret-tok"));
    }
  }

  @Test
  public void testBeginSubtransactionTabsAndDoubleSpaces() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("BEGIN SUBTRANSACTION ns.orders\t" + "SUBTXN_ID=100  SUBTXN_TOKEN='tok'");

      verify(mockTxnManager).beginWorkerSubtransaction(eq(mockSpark), eq(100L), eq("tok"));
    }
  }

  @Test
  public void testBeginSubtransactionMultipleTablesStillHydratesFromServer() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      // Comma-separated table list before SUBTXN_ID must not confuse param parsing, and the
      // worker still hydrates all tables from the server via subtxn_id/subtxn_token alone.
      parser.parsePlan(
          "BEGIN SUBTRANSACTION ns.orders, ns.customers SUBTXN_ID=100, SUBTXN_TOKEN='tok'");

      verify(mockTxnManager).beginWorkerSubtransaction(eq(mockSpark), eq(100L), eq("tok"));
    }
  }

  @Test
  public void testBeginSubtransactionMissingSubtxnIdThrows() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      assertThatThrownBy(
              () -> parser.parsePlan("BEGIN SUBTRANSACTION ns.orders SUBTXN_TOKEN='tok'"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("SUBTXN_ID");
    }
  }

  // -------------------------------------------------------------------------
  // SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE SAVEPOINT
  // -------------------------------------------------------------------------

  @Test
  public void testSavepointCreateCallsTxnManager() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("SAVEPOINT before_bulk_load");

      verify(mockTxnManager).savepointCreate(mockSpark, "BEFORE_BULK_LOAD");
    }
  }

  @Test
  public void testRollbackToSavepointCallsTxnManager() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("ROLLBACK TO SAVEPOINT before_bulk_load");

      verify(mockTxnManager).savepointRollback(mockSpark, "BEFORE_BULK_LOAD");
    }
  }

  @Test
  public void testReleaseSavepointCallsTxnManager() throws Exception {
    try (org.mockito.MockedStatic<SparkSession> staticMock =
        org.mockito.Mockito.mockStatic(SparkSession.class)) {
      staticMock.when(SparkSession::active).thenReturn(mockSpark);

      parser.parsePlan("RELEASE SAVEPOINT before_bulk_load");

      verify(mockTxnManager).savepointRelease("BEFORE_BULK_LOAD");
    }
  }
}
