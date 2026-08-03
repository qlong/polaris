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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.spark.rest.GetSubtransactionRESTResponse;
import org.apache.polaris.spark.rest.SubtransactionOperationRESTResponse;
import org.apache.polaris.spark.rest.SubtransactionRESTResponse;
import org.apache.polaris.spark.rest.TransactionRESTResponse;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

public class TransactionManagerTest {

  private PolarisRESTCatalog mockRestCatalog;
  private SparkSession mockSpark;
  private RuntimeConfig mockConf;
  private TransactionManager txnManager;

  /** Simple in-memory conf store backed by a Map. */
  private final Map<String, String> confStore = new HashMap<>();

  private static final String CATALOG = "polaris";

  @BeforeEach
  public void setup() {
    mockRestCatalog = mock(PolarisRESTCatalog.class);
    mockSpark = mock(SparkSession.class);
    mockConf = mock(RuntimeConfig.class);
    when(mockSpark.conf()).thenReturn(mockConf);

    // Wire conf mock to use confStore
    when(mockConf.getOption(anyString()))
        .thenAnswer(
            inv -> {
              String key = inv.getArgument(0);
              String val = confStore.get(key);
              return val != null ? Option.apply(val) : Option.empty();
            });
    org.mockito.Mockito.doAnswer(
            inv -> {
              confStore.put(inv.getArgument(0), inv.getArgument(1));
              return null;
            })
        .when(mockConf)
        .set(anyString(), anyString());
    org.mockito.Mockito.doAnswer(
            inv -> {
              confStore.remove(inv.getArgument(0));
              return null;
            })
        .when(mockConf)
        .unset(anyString());

    txnManager = new TransactionManager(mockRestCatalog, CATALOG);
    confStore.clear();

    // Default response for getSubtransaction — used by beginWorkerSubtransaction.
    // Tests that use different values can override this mock locally.
    GetSubtransactionRESTResponse defaultSubtxnInfo = mock(GetSubtransactionRESTResponse.class);
    when(defaultSubtxnInfo.txnId()).thenReturn(7L);
    when(defaultSubtxnInfo.txnSnapshotId()).thenReturn(99L);
    when(defaultSubtxnInfo.assignedStagingPath()).thenReturn("file:///tmp/txn-42.metadata.json");
    when(defaultSubtxnInfo.tableUuid()).thenReturn("demo_ns.orders");
    when(mockRestCatalog.getSubtransaction(anyLong(), anyString())).thenReturn(defaultSubtxnInfo);
  }

  // -------------------------------------------------------------------------
  // begin()
  // -------------------------------------------------------------------------

  @Test
  public void testBeginSetsPendingIsolation() {
    txnManager.begin(mockSpark, "REPEATABLE_READ");

    String key = String.format(TransactionManager.CONF_PENDING_ISOLATION, CATALOG);
    assertThat(confStore.get(key)).isEqualTo("REPEATABLE_READ");
  }

  @Test
  public void testBeginThrowsIfAlreadyInProgress() {
    txnManager.begin(mockSpark, "REPEATABLE_READ");

    assertThatThrownBy(() -> txnManager.begin(mockSpark, "READ_COMMITTED"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Transaction already in progress");
  }

  @Test
  public void testBeginDoesNotCallRestYet() {
    txnManager.begin(mockSpark, "REPEATABLE_READ");

    verify(mockRestCatalog, never()).beginTransaction(anyString(), anyInt());
  }

  // -------------------------------------------------------------------------
  // setIsolationLevel()
  // -------------------------------------------------------------------------

  @Test
  public void testSetIsolationLevelUpdatesConf() {
    txnManager.begin(mockSpark, "REPEATABLE_READ");
    txnManager.setIsolationLevel(mockSpark, "READ_COMMITTED");

    String key = String.format(TransactionManager.CONF_PENDING_ISOLATION, CATALOG);
    assertThat(confStore.get(key)).isEqualTo("READ_COMMITTED");
  }

  @Test
  public void testSetIsolationLevelThrowsIfNoTransaction() {
    assertThatThrownBy(() -> txnManager.setIsolationLevel(mockSpark, "REPEATABLE_READ"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No transaction in progress");
  }

  @Test
  public void testSetIsolationLevelThrowsIfActive() {
    // Simulate ACTIVE state: both pending and txn_id are set
    confStore.put(
        String.format(TransactionManager.CONF_PENDING_ISOLATION, CATALOG), "REPEATABLE_READ");
    confStore.put(String.format(TransactionManager.CONF_TXN_ID, CATALOG), "42");

    assertThatThrownBy(() -> txnManager.setIsolationLevel(mockSpark, "READ_COMMITTED"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("already ACTIVE");
  }

  // -------------------------------------------------------------------------
  // resolveMetadataLocation() — lazy-BEGIN trigger
  // -------------------------------------------------------------------------

  @Test
  public void testResolveReturnsEmptyWhenNoTransaction() {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("ns"), "orders");
    Optional<String> result = txnManager.resolveMetadataLocation(mockSpark, ident);

    assertThat(result).isEmpty();
    verify(mockRestCatalog, never()).beginTransaction(anyString(), anyInt());
  }

  @Test
  public void testResolveCallsBeginTransactionOnFirstQuery_RepeatableRead() {
    TransactionRESTResponse txnResp = mock(TransactionRESTResponse.class);
    when(txnResp.txnId()).thenReturn(123L);
    when(txnResp.txnToken()).thenReturn("tok");
    when(txnResp.txnSnapshotId()).thenReturn(47L);
    when(mockRestCatalog.beginTransaction(eq("REPEATABLE_READ"), anyInt())).thenReturn(txnResp);
    when(mockRestCatalog.resolveTableMetadataLocation(any(), anyLong()))
        .thenReturn("file:///tmp/metadata/v5.metadata.json");

    txnManager.begin(mockSpark, "REPEATABLE_READ");
    TableIdentifier ident = TableIdentifier.of(Namespace.of("ns"), "orders");
    Optional<String> result = txnManager.resolveMetadataLocation(mockSpark, ident);

    verify(mockRestCatalog)
        .beginTransaction("REPEATABLE_READ", TransactionManager.DEFAULT_LEASE_SECONDS);
    assertThat(result).hasValue("file:///tmp/metadata/v5.metadata.json");
    // txn_id and token should be stored in conf
    assertThat(confStore.get(String.format(TransactionManager.CONF_TXN_ID, CATALOG)))
        .isEqualTo("123");
    assertThat(confStore.get(String.format(TransactionManager.CONF_TXN_TOKEN, CATALOG)))
        .isEqualTo("tok");
    assertThat(confStore.get(String.format(TransactionManager.CONF_TXN_SNAPSHOT_ID, CATALOG)))
        .isEqualTo("47");
  }

  @Test
  public void testResolveReturnsEmptyForReadCommitted() {
    TransactionRESTResponse txnResp = mock(TransactionRESTResponse.class);
    when(txnResp.txnId()).thenReturn(99L);
    when(txnResp.txnToken()).thenReturn("tok");
    when(txnResp.txnSnapshotId()).thenReturn(null); // READ_COMMITTED has no snapshot
    when(mockRestCatalog.beginTransaction(eq("READ_COMMITTED"), anyInt())).thenReturn(txnResp);

    txnManager.begin(mockSpark, "READ_COMMITTED");
    TableIdentifier ident = TableIdentifier.of(Namespace.of("ns"), "orders");
    Optional<String> result = txnManager.resolveMetadataLocation(mockSpark, ident);

    assertThat(result).isEmpty();
    verify(mockRestCatalog, never()).resolveTableMetadataLocation(any(), anyLong());
  }

  @Test
  public void testResolveDoesNotCallBeginAgainOnSecondQuery() {
    TransactionRESTResponse txnResp = mock(TransactionRESTResponse.class);
    when(txnResp.txnId()).thenReturn(123L);
    when(txnResp.txnToken()).thenReturn("tok");
    when(txnResp.txnSnapshotId()).thenReturn(47L);
    when(mockRestCatalog.beginTransaction(anyString(), anyInt())).thenReturn(txnResp);
    when(mockRestCatalog.resolveTableMetadataLocation(any(), anyLong()))
        .thenReturn("file:///tmp/v5.metadata.json");

    txnManager.begin(mockSpark, "REPEATABLE_READ");
    TableIdentifier ident = TableIdentifier.of(Namespace.of("ns"), "orders");
    txnManager.resolveMetadataLocation(mockSpark, ident); // first query — triggers begin
    txnManager.resolveMetadataLocation(mockSpark, ident); // second query — should NOT re-begin

    verify(mockRestCatalog, org.mockito.Mockito.times(1)).beginTransaction(anyString(), anyInt());
  }

  // -------------------------------------------------------------------------
  // commit()
  // -------------------------------------------------------------------------

  @Test
  public void testCommitCallsRestWhenActive() {
    confStore.put(
        String.format(TransactionManager.CONF_PENDING_ISOLATION, CATALOG), "REPEATABLE_READ");
    confStore.put(String.format(TransactionManager.CONF_TXN_ID, CATALOG), "42");
    confStore.put(String.format(TransactionManager.CONF_TXN_TOKEN, CATALOG), "secret");
    confStore.put(String.format(TransactionManager.CONF_TXN_SNAPSHOT_ID, CATALOG), "7");

    txnManager.commit(mockSpark);

    verify(mockRestCatalog).commitTransaction(eq(42L), eq("secret"), anyList());
    // conf should be cleared
    assertThat(confStore.get(String.format(TransactionManager.CONF_PENDING_ISOLATION, CATALOG)))
        .isNull();
    assertThat(confStore.get(String.format(TransactionManager.CONF_TXN_ID, CATALOG))).isNull();
  }

  @Test
  public void testCommitNoOpWhenPending() {
    txnManager.begin(mockSpark, "REPEATABLE_READ"); // PENDING state

    txnManager.commit(mockSpark);

    verify(mockRestCatalog, never()).commitTransaction(anyLong(), anyString(), anyList());
    assertThat(confStore.get(String.format(TransactionManager.CONF_PENDING_ISOLATION, CATALOG)))
        .isNull();
  }

  @Test
  public void testCommitNoOpWhenNoTransaction() {
    txnManager.commit(mockSpark); // nothing to commit

    verify(mockRestCatalog, never()).commitTransaction(anyLong(), anyString(), anyList());
  }

  // -------------------------------------------------------------------------
  // rollback()
  // -------------------------------------------------------------------------

  @Test
  public void testRollbackCallsAbortWhenActive() {
    confStore.put(
        String.format(TransactionManager.CONF_PENDING_ISOLATION, CATALOG), "REPEATABLE_READ");
    confStore.put(String.format(TransactionManager.CONF_TXN_ID, CATALOG), "55");
    confStore.put(String.format(TransactionManager.CONF_TXN_TOKEN, CATALOG), "tok55");

    txnManager.rollback(mockSpark);

    verify(mockRestCatalog).abortTransaction(55L, "tok55");
    assertThat(confStore.get(String.format(TransactionManager.CONF_PENDING_ISOLATION, CATALOG)))
        .isNull();
  }

  // -------------------------------------------------------------------------
  // createSubtransactionForOrchestrator() — lazy-BEGIN trigger
  // -------------------------------------------------------------------------

  @Test
  public void testCreateSubtransactionTriggersBeginWhenPending() {
    TransactionRESTResponse txnResp = mock(TransactionRESTResponse.class);
    when(txnResp.txnId()).thenReturn(10L);
    when(txnResp.txnToken()).thenReturn("tok10");
    when(txnResp.txnSnapshotId()).thenReturn(50L);
    when(mockRestCatalog.beginTransaction(anyString(), anyInt())).thenReturn(txnResp);
    SubtransactionRESTResponse subtxnResp = mock(SubtransactionRESTResponse.class);
    when(mockRestCatalog.createSubtransaction(
            anyLong(), anyString(), anyString(), anyString(), anyInt()))
        .thenReturn(subtxnResp);

    txnManager.begin(mockSpark, "REPEATABLE_READ"); // PENDING — no REST call yet
    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    txnManager.createSubtransactionForOrchestrator(mockSpark, orders, "ROW_EXCLUSIVE");

    // beginTransaction must have fired on the first CREATE SUBTRANSACTION FOR
    verify(mockRestCatalog)
        .beginTransaction("REPEATABLE_READ", TransactionManager.DEFAULT_LEASE_SECONDS);
    verify(mockRestCatalog)
        .createSubtransaction(
            10L,
            "tok10",
            "demo_ns.orders",
            "ROW_EXCLUSIVE",
            TransactionManager.DEFAULT_LEASE_SECONDS);
    assertThat(confStore.get(String.format(TransactionManager.CONF_TXN_ID, CATALOG)))
        .isEqualTo("10");
    assertThat(confStore.get(String.format(TransactionManager.CONF_TXN_TOKEN, CATALOG)))
        .isEqualTo("tok10");
    assertThat(confStore.get(String.format(TransactionManager.CONF_TXN_SNAPSHOT_ID, CATALOG)))
        .isEqualTo("50");
  }

  @Test
  public void testCreateSubtransactionDoesNotRebegindWhenAlreadyActive() {
    // Pre-set ACTIVE state (txn_id already present)
    confStore.put(
        String.format(TransactionManager.CONF_PENDING_ISOLATION, CATALOG), "REPEATABLE_READ");
    confStore.put(String.format(TransactionManager.CONF_TXN_ID, CATALOG), "99");
    confStore.put(String.format(TransactionManager.CONF_TXN_TOKEN, CATALOG), "tok99");
    SubtransactionRESTResponse subtxnResp = mock(SubtransactionRESTResponse.class);
    when(mockRestCatalog.createSubtransaction(
            anyLong(), anyString(), anyString(), anyString(), anyInt()))
        .thenReturn(subtxnResp);

    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    txnManager.createSubtransactionForOrchestrator(mockSpark, orders, "ROW_EXCLUSIVE");

    verify(mockRestCatalog, never()).beginTransaction(anyString(), anyInt());
    verify(mockRestCatalog)
        .createSubtransaction(
            99L,
            "tok99",
            "demo_ns.orders",
            "ROW_EXCLUSIVE",
            TransactionManager.DEFAULT_LEASE_SECONDS);
  }

  @Test
  public void testCreateSubtransactionThrowsWhenNoTransaction() {
    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");

    assertThatThrownBy(
            () ->
                txnManager.createSubtransactionForOrchestrator(mockSpark, orders, "ROW_EXCLUSIVE"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No active transaction");
  }

  // -------------------------------------------------------------------------
  // Worker mode — beginWorkerSubtransaction / commit / rollback
  // -------------------------------------------------------------------------

  @Test
  public void testBeginWorkerSubtransactionSetsConfWithoutRyow() {
    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    txnManager.beginWorkerSubtransaction(mockSpark, 42L, "subtok", orders, "ROW_EXCLUSIVE");

    // Worker conf keys must be set
    assertThat(confStore.get(String.format(TransactionManager.CONF_TXN_ID, CATALOG)))
        .isEqualTo("7");
    assertThat(confStore.get(String.format(TransactionManager.CONF_TXN_SNAPSHOT_ID, CATALOG)))
        .isEqualTo("99");
    assertThat(confStore.get(String.format(TransactionManager.CONF_WORKER_SUBTXN_ID, CATALOG)))
        .isEqualTo("42");
    assertThat(confStore.get(String.format(TransactionManager.CONF_WORKER_SUBTXN_TOKEN, CATALOG)))
        .isEqualTo("subtok");
    assertThat(confStore.get(String.format(TransactionManager.CONF_WORKER_TABLES, CATALOG)))
        .isEqualTo("demo_ns.orders");
    assertThat(txnManager.getWorkerStagingPath(mockSpark, orders))
        .hasValue("file:///tmp/txn-42.metadata.json");

    // CRITICAL: staged-path conf key must NOT be set — if it were, resolveMetadataLocation would
    // return it as a RYOW path before the file exists, causing loadTable to fail.
    Optional<String> ryow = txnManager.getStagedPath(mockSpark, orders);
    assertThat(ryow).isEmpty();
  }

  @Test
  public void testBeginWorkerSubtransactionThrowsIfTransactionAlreadyActive() {
    txnManager.begin(mockSpark, "REPEATABLE_READ");
    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");

    assertThatThrownBy(
            () ->
                txnManager.beginWorkerSubtransaction(
                    mockSpark, 42L, "subtok", orders, "ROW_EXCLUSIVE"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Transaction already in progress");
  }

  @Test
  public void testWorkerStagingPathReturnedByGetter() {
    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    txnManager.beginWorkerSubtransaction(mockSpark, 42L, "subtok", orders, "ROW_EXCLUSIVE");

    Optional<String> path = txnManager.getWorkerStagingPath(mockSpark, orders);
    assertThat(path).hasValue("file:///tmp/txn-42.metadata.json");
  }

  @Test
  public void testIsWorkerMode() {
    assertThat(txnManager.isWorkerMode(mockSpark)).isFalse();

    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    txnManager.beginWorkerSubtransaction(mockSpark, 42L, "subtok", orders, "ROW_EXCLUSIVE");

    assertThat(txnManager.isWorkerMode(mockSpark)).isTrue();
  }

  @Test
  public void testCommitWorkerSubtransactionCallsRestAndClearsConf() {
    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    txnManager.beginWorkerSubtransaction(mockSpark, 42L, "subtok", orders, "ROW_EXCLUSIVE");
    // Simulate PinnedTableOperations.commit() having written the staged path
    txnManager.stageWrite(mockSpark, orders, "file:///tmp/txn-42.metadata.json", "ROW_EXCLUSIVE");

    txnManager.commitWorkerSubtransaction(mockSpark);

    verify(mockRestCatalog).commitSubtransaction(7L, 42L, "subtok");
    // All conf cleared after commit
    assertThat(confStore.get(String.format(TransactionManager.CONF_WORKER_SUBTXN_ID, CATALOG)))
        .isNull();
    assertThat(confStore.get(String.format(TransactionManager.CONF_WORKER_TABLES, CATALOG)))
        .isNull();
    assertThat(txnManager.getWorkerStagingPath(mockSpark, orders)).isEmpty();
    assertThat(confStore.get(String.format(TransactionManager.CONF_PENDING_ISOLATION, CATALOG)))
        .isNull();
  }

  @Test
  public void testRollbackWorkerSubtransactionReopensForRetryAndClearsConf() {
    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    txnManager.beginWorkerSubtransaction(mockSpark, 42L, "subtok", orders, "ROW_EXCLUSIVE");

    txnManager.rollbackWorkerSubtransaction(mockSpark);

    // rollback reopens the unit (reopen_for_retry=true) and does NOT commit it
    verify(mockRestCatalog).rollbackSubtransaction(7L, 42L, "subtok", null, true);
    verify(mockRestCatalog, never()).commitSubtransaction(anyLong(), anyLong(), anyString());
    assertThat(confStore.get(String.format(TransactionManager.CONF_WORKER_SUBTXN_ID, CATALOG)))
        .isNull();
  }

  @Test
  public void testAbortWorkerSubtransactionAbandonsAndClearsConf() {
    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    txnManager.beginWorkerSubtransaction(mockSpark, 42L, "subtok", orders, "ROW_EXCLUSIVE");

    txnManager.abortWorkerSubtransaction(mockSpark);

    // abort abandons permanently (no reopen) via the /abort endpoint
    verify(mockRestCatalog).abortSubtransaction(7L, 42L, "subtok");
    verify(mockRestCatalog, never())
        .rollbackSubtransaction(anyLong(), anyLong(), anyString(), any(), anyBoolean());
    assertThat(confStore.get(String.format(TransactionManager.CONF_WORKER_SUBTXN_ID, CATALOG)))
        .isNull();
  }

  @Test
  public void testCommitWorkerThrowsIfNotInWorkerMode() {
    assertThatThrownBy(() -> txnManager.commitWorkerSubtransaction(mockSpark))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Not in worker mode");
  }

  @Test
  public void testOrchestratorCommitThrowsInWorkerMode() {
    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    txnManager.beginWorkerSubtransaction(mockSpark, 42L, "subtok", orders, "ROW_EXCLUSIVE");

    assertThatThrownBy(() -> txnManager.commit(mockSpark))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("use COMMIT SUBTRANSACTION");
  }

  // -------------------------------------------------------------------------
  // Multi-table subtransactions
  // -------------------------------------------------------------------------

  @Test
  public void testCreateSubtransactionForMultipleTablesSendsTablesList() {
    // ACTIVE state so no lazy begin fires.
    confStore.put(
        String.format(TransactionManager.CONF_PENDING_ISOLATION, CATALOG), "REPEATABLE_READ");
    confStore.put(String.format(TransactionManager.CONF_TXN_ID, CATALOG), "77");
    confStore.put(String.format(TransactionManager.CONF_TXN_TOKEN, CATALOG), "tok77");
    SubtransactionRESTResponse subtxnResp = mock(SubtransactionRESTResponse.class);
    when(mockRestCatalog.createSubtransaction(anyLong(), anyString(), anyList(), anyInt()))
        .thenReturn(subtxnResp);

    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    TableIdentifier customers = TableIdentifier.of(Namespace.of("demo_ns"), "customers");
    txnManager.createSubtransactionForOrchestrator(
        mockSpark,
        java.util.List.of(orders, customers),
        java.util.List.of("EXCLUSIVE", "EXCLUSIVE"));

    // The multi-table overload (tables[]) must be used, NOT the scalar shorthand.
    org.mockito.ArgumentCaptor<
            java.util.List<org.apache.polaris.spark.rest.SubtransactionTableRESTRequest>>
        captor = org.mockito.ArgumentCaptor.forClass(java.util.List.class);
    verify(mockRestCatalog)
        .createSubtransaction(
            eq(77L), eq("tok77"), captor.capture(), eq(TransactionManager.DEFAULT_LEASE_SECONDS));
    verify(mockRestCatalog, never())
        .createSubtransaction(anyLong(), anyString(), anyString(), anyString(), anyInt());
    java.util.List<org.apache.polaris.spark.rest.SubtransactionTableRESTRequest> sent =
        captor.getValue();
    assertThat(sent).hasSize(2);
    assertThat(sent.get(0).getTableIdentifier()).isEqualTo("demo_ns.orders");
    assertThat(sent.get(0).getOperation()).isEqualTo("EXCLUSIVE");
    assertThat(sent.get(1).getTableIdentifier()).isEqualTo("demo_ns.customers");
    assertThat(sent.get(1).getOperation()).isEqualTo("EXCLUSIVE");
  }

  @Test
  public void testCreateSubtransactionForSingleTableInListUsesScalarShorthand() {
    confStore.put(
        String.format(TransactionManager.CONF_PENDING_ISOLATION, CATALOG), "REPEATABLE_READ");
    confStore.put(String.format(TransactionManager.CONF_TXN_ID, CATALOG), "88");
    confStore.put(String.format(TransactionManager.CONF_TXN_TOKEN, CATALOG), "tok88");
    SubtransactionRESTResponse subtxnResp = mock(SubtransactionRESTResponse.class);
    when(mockRestCatalog.createSubtransaction(
            anyLong(), anyString(), anyString(), anyString(), anyInt()))
        .thenReturn(subtxnResp);

    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    txnManager.createSubtransactionForOrchestrator(
        mockSpark, java.util.List.of(orders), java.util.List.of("ROW_EXCLUSIVE"));

    // A single-table list collapses to the scalar shorthand (backward compatible).
    verify(mockRestCatalog)
        .createSubtransaction(
            88L,
            "tok88",
            "demo_ns.orders",
            "ROW_EXCLUSIVE",
            TransactionManager.DEFAULT_LEASE_SECONDS);
    verify(mockRestCatalog, never())
        .createSubtransaction(anyLong(), anyString(), anyList(), anyInt());
  }

  @Test
  public void testCreateSubtransactionRejectsMismatchedTablesAndOperations() {
    confStore.put(
        String.format(TransactionManager.CONF_PENDING_ISOLATION, CATALOG), "REPEATABLE_READ");
    confStore.put(String.format(TransactionManager.CONF_TXN_ID, CATALOG), "1");
    confStore.put(String.format(TransactionManager.CONF_TXN_TOKEN, CATALOG), "t");

    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    TableIdentifier customers = TableIdentifier.of(Namespace.of("demo_ns"), "customers");

    assertThatThrownBy(
            () ->
                txnManager.createSubtransactionForOrchestrator(
                    mockSpark,
                    java.util.List.of(orders, customers),
                    java.util.List.of("ROW_EXCLUSIVE")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testWorkerSelfHydratesAllTablesFromOperations() {
    // Server returns a 2-table subtransaction via operations[].
    GetSubtransactionRESTResponse info = mock(GetSubtransactionRESTResponse.class);
    when(info.txnId()).thenReturn(7L);
    when(info.txnSnapshotId()).thenReturn(99L);
    SubtransactionOperationRESTResponse op1 = mock(SubtransactionOperationRESTResponse.class);
    when(op1.tableUuid()).thenReturn("demo_ns.orders");
    when(op1.assignedStagingPath()).thenReturn("file:///tmp/orders-42.metadata.json");
    SubtransactionOperationRESTResponse op2 = mock(SubtransactionOperationRESTResponse.class);
    when(op2.tableUuid()).thenReturn("demo_ns.customers");
    when(op2.assignedStagingPath()).thenReturn("file:///tmp/customers-42.metadata.json");
    when(info.operations()).thenReturn(java.util.List.of(op1, op2));
    when(mockRestCatalog.getSubtransaction(42L, "subtok")).thenReturn(info);

    txnManager.beginWorkerSubtransaction(mockSpark, 42L, "subtok");

    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    TableIdentifier customers = TableIdentifier.of(Namespace.of("demo_ns"), "customers");
    assertThat(txnManager.getWorkerTables(mockSpark))
        .containsExactly("demo_ns.orders", "demo_ns.customers");
    assertThat(txnManager.isWorkerTable(mockSpark, orders)).isTrue();
    assertThat(txnManager.isWorkerTable(mockSpark, customers)).isTrue();
    assertThat(txnManager.getWorkerStagingPath(mockSpark, orders))
        .hasValue("file:///tmp/orders-42.metadata.json");
    assertThat(txnManager.getWorkerStagingPath(mockSpark, customers))
        .hasValue("file:///tmp/customers-42.metadata.json");

    // A table NOT in the subtransaction is not a worker table.
    TableIdentifier other = TableIdentifier.of(Namespace.of("demo_ns"), "products");
    assertThat(txnManager.isWorkerTable(mockSpark, other)).isFalse();
    assertThat(txnManager.getWorkerStagingPath(mockSpark, other)).isEmpty();
  }

  @Test
  public void testCommitMultiTableWorkerRequiresAtLeastOneStaged() {
    GetSubtransactionRESTResponse info = mock(GetSubtransactionRESTResponse.class);
    when(info.txnId()).thenReturn(7L);
    when(info.txnSnapshotId()).thenReturn(99L);
    SubtransactionOperationRESTResponse op1 = mock(SubtransactionOperationRESTResponse.class);
    when(op1.tableUuid()).thenReturn("demo_ns.orders");
    when(op1.assignedStagingPath()).thenReturn("file:///tmp/orders-42.metadata.json");
    SubtransactionOperationRESTResponse op2 = mock(SubtransactionOperationRESTResponse.class);
    when(op2.tableUuid()).thenReturn("demo_ns.customers");
    when(op2.assignedStagingPath()).thenReturn("file:///tmp/customers-42.metadata.json");
    when(info.operations()).thenReturn(java.util.List.of(op1, op2));
    when(mockRestCatalog.getSubtransaction(42L, "subtok")).thenReturn(info);

    txnManager.beginWorkerSubtransaction(mockSpark, 42L, "subtok");

    // Nothing staged yet -> commit must refuse.
    assertThatThrownBy(() -> txnManager.commitWorkerSubtransaction(mockSpark))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No staged data");

    // Staging just ONE of the two tables is enough for the body-less commit to proceed.
    TableIdentifier orders = TableIdentifier.of(Namespace.of("demo_ns"), "orders");
    txnManager.stageWrite(
        mockSpark, orders, "file:///tmp/orders-42.metadata.json", "ROW_EXCLUSIVE");
    txnManager.commitWorkerSubtransaction(mockSpark);

    verify(mockRestCatalog).commitSubtransaction(7L, 42L, "subtok");
  }

  // -------------------------------------------------------------------------
  // Helper
  // -------------------------------------------------------------------------

  private static <T> T any() {
    return org.mockito.ArgumentMatchers.any();
  }
}
