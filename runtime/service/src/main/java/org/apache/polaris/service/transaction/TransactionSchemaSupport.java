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

import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.JdbcBasePersistenceImpl;

/**
 * Single decision point for "can this deployment run multi-table transactions?".
 *
 * <p>The answer is <b>not</b> "is a JDBC datasource configured?". The transaction tables ({@code
 * transactions}, {@code subtransactions}, {@code committed_table_snapshots}, {@code
 * commit_scn_counter}, {@code locks}, ...) are introduced by relational schema v5, and a v5 script
 * exists only for the dialects that ship one. A deployment can therefore be fully JDBC-backed and
 * still have none of those tables -- either because its dialect has no v5 script, or because it was
 * bootstrapped at an earlier schema version and not upgraded.
 *
 * <p>Gating on the datasource instead of the schema makes every table commit attempt an INSERT into
 * a non-existent relation, which fails the commit outright. Gating here keeps such deployments on
 * the pre-existing non-transactional path.
 */
public final class TransactionSchemaSupport {

  private TransactionSchemaSupport() {}

  /**
   * Whether the metastore behind {@code callContext} is JDBC-backed <i>and</i> its live schema
   * version includes the transaction tables.
   */
  public static boolean isSupported(CallContext callContext) {
    if (callContext == null) {
      return false;
    }
    return isSupported(callContext.getPolarisCallContext().getMetaStore());
  }

  /**
   * Whether {@code metaStore} is JDBC-backed and its live schema includes the transaction tables.
   */
  public static boolean isSupported(BasePersistence metaStore) {
    return metaStore instanceof JdbcBasePersistenceImpl jdbc && jdbc.supportsTransactions();
  }

  /**
   * Dialect-level variant for callers that run outside a request and therefore have no {@link
   * CallContext} or metastore to consult -- notably the background transaction reaper.
   *
   * <p>This is a coarser test than the metastore-backed overloads: it asks whether a v5 script
   * exists for the dialect at all, not what version the live database is actually on. That is the
   * right trade-off for a best-effort background task -- it keeps the reaper from querying tables
   * that provably cannot exist, while a Postgres deployment pinned to an older schema is left to
   * the per-request gate, which sees the live version.
   */
  public static boolean isSupported(DatabaseType databaseType) {
    return databaseType != null
        && databaseType.getLatestSchemaVersion()
            >= JdbcBasePersistenceImpl.TRANSACTION_SCHEMA_VERSION;
  }
}
