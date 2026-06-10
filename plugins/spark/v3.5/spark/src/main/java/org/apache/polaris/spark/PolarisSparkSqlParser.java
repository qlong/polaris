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

import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Matcher;
import org.apache.polaris.spark.rest.SubtransactionRESTResponse;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Seq;

/**
 * Spark SQL parser extension that intercepts Polaris transaction commands:
 * BEGIN, COMMIT, ROLLBACK, SET TRANSACTION ISOLATION LEVEL.
 *
 * <p>All other SQL is forwarded to the delegate parser unchanged.
 *
 * <p>Transaction operations are executed as side effects in parsePlan; a descriptive SELECT
 * statement is returned so spark-sql displays feedback to the user.
 */
public class PolarisSparkSqlParser implements ParserInterface {

  private static final java.util.regex.Pattern LEADING_DIGITS =
      java.util.regex.Pattern.compile("^(\\d+)");

  private final ParserInterface delegate;
  private final String catalogName;

  public PolarisSparkSqlParser(ParserInterface delegate, String catalogName) {
    this.delegate = delegate;
    this.catalogName = catalogName;
  }

  TransactionManager txnManager(SparkSession spark) {
    try {
      SparkCatalog catalog =
          (SparkCatalog)
              spark.sessionState().catalogManager().catalog(catalogName);
      return catalog.txnManager;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Polaris catalog '" + catalogName + "' not found or is not a SparkCatalog", e);
    }
  }

  @Override
  public LogicalPlan parsePlan(String sql) throws ParseException {
    String normalized = sql.trim().replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
    // strip trailing semicolons
    if (normalized.endsWith(";")) {
      normalized = normalized.substring(0, normalized.length() - 1).trim();
    }

    // Fast path: non-transaction SQL bypasses catalog lookup entirely.
    // txnManager(spark) is only invoked when a transaction keyword is detected.

    if (normalized.equals("BEGIN") || normalized.equals("BEGIN TRANSACTION")) {
      SparkSession spark = SparkSession.active();
      String isolation = defaultIsolation(spark);
      txnManager(spark).begin(spark, isolation);
      return delegate.parsePlan(statusSql("Transaction pending -- isolation: " + isolation));
    }

    if (normalized.startsWith("BEGIN TRANSACTION ISOLATION LEVEL ")) {
      SparkSession spark = SparkSession.active();
      String isolation = parseIsolation(normalized.substring("BEGIN TRANSACTION ISOLATION LEVEL ".length()).trim());
      txnManager(spark).begin(spark, isolation);
      return delegate.parsePlan(statusSql("Transaction pending -- isolation: " + isolation));
    }

    if (normalized.startsWith("SET TRANSACTION ISOLATION LEVEL ")) {
      SparkSession spark = SparkSession.active();
      String isolation = parseIsolation(normalized.substring("SET TRANSACTION ISOLATION LEVEL ".length()).trim());
      txnManager(spark).setIsolationLevel(spark, isolation);
      return delegate.parsePlan(statusSql("Isolation level set: " + isolation));
    }

    if (normalized.equals("COMMIT") || normalized.equals("COMMIT TRANSACTION")) {
      SparkSession spark = SparkSession.active();
      txnManager(spark).commit(spark);
      return delegate.parsePlan(statusSql("ok"));
    }

    if (normalized.equals("ROLLBACK") || normalized.equals("ROLLBACK TRANSACTION")) {
      SparkSession spark = SparkSession.active();
      txnManager(spark).rollback(spark);
      return delegate.parsePlan(statusSql("ok"));
    }

    // -------------------------------------------------------------------------
    // Subtransaction commands (Use Case 2 — multi-app parallel ingest)
    // -------------------------------------------------------------------------

    // COMMIT SUBTRANSACTION
    if (normalized.equals("COMMIT SUBTRANSACTION")) {
      SparkSession spark = SparkSession.active();
      txnManager(spark).commitWorkerSubtransaction(spark);
      return delegate.parsePlan(statusSql("Subtransaction committed"));
    }

    // ROLLBACK SUBTRANSACTION
    if (normalized.equals("ROLLBACK SUBTRANSACTION")) {
      SparkSession spark = SparkSession.active();
      txnManager(spark).rollbackWorkerSubtransaction(spark);
      return delegate.parsePlan(statusSql("Subtransaction rolled back"));
    }

    // CREATE SUBTRANSACTION FOR <table> [WITH (ROW_DELTA|APPEND)]
    // e.g.  CREATE SUBTRANSACTION FOR ns.orders WITH ROW_DELTA
    if (normalized.startsWith("CREATE SUBTRANSACTION FOR ")) {
      SparkSession spark = SparkSession.active();
      // Use original sql to preserve the table name's natural case (server lookup is case-sensitive).
      String rawSql = sql.trim();
      if (rawSql.endsWith(";")) rawSql = rawSql.substring(0, rawSql.length() - 1).trim();
      String rawUpper = rawSql.toUpperCase(Locale.ROOT);
      int withIdx = rawUpper.indexOf(" WITH ", "CREATE SUBTRANSACTION FOR ".length());
      String operation = "APPEND";
      String tableStr;
      if (withIdx >= 0) {
        tableStr = rawSql.substring("CREATE SUBTRANSACTION FOR ".length(), withIdx).trim();
        String opToken = rawUpper.substring(withIdx + " WITH ".length()).trim();
        if (opToken.equals("ROW_DELTA")) {
          operation = "ROW_DELTA";
        }
      } else {
        tableStr = rawSql.substring("CREATE SUBTRANSACTION FOR ".length()).trim();
      }
      org.apache.iceberg.catalog.TableIdentifier ident = parseTableIdentifierPolaris(tableStr);
      SubtransactionRESTResponse resp =
          txnManager(spark).createSubtransactionForOrchestrator(spark, ident, operation);
      String msg =
          "Subtransaction created  subtxn_id="
              + resp.subtxnId()
              + "  subtxn_token="
              + resp.subtxnToken()
              + "  staging_path="
              + resp.assignedStagingPath();
      return delegate.parsePlan(statusSql(msg));
    }

    // BEGIN WORKER SUBTRANSACTION <table> TXN_ID=<n>, SUBTXN_ID=<n>,
    //   SUBTXN_TOKEN='<s>', SNAPSHOT_ID=<n>, STAGING_PATH='<s>'
    if (normalized.startsWith("BEGIN WORKER SUBTRANSACTION ")) {
      SparkSession spark = SparkSession.active();
      // Use the original sql (not normalized) so quoted string values survive
      String rawRest = sql.trim();
      if (rawRest.endsWith(";")) rawRest = rawRest.substring(0, rawRest.length() - 1).trim();
      String rawUpper = rawRest.toUpperCase(Locale.ROOT);
      int after = "BEGIN WORKER SUBTRANSACTION ".length();
      // Extract table name (ends at first whitespace before TXN_ID=).
      // Search for " TXN_ID=" (space prefix) to avoid matching inside "SUBTXN_ID=".
      int txnIdIdx = rawUpper.indexOf(" TXN_ID=");
      if (txnIdIdx < 0) {
        throw new IllegalArgumentException(
            "BEGIN WORKER SUBTRANSACTION: missing TXN_ID=<n>");
      }
      String tableStr = rawRest.substring(after, txnIdIdx).trim();
      if (tableStr.endsWith(",")) tableStr = tableStr.substring(0, tableStr.length() - 1).trim();
      // Preserve original case — identToKey uses this for the lock guard in PinnedTableOperations,
      // which must match the lowercase key produced from the Spark catalog's loadTable ident.
      org.apache.iceberg.catalog.TableIdentifier ident = parseTableIdentifierPolaris(tableStr);

      long txnId = parseLongParam(rawRest, rawUpper, "TXN_ID");
      long subtxnId = parseLongParam(rawRest, rawUpper, "SUBTXN_ID");
      String subtxnToken = parseStringParam(rawRest, rawUpper, "SUBTXN_TOKEN");
      long snapshotId = parseLongParam(rawRest, rawUpper, "SNAPSHOT_ID");
      String stagingPath = parseStringParam(rawRest, rawUpper, "STAGING_PATH");
      String operation = "APPEND";
      int opIdx = rawUpper.indexOf("OPERATION=");
      if (opIdx >= 0) {
        String opToken = rawRest.substring(opIdx + "OPERATION=".length()).trim();
        if (opToken.startsWith(",")) opToken = opToken.substring(1).trim();
        int end = opToken.indexOf(',');
        opToken = (end >= 0 ? opToken.substring(0, end) : opToken).trim().toUpperCase(Locale.ROOT);
        if (opToken.equals("ROW_DELTA")) operation = "ROW_DELTA";
      }

      txnManager(spark)
          .beginWorkerSubtransaction(
              spark, txnId, snapshotId, subtxnId, subtxnToken, ident, stagingPath, operation);
      return delegate.parsePlan(
          statusSql("Worker subtransaction started  txn_id=" + txnId + "  subtxn_id=" + subtxnId));
    }

    // -------------------------------------------------------------------------
    // SAVEPOINT commands (Use Case 1 — client-side only)
    // -------------------------------------------------------------------------

    // SAVEPOINT <name>
    if (normalized.startsWith("SAVEPOINT ")) {
      SparkSession spark = SparkSession.active();
      String name = normalized.substring("SAVEPOINT ".length()).trim();
      txnManager(spark).savepointCreate(spark, name);
      return delegate.parsePlan(statusSql("Savepoint created: " + name));
    }

    // RELEASE SAVEPOINT <name>
    if (normalized.startsWith("RELEASE SAVEPOINT ")) {
      String name = normalized.substring("RELEASE SAVEPOINT ".length()).trim();
      SparkSession spark = SparkSession.active();
      txnManager(spark).savepointRelease(name);
      return delegate.parsePlan(statusSql("Savepoint released: " + name));
    }

    // ROLLBACK TO SAVEPOINT <name>
    if (normalized.startsWith("ROLLBACK TO SAVEPOINT ")) {
      SparkSession spark = SparkSession.active();
      String name = normalized.substring("ROLLBACK TO SAVEPOINT ".length()).trim();
      txnManager(spark).savepointRollback(spark, name);
      return delegate.parsePlan(statusSql("Rolled back to savepoint: " + name));
    }

    return delegate.parsePlan(sql);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private String defaultIsolation(SparkSession spark) {
    // Check for a session-level override; fall back to REPEATABLE_READ.
    return spark
        .conf()
        .getOption(String.format(TransactionManager.CONF_SESSION_ISOLATION, catalogName))
        .getOrElse(() -> TransactionManager.DEFAULT_ISOLATION);
  }

  private static String parseIsolation(String token) {
    if (token.equals("REPEATABLE READ")) return "REPEATABLE_READ";
    if (token.equals("READ COMMITTED")) return "READ_COMMITTED";
    throw new IllegalArgumentException("Unknown isolation level: " + token
        + ". Use REPEATABLE READ or READ COMMITTED.");
  }

  /**
   * Parses a dot-separated table identifier string (e.g. {@code "NS.ORDERS"} or
   * {@code "NS.SUB.ORDERS"}) into an Iceberg {@link org.apache.iceberg.catalog.TableIdentifier}.
   * The string is expected to already be upper-cased by the caller (via the normalized form).
   */
  private static org.apache.iceberg.catalog.TableIdentifier parseTableIdentifierPolaris(
      String tableStr) {
    String[] parts = tableStr.split("\\.");
    if (parts.length < 2) {
      throw new IllegalArgumentException(
          "Invalid table identifier '" + tableStr + "': expected at least 'namespace.table'");
    }
    String[] nsParts = Arrays.copyOf(parts, parts.length - 1);
    return org.apache.iceberg.catalog.TableIdentifier.of(
        org.apache.iceberg.catalog.Namespace.of(nsParts), parts[parts.length - 1]);
  }

  /**
   * Extracts a long value from a key=value token in the SQL string.
   * Searches case-insensitively using {@code upperSql} for the key position.
   */
  private static long parseLongParam(String sql, String upperSql, String key) {
    // Use " KEY=" (space prefix) to avoid matching "SUBTXN_ID=" when looking for "TXN_ID=".
    String keyEq = " " + key + "=";
    int idx = upperSql.indexOf(keyEq);
    if (idx < 0) {
      throw new IllegalArgumentException("BEGIN WORKER SUBTRANSACTION: missing " + key + "=<n>");
    }
    String rest = sql.substring(idx + keyEq.length()).trim();
    Matcher m = LEADING_DIGITS.matcher(rest);
    if (!m.find()) {
      throw new IllegalArgumentException(
          "BEGIN WORKER SUBTRANSACTION: expected integer after " + key + "=");
    }
    return Long.parseLong(m.group(1));
  }

  /**
   * Extracts a single-quoted string value from a key='value' token.
   * Searches case-insensitively using {@code upperSql} for the key position.
   */
  private static String parseStringParam(String sql, String upperSql, String key) {
    // Use " KEY='" (space prefix) to avoid matching subkey collisions.
    String keyEq = " " + key + "='";
    int idx = upperSql.indexOf(keyEq);
    if (idx < 0) {
      throw new IllegalArgumentException(
          "BEGIN WORKER SUBTRANSACTION: missing " + key + "='<value>'");
    }
    int start = idx + keyEq.length();
    int end = sql.indexOf('\'', start);
    if (end < 0) {
      throw new IllegalArgumentException(
          "BEGIN WORKER SUBTRANSACTION: unterminated quote for " + key);
    }
    return sql.substring(start, end);
  }

  /** Returns a SELECT statement that produces a one-row status result. */
  private static String statusSql(String message) {
    // Escape any single quotes in the message
    String escaped = message.replace("'", "''");
    return "SELECT '" + escaped + "' AS status";
  }

  // -------------------------------------------------------------------------
  // All other ParserInterface methods delegate unconditionally
  // -------------------------------------------------------------------------

  @Override
  public Expression parseExpression(String sqlText) throws ParseException {
    return delegate.parseExpression(sqlText);
  }

  @Override
  public TableIdentifier parseTableIdentifier(String sqlText) throws ParseException {
    return delegate.parseTableIdentifier(sqlText);
  }

  @Override
  public FunctionIdentifier parseFunctionIdentifier(String sqlText) throws ParseException {
    return delegate.parseFunctionIdentifier(sqlText);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Seq<String> parseMultipartIdentifier(String sqlText) throws ParseException {
    // Scala 2.12 declares scala.collection.Seq; Scala 2.13 declares scala.collection.immutable.Seq.
    // Both compile-time and runtime representations are compatible — the delegate returns the
    // correct type for the Scala version the plugin was compiled against.
    return (Seq<String>) (Object) delegate.parseMultipartIdentifier(sqlText);
  }

  @Override
  public StructType parseTableSchema(String sqlText) throws ParseException {
    return delegate.parseTableSchema(sqlText);
  }

  @Override
  public DataType parseDataType(String sqlText) throws ParseException {
    return delegate.parseDataType(sqlText);
  }

  @Override
  public LogicalPlan parseQuery(String sqlText) throws ParseException {
    return delegate.parseQuery(sqlText);
  }
}
