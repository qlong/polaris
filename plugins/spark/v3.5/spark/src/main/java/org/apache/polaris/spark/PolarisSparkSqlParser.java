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

    // CREATE SUBTRANSACTION FOR <table> [WITH (ROW_EXCLUSIVE|EXCLUSIVE|ACCESS_EXCLUSIVE)]
    // e.g.  CREATE SUBTRANSACTION FOR ns.orders WITH EXCLUSIVE
    if (normalized.startsWith("CREATE SUBTRANSACTION FOR ")) {
      SparkSession spark = SparkSession.active();
      // Use original sql to preserve the table name's natural case (server lookup is case-sensitive).
      String rawSql = sql.trim();
      if (rawSql.endsWith(";")) rawSql = rawSql.substring(0, rawSql.length() - 1).trim();
      String rawUpper = rawSql.toUpperCase(Locale.ROOT);
      int withIdx = rawUpper.indexOf(" WITH ", "CREATE SUBTRANSACTION FOR ".length());
      String operation = "ROW_EXCLUSIVE";
      String tableStr;
      if (withIdx >= 0) {
        tableStr = rawSql.substring("CREATE SUBTRANSACTION FOR ".length(), withIdx).trim();
        String opToken = rawUpper.substring(withIdx + " WITH ".length()).trim();
        if (opToken.equals("EXCLUSIVE")) {
          operation = "EXCLUSIVE";
        } else if (opToken.equals("ACCESS_EXCLUSIVE")) {
          operation = "ACCESS_EXCLUSIVE";
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
              + ", subtxn_token="
              + resp.subtxnToken()
              + ", staging_path="
              + resp.assignedStagingPath();
      return delegate.parsePlan(statusSql(msg));
    }

    // BEGIN SUBTRANSACTION <table> SUBTXN_ID=<n>, SUBTXN_TOKEN='<s>'
    //
    // Minimal worker syntax: only subtxn_id and subtxn_token are required.
    // The server fetches txn_id, txn_snapshot_id, and staging_path automatically.
    // Parameter names are case-insensitive. Whitespace (spaces, tabs, newlines) and commas
    // between parameters are all treated as equivalent separators.
    if (normalized.startsWith("BEGIN SUBTRANSACTION ")) {
      SparkSession spark = SparkSession.active();
      // rawRest preserves quoted string values exactly.
      String rawRest = sql.trim();
      if (rawRest.endsWith(";")) rawRest = rawRest.substring(0, rawRest.length() - 1).trim();

      // normUpper: uppercase + collapse any run of [,\s]+ to a single space so that
      // newlines, tabs, and comma-separated params all look the same to indexOf.
      String normUpper = rawRest.toUpperCase(Locale.ROOT).replaceAll("[,\\s]+", " ").trim();

      // normRest mirrors normUpper but preserves original case (for quoted value extraction).
      String normRest = rawRest.replaceAll("[,\\s]+", " ").trim();

      int after = "BEGIN SUBTRANSACTION ".length();
      // Extract table name — everything between the command prefix and " SUBTXN_ID=".
      int subtxnIdIdx = normUpper.indexOf(" SUBTXN_ID=");
      if (subtxnIdIdx < 0) {
        throw new IllegalArgumentException("BEGIN SUBTRANSACTION: missing SUBTXN_ID=<n>");
      }
      String tableStr = normRest.substring(after, subtxnIdIdx).trim();
      if (tableStr.endsWith(",")) tableStr = tableStr.substring(0, tableStr.length() - 1).trim();
      org.apache.iceberg.catalog.TableIdentifier ident = parseTableIdentifierPolaris(tableStr);

      long subtxnId = parseLongParam(normRest, normUpper, "SUBTXN_ID");
      String subtxnToken = parseStringParam(normRest, normUpper, "SUBTXN_TOKEN");
      String operation = "ROW_EXCLUSIVE";
      int opIdx = normUpper.indexOf(" OPERATION=");
      if (opIdx >= 0) {
        String opToken = normRest.substring(opIdx + " OPERATION=".length()).trim();
        int end = opToken.indexOf(' ');
        opToken = (end >= 0 ? opToken.substring(0, end) : opToken).trim().toUpperCase(Locale.ROOT);
        if (opToken.equals("EXCLUSIVE")) operation = "EXCLUSIVE";
        else if (opToken.equals("ACCESS_EXCLUSIVE")) operation = "ACCESS_EXCLUSIVE";
      }

      txnManager(spark).beginWorkerSubtransaction(spark, subtxnId, subtxnToken, ident, operation);
      return delegate.parsePlan(
          statusSql("Worker subtransaction started, subtxn_id=" + subtxnId));
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
      throw new IllegalArgumentException("BEGIN SUBTRANSACTION: missing " + key + "=<n>");
    }
    String rest = sql.substring(idx + keyEq.length()).trim();
    Matcher m = LEADING_DIGITS.matcher(rest);
    if (!m.find()) {
      throw new IllegalArgumentException(
          "BEGIN SUBTRANSACTION: expected integer after " + key + "=");
    }
    return Long.parseLong(m.group(1));
  }

  /**
   * Extracts a string value from a {@code KEY='value'} or {@code KEY=value} token.
   * The key search is case-insensitive (via {@code upperSql}). Single quotes are optional;
   * an unquoted value ends at the next space.
   */
  private static String parseStringParam(String sql, String upperSql, String key) {
    // Try quoted form first: " KEY='"
    String keyEqQuoted = " " + key + "='";
    int idx = upperSql.indexOf(keyEqQuoted);
    if (idx >= 0) {
      int start = idx + keyEqQuoted.length();
      int end = sql.indexOf('\'', start);
      if (end < 0) {
        throw new IllegalArgumentException(
            "BEGIN SUBTRANSACTION: unterminated quote for " + key);
      }
      return sql.substring(start, end);
    }
    // Fall back to unquoted form: " KEY=value" (value ends at next space)
    String keyEq = " " + key + "=";
    int idx2 = upperSql.indexOf(keyEq);
    if (idx2 < 0) {
      throw new IllegalArgumentException(
          "BEGIN SUBTRANSACTION: missing " + key + "=<value>");
    }
    int start = idx2 + keyEq.length();
    int end = sql.indexOf(' ', start);
    return end < 0 ? sql.substring(start) : sql.substring(start, end);
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
