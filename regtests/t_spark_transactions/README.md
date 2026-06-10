<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Multi-Table Transaction Integration Tests

pytest suite that verifies REPEATABLE READ isolation (UC1) and atomic multi-table
writes via subtransactions (UC2) against a live Polaris server.

## Prerequisites

### 1. Running Polaris server (JDBC persistence)

```bash
cd demos/multi-table-transaction
./start-polaris.sh
```

The server must use `polaris.persistence.type=relational-jdbc` (already set by
`start-polaris.sh`).  The in-memory persistence stack does not support transactions.

### 2. Tables and catalog set up

```bash
cd demos/multi-table-transaction
./setup_spark_tables.sh
```

### 3. Environment variables

```bash
source demos/multi-table-transaction/spark-env.sh
```

`spark-env.sh` sets:

| Variable | Purpose |
|---|---|
| `SPARK_HOME` | Path to a Spark 3.5 distribution (prebuilt or custom-built) |
| `ICEBERG_JAR` | Absolute path to `iceberg-spark-runtime-3.5_*.jar` |
| `POLARIS_PLUGIN_JAR` | Absolute path to `polaris-spark-*-bundle.jar` |
| `POLARIS_HOST` | Polaris server URL (default: `http://localhost:8181`) |
| `POLARIS_CATALOG` | Catalog name (default: `demo_catalog`) |
| `POLARIS_CREDENTIALS` | OAuth credentials (default: `root:s3cr3t`) |

Build the Spark plugin before running if `POLARIS_PLUGIN_JAR` does not exist:

```bash
./gradlew :polaris-spark-3.5_2.13:build -x test
```

## Running the Tests

**Always use the Python that ships with `$SPARK_HOME`** — not a venv pyspark or
system Python.  `SPARK_HOME` may point to a custom-built Spark or a prebuilt
Spark 3.5 distribution; either way, `$SPARK_HOME/python` has the matching PySpark
and py4j versions.

```bash
# 1. Load environment
source demos/multi-table-transaction/spark-env.sh

# 2. Pin Java to the mise-managed JDK (required — system Java may be wrong version)
export JAVA_HOME="$(mise which java | xargs dirname | xargs dirname)"

# 3. Point Python at Spark's bundled PySpark
export PYTHONPATH="${SPARK_HOME}/python:$(ls -1 ${SPARK_HOME}/python/lib/py4j-*-src.zip | tail -1)"

# 4. Run
python -m pytest regtests/t_spark_transactions/src/ -v
```

Or as a one-liner from the repo root:

```bash
source demos/multi-table-transaction/spark-env.sh && \
  JAVA_HOME="$(mise which java | xargs dirname | xargs dirname)" \
  PYTHONPATH="${SPARK_HOME}/python:$(ls -1 ${SPARK_HOME}/python/lib/py4j-*-src.zip | tail -1)" \
  python -m pytest regtests/t_spark_transactions/src/ -v

# Run a single file
  JAVA_HOME="$(mise which java | xargs dirname | xargs dirname)" \
  PYTHONPATH="${SPARK_HOME}/python:$(ls -1 ${SPARK_HOME}/python/lib/py4j-*-src.zip | tail -1)" \
  python -m pytest regtests/t_spark_transactions/src/test_isolation.py -v

# Run a single test
  JAVA_HOME="$(mise which java | xargs dirname | xargs dirname)" \
  PYTHONPATH="${SPARK_HOME}/python:$(ls -1 ${SPARK_HOME}/python/lib/py4j-*-src.zip | tail -1)" \
  python -m pytest regtests/t_spark_transactions/src/test_isolation.py::TestRepeatableReadIsolation::test_basic_repeatable_read -v
```

## Test Structure

```
src/
  conftest.py              pytest fixtures (SparkSession, namespace, tables)
  polaris_txn_spark.py     PolarisTxnSparkSession context-manager helper
  test_isolation.py        REPEATABLE READ and READ COMMITTED isolation tests
  test_atomic_write.py     Atomic multi-table writes via subtransactions
```

## Expected Output (all passing)

```
test_isolation.py::TestRepeatableReadIsolation::test_basic_repeatable_read PASSED
test_isolation.py::TestRepeatableReadIsolation::test_begin_returns_pending_status PASSED
test_isolation.py::TestRepeatableReadIsolation::test_rollback_clears_transaction_state PASSED
test_isolation.py::TestRepeatableReadIsolation::test_explicit_isolation_level_syntax PASSED
test_isolation.py::TestRepeatableReadIsolation::test_rollback_before_first_read PASSED
test_isolation.py::TestReadCommittedIsolation::test_read_committed_sees_concurrent_writes PASSED
test_isolation.py::TestReadCommittedIsolation::test_read_committed_rollback_clears_state PASSED
test_isolation.py::TestConcurrentIsolation::test_two_concurrent_repeatable_read_transactions PASSED
test_atomic_write.py::TestAtomicWrite::test_atomic_two_table_write PASSED
test_atomic_write.py::TestAtomicWrite::test_worker_rollback_does_not_affect_other_table PASSED
test_atomic_write.py::TestAtomicWrite::test_abort_transaction_reverts_all PASSED
test_atomic_write.py::TestAtomicWrite::test_both_workers_rollback_orchestrator_commits PASSED
test_atomic_write.py::TestAtomicWrite::test_one_worker_commits_orchestrator_rollback PASSED
test_atomic_write.py::TestAtomicWrite::test_sequential_workers_atomic_commit PASSED
test_atomic_write.py::TestSubtxnParsing::test_create_subtxn_returns_id_and_token PASSED
```
