#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""pytest fixtures for multi-table transaction integration tests.

Prerequisites:
  1. Polaris running with polaris.persistence.type=relational-jdbc
     (start with demos/multi-table-transaction/start-polaris.sh)
  2. A transaction-enabled catalog already created
     (default: demo_catalog, created by start-polaris.sh)
  3. Environment variables (see below, or source spark-env.sh):
       POLARIS_HOST           default: http://localhost:8181
       POLARIS_CATALOG        default: demo_catalog
       POLARIS_CREDENTIALS    default: root:s3cr3t
       POLARIS_PLUGIN_JAR     required: abs path to polaris-spark-*-bundle.jar
       ICEBERG_JAR            required: abs path to iceberg-spark-runtime-*.jar

Run:
  source demos/multi-table-transaction/spark-env.sh
  pytest regtests/t_spark_transactions/src/ -v
"""

import os
import uuid

import pytest

from polaris_txn_spark import PolarisTxnSparkSession


# ---------------------------------------------------------------------------
# Session-scoped: connection parameters (read once, reused by all tests)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def polaris_catalog_url() -> str:
    host = os.getenv("POLARIS_HOST", "http://localhost:8181")
    return f"{host}/api/catalog"


@pytest.fixture(scope="session")
def credentials() -> str:
    return os.getenv("POLARIS_CREDENTIALS", "root:s3cr3t")


@pytest.fixture(scope="session")
def catalog_name() -> str:
    return os.getenv("POLARIS_CATALOG", "demo_catalog")


@pytest.fixture(scope="session")
def plugin_jar() -> str:
    jar = os.getenv("POLARIS_PLUGIN_JAR", "")
    if not jar:
        pytest.skip("POLARIS_PLUGIN_JAR not set — build the Spark plugin first")
    if not os.path.isfile(jar):
        pytest.skip(f"POLARIS_PLUGIN_JAR not found: {jar}")
    return jar


@pytest.fixture(scope="session")
def iceberg_jar() -> str:
    jar = os.getenv("ICEBERG_JAR", "")
    if not jar:
        pytest.skip("ICEBERG_JAR not set — set to the iceberg-spark-runtime jar path")
    if not os.path.isfile(jar):
        pytest.skip(f"ICEBERG_JAR not found: {jar}")
    return jar


# ---------------------------------------------------------------------------
# Module-scoped: one SparkSession per test module (expensive to create)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def txn_spark(polaris_catalog_url, credentials, catalog_name, plugin_jar, iceberg_jar):
    """PolarisTxnSparkSession for the entire test module."""
    with PolarisTxnSparkSession(
        polaris_catalog_url=polaris_catalog_url,
        credentials=credentials,
        catalog_name=catalog_name,
        plugin_jar=plugin_jar,
        iceberg_jar=iceberg_jar,
    ) as spark:
        yield spark


# ---------------------------------------------------------------------------
# Function-scoped: fresh namespace + tables per test
# ---------------------------------------------------------------------------

@pytest.fixture
def test_ns(txn_spark, catalog_name):
    """Create a unique namespace with orders + customers tables seeded with known data.

    Yields a dict:
        ns           — namespace name (unique per test)
        orders       — table name
        customers    — table name
        init_orders  — initial row count (5)
        init_customers — initial row count (3)

    Teardown always runs, even on test failure.
    """
    ns = f"txn_test_{uuid.uuid4().hex[:8]}"
    cat = catalog_name

    txn_spark.sql(f"CREATE NAMESPACE {cat}.{ns}")

    try:
        # orders table: order_id, customer_id, amount, status
        txn_spark.sql(f"""
            CREATE TABLE {cat}.{ns}.orders (
                order_id INT,
                customer_id INT,
                amount DOUBLE,
                status STRING
            ) USING iceberg
        """)
        txn_spark.sql(f"""
            INSERT INTO {cat}.{ns}.orders VALUES
                (1001, 1, 100.00, 'completed'),
                (1002, 2, 150.00, 'pending'),
                (1003, 1, 200.00, 'completed'),
                (1004, 3,  75.00, 'cancelled'),
                (1005, 2, 300.00, 'pending')
        """)

        # customers table: customer_id, name, email
        txn_spark.sql(f"""
            CREATE TABLE {cat}.{ns}.customers (
                customer_id INT,
                name STRING,
                email STRING
            ) USING iceberg
        """)
        txn_spark.sql(f"""
            INSERT INTO {cat}.{ns}.customers VALUES
                (1, 'Alice', 'alice@example.com'),
                (2, 'Bob',   'bob@example.com'),
                (3, 'Carol', 'carol@example.com')
        """)

        yield {
            "ns": ns,
            "orders": "orders",
            "customers": "customers",
            "init_orders": 5,
            "init_customers": 3,
        }

    finally:
        # Best-effort cleanup; suppress errors so teardown never masks a test failure
        for table in ("orders", "customers"):
            try:
                txn_spark.sql(f"DROP TABLE IF EXISTS {cat}.{ns}.{table}")
            except Exception:
                pass
        try:
            txn_spark.sql(f"DROP NAMESPACE IF EXISTS {cat}.{ns}")
        except Exception:
            pass
