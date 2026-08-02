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

"""Inline single-session multi-table transactions — the intuitive path.

    BEGIN TRANSACTION
      INSERT INTO t1 ...
      INSERT INTO t2 ...
    COMMIT              -- both become visible atomically, or

    ROLLBACK           -- neither becomes visible

No CREATE SUBTRANSACTION / worker ceremony. Every write is staged locally by
PinnedTableOperations and collected inline at COMMIT time
(TransactionManager.collectStagedTables -> PolarisRESTCatalog.commitTransaction
with a non-empty `tables` list -> server prepareInlineCommit / finalizeInlineTables).

Regression guard: the inline COMMIT request carries an `operation` per table that
the server deserializes into the LockMode enum (ROW_EXCLUSIVE / EXCLUSIVE /
ACCESS_EXCLUSIVE). The client must emit one of those values — emitting the raw
Iceberg op ("APPEND" / "ROW_DELTA") makes the server reject the body with
HTTP 400 "Malformed request", which is exactly the bug this file protects against.
"""


def _count(spark, fq):
    return spark.sql(f"SELECT count(*) FROM {fq}").collect()[0][0]


def _refresh(spark, *fqs):
    for fq in fqs:
        spark.sql(f"REFRESH TABLE {fq}")


def test_inline_commit_two_tables(txn_spark, test_ns, catalog_name):
    """BEGIN; INSERT t1; INSERT t2; COMMIT — both tables gain one row atomically."""
    ns = test_ns["ns"]
    cat = catalog_name
    fq_orders = f"{cat}.{ns}.orders"
    fq_customers = f"{cat}.{ns}.customers"
    init_orders = test_ns["init_orders"]
    init_customers = test_ns["init_customers"]

    txn_spark.sql("BEGIN TRANSACTION")
    txn_spark.sql(f"INSERT INTO {fq_orders} VALUES (7001, 1, 10.0, 'inline')")
    txn_spark.sql(
        f"INSERT INTO {fq_customers} VALUES (71, 'Inline', 'inline@example.com')"
    )
    txn_spark.sql("COMMIT")

    _refresh(txn_spark, fq_orders, fq_customers)
    assert _count(txn_spark, fq_orders) == init_orders + 1
    assert _count(txn_spark, fq_customers) == init_customers + 1


def test_inline_rollback_two_tables(txn_spark, test_ns, catalog_name):
    """BEGIN; INSERT t1; INSERT t2; ROLLBACK — neither table changes."""
    ns = test_ns["ns"]
    cat = catalog_name
    fq_orders = f"{cat}.{ns}.orders"
    fq_customers = f"{cat}.{ns}.customers"
    init_orders = test_ns["init_orders"]
    init_customers = test_ns["init_customers"]

    txn_spark.sql("BEGIN TRANSACTION")
    txn_spark.sql(f"INSERT INTO {fq_orders} VALUES (7002, 2, 20.0, 'rolledback')")
    txn_spark.sql(
        f"INSERT INTO {fq_customers} VALUES (72, 'Rolled', 'rolled@example.com')"
    )
    txn_spark.sql("ROLLBACK")

    _refresh(txn_spark, fq_orders, fq_customers)
    assert _count(txn_spark, fq_orders) == init_orders
    assert _count(txn_spark, fq_customers) == init_customers


def test_inline_single_table_commit(txn_spark, test_ns, catalog_name):
    """A single-table write inside an inline transaction still commits normally."""
    ns = test_ns["ns"]
    cat = catalog_name
    fq_orders = f"{cat}.{ns}.orders"
    init_orders = test_ns["init_orders"]

    txn_spark.sql("BEGIN TRANSACTION")
    txn_spark.sql(f"INSERT INTO {fq_orders} VALUES (7003, 3, 30.0, 'single')")
    txn_spark.sql("COMMIT")

    _refresh(txn_spark, fq_orders)
    assert _count(txn_spark, fq_orders) == init_orders + 1
