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

"""Single-session savepoints — Use Case 1 (Postgres-style partial rollback).

Savepoints operate entirely on the inline single-session staging state (the same
per-session staged-table set that inline COMMIT collects). No worker ceremony,
no server round-trip: a savepoint captures which tables are staged at a point in
time, and ROLLBACK TO SAVEPOINT restores that set — discarding any staging done
afterward. The final COMMIT then applies only the restored set.

    BEGIN TRANSACTION
      INSERT INTO orders ...          -- staged
      SAVEPOINT s1
      INSERT INTO customers ...       -- staged after s1
      ROLLBACK TO SAVEPOINT s1        -- customers staging discarded
    COMMIT                            -- only orders lands

This mirrors PostgreSQL SAVEPOINT / ROLLBACK TO SAVEPOINT semantics for a single
connection. Requires a running Polaris with relational-jdbc persistence.
"""


def count(spark, fq: str) -> int:
    return spark.sql(f"SELECT count(*) FROM {fq}").collect()[0][0]


def count_where(spark, fq: str, predicate: str) -> int:
    return spark.sql(f"SELECT count(*) FROM {fq} WHERE {predicate}").collect()[0][0]


def refresh(spark, *fqs) -> None:
    for fq in fqs:
        spark.sql(f"REFRESH TABLE {fq}")


class TestSavepoint:
    def test_rollback_to_savepoint_discards_later_write(
        self, txn_spark, test_ns, catalog_name
    ):
        """Write staged before the savepoint survives; write staged after it is
        discarded by ROLLBACK TO SAVEPOINT and never committed."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")
        txn_spark.sql(f"INSERT INTO {fq_orders} VALUES (8001, 8, 80.00, 'kept')")
        txn_spark.sql("SAVEPOINT s1")
        txn_spark.sql(
            f"INSERT INTO {fq_customers} VALUES (81, 'Dropped', 'drop@example.com')"
        )
        txn_spark.sql("ROLLBACK TO SAVEPOINT s1")
        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders, fq_customers)
        # orders was staged before the savepoint -> committed
        assert count(txn_spark, fq_orders) == init_orders + 1
        assert count_where(txn_spark, fq_orders, "status = 'kept'") == 1
        # customers was staged after the savepoint -> discarded
        assert count(txn_spark, fq_customers) == init_customers
        assert count_where(txn_spark, fq_customers, "customer_id = 81") == 0

    def test_commit_without_rollback_keeps_all_writes(
        self, txn_spark, test_ns, catalog_name
    ):
        """A savepoint that is never rolled back to has no effect: every staged
        write across the savepoint boundary is committed."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")
        txn_spark.sql(f"INSERT INTO {fq_orders} VALUES (8002, 8, 80.00, 'before')")
        txn_spark.sql("SAVEPOINT s1")
        txn_spark.sql(
            f"INSERT INTO {fq_customers} VALUES (82, 'After', 'after@example.com')"
        )
        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders, fq_customers)
        assert count(txn_spark, fq_orders) == init_orders + 1
        assert count(txn_spark, fq_customers) == init_customers + 1

    def test_release_savepoint_then_commit(self, txn_spark, test_ns, catalog_name):
        """RELEASE SAVEPOINT discards the checkpoint but keeps all staged writes;
        the subsequent COMMIT still lands everything."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        init_orders = test_ns["init_orders"]

        txn_spark.sql("BEGIN TRANSACTION")
        txn_spark.sql(f"INSERT INTO {fq_orders} VALUES (8003, 8, 80.00, 'r1')")
        txn_spark.sql("SAVEPOINT s1")
        txn_spark.sql(f"INSERT INTO {fq_orders} VALUES (8004, 8, 80.00, 'r2')")
        txn_spark.sql("RELEASE SAVEPOINT s1")
        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        assert count(txn_spark, fq_orders) == init_orders + 2

    def test_rollback_to_savepoint_before_any_write(
        self, txn_spark, test_ns, catalog_name
    ):
        """A savepoint taken before any staged write captures an empty set;
        rolling back to it discards everything, so COMMIT is a no-op."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        init_orders = test_ns["init_orders"]

        txn_spark.sql("BEGIN TRANSACTION")
        txn_spark.sql("SAVEPOINT s0")
        txn_spark.sql(f"INSERT INTO {fq_orders} VALUES (8005, 8, 80.00, 'gone')")
        txn_spark.sql("ROLLBACK TO SAVEPOINT s0")
        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        assert count(txn_spark, fq_orders) == init_orders
