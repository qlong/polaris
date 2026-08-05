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

"""Multi-table subtransactions — one CREATE SUBTRANSACTION spanning N tables.

A subtransaction may cover more than one table. When it does, the *whole set* is one
atomic retry/rollback/commit boundary: a single worker claims the subtransaction, writes
to every table it owns, and commits or rolls back all of them together.

This is distinct from the sibling-subtransaction pattern in test_subtransaction_retry.py,
where each table gets its own subtransaction. Here a single `CREATE SUBTRANSACTION FOR
ns.orders, ns.customers` produces one subtxn_id/subtxn_token that owns both tables, and the
worker self-hydrates every table's staging path from the server's operations[] — the client
supplies only subtxn_id + subtxn_token to BEGIN.

The invariants this file guards:

  * COMMIT SUBTRANSACTION commits every table in the multi-table unit together (all-or-nothing).
  * ROLLBACK SUBTRANSACTION reopens the whole unit; a retry replaces both tables' rows, never
    accumulating the abandoned attempt.
  * ABORT SUBTRANSACTION abandons the whole unit; neither table's rows appear.
  * A single WITH <lock> on CREATE applies to every table in the unit.

These tests require a running Polaris with relational-jdbc persistence (see the
module conftest / demos/multi-table-transaction/start-polaris.sh).
"""

import re
import threading
from typing import NamedTuple


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class SubtxnInfo(NamedTuple):
    subtxn_id: int
    subtxn_token: str


def count(spark_session, fq_table: str) -> int:
    return spark_session.sql(f"SELECT count(*) FROM {fq_table}").collect()[0][0]


def count_where(spark_session, fq_table: str, predicate: str) -> int:
    return spark_session.sql(
        f"SELECT count(*) FROM {fq_table} WHERE {predicate}"
    ).collect()[0][0]


def refresh(spark_session, fq_table: str) -> None:
    spark_session.sql(f"REFRESH TABLE {fq_table}")


def parse_subtxn(status: str) -> SubtxnInfo:
    m = re.search(r"subtxn_id=(\d+),\s*subtxn_token=([^,\s]+)", status)
    assert m, f"Cannot parse subtxn info from: {status!r}"
    return SubtxnInfo(subtxn_id=int(m.group(1)), subtxn_token=m.group(2))


def create_subtxn_multi(orchestrator, ns_tables: str, lock: str = "") -> SubtxnInfo:
    """CREATE SUBTRANSACTION FOR a comma-separated table list (one atomic unit).

    ns_tables e.g. "ns.orders, ns.customers"; lock e.g. "EXCLUSIVE" (optional).
    """
    with_clause = f" WITH {lock}" if lock else ""
    result = orchestrator.sql(
        f"CREATE SUBTRANSACTION FOR {ns_tables}{with_clause}"
    ).collect()
    return parse_subtxn(result[0]["status"])


def claim_multi(session, ns_tables: str, info: SubtxnInfo) -> None:
    """A worker claims a multi-table subtransaction. The table list is informational —
    the worker self-hydrates every table it owns from the server via subtxn_id/subtxn_token."""
    session.sql(
        f"BEGIN SUBTRANSACTION {ns_tables} "
        f"SUBTXN_ID={info.subtxn_id}, SUBTXN_TOKEN='{info.subtxn_token}'"
    )


def run_in_session(txn_spark, fn, *args):
    """Run fn(new_session, *args) in a dedicated thread, re-raising any error.

    A fresh session per call models a separate worker *process* claiming the
    subtransaction — the whole point of a server-backed subtransaction.
    """
    box: list = []

    def _run():
        try:
            fn(txn_spark.new_session(), *args)
        except Exception as exc:  # noqa: BLE001 — propagate to the test thread
            box.append(exc)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=120)
    if box:
        raise box[0]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMultiTableSubtransaction:
    """One subtransaction spanning two tables is committed / rolled back / aborted as a unit."""

    def test_multi_table_subtransaction_commits_both_atomically(
        self, txn_spark, test_ns, catalog_name
    ):
        """A single CREATE SUBTRANSACTION FOR orders, customers produces one unit; the worker
        writes to both tables and COMMIT SUBTRANSACTION lands both together."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")
        subtxn = create_subtxn_multi(txn_spark, f"{ns}.orders, {ns}.customers")

        def write_both(session):
            claim_multi(session, f"{ns}.orders, {ns}.customers", subtxn)
            session.sql(
                f"INSERT INTO {fq_orders} VALUES (9401, 9, 44.00, 'multi')"
            )
            session.sql(
                f"INSERT INTO {fq_customers} VALUES (94, 'Multi', 'multi@example.com')"
            )
            session.sql("COMMIT SUBTRANSACTION")

        run_in_session(txn_spark, write_both)

        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        # Both tables in the single unit gained exactly their row.
        assert count(txn_spark, fq_orders) == init_orders + 1
        assert count_where(txn_spark, fq_orders, "status = 'multi'") == 1
        assert count(txn_spark, fq_customers) == init_customers + 1
        assert count_where(txn_spark, fq_customers, "customer_id = 94") == 1

    def test_multi_table_rollback_reopens_whole_unit(
        self, txn_spark, test_ns, catalog_name
    ):
        """ROLLBACK SUBTRANSACTION reopens the whole multi-table unit; a retry replaces both
        tables' rows and neither table accumulates the abandoned attempt."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")
        subtxn = create_subtxn_multi(txn_spark, f"{ns}.orders, {ns}.customers")

        # Attempt 1: write to both tables, then ROLLBACK -> reopens the whole unit.
        def attempt_rollback(session):
            claim_multi(session, f"{ns}.orders, {ns}.customers", subtxn)
            session.sql(
                f"INSERT INTO {fq_orders} VALUES (9501, 9, 1.00, 'abandoned')"
            )
            session.sql(
                f"INSERT INTO {fq_customers} VALUES (95, 'Abandoned', 'x@example.com')"
            )
            session.sql("ROLLBACK SUBTRANSACTION")

        # Attempt 2: a different session re-claims the SAME unit and commits different rows.
        def attempt_commit(session):
            claim_multi(session, f"{ns}.orders, {ns}.customers", subtxn)
            session.sql(
                f"INSERT INTO {fq_orders} VALUES (9502, 9, 2.00, 'retried')"
            )
            session.sql(
                f"INSERT INTO {fq_customers} VALUES (96, 'Retried', 'r@example.com')"
            )
            session.sql("COMMIT SUBTRANSACTION")

        run_in_session(txn_spark, attempt_rollback)
        run_in_session(txn_spark, attempt_commit)

        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        # Each table gains exactly the retry's row; the abandoned attempt contributes nothing.
        assert count(txn_spark, fq_orders) == init_orders + 1
        assert count_where(txn_spark, fq_orders, "status = 'retried'") == 1
        assert count_where(txn_spark, fq_orders, "status = 'abandoned'") == 0
        assert count(txn_spark, fq_customers) == init_customers + 1
        assert count_where(txn_spark, fq_customers, "customer_id = 96") == 1
        assert count_where(txn_spark, fq_customers, "customer_id = 95") == 0

    def test_multi_table_abort_drops_both(
        self, txn_spark, test_ns, catalog_name
    ):
        """ABORT SUBTRANSACTION abandons the whole multi-table unit permanently: the parent
        COMMIT skips it and neither table's row appears."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")
        subtxn = create_subtxn_multi(txn_spark, f"{ns}.orders, {ns}.customers")

        def abort_both(session):
            claim_multi(session, f"{ns}.orders, {ns}.customers", subtxn)
            session.sql(
                f"INSERT INTO {fq_orders} VALUES (9601, 9, 3.00, 'aborted')"
            )
            session.sql(
                f"INSERT INTO {fq_customers} VALUES (97, 'Aborted', 'a@example.com')"
            )
            session.sql("ABORT SUBTRANSACTION")

        run_in_session(txn_spark, abort_both)

        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        # The aborted unit contributed nothing to either table.
        assert count(txn_spark, fq_orders) == init_orders
        assert count_where(txn_spark, fq_orders, "status = 'aborted'") == 0
        assert count(txn_spark, fq_customers) == init_customers
        assert count_where(txn_spark, fq_customers, "customer_id = 97") == 0

    def test_multi_table_with_exclusive_lock_applies_to_all(
        self, txn_spark, test_ns, catalog_name
    ):
        """A single WITH EXCLUSIVE on the multi-table CREATE applies to every table in the unit;
        the unit still commits both tables together."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")
        subtxn = create_subtxn_multi(
            txn_spark, f"{ns}.orders, {ns}.customers", lock="EXCLUSIVE"
        )

        def write_both(session):
            claim_multi(session, f"{ns}.orders, {ns}.customers", subtxn)
            session.sql(
                f"INSERT INTO {fq_orders} VALUES (9701, 9, 7.00, 'locked')"
            )
            session.sql(
                f"INSERT INTO {fq_customers} VALUES (98, 'Locked', 'l@example.com')"
            )
            session.sql("COMMIT SUBTRANSACTION")

        run_in_session(txn_spark, write_both)

        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        assert count(txn_spark, fq_orders) == init_orders + 1
        assert count_where(txn_spark, fq_orders, "status = 'locked'") == 1
        assert count(txn_spark, fq_customers) == init_customers + 1
        assert count_where(txn_spark, fq_customers, "customer_id = 98") == 1

    def test_worker_reads_non_member_table_at_parent_snapshot(
        self, txn_spark, test_ns, catalog_name
    ):
        """A worker's read of a table it does NOT own still sees the parent transaction's
        pinned snapshot (REPEATABLE READ), not a concurrent writer's committed rows.

        Setup: the orchestrator opens a REPEATABLE READ transaction and creates a
        subtransaction FOR orders only -- customers is a *non-member* table (the "t3").
        A worker claims the subtransaction (inheriting the parent snapshot). While the
        worker holds its subtransaction open, a separate writer commits a new customers
        row. The worker then reads customers and must still see the pre-writer count:
        every read in any subtransaction shares the snapshot the parent transaction took.
        """
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_customers = test_ns["init_customers"]

        # Orchestrator pins the parent snapshot and creates a subtxn over orders ONLY.
        txn_spark.sql("BEGIN TRANSACTION")
        # First read triggers the lazy beginTransaction and pins the parent snapshot.
        count(txn_spark, fq_orders)
        subtxn = create_subtxn_multi(txn_spark, f"{ns}.orders")

        subtxn_claimed = threading.Event()
        writer_done = threading.Event()
        results: dict = {}
        errors: list = []

        def worker():
            session = txn_spark.new_session()
            try:
                # Claiming inherits the parent snapshot; customers is NOT owned here.
                claim_multi(session, f"{ns}.orders", subtxn)
                results["pre"] = count(session, fq_customers)

                subtxn_claimed.set()
                writer_done.wait(timeout=60)

                # A read of the non-member table must still see the parent snapshot.
                results["in_subtxn"] = count(session, fq_customers)

                # The worker only read; it staged no writes to its owned table, so the
                # honest close is ABORT (COMMIT would reject an empty subtransaction).
                session.sql("ABORT SUBTRANSACTION")
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
                subtxn_claimed.set()
                writer_done.set()

        def writer():
            writer_spark = txn_spark.new_session()
            try:
                subtxn_claimed.wait(timeout=60)
                # Commit a new customers row entirely outside the transaction.
                writer_spark.sql(
                    f"INSERT INTO {fq_customers} VALUES (99, 'Interloper', 'i@example.com')"
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                writer_done.set()

        wk_t = threading.Thread(target=worker, daemon=True)
        wr_t = threading.Thread(target=writer, daemon=True)
        wk_t.start()
        wr_t.start()
        wk_t.join(timeout=120)
        wr_t.join(timeout=120)

        if errors:
            raise errors[0]

        # The worker's non-member read is pinned to the parent snapshot: the writer's
        # committed row is invisible inside the subtransaction.
        assert results["pre"] == init_customers
        assert results["in_subtxn"] == results["pre"], (
            "Non-member read in worker subtransaction was not pinned to the parent "
            f"snapshot: pre={results['pre']}, in-subtxn={results['in_subtxn']}"
        )

        txn_spark.sql("COMMIT")

        # After the parent commits, the writer's row is visible again.
        refresh(txn_spark, fq_customers)
        assert count(txn_spark, fq_customers) == init_customers + 1
        assert count_where(txn_spark, fq_customers, "customer_id = 99") == 1
