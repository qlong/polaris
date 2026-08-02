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

"""Atomic multi-table write tests via subtransactions.

Pattern:
  Orchestrator: BEGIN TRANSACTION → CREATE SUBTRANSACTION FOR each table
                (lazy beginTransaction fires on the first CREATE SUBTRANSACTION)
  Workers:      BEGIN SUBTRANSACTION → INSERT → COMMIT/ROLLBACK SUBTRANSACTION
  Orchestrator: COMMIT / ROLLBACK

All subtxn_id and subtxn_token values are parsed from the status column returned
by CREATE SUBTRANSACTION FOR — no stdout capture needed.

Worker sessions are created via txn_spark.new_session() so they share the JVM
but start with clean conf (no orchestrator transaction state).
"""

import re
import threading
from typing import NamedTuple


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class SubtxnInfo(NamedTuple):
    subtxn_id:    int
    subtxn_token: str


def count(spark_session, fq_table: str) -> int:
    return spark_session.sql(f"SELECT count(*) FROM {fq_table}").collect()[0][0]


def refresh(spark_session, fq_table: str) -> None:
    spark_session.sql(f"REFRESH TABLE {fq_table}")


def parse_subtxn(status: str) -> SubtxnInfo:
    """Parse subtxn_id and subtxn_token from CREATE SUBTRANSACTION FOR status string.

    Expected format:
        "Subtransaction created  subtxn_id=7, subtxn_token=eyJ..., staging_path=..."
    """
    m = re.search(r"subtxn_id=(\d+),\s*subtxn_token=([^,\s]+)", status)
    assert m, f"Cannot parse subtxn info from: {status!r}"
    return SubtxnInfo(subtxn_id=int(m.group(1)), subtxn_token=m.group(2))


def create_subtxn(orchestrator, ns_table: str) -> SubtxnInfo:
    """Issue CREATE SUBTRANSACTION FOR and return the parsed subtxn info."""
    result = orchestrator.sql(f"CREATE SUBTRANSACTION FOR {ns_table}").collect()
    return parse_subtxn(result[0]["status"])


def run_worker(spark_session, ns_table: str, info: SubtxnInfo, insert_sql: str) -> None:
    """Run the full worker commit flow in the given session."""
    spark_session.sql(
        f"BEGIN SUBTRANSACTION {ns_table} "
        f"SUBTXN_ID={info.subtxn_id}, SUBTXN_TOKEN='{info.subtxn_token}'"
    )
    spark_session.sql(insert_sql)
    spark_session.sql("COMMIT SUBTRANSACTION")


def run_worker_rollback(spark_session, ns_table: str, info: SubtxnInfo, insert_sql: str) -> None:
    """Worker that permanently abandons its subtransaction (ABORT) after writing.

    Use this when the worker gives up and the parent should commit without its data.
    For the reopen-and-retry-same-subtransaction pattern, use ROLLBACK SUBTRANSACTION directly.
    """
    spark_session.sql(
        f"BEGIN SUBTRANSACTION {ns_table} "
        f"SUBTXN_ID={info.subtxn_id}, SUBTXN_TOKEN='{info.subtxn_token}'"
    )
    spark_session.sql(insert_sql)
    spark_session.sql("ABORT SUBTRANSACTION")


def parallel_workers(txn_spark, tasks):
    """Run a list of (fn, args) callables in parallel threads; re-raise first error."""
    errors: list = []
    threads = []
    for fn, args in tasks:
        def _run(f=fn, a=args):
            try:
                f(txn_spark.new_session(), *a)
            except Exception as exc:
                errors.append(exc)
        t = threading.Thread(target=_run, daemon=True)
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    if errors:
        raise errors[0]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAtomicWrite:
    """Verify that subtransactions commit atomically across multiple tables."""

    def test_atomic_two_table_write(self, txn_spark, test_ns, catalog_name):
        """Both tables get a new row when orchestrator COMMITs."""
        ns  = test_ns["ns"]
        cat = catalog_name
        fq_orders    = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders    = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        # Phase 1: orchestrator opens transaction + allocates subtransactions
        txn_spark.sql("BEGIN TRANSACTION")
        subtxn_orders    = create_subtxn(txn_spark, f"{ns}.orders")
        subtxn_customers = create_subtxn(txn_spark, f"{ns}.customers")

        # Phase 2: workers write in parallel
        parallel_workers(txn_spark, [
            (run_worker, (
                f"{ns}.orders", subtxn_orders,
                f"INSERT INTO {fq_orders} VALUES (9001, 9, 999.00, 'new')",
            )),
            (run_worker, (
                f"{ns}.customers", subtxn_customers,
                f"INSERT INTO {fq_customers} VALUES (9, 'Zara', 'zara@example.com')",
            )),
        ])

        # Phase 3: orchestrator commits
        txn_spark.sql("COMMIT")

        # Both tables must show exactly +1 row
        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        assert count(txn_spark, fq_orders)    == init_orders    + 1
        assert count(txn_spark, fq_customers) == init_customers + 1

    def test_worker_rollback_does_not_affect_other_table(self, txn_spark, test_ns, catalog_name):
        """Worker 1 commits; Worker 2 rolls back. Orchestrator commits.
        Result: only orders gets +1; customers unchanged."""
        ns  = test_ns["ns"]
        cat = catalog_name
        fq_orders    = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders    = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")
        subtxn_orders    = create_subtxn(txn_spark, f"{ns}.orders")
        subtxn_customers = create_subtxn(txn_spark, f"{ns}.customers")

        errors: list = []

        def worker_commit(session):
            try:
                run_worker(
                    session, f"{ns}.orders", subtxn_orders,
                    f"INSERT INTO {fq_orders} VALUES (9002, 9, 50.00, 'new')",
                )
            except Exception as exc:
                errors.append(exc)

        def worker_rollback(session):
            try:
                run_worker_rollback(
                    session, f"{ns}.customers", subtxn_customers,
                    f"INSERT INTO {fq_customers} VALUES (9, 'Zara', 'zara@example.com')",
                )
            except Exception as exc:
                errors.append(exc)

        t1 = threading.Thread(target=worker_commit,   args=(txn_spark.new_session(),), daemon=True)
        t2 = threading.Thread(target=worker_rollback, args=(txn_spark.new_session(),), daemon=True)
        t1.start(); t2.start()
        t1.join(timeout=120); t2.join(timeout=120)

        if errors:
            raise errors[0]

        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        assert count(txn_spark, fq_orders)    == init_orders    + 1, "orders should have +1"
        assert count(txn_spark, fq_customers) == init_customers,     "customers should be unchanged"

    def test_abort_transaction_reverts_all(self, txn_spark, test_ns, catalog_name):
        """Both workers commit subtransactions; orchestrator ROLLBACKs.
        Result: neither table changes."""
        ns  = test_ns["ns"]
        cat = catalog_name
        fq_orders    = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders    = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")
        subtxn_orders    = create_subtxn(txn_spark, f"{ns}.orders")
        subtxn_customers = create_subtxn(txn_spark, f"{ns}.customers")

        parallel_workers(txn_spark, [
            (run_worker, (
                f"{ns}.orders", subtxn_orders,
                f"INSERT INTO {fq_orders} VALUES (9003, 9, 10.00, 'new')",
            )),
            (run_worker, (
                f"{ns}.customers", subtxn_customers,
                f"INSERT INTO {fq_customers} VALUES (9, 'Zara', 'zara@example.com')",
            )),
        ])

        txn_spark.sql("ROLLBACK")

        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        assert count(txn_spark, fq_orders)    == init_orders,    "orders should be unchanged after ROLLBACK"
        assert count(txn_spark, fq_customers) == init_customers, "customers should be unchanged after ROLLBACK"

    def test_both_workers_rollback_orchestrator_commits(self, txn_spark, test_ns, catalog_name):
        """Both workers roll back their subtransactions; orchestrator COMMITs.
        Result: neither table changes — no subtransaction data survives."""
        ns  = test_ns["ns"]
        cat = catalog_name
        fq_orders    = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders    = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")
        subtxn_orders    = create_subtxn(txn_spark, f"{ns}.orders")
        subtxn_customers = create_subtxn(txn_spark, f"{ns}.customers")

        parallel_workers(txn_spark, [
            (run_worker_rollback, (
                f"{ns}.orders", subtxn_orders,
                f"INSERT INTO {fq_orders} VALUES (9004, 9, 1.00, 'new')",
            )),
            (run_worker_rollback, (
                f"{ns}.customers", subtxn_customers,
                f"INSERT INTO {fq_customers} VALUES (9, 'Zara', 'zara@example.com')",
            )),
        ])

        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        assert count(txn_spark, fq_orders)    == init_orders,    "orders should be unchanged"
        assert count(txn_spark, fq_customers) == init_customers, "customers should be unchanged"

    def test_one_worker_commits_orchestrator_rollback(self, txn_spark, test_ns, catalog_name):
        """Worker 1 commits subtransaction; Worker 2 rolls back; orchestrator ROLLBACKs.
        Result: neither table changes — the orchestrator ROLLBACK overrides Worker 1's commit."""
        ns  = test_ns["ns"]
        cat = catalog_name
        fq_orders    = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders    = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")
        subtxn_orders    = create_subtxn(txn_spark, f"{ns}.orders")
        subtxn_customers = create_subtxn(txn_spark, f"{ns}.customers")

        errors: list = []

        def worker_commit(session):
            try:
                run_worker(
                    session, f"{ns}.orders", subtxn_orders,
                    f"INSERT INTO {fq_orders} VALUES (9005, 9, 1.00, 'new')",
                )
            except Exception as exc:
                errors.append(exc)

        def worker_rollback(session):
            try:
                run_worker_rollback(
                    session, f"{ns}.customers", subtxn_customers,
                    f"INSERT INTO {fq_customers} VALUES (9, 'Zara', 'zara@example.com')",
                )
            except Exception as exc:
                errors.append(exc)

        t1 = threading.Thread(target=worker_commit,   args=(txn_spark.new_session(),), daemon=True)
        t2 = threading.Thread(target=worker_rollback, args=(txn_spark.new_session(),), daemon=True)
        t1.start(); t2.start()
        t1.join(timeout=120); t2.join(timeout=120)

        if errors:
            raise errors[0]

        txn_spark.sql("ROLLBACK")

        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        assert count(txn_spark, fq_orders)    == init_orders,    "orders should be unchanged after ROLLBACK"
        assert count(txn_spark, fq_customers) == init_customers, "customers should be unchanged after ROLLBACK"

    def test_sequential_workers_atomic_commit(self, txn_spark, test_ns, catalog_name):
        """Workers run sequentially (not in parallel). Both commit; orchestrator COMMITs.
        Result: both tables get +1 — atomicity does not require concurrent worker execution."""
        ns  = test_ns["ns"]
        cat = catalog_name
        fq_orders    = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders    = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")
        subtxn_orders    = create_subtxn(txn_spark, f"{ns}.orders")
        subtxn_customers = create_subtxn(txn_spark, f"{ns}.customers")

        # Workers run sequentially on separate sessions (no parallelism required)
        w1 = txn_spark.new_session()
        w2 = txn_spark.new_session()
        run_worker(
            w1, f"{ns}.orders", subtxn_orders,
            f"INSERT INTO {fq_orders} VALUES (9006, 9, 1.00, 'new')",
        )
        run_worker(
            w2, f"{ns}.customers", subtxn_customers,
            f"INSERT INTO {fq_customers} VALUES (9, 'Zara', 'zara@example.com')",
        )

        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        assert count(txn_spark, fq_orders)    == init_orders    + 1
        assert count(txn_spark, fq_customers) == init_customers + 1


class TestRetrySubtransaction:
    """Verify the Pillar-4 retry story end-to-end: one table's subtransaction
    fails, a *new* subtransaction for that same table succeeds under the still-open
    parent, and the parent COMMIT publishes every table atomically — without redoing
    the sibling tables' already-staged work."""

    def test_retry_failed_subtransaction_then_commit(self, txn_spark, test_ns, catalog_name):
        """orders stages once (kept); customers' first subtxn ROLLBACKs, a second
        subtxn for customers is created + committed; orchestrator COMMITs.

        Result: BOTH tables get exactly +1 — the failed table was recovered by a
        fresh subtransaction, and orders was never re-staged.
        """
        ns  = test_ns["ns"]
        cat = catalog_name
        fq_orders    = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders    = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        # Phase 1: open the parent transaction.
        txn_spark.sql("BEGIN TRANSACTION")

        # Phase 2: orders stages successfully and stays staged for the rest of the txn.
        subtxn_orders = create_subtxn(txn_spark, f"{ns}.orders")
        run_worker(
            txn_spark.new_session(), f"{ns}.orders", subtxn_orders,
            f"INSERT INTO {fq_orders} VALUES (9101, 9, 111.00, 'kept')",
        )

        # Phase 3: customers' FIRST attempt fails (simulated transient error → ROLLBACK).
        subtxn_customers_1 = create_subtxn(txn_spark, f"{ns}.customers")
        run_worker_rollback(
            txn_spark.new_session(), f"{ns}.customers", subtxn_customers_1,
            f"INSERT INTO {fq_customers} VALUES (91, 'Doomed', 'doomed@example.com')",
        )

        # Phase 4: RETRY — a brand-new subtransaction for the SAME table under the
        # SAME parent. This is the exact operation the slides/talk claim is possible.
        subtxn_customers_2 = create_subtxn(txn_spark, f"{ns}.customers")
        assert subtxn_customers_2.subtxn_id != subtxn_customers_1.subtxn_id, (
            "retry must allocate a distinct subtxn_id"
        )
        run_worker(
            txn_spark.new_session(), f"{ns}.customers", subtxn_customers_2,
            f"INSERT INTO {fq_customers} VALUES (9, 'Zara', 'zara@example.com')",
        )

        # Phase 5: parent commit publishes both tables atomically.
        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        # orders: exactly +1 (staged once, never retried).
        assert count(txn_spark, fq_orders) == init_orders + 1, (
            "orders should have +1 from its single staged subtransaction"
        )
        # customers: exactly +1 — only the retry survived; the rolled-back attempt did not.
        assert count(txn_spark, fq_customers) == init_customers + 1, (
            "customers should have +1 from the retry subtransaction only "
            "(the rolled-back first attempt must not survive)"
        )
        # The 'Doomed' row from the failed attempt must not exist.
        doomed = count(txn_spark, f"{fq_customers} WHERE customer_id = 91")
        assert doomed == 0, "the rolled-back first attempt must leave no data"

    def test_in_progress_subtransaction_blocks_parent_commit(
        self, txn_spark, test_ns, catalog_name
    ):
        """SAFETY: a worker begins + inserts but never COMMIT/ROLLBACKs (a crash
        mid-flight). The parent COMMIT must be REJECTED while that subtransaction is
        still IN_PROGRESS — the server refuses to publish a half-finished transaction.
        Neither table changes."""
        import pytest

        ns  = test_ns["ns"]
        cat = catalog_name
        fq_orders    = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders    = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")

        subtxn_orders = create_subtxn(txn_spark, f"{ns}.orders")
        run_worker(
            txn_spark.new_session(), f"{ns}.orders", subtxn_orders,
            f"INSERT INTO {fq_orders} VALUES (9102, 9, 222.00, 'kept')",
        )

        # customers subtxn: begin + insert, then the worker "crashes" — it never
        # issues COMMIT/ROLLBACK SUBTRANSACTION, so the subtxn stays IN_PROGRESS.
        subtxn_customers = create_subtxn(txn_spark, f"{ns}.customers")
        crashed = txn_spark.new_session()
        crashed.sql(
            f"BEGIN SUBTRANSACTION {ns}.customers "
            f"SUBTXN_ID={subtxn_customers.subtxn_id}, "
            f"SUBTXN_TOKEN='{subtxn_customers.subtxn_token}'"
        )
        crashed.sql(
            f"INSERT INTO {fq_customers} VALUES (92, 'Abandoned', 'abandoned@example.com')"
        )

        # Parent COMMIT must be rejected: a subtransaction is still in progress.
        with pytest.raises(Exception) as excinfo:
            txn_spark.sql("COMMIT")
        assert "IN_PROGRESS" in str(excinfo.value), (
            "parent COMMIT must be rejected while a subtransaction is IN_PROGRESS; "
            f"got: {excinfo.value}"
        )

        # Nothing was published — both tables are unchanged.
        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        assert count(txn_spark, fq_orders)    == init_orders,    "orders unchanged"
        assert count(txn_spark, fq_customers) == init_customers, "customers unchanged"

    def test_retry_after_aborting_crashed_subtransaction_then_commit(
        self, txn_spark, test_ns, catalog_name
    ):
        """RECOVERY: a worker crashes mid-flight (begin + insert, no terminal
        statement). The orchestrator aborts the stale subtransaction (a session
        re-binds to it and ROLLBACK SUBTRANSACTION), then retries the table with a
        fresh subtransaction. The parent COMMITs — both tables land +1 and the
        crashed attempt leaves no data. orders was staged exactly once."""
        ns  = test_ns["ns"]
        cat = catalog_name
        fq_orders    = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders    = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")

        subtxn_orders = create_subtxn(txn_spark, f"{ns}.orders")
        run_worker(
            txn_spark.new_session(), f"{ns}.orders", subtxn_orders,
            f"INSERT INTO {fq_orders} VALUES (9103, 9, 333.00, 'kept')",
        )

        # customers' first worker crashes: begin + insert, no COMMIT/ROLLBACK.
        subtxn_customers_1 = create_subtxn(txn_spark, f"{ns}.customers")
        crashed = txn_spark.new_session()
        crashed.sql(
            f"BEGIN SUBTRANSACTION {ns}.customers "
            f"SUBTXN_ID={subtxn_customers_1.subtxn_id}, "
            f"SUBTXN_TOKEN='{subtxn_customers_1.subtxn_token}'"
        )
        crashed.sql(
            f"INSERT INTO {fq_customers} VALUES (92, 'Abandoned', 'abandoned@example.com')"
        )

        # RECOVERY step 1: abort the stale subtransaction so it no longer blocks the
        # parent. A new session re-binds to it and issues ABORT SUBTRANSACTION to
        # permanently abandon it (not reopen-for-retry).
        cleanup = txn_spark.new_session()
        cleanup.sql(
            f"BEGIN SUBTRANSACTION {ns}.customers "
            f"SUBTXN_ID={subtxn_customers_1.subtxn_id}, "
            f"SUBTXN_TOKEN='{subtxn_customers_1.subtxn_token}'"
        )
        cleanup.sql("ABORT SUBTRANSACTION")

        # RECOVERY step 2: a brand-new subtransaction for the SAME table succeeds.
        subtxn_customers_2 = create_subtxn(txn_spark, f"{ns}.customers")
        assert subtxn_customers_2.subtxn_id != subtxn_customers_1.subtxn_id, (
            "retry must allocate a distinct subtxn_id"
        )
        run_worker(
            txn_spark.new_session(), f"{ns}.customers", subtxn_customers_2,
            f"INSERT INTO {fq_customers} VALUES (9, 'Zara', 'zara@example.com')",
        )

        # Parent can now commit: no subtransaction is left in progress.
        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        assert count(txn_spark, fq_orders) == init_orders + 1, (
            "orders should have +1 from its single staged subtransaction"
        )
        assert count(txn_spark, fq_customers) == init_customers + 1, (
            "customers should have +1 from the retry subtransaction only"
        )
        abandoned = count(txn_spark, f"{fq_customers} WHERE customer_id = 92")
        assert abandoned == 0, "the crashed first attempt must leave no data"


class TestSubtxnParsing:
    """Lightweight tests for the CREATE SUBTRANSACTION FOR return value."""

    def test_create_subtxn_returns_id_and_token(self, txn_spark, test_ns, catalog_name):
        """CREATE SUBTRANSACTION FOR returns a non-empty subtxn_id and token."""
        ns = test_ns["ns"]
        try:
            txn_spark.sql("BEGIN TRANSACTION")
            info = create_subtxn(txn_spark, f"{ns}.orders")
            assert info.subtxn_id > 0,            "subtxn_id should be a positive integer"
            assert len(info.subtxn_token) > 10,   "subtxn_token should be a non-trivial string"
        finally:
            txn_spark.sql("ROLLBACK")
