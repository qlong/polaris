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

"""Cross-process subtransaction retry — Use Case 2 (parallel ingest, retry by any worker).

A "subtransaction" is a server-backed retry/rollback boundary: one row in `subtransactions`
with its own subtxn_id + subtxn_token. The orchestrator allocates the subtransaction
(CREATE SUBTRANSACTION FOR <table>); a worker claims it by subtxn_id + subtxn_token
(BEGIN SUBTRANSACTION ... SUBTXN_ID=<n>, SUBTXN_TOKEN='<t>'), writes, and either
commits it (COMMIT SUBTRANSACTION) or rolls it back.

The distinction this file guards:

  * ROLLBACK SUBTRANSACTION *reopens* the subtransaction for retry (attempt++). The same
    subtxn_id + subtxn_token can then be re-claimed by a *different* session and
    committed. This is the retry-by-another-worker path — the reason the
    subtransaction lives on the server rather than in one Spark session.

  * A subtransaction is committed *atomically as a whole*: a failed-then-retried attempt
    contributes exactly the retry's rows, never the abandoned attempt's rows.

  * Retrying one subtransaction never disturbs a sibling subtransaction that already committed.

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


def create_subtxn(orchestrator, ns_table: str) -> SubtxnInfo:
    result = orchestrator.sql(f"CREATE SUBTRANSACTION FOR {ns_table}").collect()
    return parse_subtxn(result[0]["status"])


def claim(session, ns_table: str, info: SubtxnInfo) -> None:
    session.sql(
        f"BEGIN SUBTRANSACTION {ns_table} "
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

class TestCrossProcessUnitRetry:
    """A rolled-back subtransaction is reopened and retried by a different session."""

    def test_rollback_then_retry_by_other_session_commits(
        self, txn_spark, test_ns, catalog_name
    ):
        """Worker A rolls its subtransaction back; Worker B re-claims the SAME subtransaction and
        commits. The orchestrator commits. Only Worker B's row survives, and the
        sibling subtransaction that committed on the first try is untouched."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")
        subtxn_orders = create_subtxn(txn_spark, f"{ns}.orders")
        subtxn_customers = create_subtxn(txn_spark, f"{ns}.customers")

        # Sibling subtransaction: customers commits once, on the first attempt.
        def commit_customers(session):
            claim(session, f"{ns}.customers", subtxn_customers)
            session.sql(
                f"INSERT INTO {fq_customers} VALUES (91, 'Sibling', 'sib@example.com')"
            )
            session.sql("COMMIT SUBTRANSACTION")

        run_in_session(txn_spark, commit_customers)

        # Attempt 1 on orders: write a DIFFERENT row, then ROLLBACK -> reopens subtransaction.
        def attempt_rollback(session):
            claim(session, f"{ns}.orders", subtxn_orders)
            session.sql(
                f"INSERT INTO {fq_orders} VALUES (9101, 9, 11.00, 'abandoned')"
            )
            session.sql("ROLLBACK SUBTRANSACTION")

        run_in_session(txn_spark, attempt_rollback)

        # Attempt 2 on orders: a *different* session re-claims the same subtransaction and commits.
        def attempt_commit(session):
            claim(session, f"{ns}.orders", subtxn_orders)
            session.sql(
                f"INSERT INTO {fq_orders} VALUES (9102, 9, 22.00, 'retried')"
            )
            session.sql("COMMIT SUBTRANSACTION")

        run_in_session(txn_spark, attempt_commit)

        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)

        # orders: exactly +1 — the retry's row, not the abandoned attempt's row.
        assert count(txn_spark, fq_orders) == init_orders + 1
        assert count_where(txn_spark, fq_orders, "status = 'retried'") == 1
        assert count_where(txn_spark, fq_orders, "status = 'abandoned'") == 0
        # sibling subtransaction preserved
        assert count(txn_spark, fq_customers) == init_customers + 1
        assert count_where(txn_spark, fq_customers, "customer_id = 91") == 1

    def test_retry_is_unit_atomic_not_additive(
        self, txn_spark, test_ns, catalog_name
    ):
        """Rolling a subtransaction back and retrying it does NOT accumulate rows: the
        abandoned attempt contributes nothing, so orders gains exactly one row
        even though two INSERTs were issued against the subtransaction across attempts."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        init_orders = test_ns["init_orders"]

        txn_spark.sql("BEGIN TRANSACTION")
        subtxn_orders = create_subtxn(txn_spark, f"{ns}.orders")

        def attempt_rollback(session):
            claim(session, f"{ns}.orders", subtxn_orders)
            session.sql(
                f"INSERT INTO {fq_orders} VALUES (9201, 9, 1.00, 'attempt1')"
            )
            session.sql("ROLLBACK SUBTRANSACTION")

        def attempt_commit(session):
            claim(session, f"{ns}.orders", subtxn_orders)
            session.sql(
                f"INSERT INTO {fq_orders} VALUES (9202, 9, 2.00, 'attempt2')"
            )
            session.sql("COMMIT SUBTRANSACTION")

        run_in_session(txn_spark, attempt_rollback)
        run_in_session(txn_spark, attempt_commit)

        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        assert count(txn_spark, fq_orders) == init_orders + 1
        assert count_where(txn_spark, fq_orders, "status = 'attempt2'") == 1
        assert count_where(txn_spark, fq_orders, "status = 'attempt1'") == 0

    def test_abort_unit_is_permanent_not_retryable(
        self, txn_spark, test_ns, catalog_name
    ):
        """ABORT SUBTRANSACTION abandons the subtransaction permanently: the parent COMMIT
        skips it and its row never appears, in contrast to ROLLBACK's reopen."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        init_orders = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        txn_spark.sql("BEGIN TRANSACTION")
        subtxn_orders = create_subtxn(txn_spark, f"{ns}.orders")
        subtxn_customers = create_subtxn(txn_spark, f"{ns}.customers")

        def abort_orders(session):
            claim(session, f"{ns}.orders", subtxn_orders)
            session.sql(
                f"INSERT INTO {fq_orders} VALUES (9301, 9, 3.00, 'aborted')"
            )
            session.sql("ABORT SUBTRANSACTION")

        def commit_customers(session):
            claim(session, f"{ns}.customers", subtxn_customers)
            session.sql(
                f"INSERT INTO {fq_customers} VALUES (93, 'Kept', 'kept@example.com')"
            )
            session.sql("COMMIT SUBTRANSACTION")

        run_in_session(txn_spark, abort_orders)
        run_in_session(txn_spark, commit_customers)

        txn_spark.sql("COMMIT")

        refresh(txn_spark, fq_orders)
        refresh(txn_spark, fq_customers)
        # aborted subtransaction contributed nothing
        assert count(txn_spark, fq_orders) == init_orders
        assert count_where(txn_spark, fq_orders, "status = 'aborted'") == 0
        # the committed sibling still lands
        assert count(txn_spark, fq_customers) == init_customers + 1
