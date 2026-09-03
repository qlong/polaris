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

"""USE SNAPSHOT <handle> / USE SNAPSHOT NONE — consuming an exported TxnSnapshot in Spark.

This is the client-side half of the "exportable snapshot" superpower: a plain Spark
session, with NO local BEGIN TRANSACTION, attaches its reads to a snapshot that some
*other* coordinator minted via ``POST /transaction-snapshots``.  The coordinator here
is the module's PolarisClient (standing in for a notebook / Airflow task / another
engine); Spark consumes the same opaque ``txn_snapshot`` handle the coordinator exported.

The handle is a server-minted, case-sensitive base64url string; the client stores and
echoes it verbatim and never parses it.  Standalone read snapshots are reclaimed purely
by TTL — there is no server-side DELETE, so the coordinator never releases one.

Verb semantics under test:

  * ``USE SNAPSHOT <handle>`` — pin this session's reads to the exported read_point.
    No transaction is started; the session merely consumes a shared cutoff.
  * ``USE SNAPSHOT NONE``     — detach; reads return to latest (READ COMMITTED).
  * Attach is read-only: a write while attached must fail rather than stage against
    a non-existent transaction.
  * Attaching while a local transaction is in progress is rejected.

These require a running Polaris with relational-jdbc persistence (see the module
conftest / demos/multi-table-transaction/start-polaris.sh).
"""

import os
import sys
import threading

import pytest

# ---------------------------------------------------------------------------
# PolarisClient — import from t_api_transactions (mint/release snapshots via REST)
# ---------------------------------------------------------------------------

_POLARIS_CLIENT_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "t_api_transactions", "src"
)
if _POLARIS_CLIENT_DIR not in sys.path:
    sys.path.insert(0, _POLARIS_CLIENT_DIR)

from polaris_client import PolarisClient  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def count(spark_session, fq_table: str) -> int:
    return spark_session.sql(f"SELECT count(*) FROM {fq_table}").collect()[0][0]


def refresh(spark_session, fq_table: str) -> None:
    spark_session.sql(f"REFRESH TABLE {fq_table}")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def polaris_client(catalog_name, credentials) -> PolarisClient:
    host = os.getenv("POLARIS_HOST", "http://localhost:8181")
    return PolarisClient(host=host, catalog=catalog_name, credentials=credentials)


@pytest.fixture
def exported_snapshot(polaris_client, test_ns):
    """Coordinator mints a snapshot via REST; no teardown (TTL-reclaimed).

    Depends on ``test_ns`` so the snapshot's read_point is captured *after* the
    per-test tables are created and seeded — otherwise the pinned read_point
    predates the tables' first commit and loadTable raises NoSuchTable.

    Returns the response dict carrying the opaque ``txn_snapshot`` handle the
    coordinator hands out to consumers. Standalone read handles have no
    server-side DELETE, so teardown is a no-op.
    """
    resp = polaris_client.post("/transaction-snapshots", {"lease_seconds": 300})
    resp.raise_for_status()
    snap = resp.json()
    yield snap


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestUseSnapshotAttach:
    """A plain Spark session consumes a coordinator-minted snapshot via USE SNAPSHOT."""

    def test_attach_pins_reads_across_concurrent_writer(
        self, txn_spark, exported_snapshot, test_ns, catalog_name
    ):
        """USE SNAPSHOT <handle> holds a consistent view while a writer commits underneath.

        No BEGIN on the reader — it only attaches to the coordinator's read_point.
        A separate writer session inserts into both tables; the attached reader must
        still see the baseline counts (the exported snapshot predates the writes).
        """
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        handle = exported_snapshot["txn_snapshot"]

        # Attach the reader to the exported snapshot (no local transaction). The handle is
        # an opaque, case-sensitive base64url token — echo it verbatim, never coerce it.
        status = txn_spark.sql(f"USE SNAPSHOT '{handle}'").collect()[0]["status"]
        assert handle in status, f"Unexpected USE SNAPSHOT status: {status}"

        try:
            pre = (count(txn_spark, fq_orders), count(txn_spark, fq_customers))
            assert pre == (test_ns["init_orders"], test_ns["init_customers"]), (
                f"attached baseline wrong: {pre}"
            )

            # Writer commits underneath, on an independent session (no txn state).
            writer = txn_spark.new_session()
            writer.sql(f"INSERT INTO {fq_orders} VALUES (9101, 9, 999.00, 'new')")
            writer.sql(f"INSERT INTO {fq_customers} VALUES (9, 'Zed', 'zed@example.com')")

            # Refresh so a naive reader would pick up the write; the pin must hold.
            refresh(txn_spark, fq_orders)
            refresh(txn_spark, fq_customers)
            pinned = (count(txn_spark, fq_orders), count(txn_spark, fq_customers))
            assert pinned == pre, (
                f"USE SNAPSHOT did not hold the read_point: pre={pre}, after-write={pinned}"
            )
        finally:
            txn_spark.sql("USE SNAPSHOT NONE")

    def test_detach_returns_to_latest(
        self, txn_spark, exported_snapshot, test_ns, catalog_name
    ):
        """USE SNAPSHOT NONE detaches; the session then reads the latest committed state."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        handle = exported_snapshot["txn_snapshot"]

        txn_spark.sql(f"USE SNAPSHOT '{handle}'")
        try:
            # Writer advances orders while we are pinned.
            writer = txn_spark.new_session()
            writer.sql(f"INSERT INTO {fq_orders} VALUES (9102, 1, 1.00, 'new')")

            refresh(txn_spark, fq_orders)
            pinned = count(txn_spark, fq_orders)
            assert pinned == test_ns["init_orders"], f"still pinned expected, got {pinned}"
        finally:
            txn_spark.sql("USE SNAPSHOT NONE")

        # Detached — must now see the writer's row.
        refresh(txn_spark, fq_orders)
        latest = count(txn_spark, fq_orders)
        assert latest == test_ns["init_orders"] + 1, (
            f"after USE SNAPSHOT NONE expected latest ({test_ns['init_orders'] + 1}), got {latest}"
        )

    def test_two_spark_sessions_same_id_agree(
        self, txn_spark, exported_snapshot, test_ns, catalog_name
    ):
        """Two independent Spark sessions attach to the SAME exported handle and agree.

        Mirrors the demo's cross-engine parity (Reader S + Reader T) using two Spark
        sessions that share nothing but the coordinator's txn_snapshot handle.
        """
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        handle = exported_snapshot["txn_snapshot"]

        reader_a = txn_spark
        reader_b = txn_spark.new_session()

        reader_a.sql(f"USE SNAPSHOT '{handle}'")
        reader_b.sql(f"USE SNAPSHOT '{handle}'")
        try:
            # Writer moves the world after both readers attach.
            writer = txn_spark.new_session()
            writer.sql(f"INSERT INTO {fq_orders} VALUES (9103, 2, 5.00, 'new')")

            refresh(reader_a, fq_orders)
            refresh(reader_b, fq_orders)
            a = count(reader_a, fq_orders)
            b = count(reader_b, fq_orders)
            assert a == b == test_ns["init_orders"], (
                f"two sessions at the same snapshot disagree or drifted: a={a}, b={b}"
            )
        finally:
            reader_a.sql("USE SNAPSHOT NONE")
            reader_b.sql("USE SNAPSHOT NONE")


class TestUseSnapshotGuards:
    """Negative / safety behaviors of the attach verb."""

    def test_attach_is_read_only(
        self, txn_spark, exported_snapshot, test_ns, catalog_name
    ):
        """A write while attached to an external snapshot must fail, not silently stage.

        An attached session owns no transaction, so there is nothing to write into;
        the plugin returns a static pinned table and the INSERT is rejected.
        """
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        handle = exported_snapshot["txn_snapshot"]

        txn_spark.sql(f"USE SNAPSHOT '{handle}'")
        try:
            with pytest.raises(Exception):
                txn_spark.sql(f"INSERT INTO {fq_orders} VALUES (9104, 1, 2.00, 'nope')")

            # The rejected write must not have landed: detach and confirm baseline.
            txn_spark.sql("USE SNAPSHOT NONE")
            refresh(txn_spark, fq_orders)
            assert count(txn_spark, fq_orders) == test_ns["init_orders"], (
                "a write while attached to a snapshot must not persist"
            )
        finally:
            # Idempotent — detach may already have run above.
            try:
                txn_spark.sql("USE SNAPSHOT NONE")
            except Exception:
                pass

    def test_attach_while_transaction_in_progress_is_rejected(
        self, txn_spark, exported_snapshot, test_ns, catalog_name
    ):
        """USE SNAPSHOT is rejected while a local transaction is pending/active."""
        handle = exported_snapshot["txn_snapshot"]
        txn_spark.sql("BEGIN TRANSACTION")
        try:
            with pytest.raises(Exception):
                txn_spark.sql(f"USE SNAPSHOT '{handle}'")
        finally:
            txn_spark.sql("ROLLBACK")

    def test_plain_use_namespace_still_works(self, txn_spark, test_ns, catalog_name):
        """A plain USE <namespace> must fall through untouched (not parsed as a snapshot)."""
        ns = test_ns["ns"]
        cat = catalog_name
        # Should not raise and should not attach a snapshot.
        txn_spark.sql(f"USE {cat}.{ns}")
        # A subsequent unqualified read resolves against the used namespace at latest.
        assert count(txn_spark, "orders") == test_ns["init_orders"]
