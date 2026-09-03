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

"""End-to-end multi-table transaction tests across REST, Spark, and Trino.

This suite ties together the three layers that the other suites test in isolation:
  - REST API      (PolarisClient)        — subtransaction state machine, snapshots
  - Spark SQL      (PolarisTxnSparkSession) — atomic multi-table writes, isolation
  - Trino          (trino.dbapi)          — consistent cross-engine reads

Scenarios:
  TestThreeTableAtomicity       — 3-table atomic commit / partial rollback
  TestOCCConflict               — concurrent writers → Iceberg OCC conflict
  TestReadCommittedWithWrites   — READ COMMITTED subtxn write visible / rolled-back
  TestSubtransactionErrorPaths  — double-commit, wrong-token (REST, precise control)
  TestEndToEndSparkAndTrino     — Trino reads a consistent view while Spark writes

Prerequisites (same as test_isolation.py):
  source demos/multi-table-transaction/spark-env.sh

Trino tests additionally require a running Trino server with the Polaris REST catalog
and the isolation-driven snapshot pinning implemented in the Trino repo. Env vars:
  TRINO_HOST     default: localhost
  TRINO_PORT     default: 8080
  TRINO_CATALOG  default: polaris
  pip install trino

Trino tests skip cleanly if the 'trino' package is absent or the server is unreachable.
The Spark/REST tests do not require Trino.

Run:
  source demos/multi-table-transaction/spark-env.sh
  pytest regtests/t_spark_transactions/src/test_e2e_multi_engine.py -v -s
"""

import os
import re
import sys
import threading
from typing import NamedTuple

import pytest

# ---------------------------------------------------------------------------
# Trino Python client — optional; individual Trino tests skip if absent
# ---------------------------------------------------------------------------

try:
    import trino as _trino_module  # noqa: F401
    import trino.dbapi
    import trino.transaction
    TRINO_AVAILABLE = True
except ImportError:
    TRINO_AVAILABLE = False

# ---------------------------------------------------------------------------
# PolarisClient — import from t_api_transactions (for direct REST calls)
# ---------------------------------------------------------------------------

_POLARIS_CLIENT_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "t_api_transactions", "src"
)
if _POLARIS_CLIENT_DIR not in sys.path:
    sys.path.insert(0, _POLARIS_CLIENT_DIR)

from polaris_client import PolarisClient  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers (copied from test_atomic_write.py — no cross-file test-helper imports)
# ---------------------------------------------------------------------------

class SubtxnInfo(NamedTuple):
    subtxn_id:    int
    subtxn_token: str


def count(spark_session, fq_table: str) -> int:
    return spark_session.sql(f"SELECT count(*) FROM {fq_table}").collect()[0][0]


def refresh(spark_session, fq_table: str) -> None:
    spark_session.sql(f"REFRESH TABLE {fq_table}")


def parse_subtxn(status: str) -> SubtxnInfo:
    """Parse subtxn_id and subtxn_token from CREATE SUBTRANSACTION FOR status string."""
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
# Local fixtures (copied from test_xengine_repeatable_read.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def trino_catalog() -> str:
    return os.getenv("TRINO_CATALOG", "polaris")


@pytest.fixture(scope="session")
def trino_host() -> str:
    return os.getenv("TRINO_HOST", "localhost")


@pytest.fixture(scope="session")
def trino_port() -> int:
    return int(os.getenv("TRINO_PORT", "8080"))


def _require_trino():
    if not TRINO_AVAILABLE:
        pytest.skip("trino package not installed — run: pip install trino")


def _open_trino(host, port, catalog, schema, isolation_level=None):
    """Open a Trino connection; skip the test if Trino is unreachable.

    isolation_level None → autocommit (default). Pass a
    trino.transaction.IsolationLevel to open a real multi-statement transaction.
    """
    _require_trino()
    kwargs = dict(host=host, port=port, user="test", catalog=catalog, schema=schema)
    if isolation_level is not None:
        kwargs["isolation_level"] = isolation_level
    try:
        conn = trino.dbapi.connect(**kwargs)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
    except Exception as exc:
        pytest.skip(f"Trino not reachable at {host}:{port} — {exc}")
        return None
    return conn


@pytest.fixture
def trino_conn(trino_host, trino_port, trino_catalog, test_ns):
    """Autocommit Trino connection (each statement its own transaction)."""
    conn = _open_trino(trino_host, trino_port, trino_catalog, test_ns["ns"])
    yield conn
    try:
        conn.close()
    except Exception:
        pass


@pytest.fixture
def polaris_client(catalog_name, credentials) -> PolarisClient:
    host = os.getenv("POLARIS_HOST", "http://localhost:8181")
    return PolarisClient(host=host, catalog=catalog_name, credentials=credentials)


# ---------------------------------------------------------------------------
# Spark helper: seed a third table so we can exercise 3-table atomicity
# ---------------------------------------------------------------------------


def _create_invoices(txn_spark, cat, ns) -> int:
    """Create + seed an invoices table (2 rows). Returns the initial row count."""
    txn_spark.sql(f"""
        CREATE TABLE {cat}.{ns}.invoices (
            invoice_id INT,
            amount DOUBLE
        ) USING iceberg
    """)
    txn_spark.sql(f"""
        INSERT INTO {cat}.{ns}.invoices VALUES
            (5001, 500.00),
            (5002, 750.00)
    """)
    return 2


# ---------------------------------------------------------------------------
# TestThreeTableAtomicity
# ---------------------------------------------------------------------------


class TestThreeTableAtomicity:
    """Extend the 2-table atomicity pattern to 3 tables."""

    def test_three_workers_all_commit(self, txn_spark, test_ns, catalog_name):
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        fq_invoices = f"{cat}.{ns}.invoices"
        init_orders = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        try:
            init_invoices = _create_invoices(txn_spark, cat, ns)

            txn_spark.sql("BEGIN TRANSACTION")
            s_orders = create_subtxn(txn_spark, f"{ns}.orders")
            s_customers = create_subtxn(txn_spark, f"{ns}.customers")
            s_invoices = create_subtxn(txn_spark, f"{ns}.invoices")

            parallel_workers(txn_spark, [
                (run_worker, (
                    f"{ns}.orders", s_orders,
                    f"INSERT INTO {fq_orders} VALUES (9001, 9, 999.00, 'new')",
                )),
                (run_worker, (
                    f"{ns}.customers", s_customers,
                    f"INSERT INTO {fq_customers} VALUES (9, 'Zara', 'zara@example.com')",
                )),
                (run_worker, (
                    f"{ns}.invoices", s_invoices,
                    f"INSERT INTO {fq_invoices} VALUES (5009, 909.00)",
                )),
            ])

            txn_spark.sql("COMMIT")

            reader = txn_spark.new_session()
            refresh(reader, fq_orders)
            refresh(reader, fq_customers)
            refresh(reader, fq_invoices)
            assert count(reader, fq_orders) == init_orders + 1
            assert count(reader, fq_customers) == init_customers + 1
            assert count(reader, fq_invoices) == init_invoices + 1
        finally:
            txn_spark.sql(f"DROP TABLE IF EXISTS {fq_invoices}")

    def test_three_workers_one_rollback(self, txn_spark, test_ns, catalog_name):
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"
        fq_invoices = f"{cat}.{ns}.invoices"
        init_orders = test_ns["init_orders"]
        init_customers = test_ns["init_customers"]

        try:
            init_invoices = _create_invoices(txn_spark, cat, ns)

            txn_spark.sql("BEGIN TRANSACTION")
            s_orders = create_subtxn(txn_spark, f"{ns}.orders")
            s_customers = create_subtxn(txn_spark, f"{ns}.customers")
            s_invoices = create_subtxn(txn_spark, f"{ns}.invoices")

            # orders + customers commit; invoices rolls back
            parallel_workers(txn_spark, [
                (run_worker, (
                    f"{ns}.orders", s_orders,
                    f"INSERT INTO {fq_orders} VALUES (9002, 9, 111.00, 'new')",
                )),
                (run_worker, (
                    f"{ns}.customers", s_customers,
                    f"INSERT INTO {fq_customers} VALUES (10, 'Yan', 'yan@example.com')",
                )),
                (run_worker_rollback, (
                    f"{ns}.invoices", s_invoices,
                    f"INSERT INTO {fq_invoices} VALUES (5010, 111.00)",
                )),
            ])

            txn_spark.sql("COMMIT")

            reader = txn_spark.new_session()
            refresh(reader, fq_orders)
            refresh(reader, fq_customers)
            refresh(reader, fq_invoices)
            assert count(reader, fq_orders) == init_orders + 1
            assert count(reader, fq_customers) == init_customers + 1
            assert count(reader, fq_invoices) == init_invoices, (
                "rolled-back invoices subtransaction must not add a row"
            )
        finally:
            txn_spark.sql(f"DROP TABLE IF EXISTS {fq_invoices}")


# ---------------------------------------------------------------------------
# TestOCCConflict
# ---------------------------------------------------------------------------


class TestOCCConflict:
    """Two independent Spark transactions writing the same table → OCC conflict."""

    def test_concurrent_inserts_occ_conflict(self, txn_spark, test_ns, catalog_name):
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        init_orders = test_ns["init_orders"]

        # Session A pins a snapshot (REPEATABLE READ) via a read.
        txn_spark.sql("BEGIN TRANSACTION")
        _ = count(txn_spark, fq_orders)  # first read pins the snapshot

        # Session B: independent session, writes + commits a new row.
        sess_b = txn_spark.new_session()
        sess_b.sql(f"INSERT INTO {fq_orders} VALUES (9101, 9, 11.00, 'b')")

        # Session A now tries to write + commit; its pinned view is stale so the
        # commit must fail (Iceberg OCC detects the concurrent pointer swap).
        a_commit_failed = False
        try:
            txn_spark.sql(f"INSERT INTO {fq_orders} VALUES (9102, 9, 22.00, 'a')")
            txn_spark.sql("COMMIT")
        except Exception as exc:
            a_commit_failed = True
            msg = str(exc).lower()
            assert ("commit" in msg or "conflict" in msg or "concurrent" in msg
                    or "stale" in msg or "metadata" in msg), (
                f"Unexpected exception for OCC conflict: {exc}"
            )
        finally:
            if not a_commit_failed:
                try:
                    txn_spark.sql("ROLLBACK")
                except Exception:
                    pass

        assert a_commit_failed, "Session A's commit should have failed under OCC"

        reader = txn_spark.new_session()
        refresh(reader, fq_orders)
        assert count(reader, fq_orders) == init_orders + 1, (
            "only Session B's write should have landed"
        )


# ---------------------------------------------------------------------------
# TestReadCommittedWithWrites
# ---------------------------------------------------------------------------


class TestReadCommittedWithWrites:
    """READ COMMITTED write behavior.

    Two facets:
      1. Plain reads: an open READ COMMITTED transaction sees rows a concurrent writer
         commits after it began (no snapshot pinning).
      2. Subtransaction/staged writes: these ARE supported under READ COMMITTED. The
         server routes them directly against the latest committed state (no snapshot
         baseline, no serialization-failure check — see TransactionService.prepareInline
         Commit / prepareCommit). The Spark plugin stages the worker's write against the
         table's latest metadata (SparkCatalog.loadTable worker+unpinned path).
    """

    def test_read_committed_in_txn_sees_concurrent_commit(
        self, txn_spark, test_ns, catalog_name
    ):
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        init_orders = test_ns["init_orders"]

        # Open a READ COMMITTED transaction and take a baseline read (no pinning).
        txn_spark.sql("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
        pre = count(txn_spark, fq_orders)
        assert pre == init_orders

        # A separate session commits a new row.
        writer = txn_spark.new_session()
        writer.sql(f"INSERT INTO {fq_orders} VALUES (9201, 9, 33.00, 'rc')")

        # Under READ COMMITTED the open transaction sees the newly committed row.
        in_txn = count(txn_spark, fq_orders)
        assert in_txn == pre + 1, (
            f"READ COMMITTED should see the concurrent commit: pre={pre}, in_txn={in_txn}"
        )
        txn_spark.sql("COMMIT")

        # A fresh reader also sees it.
        reader = txn_spark.new_session()
        refresh(reader, fq_orders)
        assert count(reader, fq_orders) == init_orders + 1

    def test_read_committed_subtxn_write_visible_to_fresh_reader(
        self, txn_spark, test_ns, catalog_name
    ):
        """A subtransaction write under a READ COMMITTED parent commits and is visible.

        Exercises the worker-staging path with an unpinned (READ COMMITTED) parent — the
        Spark plugin stages against latest metadata; the server routes it direct at commit.
        """
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        init_orders = test_ns["init_orders"]

        txn_spark.sql("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
        s_orders = create_subtxn(txn_spark, f"{ns}.orders")
        parallel_workers(txn_spark, [
            (run_worker, (
                f"{ns}.orders", s_orders,
                f"INSERT INTO {fq_orders} VALUES (9203, 9, 55.00, 'rc-sub')",
            )),
        ])
        txn_spark.sql("COMMIT")

        reader = txn_spark.new_session()
        refresh(reader, fq_orders)
        assert count(reader, fq_orders) == init_orders + 1, (
            "committed READ COMMITTED subtransaction write must be visible"
        )

    def test_read_committed_subtxn_rollback_not_visible(
        self, txn_spark, test_ns, catalog_name
    ):
        """A rolled-back subtransaction under a READ COMMITTED parent adds no row."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        init_orders = test_ns["init_orders"]

        txn_spark.sql("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
        s_orders = create_subtxn(txn_spark, f"{ns}.orders")
        parallel_workers(txn_spark, [
            (run_worker_rollback, (
                f"{ns}.orders", s_orders,
                f"INSERT INTO {fq_orders} VALUES (9204, 9, 66.00, 'rc-sub')",
            )),
        ])
        txn_spark.sql("COMMIT")

        reader = txn_spark.new_session()
        refresh(reader, fq_orders)
        assert count(reader, fq_orders) == init_orders, (
            "rolled-back READ COMMITTED subtransaction must not add a row"
        )


# ---------------------------------------------------------------------------
# TestSubtransactionErrorPaths  (REST, precise server-behaviour control)
# ---------------------------------------------------------------------------


class TestSubtransactionErrorPaths:
    """Error paths for the subtransaction state machine, exercised via REST."""

    def _begin(self, client, isolation="REPEATABLE_READ"):
        resp = client.post("/transactions", {"isolation": isolation, "lease_seconds": 300})
        resp.raise_for_status()
        return resp.json()

    def _create_subtxn(self, client, txn, ns, table):
        resp = client.post(
            f"/transactions/{txn['txn_id']}/subtransactions",
            {"table_identifier": f"{ns}.{table}", "operation": "ROW_EXCLUSIVE", "lease_seconds": 60},
            extra_headers={"X-Txn-Token": txn["txn_token"]},
        )
        resp.raise_for_status()
        return resp.json()

    def _abort(self, client, txn):
        try:
            client.post(
                f"/transactions/{txn['txn_id']}/abort",
                extra_headers={"X-Txn-Token": txn["txn_token"]},
            )
        except Exception:
            pass

    def test_double_commit_subtransaction_returns_error(
        self, polaris_client, test_ns
    ):
        ns = test_ns["ns"]
        client = polaris_client
        txn = self._begin(client)
        try:
            sub = self._create_subtxn(client, txn, ns, "orders")
            path = f"/transactions/{txn['txn_id']}/subtransactions/{sub['subtxn_id']}/commit"
            body = {"staged_metadata_location": sub["assigned_staging_path"]}
            headers = {"X-Subtxn-Token": sub["subtxn_token"]}

            first = client.post(path, body, extra_headers=headers)
            # First commit records the staged location (2xx expected, but tolerate 4xx
            # if the fabricated staging path is rejected — the point is the SECOND call).
            assert first.status_code < 500, (
                f"First commit should not 5xx: {first.status_code} {first.text}"
            )

            second = client.post(path, body, extra_headers=headers)
            assert 400 <= second.status_code < 500, (
                f"Second commit of the same subtransaction must be a 4xx, "
                f"got {second.status_code}: {second.text}"
            )
        finally:
            self._abort(client, txn)

    def test_commit_subtransaction_wrong_token_rejected(
        self, polaris_client, test_ns
    ):
        ns = test_ns["ns"]
        client = polaris_client
        txn = self._begin(client)
        try:
            sub = self._create_subtxn(client, txn, ns, "orders")
            resp = client.post(
                f"/transactions/{txn['txn_id']}/subtransactions/{sub['subtxn_id']}/commit",
                {"staged_metadata_location": sub["assigned_staging_path"]},
                extra_headers={"X-Subtxn-Token": "not-a-real-token"},
            )
            assert resp.status_code in (401, 403), (
                f"Expected 401/403 for wrong subtxn token, got {resp.status_code}: {resp.text}"
            )
        finally:
            self._abort(client, txn)


# ---------------------------------------------------------------------------
# TestEndToEndSparkAndTrino  (Trino reads while Spark writes)
# ---------------------------------------------------------------------------


class TestEndToEndSparkAndTrino:
    """Cross-engine: Trino holds a consistent read view while Spark commits."""

    def _spark_subtxn_insert(self, txn_spark, ns, cat, insert_sql):
        """Run one full BEGIN → subtxn → COMMIT cycle inserting one orders row."""
        fq_orders = f"{cat}.{ns}.orders"
        txn_spark.sql("BEGIN TRANSACTION")
        s_orders = create_subtxn(txn_spark, f"{ns}.orders")
        parallel_workers(txn_spark, [
            (run_worker, (f"{ns}.orders", s_orders, insert_sql)),
        ])
        txn_spark.sql("COMMIT")

    def test_trino_python_client_isolation_does_not_pin(
        self, txn_spark, test_ns, catalog_name, trino_host, trino_port, trino_catalog
    ):
        """The Python trino client's isolation_level=REPEATABLE_READ does NOT pin.

        Trino only forwards an isolation level to a connector when it is written in the SQL
        (START TRANSACTION ISOLATION LEVEL ...). The Python client sends isolation as an HTTP
        header that Trino does NOT pass to the connector's beginTransaction, so the connector
        sees the engine default (READ_UNCOMMITTED) and allocates NO Polaris snapshot. Reads
        therefore reflect the latest committed data, not a pinned baseline.

        REPEATABLE-READ pinning is exercised two ways elsewhere:
          - SQL-level `START TRANSACTION ISOLATION LEVEL REPEATABLE READ` (manual / CLI; see
            demos/multi-table-transaction/demo_interactive.md Part 3), and
          - the explicit `SET SESSION polaris_read_snapshot` path
            (test_trino_explicit_snapshot_pinned_across_spark_subtxn_commit).
        """
        _require_trino()
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"

        conn = _open_trino(
            trino_host, trino_port, trino_catalog, ns,
            isolation_level=trino.transaction.IsolationLevel.REPEATABLE_READ,
        )
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT count(*) FROM {trino_catalog}.{ns}.orders")
            baseline = cur.fetchone()[0]

            # Spark inserts and commits a new row while the client "transaction" is open.
            self._spark_subtxn_insert(
                txn_spark, ns, cat,
                f"INSERT INTO {fq_orders} VALUES (9301, 9, 55.00, 'x')",
            )

            # Because the client isolation never reached the connector, no snapshot was pinned:
            # the next read sees the latest committed data (baseline + 1), NOT a pinned baseline.
            cur.execute(f"SELECT count(*) FROM {trino_catalog}.{ns}.orders")
            in_txn = cur.fetchone()[0]
            assert in_txn == baseline + 1, (
                f"Client-set isolation should NOT pin (reads latest): "
                f"baseline={baseline}, in_txn={in_txn}"
            )
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def test_trino_read_committed_sees_commit_within_same_txn(
        self, txn_spark, test_ns, catalog_name, trino_host, trino_port, trino_catalog
    ):
        """READ COMMITTED sees a commit that lands mid-transaction — the direct contrast
        to the REPEATABLE READ test above.

        In ONE READ COMMITTED Trino transaction:
          read 1 → baseline
          (Spark commits a new row)
          read 2 → baseline + 1   ← sees the latest committed data (NOT pinned)

        This is the behavior that distinguishes READ COMMITTED from REPEATABLE READ: no
        Polaris snapshot is pinned, and the connector's per-transaction table cache is
        invalidated each statement so every read re-resolves to the latest snapshot.
        """
        _require_trino()
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"

        conn = _open_trino(
            trino_host, trino_port, trino_catalog, ns,
            isolation_level=trino.transaction.IsolationLevel.READ_COMMITTED,
        )
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT count(*) FROM {trino_catalog}.{ns}.orders")
            baseline = cur.fetchone()[0]

            # Spark commits a new row while the READ COMMITTED transaction is still open.
            self._spark_subtxn_insert(
                txn_spark, ns, cat,
                f"INSERT INTO {fq_orders} VALUES (9302, 9, 66.00, 'rc')",
            )

            # Same transaction, next statement: must see the newly committed row.
            cur.execute(f"SELECT count(*) FROM {trino_catalog}.{ns}.orders")
            after = cur.fetchone()[0]
            assert after == baseline + 1, (
                f"READ COMMITTED must see the mid-transaction commit: "
                f"baseline={baseline}, after={after}"
            )
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def test_trino_explicit_snapshot_pinned_across_spark_subtxn_commit(
        self, txn_spark, test_ns, catalog_name, polaris_client,
        trino_host, trino_port, trino_catalog
    ):
        """SET SESSION polaris_read_snapshot='<handle>' pins Trino reads explicitly."""
        _require_trino()
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"

        resp = polaris_client.post("/transaction-snapshots", {"lease_seconds": 300})
        resp.raise_for_status()
        handle = resp.json()["txn_snapshot"]

        conn = _open_trino(trino_host, trino_port, trino_catalog, ns)
        try:
            cur = conn.cursor()
            # polaris_read_snapshot is a string session property carrying the opaque,
            # case-sensitive base64url handle verbatim.
            cur.execute(
                f"SET SESSION {trino_catalog}.polaris_read_snapshot = '{handle}'"
            )
            cur.fetchall()

            cur.execute(f"SELECT count(*) FROM {trino_catalog}.{ns}.orders")
            baseline = cur.fetchone()[0]

            self._spark_subtxn_insert(
                txn_spark, ns, cat,
                f"INSERT INTO {fq_orders} VALUES (9303, 9, 77.00, 'pin')",
            )

            cur.execute(f"SELECT count(*) FROM {trino_catalog}.{ns}.orders")
            pinned = cur.fetchone()[0]
            assert pinned == baseline, (
                f"Explicit snapshot pin violated: baseline={baseline}, pinned={pinned}"
            )

            # Standalone read handles are TTL-reclaimed — there is no server-side
            # release. Detaching is just RESET SESSION.
            cur.execute(f"RESET SESSION {trino_catalog}.polaris_read_snapshot")
            cur.fetchall()
            cur.execute(f"SELECT count(*) FROM {trino_catalog}.{ns}.orders")
            post = cur.fetchone()[0]
            assert post == baseline + 1, (
                f"After unpinning Trino should see the new row: baseline={baseline}, post={post}"
            )
        finally:
            try:
                conn.close()
            except Exception:
                pass
