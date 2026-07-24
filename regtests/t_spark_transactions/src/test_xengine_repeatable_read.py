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

"""Cross-engine repeatable-read test: Spark (x2) + Trino each maintain an independent
consistent snapshot while a Spark writer commits new rows into both tables.

Demonstrates REPEATABLE READ isolation across query engines sharing the same Polaris catalog:
  - Before any write: all three readers see the same baseline data
  - After write commits (while transactions still open): all three still see baseline
  - After transactions end: all three see the new data

Prerequisites (same as test_isolation.py):
  source demos/multi-table-transaction/spark-env.sh

Additional env vars for Trino:
  TRINO_HOST     default: localhost
  TRINO_PORT     default: 8080
  TRINO_CATALOG  default: polaris  (must point to the same Polaris REST catalog)

  pip install trino

  Trino must be running with the Polaris REST catalog connector and the
  Tier-2 polaris_read_snapshot session property (implemented in the Trino repo).

If the 'trino' package is absent, the test is skipped entirely.
If Trino is unreachable (connection refused), the test is skipped.

Run:
  source demos/multi-table-transaction/spark-env.sh
  pip install trino
  pytest regtests/t_spark_transactions/src/test_xengine_repeatable_read.py -v -s
"""

import os
import sys
import threading

import pytest

# ---------------------------------------------------------------------------
# Trino Python client — optional; skip the entire test module if absent
# ---------------------------------------------------------------------------

try:
    import trino as _trino_module  # noqa: F401
    import trino.dbapi
    TRINO_AVAILABLE = True
except ImportError:
    TRINO_AVAILABLE = False

# ---------------------------------------------------------------------------
# PolarisClient — import from t_api_transactions (for standalone snapshot REST calls)
# ---------------------------------------------------------------------------

_POLARIS_CLIENT_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "t_api_transactions", "src"
)
if _POLARIS_CLIENT_DIR not in sys.path:
    sys.path.insert(0, _POLARIS_CLIENT_DIR)

from polaris_client import PolarisClient  # noqa: E402


# ---------------------------------------------------------------------------
# Module-level skip: bail out early if trino package is not installed
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.skipif(
    not TRINO_AVAILABLE,
    reason="trino package not installed — run: pip install trino",
)


# ---------------------------------------------------------------------------
# Helpers (same pattern as test_isolation.py)
# ---------------------------------------------------------------------------


def spark_count(session, fq_table: str) -> int:
    return session.sql(f"SELECT count(*) FROM {fq_table}").collect()[0][0]


def spark_refresh(session, fq_table: str) -> None:
    session.sql(f"REFRESH TABLE {fq_table}")


# ---------------------------------------------------------------------------
# Local fixtures (not in conftest.py to avoid polluting the shared fixture namespace)
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


@pytest.fixture
def trino_conn(trino_host, trino_port, trino_catalog, test_ns):
    """Open a Trino connection; skip if Trino is unreachable."""
    ns = test_ns["ns"]
    try:
        conn = trino.dbapi.connect(
            host=trino_host,
            port=trino_port,
            user="test",
            catalog=trino_catalog,
            schema=ns,
        )
        # Probe the connection to detect unreachable host early
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
    except Exception as exc:
        pytest.skip(f"Trino not reachable at {trino_host}:{trino_port} — {exc}")
        return None
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
# Test
# ---------------------------------------------------------------------------


class TestXEngineRepeatableRead:
    """Verify cross-engine repeatable-read isolation: Spark + Trino share the same catalog
    and each independently maintain a consistent snapshot while a Spark writer commits."""

    def test_spark_and_trino_see_consistent_snapshot(
        self,
        txn_spark,
        test_ns,
        catalog_name,
        trino_conn,
        trino_catalog,
        polaris_client,
    ):
        """Three readers (Spark x2 + Trino) pin independent snapshots.

        Phase 1 — All readers begin transactions and read baseline counts.
        Phase 2 — Writer inserts 1 new row into each table.
        Phase 3 — All readers re-read: must still see baseline (repeatable read).
        Phase 4 — All readers end their transactions.
        Phase 5 — All readers (with fresh sessions) see the new rows.
        """
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"

        init_orders = test_ns["init_orders"]      # 5
        init_customers = test_ns["init_customers"]  # 3

        # ------------------------------------------------------------------
        # Synchronization events
        # ------------------------------------------------------------------
        r1_pinned = threading.Event()      # Spark reader 1 has pinned its snapshot
        r2_pinned = threading.Event()      # Spark reader 2 has pinned its snapshot
        trino_pinned = threading.Event()   # Trino reader has pinned its snapshot
        writer_committed = threading.Event()   # Writer has committed new rows
        r1_in_txn_done = threading.Event()    # Reader 1 recorded in-txn count
        r2_in_txn_done = threading.Event()    # Reader 2 recorded in-txn count
        trino_in_txn_done = threading.Event() # Trino reader recorded in-txn count
        readers_done = threading.Event()       # Set by main thread; unblocks readers for phase 4+5

        results: dict = {}
        errors: list = []

        # Trino snapshot handle — allocated by the Trino thread, released in teardown
        trino_snapshot: dict = {}

        # ------------------------------------------------------------------
        # Spark Reader 1 (txn_spark — the module-scoped session)
        # ------------------------------------------------------------------

        def spark_reader1():
            try:
                txn_spark.sql("BEGIN TRANSACTION")
                # First SELECT triggers lazy beginTransaction → pins snapshot
                pre = (spark_count(txn_spark, fq_orders), spark_count(txn_spark, fq_customers))
                results["r1_pre"] = pre
                r1_pinned.set()

                # Wait for writer to commit
                writer_committed.wait(timeout=60)

                # Re-read while transaction is still open — must still see baseline
                in_txn = (spark_count(txn_spark, fq_orders), spark_count(txn_spark, fq_customers))
                results["r1_in_txn"] = in_txn
                r1_in_txn_done.set()
                readers_done.wait(timeout=60)

                # End transaction
                txn_spark.sql("COMMIT")

                # After COMMIT: fresh reads should see the writer's rows
                spark_refresh(txn_spark, fq_orders)
                spark_refresh(txn_spark, fq_customers)
                post = (spark_count(txn_spark, fq_orders), spark_count(txn_spark, fq_customers))
                results["r1_post"] = post

            except Exception as exc:
                errors.append(("spark_reader1", exc))
                # unblock all waiters so the test doesn't hang
                r1_pinned.set()
                r1_in_txn_done.set()
                try:
                    txn_spark.sql("ROLLBACK")
                except Exception:
                    pass

        # ------------------------------------------------------------------
        # Spark Reader 2 (new_session — independent session, own transaction)
        # ------------------------------------------------------------------

        def spark_reader2():
            sess = txn_spark.new_session()
            try:
                sess.sql("BEGIN TRANSACTION")
                # Wait for Reader 1 to be pinned so we know we're starting after baseline
                r1_pinned.wait(timeout=60)
                pre = (spark_count(sess, fq_orders), spark_count(sess, fq_customers))
                results["r2_pre"] = pre
                r2_pinned.set()

                writer_committed.wait(timeout=60)

                in_txn = (spark_count(sess, fq_orders), spark_count(sess, fq_customers))
                results["r2_in_txn"] = in_txn
                r2_in_txn_done.set()
                readers_done.wait(timeout=60)

                sess.sql("COMMIT")

                spark_refresh(sess, fq_orders)
                spark_refresh(sess, fq_customers)
                post = (spark_count(sess, fq_orders), spark_count(sess, fq_customers))
                results["r2_post"] = post

            except Exception as exc:
                errors.append(("spark_reader2", exc))
                r2_pinned.set()
                r2_in_txn_done.set()
                try:
                    sess.sql("ROLLBACK")
                except Exception:
                    pass

        # ------------------------------------------------------------------
        # Trino Reader — uses polaris_read_snapshot session property
        # ------------------------------------------------------------------

        def trino_reader():
            try:
                # Allocate a standalone Polaris snapshot (Trino's equivalent of BEGIN TRANSACTION)
                # Wait until Reader 1 is pinned so no write has happened yet
                r1_pinned.wait(timeout=60)

                resp = polaris_client.post("/transaction-snapshots", {"lease_seconds": 300})
                resp.raise_for_status()
                snap_id = resp.json()["txn_snapshot_id"]
                trino_snapshot["id"] = snap_id

                cur = trino_conn.cursor()
                # Pin Trino to this snapshot
                cur.execute(
                    f"SET SESSION {trino_catalog}.polaris_read_snapshot = {int(snap_id)}"
                )
                cur.fetchall()  # consume SET SESSION result

                # First read — Trino uses the pinned snapshot for all subsequent queries in this session
                cur.execute(f"SELECT count(*) FROM {trino_catalog}.{ns}.orders")
                orders_pre = cur.fetchone()[0]
                cur.execute(f"SELECT count(*) FROM {trino_catalog}.{ns}.customers")
                customers_pre = cur.fetchone()[0]
                results["trino_pre"] = (orders_pre, customers_pre)
                trino_pinned.set()

                writer_committed.wait(timeout=60)

                # Re-read while snapshot is still pinned — must still see baseline
                cur.execute(f"SELECT count(*) FROM {trino_catalog}.{ns}.orders")
                orders_in = cur.fetchone()[0]
                cur.execute(f"SELECT count(*) FROM {trino_catalog}.{ns}.customers")
                customers_in = cur.fetchone()[0]
                results["trino_in_txn"] = (orders_in, customers_in)
                trino_in_txn_done.set()
                readers_done.wait(timeout=60)

                # Release the snapshot ("end transaction")
                polaris_client.delete(f"/transaction-snapshots/{snap_id}")
                trino_snapshot.pop("id", None)

                # After releasing the snapshot: clear polaris_read_snapshot and re-read
                cur.execute(f"RESET SESSION {trino_catalog}.polaris_read_snapshot")
                cur.fetchall()
                cur.execute(f"SELECT count(*) FROM {trino_catalog}.{ns}.orders")
                orders_post = cur.fetchone()[0]
                cur.execute(f"SELECT count(*) FROM {trino_catalog}.{ns}.customers")
                customers_post = cur.fetchone()[0]
                results["trino_post"] = (orders_post, customers_post)

            except Exception as exc:
                errors.append(("trino_reader", exc))
                trino_pinned.set()
                trino_in_txn_done.set()

        # ------------------------------------------------------------------
        # Writer — Spark session, no transaction (INSERT directly)
        # ------------------------------------------------------------------

        def writer():
            ws = txn_spark.new_session()
            try:
                # Wait for all three readers to have pinned their snapshots before writing
                r1_pinned.wait(timeout=60)
                r2_pinned.wait(timeout=60)
                trino_pinned.wait(timeout=60)

                ws.sql(f"INSERT INTO {fq_orders} VALUES (9001, 9, 999.00, 'new')")
                ws.sql(f"INSERT INTO {fq_customers} VALUES (9, 'Zara', 'zara@example.com')")
            except Exception as exc:
                errors.append(("writer", exc))
            finally:
                writer_committed.set()

        # ------------------------------------------------------------------
        # Launch threads
        # ------------------------------------------------------------------

        t_r1 = threading.Thread(target=spark_reader1, daemon=True, name="spark-reader-1")
        t_r2 = threading.Thread(target=spark_reader2, daemon=True, name="spark-reader-2")
        t_tr = threading.Thread(target=trino_reader, daemon=True, name="trino-reader")
        t_wr = threading.Thread(target=writer, daemon=True, name="writer")

        t_r1.start()
        t_r2.start()
        t_tr.start()
        t_wr.start()

        # Wait for all three readers to have completed their in-transaction re-reads
        # (each sets its *_in_txn_done event after recording results["r*_in_txn"]).
        # Only then do we signal readers_done, which unblocks them to COMMIT / release
        # their snapshots and do post-transaction reads.
        r1_in_txn_done.wait(timeout=120)
        r2_in_txn_done.wait(timeout=120)
        trino_in_txn_done.wait(timeout=120)
        readers_done.set()

        t_r1.join(timeout=120)
        t_r2.join(timeout=120)
        t_tr.join(timeout=120)
        t_wr.join(timeout=120)

        # Release Trino snapshot if it was never released (e.g., error path)
        if "id" in trino_snapshot:
            try:
                polaris_client.delete(f"/transaction-snapshots/{trino_snapshot['id']}")
            except Exception:
                pass

        if errors:
            raise AssertionError(
                "Errors in test threads:\n"
                + "\n".join(f"  [{name}] {exc}" for name, exc in errors)
            ) from errors[0][1]

        # ------------------------------------------------------------------
        # Phase 1+2 assertions: pre-write counts must equal baseline
        # ------------------------------------------------------------------

        assert results["r1_pre"] == (init_orders, init_customers), (
            f"Spark reader 1 baseline mismatch: expected ({init_orders}, {init_customers}), "
            f"got {results['r1_pre']}"
        )
        assert results["r2_pre"] == (init_orders, init_customers), (
            f"Spark reader 2 baseline mismatch: expected ({init_orders}, {init_customers}), "
            f"got {results['r2_pre']}"
        )
        assert results["trino_pre"] == (init_orders, init_customers), (
            f"Trino baseline mismatch: expected ({init_orders}, {init_customers}), "
            f"got {results['trino_pre']}"
        )

        # ------------------------------------------------------------------
        # Phase 3 assertions: in-transaction re-reads must still show baseline
        # (repeatable read guarantee — write committed but readers are pinned)
        # ------------------------------------------------------------------

        assert results["r1_in_txn"] == results["r1_pre"], (
            f"Spark reader 1 REPEATABLE READ violated: "
            f"pre={results['r1_pre']}, in_txn={results['r1_in_txn']}"
        )
        assert results["r2_in_txn"] == results["r2_pre"], (
            f"Spark reader 2 REPEATABLE READ violated: "
            f"pre={results['r2_pre']}, in_txn={results['r2_in_txn']}"
        )
        assert results["trino_in_txn"] == results["trino_pre"], (
            f"Trino REPEATABLE READ violated: "
            f"pre={results['trino_pre']}, in_txn={results['trino_in_txn']}"
        )

        # ------------------------------------------------------------------
        # Phase 5 assertions: after transactions end, new rows must be visible
        # ------------------------------------------------------------------

        expected_post = (init_orders + 1, init_customers + 1)

        assert results.get("r1_post") == expected_post, (
            f"Spark reader 1 post-COMMIT counts wrong: "
            f"expected {expected_post}, got {results.get('r1_post')}"
        )
        assert results.get("r2_post") == expected_post, (
            f"Spark reader 2 post-COMMIT counts wrong: "
            f"expected {expected_post}, got {results.get('r2_post')}"
        )
        # Trino post-snapshot read (0 = reset/no pin); best-effort — only assert if present
        if "trino_post" in results:
            assert results["trino_post"] == expected_post, (
                f"Trino post-snapshot counts wrong: "
                f"expected {expected_post}, got {results['trino_post']}"
            )
