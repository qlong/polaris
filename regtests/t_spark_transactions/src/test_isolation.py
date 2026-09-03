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

"""Transaction isolation tests: REPEATABLE READ and READ COMMITTED.

REPEATABLE READ pattern: observer pins a consistent snapshot with BEGIN TRANSACTION;
a concurrent writer inserts rows into both tables; the observer's in-transaction reads
still see the pre-writer counts (no torn read); after COMMIT the observer sees the
writer's new rows.

All multi-session tests use threading.Event for deterministic observer/writer
synchronization.  Writer sessions are created via txn_spark.new_session() — they share
the same JVM SparkContext but have independent session conf (no transaction state).

NoCtsHistory tests (regression for the orElseThrow bug): a REPEATABLE READ transaction
must be able to loadTable on a table that has no committed_table_snapshots rows at or
below the snapshot's read_point.  Before the fix, the handler threw NoSuchTableException
instead of falling through to the live table.  Tables created and seeded via direct
Iceberg writes (CREATE TABLE + INSERT outside any Polaris transaction) have CTS rows, so
the existing tests never hit this path.  These new tests use a table that is created but
never written before the snapshot is taken — exactly the zero-CTS scenario.
"""

import threading
import uuid


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def count(spark_session, fq_table: str) -> int:
    return spark_session.sql(f"SELECT count(*) FROM {fq_table}").collect()[0][0]


def refresh(spark_session, fq_table: str) -> None:
    spark_session.sql(f"REFRESH TABLE {fq_table}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRepeatableReadIsolation:
    """Verify that REPEATABLE READ isolation prevents torn reads across two tables."""

    def test_basic_repeatable_read(self, txn_spark, test_ns, catalog_name):
        """Observer pins snapshot; writer inserts; observer in-txn reads are unchanged."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders    = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"

        snapshot_pinned = threading.Event()
        writer_done     = threading.Event()

        results: dict = {}
        errors:  list = []

        def observer():
            try:
                status = txn_spark.sql("BEGIN TRANSACTION").collect()[0]["status"]
                assert status == "Transaction pending -- isolation: REPEATABLE_READ", (
                    f"Unexpected BEGIN status: {status}"
                )
                # First SELECT triggers the lazy beginTransaction REST call and pins the snapshot
                pre_orders    = count(txn_spark, fq_orders)
                pre_customers = count(txn_spark, fq_customers)
                results["pre"] = (pre_orders, pre_customers)

                snapshot_pinned.set()
                writer_done.wait(timeout=60)

                # In-transaction reads — snapshot must still be pinned
                in_orders    = count(txn_spark, fq_orders)
                in_customers = count(txn_spark, fq_customers)
                results["in_txn"] = (in_orders, in_customers)

                txn_spark.sql("COMMIT")

                # Post-commit reads — must see the writer's new rows
                refresh(txn_spark, fq_orders)
                refresh(txn_spark, fq_customers)
                post_orders    = count(txn_spark, fq_orders)
                post_customers = count(txn_spark, fq_customers)
                results["post"] = (post_orders, post_customers)
            except Exception as exc:
                errors.append(exc)
                snapshot_pinned.set()   # unblock writer so test doesn't hang
                writer_done.set()

        def writer():
            writer_spark = txn_spark.new_session()
            try:
                snapshot_pinned.wait(timeout=60)
                writer_spark.sql(
                    f"INSERT INTO {fq_orders} VALUES (9001, 9, 999.00, 'new')"
                )
                writer_spark.sql(
                    f"INSERT INTO {fq_customers} VALUES (9, 'Zara', 'zara@example.com')"
                )
            except Exception as exc:
                errors.append(exc)
            finally:
                writer_done.set()

        obs_t = threading.Thread(target=observer, daemon=True)
        wrt_t = threading.Thread(target=writer,   daemon=True)
        obs_t.start(); wrt_t.start()
        obs_t.join(timeout=120)
        wrt_t.join(timeout=120)

        if errors:
            raise errors[0]

        pre    = results["pre"]
        in_txn = results["in_txn"]
        post   = results["post"]

        assert in_txn == pre, (
            f"REPEATABLE READ violated: pre-writer={pre}, in-transaction={in_txn}"
        )
        assert post == (pre[0] + 1, pre[1] + 1), (
            f"Post-commit counts wrong: expected ({pre[0]+1}, {pre[1]+1}), got {post}"
        )

    def test_begin_returns_pending_status(self, txn_spark, test_ns, catalog_name):
        """BEGIN TRANSACTION returns the expected status string before any table access."""
        try:
            row = txn_spark.sql("BEGIN TRANSACTION").collect()[0]
            assert row["status"] == "Transaction pending -- isolation: REPEATABLE_READ"
        finally:
            txn_spark.sql("ROLLBACK")

    def test_rollback_clears_transaction_state(self, txn_spark, test_ns, catalog_name):
        """ROLLBACK removes all transaction conf keys from the session."""
        ns  = test_ns["ns"]
        cat = catalog_name
        fq  = f"{cat}.{ns}.orders"

        txn_spark.sql("BEGIN TRANSACTION")
        count(txn_spark, fq)  # trigger lazy beginTransaction so txn_id is set

        txn_id_before = txn_spark.conf_get(
            f"spark.sql.catalog.{cat}.active_txn_id"
        )
        assert txn_id_before is not None, "active_txn_id should be set after first SELECT"

        txn_spark.sql("ROLLBACK")

        txn_id_after = txn_spark.conf_get(
            f"spark.sql.catalog.{cat}.active_txn_id"
        )
        assert txn_id_after is None, (
            f"active_txn_id should be cleared after ROLLBACK, got: {txn_id_after}"
        )

    def test_explicit_isolation_level_syntax(self, txn_spark, test_ns, catalog_name):
        """BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ is equivalent to the default."""
        try:
            row = txn_spark.sql(
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"
            ).collect()[0]
            assert row["status"] == "Transaction pending -- isolation: REPEATABLE_READ", (
                f"Unexpected status: {row['status']}"
            )
        finally:
            txn_spark.sql("ROLLBACK")

    def test_rollback_before_first_read(self, txn_spark, test_ns, catalog_name):
        """ROLLBACK immediately after BEGIN (before any table access) clears pending state.

        The server-side transaction is only started lazily on the first loadTable call,
        so this tests the PENDING→NONE path without ever going ACTIVE.
        """
        cat = catalog_name

        txn_spark.sql("BEGIN TRANSACTION")
        # No table reads — active_txn_id has never been set (still PENDING)
        txn_spark.sql("ROLLBACK")

        txn_id = txn_spark.conf_get(f"spark.sql.catalog.{cat}.active_txn_id")
        assert txn_id is None, (
            f"active_txn_id should be None after early ROLLBACK, got: {txn_id}"
        )
        isolation = txn_spark.conf_get(f"spark.sql.catalog.{cat}.pending_txn_isolation")
        assert isolation is None, (
            f"pending_txn_isolation should be cleared after ROLLBACK, got: {isolation}"
        )


class TestReadCommittedIsolation:
    """Verify that READ COMMITTED does NOT pin a snapshot."""

    def test_read_committed_sees_concurrent_writes(self, txn_spark, test_ns, catalog_name):
        """In-transaction reads under READ COMMITTED see writes committed after BEGIN."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders    = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"

        snapshot_pinned = threading.Event()
        writer_done     = threading.Event()

        results: dict = {}
        errors:  list = []

        def observer():
            try:
                txn_spark.sql("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
                # First read (no snapshot pinning under READ COMMITTED)
                pre_orders    = count(txn_spark, fq_orders)
                pre_customers = count(txn_spark, fq_customers)
                results["pre"] = (pre_orders, pre_customers)

                snapshot_pinned.set()
                writer_done.wait(timeout=60)

                # In-transaction reads — under READ COMMITTED, must see writer's rows
                in_orders    = count(txn_spark, fq_orders)
                in_customers = count(txn_spark, fq_customers)
                results["in_txn"] = (in_orders, in_customers)

                txn_spark.sql("COMMIT")
            except Exception as exc:
                errors.append(exc)
                snapshot_pinned.set()
                writer_done.set()

        def writer():
            writer_spark = txn_spark.new_session()
            try:
                snapshot_pinned.wait(timeout=60)
                writer_spark.sql(
                    f"INSERT INTO {fq_orders} VALUES (9002, 9, 50.00, 'new')"
                )
                writer_spark.sql(
                    f"INSERT INTO {fq_customers} VALUES (9, 'Zara', 'zara@example.com')"
                )
            except Exception as exc:
                errors.append(exc)
            finally:
                writer_done.set()

        obs_t = threading.Thread(target=observer, daemon=True)
        wrt_t = threading.Thread(target=writer,   daemon=True)
        obs_t.start(); wrt_t.start()
        obs_t.join(timeout=120)
        wrt_t.join(timeout=120)

        if errors:
            raise errors[0]

        pre    = results["pre"]
        in_txn = results["in_txn"]

        assert in_txn == (pre[0] + 1, pre[1] + 1), (
            f"READ COMMITTED should see writer's rows: pre={pre}, in_txn={in_txn}"
        )

    def test_read_committed_rollback_clears_state(self, txn_spark, test_ns, catalog_name):
        """ROLLBACK after a READ COMMITTED transaction clears all session conf keys."""
        ns  = test_ns["ns"]
        cat = catalog_name
        fq  = f"{cat}.{ns}.orders"

        txn_spark.sql("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
        count(txn_spark, fq)  # trigger lazy beginTransaction so active_txn_id is set

        txn_id_before = txn_spark.conf_get(f"spark.sql.catalog.{cat}.active_txn_id")
        assert txn_id_before is not None, "active_txn_id should be set after first SELECT"

        txn_spark.sql("ROLLBACK")

        txn_id_after = txn_spark.conf_get(f"spark.sql.catalog.{cat}.active_txn_id")
        assert txn_id_after is None, (
            f"active_txn_id should be cleared after ROLLBACK, got: {txn_id_after}"
        )


class TestConcurrentIsolation:
    """Verify that independent transactions each maintain their own snapshot."""

    def test_two_concurrent_repeatable_read_transactions(self, txn_spark, test_ns, catalog_name):
        """Two independent REPEATABLE READ sessions each see a consistent snapshot
        unaffected by a writer that inserts between both sessions' BEGIN and their
        in-transaction reads."""
        ns = test_ns["ns"]
        cat = catalog_name
        fq_orders    = f"{cat}.{ns}.orders"
        fq_customers = f"{cat}.{ns}.customers"

        s1_pinned   = threading.Event()
        s2_pinned   = threading.Event()
        writer_done = threading.Event()

        results: dict = {}
        errors:  list = []

        def observer1():
            try:
                txn_spark.sql("BEGIN TRANSACTION")
                pre = (count(txn_spark, fq_orders), count(txn_spark, fq_customers))
                results["s1_pre"] = pre
                s1_pinned.set()
                writer_done.wait(timeout=60)
                results["s1_in"] = (count(txn_spark, fq_orders), count(txn_spark, fq_customers))
                txn_spark.sql("COMMIT")
            except Exception as exc:
                errors.append(exc)
                s1_pinned.set()
                try:
                    txn_spark.sql("ROLLBACK")
                except Exception:
                    pass

        def observer2():
            sess = txn_spark.new_session()
            try:
                sess.sql("BEGIN TRANSACTION")
                pre = (count(sess, fq_orders), count(sess, fq_customers))
                results["s2_pre"] = pre
                s2_pinned.set()
                writer_done.wait(timeout=60)
                results["s2_in"] = (count(sess, fq_orders), count(sess, fq_customers))
                sess.sql("COMMIT")
            except Exception as exc:
                errors.append(exc)
                s2_pinned.set()
                try:
                    sess.sql("ROLLBACK")
                except Exception:
                    pass

        def writer():
            ws = txn_spark.new_session()
            try:
                s1_pinned.wait(timeout=60)
                s2_pinned.wait(timeout=60)
                ws.sql(f"INSERT INTO {fq_orders} VALUES (9003, 9, 1.00, 'new')")
                ws.sql(f"INSERT INTO {fq_customers} VALUES (9, 'Zara', 'zara@example.com')")
            except Exception as exc:
                errors.append(exc)
            finally:
                writer_done.set()

        t1 = threading.Thread(target=observer1, daemon=True)
        t2 = threading.Thread(target=observer2, daemon=True)
        tw = threading.Thread(target=writer,    daemon=True)
        t1.start(); t2.start(); tw.start()
        t1.join(timeout=120); t2.join(timeout=120); tw.join(timeout=120)

        if errors:
            raise errors[0]

        assert results["s1_in"] == results["s1_pre"], (
            f"Session 1 REPEATABLE READ violated: pre={results['s1_pre']}, in={results['s1_in']}"
        )
        assert results["s2_in"] == results["s2_pre"], (
            f"Session 2 REPEATABLE READ violated: pre={results['s2_pre']}, in={results['s2_in']}"
        )


class TestNoCtsHistoryRepeatableRead:
    """Regression tests for the orElseThrow bug.

    When a REPEATABLE_READ transaction sends ?txn_snapshot=<handle> to loadTable and the
    table has no committed_table_snapshots rows visible at that read_point, the handler
    must fall through to the live table — not throw NoSuchTableException.

    The standard test_ns fixture seeds tables via direct Iceberg writes BEFORE any
    transaction is started, so those tables always have CTS rows.  These tests create a
    fresh table *inside* the transaction (or just before, with no prior transaction
    commits) to exercise the zero-CTS path.
    """

    def test_repeatable_read_on_empty_table_does_not_throw(self, txn_spark, catalog_name):
        """loadTable under REPEATABLE_READ on a table with no CTS history must not 404.

        Regression: IcebergCatalogHandler.loadTable used orElseThrow when
        resolveTableMetadataLocation returned Optional.empty(), so a freshly-created table
        (no committed_table_snapshots rows) threw NoSuchTableException instead of falling
        through to the live table.
        """
        ns = f"txn_nocts_{uuid.uuid4().hex[:8]}"
        cat = catalog_name
        fq = f"{cat}.{ns}.fresh"

        txn_spark.sql(f"CREATE NAMESPACE {cat}.{ns}")
        try:
            # Create the table but do NOT insert any rows — no CTS rows exist yet.
            txn_spark.sql(f"""
                CREATE TABLE {fq} (id INT, val STRING) USING iceberg
            """)

            # BEGIN a REPEATABLE_READ transaction and immediately read the empty table.
            # Before the fix this threw NoSuchTableException (HTTP 404) from the handler's
            # orElseThrow; after the fix it falls through to the live table and returns 0.
            txn_spark.sql("BEGIN TRANSACTION")
            try:
                n = txn_spark.sql(f"SELECT count(*) FROM {fq}").collect()[0][0]
                assert n == 0, f"Expected 0 rows in fresh table, got {n}"
            finally:
                txn_spark.sql("ROLLBACK")
        finally:
            try:
                txn_spark.sql(f"DROP TABLE IF EXISTS {fq}")
            except Exception:
                pass
            try:
                txn_spark.sql(f"DROP NAMESPACE IF EXISTS {cat}.{ns}")
            except Exception:
                pass

    def test_repeatable_read_pins_snapshot_after_write_to_previously_empty_table(
        self, txn_spark, catalog_name
    ):
        """Writer inserts into a table that had no CTS rows when the observer began.

        The observer's REPEATABLE_READ snapshot is taken before the writer's insert.
        The observer must see the table as empty (zero CTS rows at snapshot time →
        fall-through to live table's pre-insert state), while the writer's insert is
        invisible inside the transaction and visible after COMMIT.
        """
        ns = f"txn_nocts_{uuid.uuid4().hex[:8]}"
        cat = catalog_name
        fq = f"{cat}.{ns}.fresh"

        txn_spark.sql(f"CREATE NAMESPACE {cat}.{ns}")
        try:
            txn_spark.sql(f"CREATE TABLE {fq} (id INT, val STRING) USING iceberg")

            snapshot_pinned = threading.Event()
            writer_done = threading.Event()
            results: dict = {}
            errors: list = []

            def observer():
                try:
                    txn_spark.sql("BEGIN TRANSACTION")
                    # First read on a zero-CTS table — must not throw.
                    pre = txn_spark.sql(f"SELECT count(*) FROM {fq}").collect()[0][0]
                    results["pre"] = pre
                    snapshot_pinned.set()
                    writer_done.wait(timeout=60)
                    # In-transaction read — must still see pre-writer count.
                    in_txn = txn_spark.sql(f"SELECT count(*) FROM {fq}").collect()[0][0]
                    results["in_txn"] = in_txn
                    txn_spark.sql("COMMIT")
                    # Post-commit — must see writer's row.
                    txn_spark.sql(f"REFRESH TABLE {fq}")
                    results["post"] = txn_spark.sql(
                        f"SELECT count(*) FROM {fq}"
                    ).collect()[0][0]
                except Exception as exc:
                    errors.append(exc)
                    snapshot_pinned.set()
                    writer_done.set()
                    try:
                        txn_spark.sql("ROLLBACK")
                    except Exception:
                        pass

            def writer():
                ws = txn_spark.new_session()
                try:
                    snapshot_pinned.wait(timeout=60)
                    ws.sql(f"INSERT INTO {fq} VALUES (1, 'after-snapshot')")
                except Exception as exc:
                    errors.append(exc)
                finally:
                    writer_done.set()

            obs_t = threading.Thread(target=observer, daemon=True)
            wrt_t = threading.Thread(target=writer, daemon=True)
            obs_t.start(); wrt_t.start()
            obs_t.join(timeout=120); wrt_t.join(timeout=120)

            if errors:
                raise errors[0]

            assert results["pre"] == 0, f"Table should be empty at BEGIN, got {results['pre']}"
            assert results["in_txn"] == 0, (
                f"REPEATABLE READ violated on zero-CTS table: "
                f"pre={results['pre']}, in_txn={results['in_txn']}"
            )
            assert results["post"] == 1, (
                f"Post-commit count wrong: expected 1, got {results['post']}"
            )
        finally:
            try:
                txn_spark.sql(f"DROP TABLE IF EXISTS {fq}")
            except Exception:
                pass
            try:
                txn_spark.sql(f"DROP NAMESPACE IF EXISTS {cat}.{ns}")
            except Exception:
                pass
