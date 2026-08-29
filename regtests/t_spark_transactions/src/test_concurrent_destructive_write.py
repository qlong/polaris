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

"""Concurrency: destructive (EXCLUSIVE) subtransaction write racing a
concurrent commit to the SAME table.

Design contract (multi_table_transactions plan + TransactionService.prepareCommit
Javadoc): a destructive op (DELETE / OVERWRITE, mapped to an EXCLUSIVE lock) that
races a concurrent commit to the same table is resolved by FIRST-COMMITTER-WINS.
Whichever writer takes/holds the lock first commits; the other is rejected with a
serialization failure (HTTP 409). The system must NEVER let both silently succeed
into a corrupt table (a lost write, duplicate primary keys, or a DELETE/OVERWRITE
that silently degraded to an append).

This is a genuine race, so the test does NOT assert which writer wins — that
depends on lock-acquisition timing and is not part of the contract. In the
interleaving below T1 acquires the EXCLUSIVE lock at CREATE SUBTRANSACTION time,
so T1 typically wins and T2's independent append is the side that 409s; but the
assertions hold for either winner. What IS guaranteed, and what this test pins:

  1. Exactly one writer wins; the loser observes a 409 / commit exception.
  2. No corruption in the final table:
       - no duplicate primary keys (order_id), and
       - if T1's destructive op won, its effect actually took (a DELETE removed
         the rows; an OVERWRITE replaced rather than appended).

Historical bug this guards against (keyspace split — now fixed): locks and
committed-snapshot lookups were keyed by the qualified name (ns.table) on the
transaction layer, but committed_table_snapshots rows and doCommit's
checkConflictAtCommit were keyed by the NUMERIC entity id. The two keyspaces were
disjoint, so both the prepareCommit conflict check and the doCommit EXCLUSIVE-lock
enforcement were inert — every op routed to `direct`, the held EXCLUSIVE lock was
invisible at swap time, and concurrent destructive writes silently corrupted the
table with no 409. Both writers "succeeded"; DELETE no-op'd and OVERWRITE degraded
to an append.

Two scenarios are exercised:

  A. DELETE-vs-concurrent-append (test_delete_races_concurrent_append)
  B. OVERWRITE-vs-concurrent-append (test_overwrite_races_concurrent_append)

Each prints a VERDICT line describing the observed winner, then asserts the two
invariants above.
"""

import re
from typing import NamedTuple


# ---------------------------------------------------------------------------
# Helpers (mirrors test_atomic_write.py conventions)
# ---------------------------------------------------------------------------

class SubtxnInfo(NamedTuple):
    subtxn_id:    int
    subtxn_token: str


def count(spark_session, fq_table: str) -> int:
    return spark_session.sql(f"SELECT count(*) FROM {fq_table}").collect()[0][0]


def rows(spark_session, fq_table: str):
    """Return a sorted list of order_id values for stable comparison. May contain
    duplicates if the table is corrupt (that is exactly what we check for)."""
    refresh(spark_session, fq_table)
    return sorted(
        r[0] for r in spark_session.sql(
            f"SELECT order_id FROM {fq_table}"
        ).collect()
    )


def duplicate_pks(order_ids) -> list:
    """order_ids that appear more than once — a corruption signature."""
    seen, dups = set(), set()
    for oid in order_ids:
        (dups if oid in seen else seen).add(oid)
    return sorted(dups)


def refresh(spark_session, fq_table: str) -> None:
    spark_session.sql(f"REFRESH TABLE {fq_table}")


def parse_subtxn(status: str) -> SubtxnInfo:
    m = re.search(r"subtxn_id=(\d+),\s*subtxn_token=([^,\s]+)", status)
    assert m, f"Cannot parse subtxn info from: {status!r}"
    return SubtxnInfo(subtxn_id=int(m.group(1)), subtxn_token=m.group(2))


def create_subtxn(orchestrator, ns_table: str, lock: str = "") -> SubtxnInfo:
    suffix = f" WITH {lock}" if lock else ""
    result = orchestrator.sql(
        f"CREATE SUBTRANSACTION FOR {ns_table}{suffix}"
    ).collect()
    return parse_subtxn(result[0]["status"])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestConcurrentDestructiveWrite:
    """Race a destructive EXCLUSIVE subtransaction write against a concurrent
    committed append to the same table, and check for lost writes."""

    def test_delete_races_concurrent_append(self, txn_spark, test_ns, catalog_name):
        """Interleave:
             T1 (txn, EXCLUSIVE): DELETE completed orders  (stages against base tip)
             T2 (independent):    INSERT a brand-new order  (races the lock holder)
             T1: COMMIT SUBTRANSACTION -> orchestrator COMMIT

        First-committer-wins: T1 takes the EXCLUSIVE lock at CREATE SUBTRANSACTION,
        so T1 normally wins and T2's append is rejected with a 409; but either
        winner is acceptable. What must hold regardless of winner:
          - exactly one of {T1 destructive commit, T2 append} succeeds; the loser
            raises a 409 / commit exception, and
          - the final table is not corrupt: no duplicate primary keys, and if T1's
            DELETE won, the completed rows are actually gone.
        """
        ns  = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"

        # baseline: 5 orders; completed = {1001, 1003}
        before = rows(txn_spark, fq_orders)
        assert before == [1001, 1002, 1003, 1004, 1005]

        # --- T1: open transaction, allocate EXCLUSIVE subtransaction on orders
        txn_spark.sql("BEGIN TRANSACTION")
        sub = create_subtxn(txn_spark, f"{ns}.orders", lock="EXCLUSIVE")

        worker = txn_spark.new_session()
        worker.sql(
            f"BEGIN SUBTRANSACTION {ns}.orders "
            f"SUBTXN_ID={sub.subtxn_id}, SUBTXN_TOKEN='{sub.subtxn_token}'"
        )
        # destructive write, staged against the base snapshot tip
        worker.sql(f"DELETE FROM {fq_orders} WHERE status = 'completed'")

        # --- T2: concurrent independent append, racing the EXCLUSIVE lock holder.
        # First-committer-wins means this may be REJECTED (T1 holds the lock) — that
        # is a design-correct outcome, not a precondition, so capture rather than assert.
        other = txn_spark.new_session()
        t2_error = None
        try:
            other.sql(f"INSERT INTO {fq_orders} VALUES (7001, 7, 42.00, 'pending')")
        except Exception as exc:  # noqa: BLE001 - the lock holder may block us
            t2_error = exc
        t2_committed = t2_error is None

        # --- T1: now commit the subtransaction and the parent transaction
        commit_error = None
        try:
            worker.sql("COMMIT SUBTRANSACTION")
            txn_spark.sql("COMMIT")
        except Exception as exc:  # noqa: BLE001 - we want to inspect any failure
            commit_error = exc
            # keep the orchestrator session clean for later tests
            try:
                txn_spark.sql("ROLLBACK")
            except Exception:
                pass
        t1_committed = commit_error is None

        final = rows(txn_spark, fq_orders)
        dups = duplicate_pks(final)
        completed_gone = 1001 not in final and 1003 not in final

        print("\n================ VERDICT: DELETE vs concurrent append ================")
        print(f"  baseline order_ids            : {before}")
        print(f"  T2 append committed?          : {t2_committed}  (err={t2_error!r})")
        print(f"  T1 destructive commit ok?     : {t1_committed}  (err={commit_error!r})")
        print(f"  final order_ids               : {final}")
        print(f"  duplicate PKs                 : {dups}")
        if t1_committed and not t2_committed:
            print("  -> T1 (DELETE) won the lock; T2's append 409'd. First-committer-wins.")
        elif t2_committed and not t1_committed:
            print("  -> T2's append won; T1's destructive commit 409'd. First-committer-wins.")
        elif t1_committed and t2_committed:
            print("  -> *** BOTH succeeded: no conflict was enforced. BUG. ***")
        else:
            print("  -> both failed (unexpected).")
        print("======================================================================")

        # Invariant 1: first-committer-wins — exactly one side wins, the other 409s.
        assert t1_committed != t2_committed, (
            "CONFLICT NOT ENFORCED: a destructive EXCLUSIVE write and a concurrent "
            f"append to the same table must not both succeed "
            f"(t1_committed={t1_committed}, t2_committed={t2_committed}). "
            "This is the pre-fix silent-corruption signature."
        )
        # Invariant 2a: no corruption — a committed row must never be duplicated.
        assert not dups, f"CORRUPT TABLE: duplicate primary keys after the race: {dups}"
        # Invariant 2b: if T1's DELETE won, its effect actually took.
        if t1_committed:
            assert completed_gone, (
                "LOST DELETE: T1's destructive commit succeeded but the completed "
                f"rows (1001/1003) are still present: {final}."
            )

    def test_overwrite_races_concurrent_append(self, txn_spark, test_ns, catalog_name):
        """Same race, but T1 performs a full-table OVERWRITE (REPLACE) instead of DELETE.

        First-committer-wins: T1 holds the EXCLUSIVE lock, so T1 normally wins and
        T2's append 409s; either winner is acceptable. Invariants regardless of winner:
          - exactly one side wins; the loser 409s, and
          - no corruption: no duplicate PKs, and if T1's OVERWRITE won it actually
            REPLACED the table (final == exactly the overwrite's single row {8001}),
            not degraded to an append (the historical bug). If T2 won instead, the
            overwrite row (8001) must be absent.
        """
        ns  = test_ns["ns"]
        cat = catalog_name
        fq_orders = f"{cat}.{ns}.orders"

        before = rows(txn_spark, fq_orders)
        assert before == [1001, 1002, 1003, 1004, 1005]

        txn_spark.sql("BEGIN TRANSACTION")
        sub = create_subtxn(txn_spark, f"{ns}.orders", lock="EXCLUSIVE")

        worker = txn_spark.new_session()
        worker.sql(
            f"BEGIN SUBTRANSACTION {ns}.orders "
            f"SUBTXN_ID={sub.subtxn_id}, SUBTXN_TOKEN='{sub.subtxn_token}'"
        )
        # full replace: after this, from T1's view the table is just {8001}
        worker.sql(f"INSERT OVERWRITE {fq_orders} VALUES (8001, 8, 1.00, 'replaced')")

        # concurrent independent append, racing the EXCLUSIVE lock holder (may 409)
        other = txn_spark.new_session()
        t2_error = None
        try:
            other.sql(f"INSERT INTO {fq_orders} VALUES (7002, 7, 42.00, 'pending')")
        except Exception as exc:  # noqa: BLE001 - the lock holder may block us
            t2_error = exc
        t2_committed = t2_error is None

        commit_error = None
        try:
            worker.sql("COMMIT SUBTRANSACTION")
            txn_spark.sql("COMMIT")
        except Exception as exc:  # noqa: BLE001
            commit_error = exc
            try:
                txn_spark.sql("ROLLBACK")
            except Exception:
                pass
        t1_committed = commit_error is None

        final = rows(txn_spark, fq_orders)
        dups = duplicate_pks(final)

        print("\n============== VERDICT: OVERWRITE vs concurrent append ==============")
        print(f"  baseline order_ids            : {before}")
        print(f"  T2 append committed?          : {t2_committed}  (err={t2_error!r})")
        print(f"  T1 overwrite commit ok?       : {t1_committed}  (err={commit_error!r})")
        print(f"  final order_ids               : {final}")
        print(f"  duplicate PKs                 : {dups}")
        if t1_committed and not t2_committed:
            print("  -> T1 (OVERWRITE) won the lock; T2's append 409'd. First-committer-wins.")
        elif t2_committed and not t1_committed:
            print("  -> T2's append won; T1's overwrite 409'd. First-committer-wins.")
        elif t1_committed and t2_committed:
            print("  -> *** BOTH succeeded: OVERWRITE degraded to append / no conflict. BUG. ***")
        else:
            print("  -> both failed (unexpected).")
        print("=====================================================================")

        # Invariant 1: first-committer-wins — exactly one side wins, the other 409s.
        assert t1_committed != t2_committed, (
            "CONFLICT NOT ENFORCED: a destructive OVERWRITE and a concurrent append "
            f"to the same table must not both succeed "
            f"(t1_committed={t1_committed}, t2_committed={t2_committed}). "
            "Both-succeed is the pre-fix bug where OVERWRITE silently degraded to append."
        )
        # Invariant 2a: no corruption — no duplicate primary keys.
        assert not dups, f"CORRUPT TABLE: duplicate primary keys after the race: {dups}"
        # Invariant 2b: the winner's semantics actually took.
        if t1_committed:
            assert final == [8001], (
                "OVERWRITE DEGRADED: T1's OVERWRITE won but did not replace the table "
                f"(expected exactly [8001], got {final}). Silent degrade-to-append."
            )
        else:
            assert 8001 not in final, (
                "GHOST WRITE: T1's overwrite 409'd but its row (8001) is present: "
                f"{final}."
            )
