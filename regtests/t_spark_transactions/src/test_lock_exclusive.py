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

"""EXCLUSIVE lock contention tests.

Verifies the wait-on-conflict behaviour described in the lock protocol design:
  - ROW_EXCLUSIVE is compatible with ROW_EXCLUSIVE (both granted immediately)
  - EXCLUSIVE waits when ROW_EXCLUSIVE is in progress, then is granted once
    the ROW_EXCLUSIVE subtransaction commits/aborts
  - When multiple sessions compete for EXCLUSIVE, exactly one wins and the
    other waits (then succeeds after the winner releases)

All synchronisation uses threading.Event to avoid sleeps.
"""

import re
import threading

import pytest


# ---------------------------------------------------------------------------
# Helpers (mirrored from test_atomic_write.py)
# ---------------------------------------------------------------------------

def count(spark_session, fq_table: str) -> int:
    return spark_session.sql(f"SELECT count(*) FROM {fq_table}").collect()[0][0]


def refresh(spark_session, fq_table: str) -> None:
    spark_session.sql(f"REFRESH TABLE {fq_table}")


def parse_subtxn(status: str):
    m = re.search(r"subtxn_id=(\d+),\s*subtxn_token=([^,\s]+)", status)
    assert m, f"Cannot parse subtxn info from: {status!r}"
    return int(m.group(1)), m.group(2)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestExclusiveLockWait:
    """EXCLUSIVE acquires wait for in-flight ROW_EXCLUSIVE holders."""

    def test_exclusive_waits_for_row_exclusive(self, txn_spark, test_ns, catalog_name):
        """S1 holds ROW_EXCLUSIVE; S2 requests EXCLUSIVE.
        S2 must wait until S1 commits its subtransaction, then S2 is granted."""
        ns  = test_ns["ns"]
        cat = catalog_name
        fq  = f"{cat}.{ns}.orders"
        init = test_ns["init_orders"]

        s1_lock_acquired  = threading.Event()
        s1_commit_ok      = threading.Event()
        s2_got_exclusive  = threading.Event()

        results: dict = {}
        errors:  list = []

        # --- S1: ROW_EXCLUSIVE orchestrator ---
        def s1_thread():
            try:
                txn_spark.sql("BEGIN TRANSACTION")
                row = txn_spark.sql(f"CREATE SUBTRANSACTION FOR {ns}.orders").collect()[0]
                subtxn_id, subtxn_tok = parse_subtxn(row["status"])
                results["s1_subtxn_id"] = subtxn_id
                s1_lock_acquired.set()

                # Wait until S2 is also trying (give it time to hit the server)
                s2_got_exclusive.wait(timeout=30)

                # Commit the ROW_EXCLUSIVE subtransaction — S2 should unblock
                w = txn_spark.new_session()
                w.sql(
                    f"BEGIN SUBTRANSACTION {ns}.orders "
                    f"SUBTXN_ID={subtxn_id}, SUBTXN_TOKEN='{subtxn_tok}'"
                )
                w.sql(f"INSERT INTO {fq} VALUES (8001, 1, 1.00, 'new')")
                w.sql("COMMIT SUBTRANSACTION")
                txn_spark.sql("COMMIT")
                s1_commit_ok.set()
            except Exception as exc:
                errors.append(("s1", exc))
                s1_lock_acquired.set()
                s1_commit_ok.set()

        # --- S2: EXCLUSIVE requester ---
        def s2_thread():
            s2 = txn_spark.new_session()
            try:
                s1_lock_acquired.wait(timeout=30)
                s2.sql("BEGIN TRANSACTION")
                # Signal that S2 is now trying (so S1 can proceed to commit)
                s2_got_exclusive.set()
                # This call blocks (retries) until S1 releases its ROW_EXCLUSIVE lock
                row = s2.sql(
                    f"CREATE SUBTRANSACTION FOR {ns}.orders WITH EXCLUSIVE"
                ).collect()[0]
                subtxn_id, subtxn_tok = parse_subtxn(row["status"])
                results["s2_subtxn_id"] = subtxn_id

                # S2 now holds EXCLUSIVE — do a write and commit
                w = txn_spark.new_session()
                w.sql(
                    f"BEGIN SUBTRANSACTION {ns}.orders "
                    f"SUBTXN_ID={subtxn_id}, SUBTXN_TOKEN='{subtxn_tok}'"
                )
                w.sql(f"INSERT INTO {fq} VALUES (8002, 2, 2.00, 'exclusive')")
                w.sql("COMMIT SUBTRANSACTION")
                s2.sql("COMMIT")
            except Exception as exc:
                errors.append(("s2", exc))
                s2_got_exclusive.set()
            finally:
                try:
                    s2.sql("ROLLBACK")
                except Exception:
                    pass

        t1 = threading.Thread(target=s1_thread, daemon=True)
        t2 = threading.Thread(target=s2_thread, daemon=True)
        t1.start(); t2.start()
        t1.join(timeout=120); t2.join(timeout=120)

        if errors:
            raise errors[0][1]

        assert "s2_subtxn_id" in results, "S2 should have acquired EXCLUSIVE after S1 committed"
        assert results["s1_subtxn_id"] != results["s2_subtxn_id"]

        # Both writes must be visible
        refresh(txn_spark, fq)
        assert count(txn_spark, fq) == init + 2, "Both S1 and S2 writes should be committed"

    def test_two_exclusive_contenders_one_wins(self, txn_spark, test_ns, catalog_name):
        """S1 holds ROW_EXCLUSIVE; S2 and S3 both try EXCLUSIVE.
        Both wait. After S1 commits its subtransaction, one of S2/S3 wins (granted EXCLUSIVE).
        The winner completes; the loser waits for the winner to finish, then also wins.
        Net result: orders table has +3 rows (S1 + S2 + S3 each insert one row)."""
        ns   = test_ns["ns"]
        cat  = catalog_name
        fq   = f"{cat}.{ns}.orders"
        init = test_ns["init_orders"]

        s1_lock_acquired = threading.Event()
        s1_can_commit    = threading.Event()

        results: dict = {"exclusive_grants": []}
        errors:  list = []

        def s1_thread():
            try:
                txn_spark.sql("BEGIN TRANSACTION")
                row = txn_spark.sql(f"CREATE SUBTRANSACTION FOR {ns}.orders").collect()[0]
                subtxn_id, subtxn_tok = parse_subtxn(row["status"])
                s1_lock_acquired.set()

                s1_can_commit.wait(timeout=60)

                w = txn_spark.new_session()
                w.sql(
                    f"BEGIN SUBTRANSACTION {ns}.orders "
                    f"SUBTXN_ID={subtxn_id}, SUBTXN_TOKEN='{subtxn_tok}'"
                )
                w.sql(f"INSERT INTO {fq} VALUES (8010, 1, 1.00, 's1')")
                w.sql("COMMIT SUBTRANSACTION")
                txn_spark.sql("COMMIT")
            except Exception as exc:
                errors.append(("s1", exc))
                s1_lock_acquired.set()
                s1_can_commit.set()

        def exclusive_thread(label: str, insert_val: int):
            sess = txn_spark.new_session()
            try:
                s1_lock_acquired.wait(timeout=60)
                sess.sql("BEGIN TRANSACTION")
                # Signal S1 to commit after both S2 and S3 are trying
                s1_can_commit.set()
                # Retry loop in TransactionManager handles the wait
                row = sess.sql(
                    f"CREATE SUBTRANSACTION FOR {ns}.orders WITH EXCLUSIVE"
                ).collect()[0]
                subtxn_id, subtxn_tok = parse_subtxn(row["status"])
                with threading.Lock():
                    results["exclusive_grants"].append(label)

                w = txn_spark.new_session()
                w.sql(
                    f"BEGIN SUBTRANSACTION {ns}.orders "
                    f"SUBTXN_ID={subtxn_id}, SUBTXN_TOKEN='{subtxn_tok}'"
                )
                w.sql(f"INSERT INTO {fq} VALUES ({insert_val}, 2, 2.00, '{label}')")
                w.sql("COMMIT SUBTRANSACTION")
                sess.sql("COMMIT")
            except Exception as exc:
                errors.append((label, exc))
                s1_can_commit.set()
            finally:
                try:
                    sess.sql("ROLLBACK")
                except Exception:
                    pass

        t1 = threading.Thread(target=s1_thread,                              daemon=True)
        t2 = threading.Thread(target=exclusive_thread, args=("s2", 8011),    daemon=True)
        t3 = threading.Thread(target=exclusive_thread, args=("s3", 8012),    daemon=True)
        t1.start(); t2.start(); t3.start()
        t1.join(timeout=180); t2.join(timeout=180); t3.join(timeout=180)

        if errors:
            raise errors[0][1]

        grants = results["exclusive_grants"]
        assert len(grants) == 2, f"Both S2 and S3 should eventually get EXCLUSIVE; got: {grants}"
        assert set(grants) == {"s2", "s3"}

        refresh(txn_spark, fq)
        assert count(txn_spark, fq) == init + 3, (
            f"S1 + S2 + S3 each inserted one row; expected {init + 3}, "
            f"got {count(txn_spark, fq)}"
        )

    def test_row_exclusive_blocked_by_exclusive(self, txn_spark, test_ns, catalog_name):
        """While S1 holds EXCLUSIVE, S2 trying ROW_EXCLUSIVE must be rejected immediately
        (409 with no retry hint — ROW_EXCLUSIVE does not wait for EXCLUSIVE to be released)."""
        ns  = test_ns["ns"]
        cat = catalog_name

        s1_exclusive_held = threading.Event()
        test_done         = threading.Event()

        errors:  list = []
        results: dict = {}

        def s1_thread():
            try:
                txn_spark.sql("BEGIN TRANSACTION")
                txn_spark.sql(f"CREATE SUBTRANSACTION FOR {ns}.orders WITH EXCLUSIVE")
                s1_exclusive_held.set()
                test_done.wait(timeout=30)
            except Exception as exc:
                errors.append(("s1", exc))
                s1_exclusive_held.set()
            finally:
                try:
                    txn_spark.sql("ROLLBACK")
                except Exception:
                    pass

        def s2_thread():
            s2 = txn_spark.new_session()
            try:
                s1_exclusive_held.wait(timeout=30)
                s2.sql("BEGIN TRANSACTION")
                try:
                    s2.sql(f"CREATE SUBTRANSACTION FOR {ns}.orders")
                    results["s2_succeeded"] = True
                except Exception as exc:
                    results["s2_error"] = str(exc)
            finally:
                test_done.set()
                try:
                    s2.sql("ROLLBACK")
                except Exception:
                    pass

        t1 = threading.Thread(target=s1_thread, daemon=True)
        t2 = threading.Thread(target=s2_thread, daemon=True)
        t1.start(); t2.start()
        t1.join(timeout=60); t2.join(timeout=60)

        if errors:
            raise errors[0][1]

        assert "s2_succeeded" not in results, (
            "ROW_EXCLUSIVE should be rejected while EXCLUSIVE is held, but it succeeded"
        )
        assert "s2_error" in results, "S2 should have received an error"
