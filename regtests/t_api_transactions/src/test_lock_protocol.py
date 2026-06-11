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

"""Lock protocol correctness tests for the Polaris transaction REST API.

Tests the compatibility matrix, response shapes, lock release semantics, and
per-resource scope — all via direct HTTP calls with no Iceberg file I/O.

Lock mode compatibility matrix  (rows = held, cols = requested):

               ROW_EXCLUSIVE   EXCLUSIVE   ACCESS_EXCLUSIVE
ROW_EXCLUSIVE       ok            409             409
EXCLUSIVE           409           409             409
ACCESS_EXCLUSIVE    409           409             409

Conflict semantics:
  - EXCLUSIVE or ACCESS_EXCLUSIVE blocked → 409 type="LockConflictError"  (retryable)
  - ROW_EXCLUSIVE blocked by EXCLUSIVE/AX  → 409 type≠"LockConflictError" (fail-fast)
"""

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _begin(client, isolation="REPEATABLE_READ"):
    resp = client.post("/transactions", {"isolation": isolation, "lease_seconds": 300})
    resp.raise_for_status()
    return resp.json()


def _create_subtxn(client, txn, ns, table, operation="ROW_EXCLUSIVE"):
    """POST createSubtransaction; returns the raw Response (caller checks status)."""
    return client.post(
        f"/transactions/{txn['txn_id']}/subtransactions",
        {"table_identifier": f"{ns}.{table}", "operation": operation, "lease_seconds": 60},
        extra_headers={"X-Txn-Token": txn["txn_token"]},
    )


def _abort(client, txn):
    """Best-effort abort — silently ignores errors (txn may already be finished)."""
    try:
        client.post(
            f"/transactions/{txn['txn_id']}/abort",
            extra_headers={"X-Txn-Token": txn["txn_token"]},
        )
    except Exception:
        pass


def _is_retryable_conflict(resp) -> bool:
    """True when a 409 response signals a retryable lock conflict."""
    return resp.json().get("error", {}).get("type") == "LockConflictError"


# ---------------------------------------------------------------------------
# Tests: lock mode echoed in subtransaction response
# ---------------------------------------------------------------------------

class TestLockModeInResponse:
    """createSubtransaction must echo the requested lock mode in the 'operation' field."""

    def test_row_exclusive_mode_in_response(self, client, txn, namespace, table_orders):
        resp = _create_subtxn(client, txn, namespace, table_orders, "ROW_EXCLUSIVE")
        assert resp.status_code == 200
        assert resp.json()["operation"] == "ROW_EXCLUSIVE"

    def test_exclusive_mode_in_response(self, client, txn, namespace, table_orders):
        resp = _create_subtxn(client, txn, namespace, table_orders, "EXCLUSIVE")
        assert resp.status_code == 200
        assert resp.json()["operation"] == "EXCLUSIVE"

    def test_access_exclusive_mode_in_response(self, client, txn, namespace, table_orders):
        resp = _create_subtxn(client, txn, namespace, table_orders, "ACCESS_EXCLUSIVE")
        assert resp.status_code == 200
        assert resp.json()["operation"] == "ACCESS_EXCLUSIVE"


# ---------------------------------------------------------------------------
# Tests: compatibility matrix
# ---------------------------------------------------------------------------

class TestLockCompatibilityMatrix:
    """All nine (held × requested) pairs in the 3×3 compatibility matrix.

    Only ROW_EXCLUSIVE/ROW_EXCLUSIVE is compatible; all other pairs conflict.
    The conflict type (retryable vs fail-fast) depends on the *requested* mode:
      - EXCLUSIVE or AX requested → retryable (LockConflictError)
      - ROW_EXCLUSIVE requested   → fail-fast (no retry hint)
    """

    # ---- Compatible pair -----------------------------------------------

    def test_row_exclusive_compatible_with_row_exclusive(
        self, client, namespace, table_orders
    ):
        """Two concurrent ROW_EXCLUSIVE grants on the same table must both succeed."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            r1 = _create_subtxn(client, t1, namespace, table_orders, "ROW_EXCLUSIVE")
            r2 = _create_subtxn(client, t2, namespace, table_orders, "ROW_EXCLUSIVE")
            assert r1.status_code == 200, f"t1 ROW_EXCLUSIVE failed: {r1.text}"
            assert r2.status_code == 200, f"t2 ROW_EXCLUSIVE failed: {r2.text}"
        finally:
            _abort(client, t1)
            _abort(client, t2)

    # ---- ROW_EXCLUSIVE held → conflicts --------------------------------

    def test_exclusive_retryable_when_row_exclusive_held(
        self, client, namespace, table_orders
    ):
        """EXCLUSIVE blocked by ROW_EXCLUSIVE → 409 LockConflictError (retryable)."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            _create_subtxn(client, t1, namespace, table_orders, "ROW_EXCLUSIVE").raise_for_status()
            resp = _create_subtxn(client, t2, namespace, table_orders, "EXCLUSIVE")
            assert resp.status_code == 409
            assert _is_retryable_conflict(resp), (
                f"EXCLUSIVE blocked by ROW_EXCLUSIVE should be retryable; body={resp.text}"
            )
        finally:
            _abort(client, t1)
            _abort(client, t2)

    def test_access_exclusive_retryable_when_row_exclusive_held(
        self, client, namespace, table_orders
    ):
        """ACCESS_EXCLUSIVE blocked by ROW_EXCLUSIVE → 409 LockConflictError (retryable)."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            _create_subtxn(client, t1, namespace, table_orders, "ROW_EXCLUSIVE").raise_for_status()
            resp = _create_subtxn(client, t2, namespace, table_orders, "ACCESS_EXCLUSIVE")
            assert resp.status_code == 409
            assert _is_retryable_conflict(resp), (
                f"ACCESS_EXCLUSIVE blocked by ROW_EXCLUSIVE should be retryable; body={resp.text}"
            )
        finally:
            _abort(client, t1)
            _abort(client, t2)

    # ---- EXCLUSIVE held → conflicts ------------------------------------

    def test_row_exclusive_fail_fast_when_exclusive_held(
        self, client, namespace, table_orders
    ):
        """ROW_EXCLUSIVE blocked by EXCLUSIVE → 409 fail-fast (no LockConflictError)."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            _create_subtxn(client, t1, namespace, table_orders, "EXCLUSIVE").raise_for_status()
            resp = _create_subtxn(client, t2, namespace, table_orders, "ROW_EXCLUSIVE")
            assert resp.status_code == 409
            assert not _is_retryable_conflict(resp), (
                f"ROW_EXCLUSIVE blocked by EXCLUSIVE should be fail-fast; body={resp.text}"
            )
        finally:
            _abort(client, t1)
            _abort(client, t2)

    def test_exclusive_retryable_when_exclusive_held(
        self, client, namespace, table_orders
    ):
        """EXCLUSIVE blocked by EXCLUSIVE → 409 LockConflictError (retryable)."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            _create_subtxn(client, t1, namespace, table_orders, "EXCLUSIVE").raise_for_status()
            resp = _create_subtxn(client, t2, namespace, table_orders, "EXCLUSIVE")
            assert resp.status_code == 409
            assert _is_retryable_conflict(resp), (
                f"EXCLUSIVE blocked by EXCLUSIVE should be retryable; body={resp.text}"
            )
        finally:
            _abort(client, t1)
            _abort(client, t2)

    def test_access_exclusive_retryable_when_exclusive_held(
        self, client, namespace, table_orders
    ):
        """ACCESS_EXCLUSIVE blocked by EXCLUSIVE → 409 LockConflictError (retryable)."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            _create_subtxn(client, t1, namespace, table_orders, "EXCLUSIVE").raise_for_status()
            resp = _create_subtxn(client, t2, namespace, table_orders, "ACCESS_EXCLUSIVE")
            assert resp.status_code == 409
            assert _is_retryable_conflict(resp), (
                f"ACCESS_EXCLUSIVE blocked by EXCLUSIVE should be retryable; body={resp.text}"
            )
        finally:
            _abort(client, t1)
            _abort(client, t2)

    # ---- ACCESS_EXCLUSIVE held → conflicts ----------------------------

    def test_row_exclusive_fail_fast_when_access_exclusive_held(
        self, client, namespace, table_orders
    ):
        """ROW_EXCLUSIVE blocked by ACCESS_EXCLUSIVE → 409 fail-fast."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            _create_subtxn(client, t1, namespace, table_orders, "ACCESS_EXCLUSIVE").raise_for_status()
            resp = _create_subtxn(client, t2, namespace, table_orders, "ROW_EXCLUSIVE")
            assert resp.status_code == 409
            assert not _is_retryable_conflict(resp), (
                f"ROW_EXCLUSIVE blocked by ACCESS_EXCLUSIVE should be fail-fast; body={resp.text}"
            )
        finally:
            _abort(client, t1)
            _abort(client, t2)

    def test_exclusive_retryable_when_access_exclusive_held(
        self, client, namespace, table_orders
    ):
        """EXCLUSIVE blocked by ACCESS_EXCLUSIVE → 409 LockConflictError (retryable)."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            _create_subtxn(client, t1, namespace, table_orders, "ACCESS_EXCLUSIVE").raise_for_status()
            resp = _create_subtxn(client, t2, namespace, table_orders, "EXCLUSIVE")
            assert resp.status_code == 409
            assert _is_retryable_conflict(resp), (
                f"EXCLUSIVE blocked by ACCESS_EXCLUSIVE should be retryable; body={resp.text}"
            )
        finally:
            _abort(client, t1)
            _abort(client, t2)

    def test_access_exclusive_retryable_when_access_exclusive_held(
        self, client, namespace, table_orders
    ):
        """ACCESS_EXCLUSIVE blocked by ACCESS_EXCLUSIVE → 409 LockConflictError (retryable)."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            _create_subtxn(client, t1, namespace, table_orders, "ACCESS_EXCLUSIVE").raise_for_status()
            resp = _create_subtxn(client, t2, namespace, table_orders, "ACCESS_EXCLUSIVE")
            assert resp.status_code == 409
            assert _is_retryable_conflict(resp), (
                f"ACCESS_EXCLUSIVE blocked by ACCESS_EXCLUSIVE should be retryable; body={resp.text}"
            )
        finally:
            _abort(client, t1)
            _abort(client, t2)


# ---------------------------------------------------------------------------
# Tests: lock release
# ---------------------------------------------------------------------------

class TestLockRelease:
    """Locks must be released when a transaction ends, unblocking subsequent acquirers."""

    def test_exclusive_released_on_txn_abort(self, client, namespace, table_orders):
        """After txn A (EXCLUSIVE) aborts, txn B can acquire EXCLUSIVE on the same table."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            _create_subtxn(client, t1, namespace, table_orders, "EXCLUSIVE").raise_for_status()

            # Confirm conflict while t1 holds the lock
            conflict = _create_subtxn(client, t2, namespace, table_orders, "EXCLUSIVE")
            assert conflict.status_code == 409

            # Abort t1 — releases all its locks
            _abort(client, t1)

            # t2 should now get EXCLUSIVE
            resp = _create_subtxn(client, t2, namespace, table_orders, "EXCLUSIVE")
            assert resp.status_code == 200, (
                f"EXCLUSIVE should be granted after t1 aborted; got {resp.status_code}: {resp.text}"
            )
        finally:
            _abort(client, t1)
            _abort(client, t2)

    def test_access_exclusive_released_on_txn_abort(self, client, namespace, table_orders):
        """After ACCESS_EXCLUSIVE holder aborts, EXCLUSIVE can be acquired."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            _create_subtxn(client, t1, namespace, table_orders, "ACCESS_EXCLUSIVE").raise_for_status()

            conflict = _create_subtxn(client, t2, namespace, table_orders, "EXCLUSIVE")
            assert conflict.status_code == 409

            _abort(client, t1)

            resp = _create_subtxn(client, t2, namespace, table_orders, "EXCLUSIVE")
            assert resp.status_code == 200, (
                f"EXCLUSIVE should succeed after ACCESS_EXCLUSIVE aborted; "
                f"got {resp.status_code}: {resp.text}"
            )
        finally:
            _abort(client, t1)
            _abort(client, t2)

    def test_row_exclusive_released_on_subtxn_commit(
        self, client, namespace, table_orders
    ):
        """ROW_EXCLUSIVE is released when its subtransaction is committed (not when the
        parent transaction ends). After that point, EXCLUSIVE must be grantable."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            # t1 acquires ROW_EXCLUSIVE
            r = _create_subtxn(client, t1, namespace, table_orders, "ROW_EXCLUSIVE")
            r.raise_for_status()
            sub = r.json()

            # Confirm EXCLUSIVE is blocked while ROW_EXCLUSIVE is held
            conflict = _create_subtxn(client, t2, namespace, table_orders, "EXCLUSIVE")
            assert conflict.status_code == 409

            # Commit t1's subtransaction — this releases the ROW_EXCLUSIVE lock
            client.post(
                f"/transactions/{t1['txn_id']}/subtransactions/{sub['subtxn_id']}/commit",
                {"staged_metadata_location": sub["assigned_staging_path"]},
                extra_headers={"X-Subtxn-Token": sub["subtxn_token"]},
            ).raise_for_status()

            # EXCLUSIVE must now be granted for t2
            resp = _create_subtxn(client, t2, namespace, table_orders, "EXCLUSIVE")
            assert resp.status_code == 200, (
                f"EXCLUSIVE should be granted after ROW_EXCLUSIVE subtxn committed; "
                f"got {resp.status_code}: {resp.text}"
            )
        finally:
            _abort(client, t1)
            _abort(client, t2)


# ---------------------------------------------------------------------------
# Tests: lock scope (per-table isolation)
# ---------------------------------------------------------------------------

class TestLockScope:
    """Locks are per-table-resource (table UUID).  A lock on one table must not
    block operations on a different table."""

    def test_exclusive_on_one_table_does_not_block_other_table(
        self, client, namespace, table_orders, table_customers
    ):
        """EXCLUSIVE on table_orders must not block EXCLUSIVE on table_customers."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            _create_subtxn(client, t1, namespace, table_orders, "EXCLUSIVE").raise_for_status()
            resp = _create_subtxn(client, t2, namespace, table_customers, "EXCLUSIVE")
            assert resp.status_code == 200, (
                f"EXCLUSIVE on table_customers should succeed while table_orders is locked; "
                f"got {resp.status_code}: {resp.text}"
            )
        finally:
            _abort(client, t1)
            _abort(client, t2)

    def test_row_exclusive_on_one_table_does_not_block_other_table(
        self, client, namespace, table_orders, table_customers
    ):
        """ROW_EXCLUSIVE on table_orders must not block EXCLUSIVE on table_customers."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            _create_subtxn(client, t1, namespace, table_orders, "ROW_EXCLUSIVE").raise_for_status()
            resp = _create_subtxn(client, t2, namespace, table_customers, "EXCLUSIVE")
            assert resp.status_code == 200, (
                f"EXCLUSIVE on table_customers should not be blocked by ROW_EXCLUSIVE on "
                f"table_orders; got {resp.status_code}: {resp.text}"
            )
        finally:
            _abort(client, t1)
            _abort(client, t2)

    def test_same_txn_can_lock_multiple_tables(
        self, client, txn, namespace, table_orders, table_customers
    ):
        """A single transaction can hold EXCLUSIVE locks on multiple distinct tables."""
        r1 = _create_subtxn(client, txn, namespace, table_orders, "EXCLUSIVE")
        r2 = _create_subtxn(client, txn, namespace, table_customers, "EXCLUSIVE")
        assert r1.status_code == 200, f"EXCLUSIVE on orders failed: {r1.text}"
        assert r2.status_code == 200, f"EXCLUSIVE on customers failed: {r2.text}"
        assert r1.json()["subtxn_id"] != r2.json()["subtxn_id"]
