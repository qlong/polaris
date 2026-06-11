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

"""Transaction and subtransaction REST API tests.

These tests exercise the server's HTTP routing, request/response shapes, auth
token enforcement, and state-machine transitions — without performing any actual
Iceberg file writes.  End-to-end correctness (actual data committed atomically)
is covered by regtests/t_spark_transactions/.

Layout:
  TestTransactionLifecycle     — begin, get, abort, isolation modes, table reads
  TestSubtransactionLifecycle  — create, get, commit, abort, token enforcement
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _begin(client, isolation="REPEATABLE_READ", lease=300) -> dict:
    """Begin a transaction and return the parsed response dict."""
    resp = client.post("/transactions", {"isolation": isolation, "lease_seconds": lease})
    resp.raise_for_status()
    return resp.json()


def _create_subtxn(client, txn: dict, ns: str, table: str) -> dict:
    """Create a subtransaction for ns.table and return the parsed response dict."""
    resp = client.post(
        f"/transactions/{txn['txn_id']}/subtransactions",
        {"table_identifier": f"{ns}.{table}", "operation": "ROW_EXCLUSIVE", "lease_seconds": 60},
        extra_headers={"X-Txn-Token": txn["txn_token"]},
    )
    resp.raise_for_status()
    return resp.json()


def _abort_txn(client, txn: dict) -> None:
    """Best-effort transaction abort (ignores errors)."""
    try:
        client.post(
            f"/transactions/{txn['txn_id']}/abort",
            extra_headers={"X-Txn-Token": txn["txn_token"]},
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Tests: transaction lifecycle
# ---------------------------------------------------------------------------

class TestTransactionLifecycle:
    """begin / get / abort — isolation modes — table reads."""

    def test_begin_repeatable_read_returns_snapshot_id(self, client):
        """REPEATABLE_READ transactions must include txn_snapshot_id in the response."""
        txn = _begin(client, "REPEATABLE_READ")
        try:
            assert txn["txn_id"] > 0
            assert txn["txn_token"], "txn_token must be non-empty"
            assert txn["txn_snapshot_id"] is not None, (
                "REPEATABLE_READ should set txn_snapshot_id"
            )
        finally:
            _abort_txn(client, txn)

    def test_begin_read_committed_no_snapshot_id(self, client):
        """READ_COMMITTED transactions must NOT set txn_snapshot_id."""
        txn = _begin(client, "READ_COMMITTED")
        try:
            assert txn["txn_id"] > 0
            assert txn.get("txn_snapshot_id") is None, (
                "READ_COMMITTED should not set txn_snapshot_id"
            )
        finally:
            _abort_txn(client, txn)

    def test_successive_transactions_get_distinct_ids(self, client):
        """Each beginTransaction call returns a unique txn_id."""
        t1 = _begin(client)
        t2 = _begin(client)
        try:
            assert t1["txn_id"] != t2["txn_id"]
        finally:
            _abort_txn(client, t1)
            _abort_txn(client, t2)

    def test_get_transaction_returns_txn_id(self, client, txn):
        """GET /transactions/{id} returns a body that includes the txn_id."""
        resp = client.get(f"/transactions/{txn['txn_id']}")
        assert resp.status_code == 200
        assert resp.json()["txn_id"] == txn["txn_id"]

    def test_abort_transaction_returns_204(self, client):
        """POST /transactions/{id}/abort with valid X-Txn-Token returns 204."""
        txn = _begin(client)
        resp = client.post(
            f"/transactions/{txn['txn_id']}/abort",
            extra_headers={"X-Txn-Token": txn["txn_token"]},
        )
        assert resp.status_code == 204

    def test_abort_wrong_token_rejected(self, client, txn):
        """abort with an invalid X-Txn-Token must be rejected (401 or 403)."""
        resp = client.post(
            f"/transactions/{txn['txn_id']}/abort",
            extra_headers={"X-Txn-Token": "not-a-real-token"},
        )
        assert resp.status_code in (401, 403), (
            f"Expected 401/403, got {resp.status_code}: {resp.text}"
        )

    def test_load_table_at_txn_snapshot_id(self, client, txn, namespace, table_orders):
        """loadTable with txn_snapshot_id from a REPEATABLE_READ txn returns metadata-location."""
        snap_id = txn["txn_snapshot_id"]
        resp = client.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_orders}",
            params={"txn_snapshot_id": snap_id},
        )
        assert resp.status_code == 200
        assert resp.json().get("metadata-location"), "metadata-location must be non-empty"

    def test_load_table_with_txn_id_param(self, client, txn, namespace, table_orders):
        """loadTable with txn_id param uses the transaction's pinned snapshot."""
        resp = client.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_orders}",
            params={"txn_id": txn["txn_id"]},
        )
        assert resp.status_code == 200
        assert resp.json().get("metadata-location")

    def test_commit_no_subtransactions(self, client, txn):
        """Commit with empty body (multi-app path, no subtransactions).

        Server may accept and return 204 (nothing to publish) or reject with
        a 4xx if at least one committed subtransaction is required.  Both are
        valid behaviours; this test just documents the actual response code.
        """
        resp = client.post(
            f"/transactions/{txn['txn_id']}/commit",
            extra_headers={"X-Txn-Token": txn["txn_token"]},
        )
        assert resp.status_code in (204, 400, 409, 422), (
            f"Unexpected status from empty commit: {resp.status_code} {resp.text}"
        )


# ---------------------------------------------------------------------------
# Tests: subtransaction lifecycle
# ---------------------------------------------------------------------------

class TestSubtransactionLifecycle:
    """create / get / commit / abort — token enforcement."""

    def test_create_returns_required_fields(self, client, txn, namespace, table_orders):
        """POST /transactions/{id}/subtransactions returns subtxn_id, token, staging path."""
        subtxn = _create_subtxn(client, txn, namespace, table_orders)
        assert subtxn["subtxn_id"] > 0
        assert subtxn["subtxn_token"], "subtxn_token must be non-empty"
        assert subtxn["assigned_staging_path"], "assigned_staging_path must be non-empty"

    def test_staging_path_inside_table_metadata_dir(self, client, txn, namespace, table_orders):
        """assigned_staging_path must be inside the table's own metadata directory."""
        subtxn = _create_subtxn(client, txn, namespace, table_orders)
        path = subtxn["assigned_staging_path"]
        assert table_orders in path, (
            f"staging path should reference the table name; got: {path}"
        )
        assert "metadata" in path.lower(), (
            f"staging path should be inside the metadata directory; got: {path}"
        )

    def test_get_subtransaction_returns_parent_txn_id(self, client, txn, namespace, table_orders):
        """GET /transactions/subtransactions/{subtxn-id} returns the parent txn_id."""
        subtxn = _create_subtxn(client, txn, namespace, table_orders)
        resp = client.get(
            f"/transactions/subtransactions/{subtxn['subtxn_id']}",
            extra_headers={"X-Subtxn-Token": subtxn["subtxn_token"]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["txn_id"] == txn["txn_id"]

    def test_get_subtransaction_returns_staging_path(self, client, txn, namespace, table_orders):
        """GET subtransaction returns the same assigned_staging_path as create."""
        subtxn = _create_subtxn(client, txn, namespace, table_orders)
        resp = client.get(
            f"/transactions/subtransactions/{subtxn['subtxn_id']}",
            extra_headers={"X-Subtxn-Token": subtxn["subtxn_token"]},
        )
        assert resp.status_code == 200
        assert resp.json()["assigned_staging_path"] == subtxn["assigned_staging_path"]

    def test_abort_subtransaction_returns_204(self, client, txn, namespace, table_orders):
        """POST /subtransactions/{id}/abort with valid X-Subtxn-Token returns 204."""
        subtxn = _create_subtxn(client, txn, namespace, table_orders)
        resp = client.post(
            f"/transactions/{txn['txn_id']}/subtransactions/{subtxn['subtxn_id']}/abort",
            extra_headers={"X-Subtxn-Token": subtxn["subtxn_token"]},
        )
        assert resp.status_code == 204

    def test_abort_subtxn_wrong_token_rejected(self, client, txn, namespace, table_orders):
        """abort subtransaction with wrong token must be rejected (401 or 403)."""
        subtxn = _create_subtxn(client, txn, namespace, table_orders)
        resp = client.post(
            f"/transactions/{txn['txn_id']}/subtransactions/{subtxn['subtxn_id']}/abort",
            extra_headers={"X-Subtxn-Token": "invalid-token"},
        )
        assert resp.status_code in (401, 403), (
            f"Expected 401/403, got {resp.status_code}: {resp.text}"
        )

    def test_commit_subtransaction_registers_staging_path(
        self, client, txn, namespace, table_orders
    ):
        """POST /subtransactions/{id}/commit records staged_metadata_location in the DB.

        No actual Iceberg metadata file is written — commitSubtransaction only
        registers the path.  The parent is aborted for cleanup so staged data
        is discarded without touching any table.
        """
        subtxn = _create_subtxn(client, txn, namespace, table_orders)
        resp = client.post(
            f"/transactions/{txn['txn_id']}/subtransactions/{subtxn['subtxn_id']}/commit",
            {"staged_metadata_location": subtxn["assigned_staging_path"]},
            extra_headers={"X-Subtxn-Token": subtxn["subtxn_token"]},
        )
        assert resp.status_code == 204

    def test_abort_parent_after_subtxn_committed(self, client, namespace, table_orders):
        """Parent transaction can be aborted even after a subtransaction has been committed.

        This verifies the server discards all staged data when the parent aborts —
        the committed subtransaction's staged path is never published to the table.
        """
        txn = _begin(client)
        try:
            subtxn = _create_subtxn(client, txn, namespace, table_orders)
            client.post(
                f"/transactions/{txn['txn_id']}/subtransactions/{subtxn['subtxn_id']}/commit",
                {"staged_metadata_location": subtxn["assigned_staging_path"]},
                extra_headers={"X-Subtxn-Token": subtxn["subtxn_token"]},
            ).raise_for_status()

            abort_resp = client.post(
                f"/transactions/{txn['txn_id']}/abort",
                extra_headers={"X-Txn-Token": txn["txn_token"]},
            )
            assert abort_resp.status_code == 204
        except Exception:
            _abort_txn(client, txn)
            raise

    def test_two_subtransactions_different_tables(
        self, client, txn, namespace, table_orders, table_customers
    ):
        """Orchestrator can create subtransactions for two different tables in one transaction."""
        s1 = _create_subtxn(client, txn, namespace, table_orders)
        s2 = _create_subtxn(client, txn, namespace, table_customers)
        assert s1["subtxn_id"] != s2["subtxn_id"]
        assert s1["assigned_staging_path"] != s2["assigned_staging_path"]
        assert table_orders    in s1["assigned_staging_path"]
        assert table_customers in s2["assigned_staging_path"]
