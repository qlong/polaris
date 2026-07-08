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

"""TxnSnapshot REST API tests.

TxnSnapshot provides consistent multi-table reads without opening a write
transaction.  Endpoint: POST /transaction-snapshots

The snapshot pins a single read_point (a commit-order cutoff) so that all
loadTable calls with the same txn_snapshot_id see the same logical timestamp,
regardless of concurrent writers.
"""


class TestTransactionSnapshotCRUD:
    """Create / Get / Release / Renew lifecycle."""

    def test_create_returns_required_fields(self, snapshot):
        """POST /transaction-snapshots returns all required fields."""
        assert snapshot["txn_snapshot_id"] > 0
        assert isinstance(snapshot["read_point"], int)
        assert "expires_at" in snapshot

    def test_read_point_is_non_negative(self, snapshot):
        """read_point is a non-negative commit-order cutoff."""
        assert snapshot["read_point"] >= 0

    def test_get_snapshot_returns_same_id(self, client, snapshot):
        """GET /transaction-snapshots/{id} echoes the same txn_snapshot_id."""
        snap_id = snapshot["txn_snapshot_id"]
        resp = client.get(f"/transaction-snapshots/{snap_id}")
        assert resp.status_code == 200
        assert resp.json()["txn_snapshot_id"] == snap_id

    def test_each_create_returns_unique_id(self, client):
        """Two successive snapshots get distinct IDs."""
        r1 = client.post("/transaction-snapshots", {"lease_seconds": 60})
        r2 = client.post("/transaction-snapshots", {"lease_seconds": 60})
        r1.raise_for_status()
        r2.raise_for_status()
        id1 = r1.json()["txn_snapshot_id"]
        id2 = r2.json()["txn_snapshot_id"]
        try:
            assert id1 != id2
        finally:
            client.delete(f"/transaction-snapshots/{id1}")
            client.delete(f"/transaction-snapshots/{id2}")

    def test_release_returns_204(self, client):
        """DELETE /transaction-snapshots/{id} returns 204."""
        resp = client.post("/transaction-snapshots", {"lease_seconds": 60})
        resp.raise_for_status()
        snap_id = resp.json()["txn_snapshot_id"]
        del_resp = client.delete(f"/transaction-snapshots/{snap_id}")
        assert del_resp.status_code == 204

    def test_renew_snapshot(self, client, snapshot):
        """POST /transaction-snapshots/{id}/renew returns 200 with same id."""
        snap_id = snapshot["txn_snapshot_id"]
        resp = client.post(f"/transaction-snapshots/{snap_id}/renew", {"lease_seconds": 600})
        assert resp.status_code == 200
        data = resp.json()
        assert data["txn_snapshot_id"] == snap_id
        assert "expires_at" in data


class TestSnapshotConsistentRead:
    """Load table(s) at a pinned snapshot — verifies consistent read semantics."""

    def test_load_table_at_snapshot(self, client, snapshot, namespace, table_orders):
        """GET /namespaces/{ns}/tables/{t}?txn_snapshot_id={id} returns metadata-location."""
        snap_id = snapshot["txn_snapshot_id"]
        resp = client.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_orders}",
            params={"txn_snapshot_id": snap_id},
        )
        assert resp.status_code == 200
        assert resp.json().get("metadata-location"), "metadata-location must be non-empty"

    def test_two_tables_succeed_at_same_snapshot(
        self, client, snapshot, namespace, table_orders, table_customers
    ):
        """Both tables load successfully at the same snapshot ID.

        The server resolves both using the same visibility window — the pinned
        snapshot guarantees a consistent cross-table read.
        """
        snap_id = snapshot["txn_snapshot_id"]
        r1 = client.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_orders}",
            params={"txn_snapshot_id": snap_id},
        )
        r2 = client.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_customers}",
            params={"txn_snapshot_id": snap_id},
        )
        assert r1.status_code == 200, f"orders load failed: {r1.text}"
        assert r2.status_code == 200, f"customers load failed: {r2.text}"
        assert r1.json()["metadata-location"]
        assert r2.json()["metadata-location"]

    def test_load_without_snapshot_also_works(self, client, namespace, table_orders):
        """Baseline: loadTable without any snapshot param returns 200 (READ COMMITTED)."""
        resp = client.iceberg_get(f"/namespaces/{namespace}/tables/{table_orders}")
        assert resp.status_code == 200
        assert resp.json().get("metadata-location")
