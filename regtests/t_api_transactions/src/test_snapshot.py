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

The snapshot is returned as an opaque string handle (``txn_snapshot``).  The
handle encodes a read_point (a commit-order cutoff) so that all loadTable calls
with the same handle see the same logical timestamp, regardless of concurrent
writers.  Handles are reclaimed purely by TTL — there is no server-side DELETE
or renew for standalone read handles.
"""


class TestTransactionSnapshotCreate:
    """Create lifecycle."""

    def test_create_returns_required_fields(self, snapshot):
        """POST /transaction-snapshots returns a non-empty opaque handle."""
        handle = snapshot.get("txn_snapshot")
        assert handle, "txn_snapshot must be a non-empty string"
        assert isinstance(handle, str)

    def test_read_point_is_non_negative(self, snapshot):
        """read_point (informational) is a non-negative commit-order cutoff."""
        assert snapshot["read_point"] >= 0

    def test_successive_creates_return_valid_handles(self, client):
        """Two successive snapshot creates both return well-formed opaque handles.

        Handles encode a read_point; if the commit clock has not advanced between
        two requests they may be identical (same read_point, same issued_at second)
        — that is correct behavior, not a collision.  What must hold is that each
        response carries a non-empty, URL-safe base64 string.
        """
        import re
        r1 = client.post("/transaction-snapshots", {"lease_seconds": 60})
        r2 = client.post("/transaction-snapshots", {"lease_seconds": 60})
        r1.raise_for_status()
        r2.raise_for_status()
        h1 = r1.json()["txn_snapshot"]
        h2 = r2.json()["txn_snapshot"]
        url_safe_b64 = re.compile(r'^[A-Za-z0-9_-]+$')
        assert h1 and url_safe_b64.match(h1), f"h1 is not a valid base64url handle: {h1!r}"
        assert h2 and url_safe_b64.match(h2), f"h2 is not a valid base64url handle: {h2!r}"


class TestSnapshotConsistentRead:
    """Load table(s) at a pinned snapshot — verifies consistent read semantics."""

    def test_load_table_at_snapshot(self, client, snapshot, namespace, table_orders):
        """GET /namespaces/{ns}/tables/{t}?txn_snapshot={handle} returns metadata-location."""
        handle = snapshot["txn_snapshot"]
        resp = client.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_orders}",
            params={"txn_snapshot": handle},
        )
        assert resp.status_code == 200
        assert resp.json().get("metadata-location"), "metadata-location must be non-empty"

    def test_two_tables_succeed_at_same_snapshot(
        self, client, snapshot, namespace, table_orders, table_customers
    ):
        """Both tables load successfully at the same snapshot handle.

        The server resolves both using the same visibility window — the pinned
        snapshot guarantees a consistent cross-table read.
        """
        handle = snapshot["txn_snapshot"]
        r1 = client.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_orders}",
            params={"txn_snapshot": handle},
        )
        r2 = client.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_customers}",
            params={"txn_snapshot": handle},
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

    def test_malformed_handle_returns_400(self, client, namespace, table_orders):
        """A malformed handle must be rejected fail-closed (400), never a torn read."""
        resp = client.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_orders}",
            params={"txn_snapshot": "not-a-real-handle!!!"},
        )
        assert resp.status_code == 400, (
            f"expected 400 for malformed handle, got {resp.status_code}: {resp.text}"
        )


class TestSnapshotExportableHandle:
    """The exportable-handle contract behind cross-engine consumption.

    A TxnSnapshot handle is minted once and handed to any number of independent
    engines (Spark ``USE SNAPSHOT <handle>`` and Trino
    ``SET SESSION polaris_read_snapshot='<handle>'`` in the demo).  These tests
    assert the properties those consumers rely on: the handle is not bound to the
    creating session, it resolves the SAME metadata-location on every load, and
    every engine holding it sees one consistent read point.
    """

    def test_snapshot_consumable_by_independent_client(
        self, polaris_host, catalog_name, credentials, snapshot, namespace, table_orders
    ):
        """A handle minted by one client is consumable by a fresh client/token.

        The handle is a bare read_point reference — it carries no session affinity,
        which is exactly what lets a coordinator hand it to other engines.
        """
        from polaris_client import PolarisClient

        other = PolarisClient(host=polaris_host, catalog=catalog_name, credentials=credentials)
        handle = snapshot["txn_snapshot"]
        resp = other.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_orders}",
            params={"txn_snapshot": handle},
        )
        assert resp.status_code == 200, f"independent client load failed: {resp.text}"
        assert resp.json().get("metadata-location")

    def test_pinned_load_is_stable_across_repeated_reads(
        self, client, snapshot, namespace, table_orders
    ):
        """Loading the same table at the same snapshot twice resolves the same location.

        This is the invariant a pinned reader depends on: the read_point does not
        drift between calls, so repeated queries in an attached session are stable.
        """
        handle = snapshot["txn_snapshot"]
        params = {"txn_snapshot": handle}
        loc1 = client.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_orders}", params=params
        ).json()["metadata-location"]
        loc2 = client.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_orders}", params=params
        ).json()["metadata-location"]
        assert loc1 == loc2, f"pinned metadata-location drifted: {loc1} != {loc2}"

    def test_two_engines_same_handle_resolve_identically(
        self, client, polaris_host, catalog_name, credentials, snapshot, namespace, table_orders
    ):
        """Two independent clients holding the same handle resolve the identical location.

        This is the cross-engine parity the demo shows on screen: Reader S (Spark)
        and Reader T (Trino) return the same view because they share one read_point.
        Here two independent PolarisClients stand in for the two engines.
        """
        from polaris_client import PolarisClient

        handle = snapshot["txn_snapshot"]
        params = {"txn_snapshot": handle}

        engine_a = client
        engine_b = PolarisClient(host=polaris_host, catalog=catalog_name, credentials=credentials)

        loc_a = engine_a.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_orders}", params=params
        ).json()["metadata-location"]
        loc_b = engine_b.iceberg_get(
            f"/namespaces/{namespace}/tables/{table_orders}", params=params
        ).json()["metadata-location"]

        assert loc_a == loc_b, (
            f"two engines at the same snapshot disagree: A={loc_a}, B={loc_b}"
        )
