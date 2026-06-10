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

"""pytest fixtures for Polaris transaction REST API integration tests.

Prerequisites:
  1. Polaris running with polaris.persistence.type=relational-jdbc
     (start with demos/multi-table-transaction/start-polaris.sh)
  2. Tables seeded:
     (run demos/multi-table-transaction/setup_spark_tables.sh)
  3. Environment variables (or source spark-env.sh):
       POLARIS_HOST        default: http://localhost:8181
       POLARIS_CATALOG     default: demo_catalog
       POLARIS_CREDENTIALS default: root:s3cr3t
       NAMESPACE           default: demo_ns
       TABLE_ORDERS        default: orders
       TABLE_CUSTOMERS     default: customers

Run:
  source demos/multi-table-transaction/spark-env.sh
  python -m pytest regtests/t_api_transactions/src/ -v
"""

import os

import pytest

from polaris_client import PolarisClient


# ---------------------------------------------------------------------------
# Session-scoped: connection parameters
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def polaris_host() -> str:
    return os.getenv("POLARIS_HOST", "http://localhost:8181")


@pytest.fixture(scope="session")
def credentials() -> str:
    return os.getenv("POLARIS_CREDENTIALS", "root:s3cr3t")


@pytest.fixture(scope="session")
def catalog_name() -> str:
    return os.getenv("POLARIS_CATALOG", "demo_catalog")


@pytest.fixture(scope="session")
def namespace() -> str:
    return os.getenv("NAMESPACE", "demo_ns")


@pytest.fixture(scope="session")
def table_orders() -> str:
    return os.getenv("TABLE_ORDERS", "orders")


@pytest.fixture(scope="session")
def table_customers() -> str:
    return os.getenv("TABLE_CUSTOMERS", "customers")


@pytest.fixture(scope="session")
def client(polaris_host, catalog_name, credentials) -> PolarisClient:
    """Single PolarisClient for the entire test session (token cached)."""
    return PolarisClient(host=polaris_host, catalog=catalog_name, credentials=credentials)


# ---------------------------------------------------------------------------
# Function-scoped: managed transaction (auto-aborted on teardown)
# ---------------------------------------------------------------------------

@pytest.fixture
def txn(client) -> dict:
    """Begin a REPEATABLE_READ transaction; yield its response dict; abort on teardown.

    Tests that commit or abort the transaction explicitly are safe — the teardown
    abort silently ignores 4xx responses from an already-finished transaction.
    """
    resp = client.post("/transactions", {"isolation": "REPEATABLE_READ", "lease_seconds": 300})
    resp.raise_for_status()
    data = resp.json()
    yield data
    try:
        client.post(
            f"/transactions/{data['txn_id']}/abort",
            extra_headers={"X-Txn-Token": data["txn_token"]},
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Function-scoped: managed snapshot (auto-released on teardown)
# ---------------------------------------------------------------------------

@pytest.fixture
def snapshot(client) -> dict:
    """Create a TxnSnapshot; yield its response dict; release on teardown."""
    resp = client.post("/transaction-snapshots", {"lease_seconds": 60})
    resp.raise_for_status()
    data = resp.json()
    yield data
    try:
        client.delete(f"/transaction-snapshots/{data['txn_snapshot_id']}")
    except Exception:
        pass
