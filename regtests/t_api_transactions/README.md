<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Transaction REST API Integration Tests

pytest suite that tests the Polaris transaction and snapshot REST APIs directly
via HTTP (`requests`).  No Spark required — just Python and a running server.

These tests cover HTTP routing, request/response shapes, auth token enforcement,
and state-machine transitions.  End-to-end correctness (actual Iceberg data
committed atomically) is covered by `regtests/t_spark_transactions/`.

## Prerequisites

### 1. Running Polaris server (JDBC persistence)

```bash
cd demos/multi-table-transaction
./start-polaris.sh
```

### 2. Tables and catalog set up

```bash
cd demos/multi-table-transaction
./setup_spark_tables.sh
```

### 3. Environment variables

```bash
source demos/multi-table-transaction/spark-env.sh
```

| Variable | Default | Purpose |
|---|---|---|
| `POLARIS_HOST` | `http://localhost:8181` | Polaris server URL |
| `POLARIS_CATALOG` | `demo_catalog` | Catalog name |
| `POLARIS_CREDENTIALS` | `root:s3cr3t` | OAuth `client_id:client_secret` |
| `NAMESPACE` | `demo_ns` | Namespace containing the test tables |
| `TABLE_ORDERS` | `orders` | Orders table name |
| `TABLE_CUSTOMERS` | `customers` | Customers table name |

### 4. Python dependencies

```bash
pip install pytest requests
```

## Running the Tests

```bash
# Load environment, then run
source demos/multi-table-transaction/spark-env.sh
python -m pytest regtests/t_api_transactions/src/ -v
```

One-liner from the repo root:

```bash
source demos/multi-table-transaction/spark-env.sh && \
  python -m pytest regtests/t_api_transactions/src/ -v

# Single file
source demos/multi-table-transaction/spark-env.sh && \
  python -m pytest regtests/t_api_transactions/src/test_snapshot.py -v

# Single test
source demos/multi-table-transaction/spark-env.sh && \
  python -m pytest regtests/t_api_transactions/src/test_transaction.py::TestSubtransactionLifecycle::test_create_returns_required_fields -v
```

No `JAVA_HOME` or `PYTHONPATH` setup needed — these tests use only the standard
`requests` library and talk to the server over HTTP.

## Test Structure

```
src/
  polaris_client.py      thin HTTP wrapper (OAuth token, polaris/iceberg URL prefixes)
  conftest.py            fixtures: client, txn (auto-aborted), snapshot (auto-released)
  test_snapshot.py       TxnSnapshot CRUD and consistent multi-table reads
  test_transaction.py    Transaction lifecycle and subtransaction lifecycle
```

### What is tested

**`test_snapshot.py`** — `POST /transaction-snapshots`:
- Response shape: `txn_snapshot_id`, `read_point`, `expires_at`
- `read_point >= 0` invariant
- `GET` returns same id
- Successive creates return distinct ids
- `DELETE` returns 204
- `POST /{id}/renew` returns updated `expires_at`
- `loadTable` with `?txn_snapshot_id=` returns `metadata-location`
- Two tables loaded at the same snapshot both succeed

**`test_transaction.py`** — `POST /transactions` and subtransaction endpoints:
- `REPEATABLE_READ` includes `txn_snapshot_id`; `READ_COMMITTED` does not
- Successive begins return distinct `txn_id`s
- `GET /transactions/{id}` echoes the id
- `POST /abort` with valid token returns 204; invalid token returns 401/403
- `loadTable` with `?txn_snapshot_id=` and `?txn_id=` both return `metadata-location`
- `POST /subtransactions` returns `subtxn_id`, `subtxn_token`, `assigned_staging_path`
- Staging path is inside the table's own `metadata/` directory
- `GET /subtransactions/{id}` returns parent `txn_id` and matching staging path
- `POST /subtransactions/{id}/abort` returns 204; wrong token returns 401/403
- `POST /subtransactions/{id}/commit` records the staging path (HTTP 204)
- Aborting the parent after `commitSubtransaction` returns 204 (staged data discarded)
- Two subtransactions for different tables get distinct ids and distinct staging paths

## Expected Output (all passing)

```
test_snapshot.py::TestTransactionSnapshotCRUD::test_create_returns_required_fields PASSED
test_snapshot.py::TestTransactionSnapshotCRUD::test_read_point_is_non_negative PASSED
test_snapshot.py::TestTransactionSnapshotCRUD::test_get_snapshot_returns_same_id PASSED
test_snapshot.py::TestTransactionSnapshotCRUD::test_each_create_returns_unique_id PASSED
test_snapshot.py::TestTransactionSnapshotCRUD::test_release_returns_204 PASSED
test_snapshot.py::TestTransactionSnapshotCRUD::test_renew_snapshot PASSED
test_snapshot.py::TestSnapshotConsistentRead::test_load_table_at_snapshot PASSED
test_snapshot.py::TestSnapshotConsistentRead::test_two_tables_succeed_at_same_snapshot PASSED
test_snapshot.py::TestSnapshotConsistentRead::test_load_without_snapshot_also_works PASSED
test_transaction.py::TestTransactionLifecycle::test_begin_repeatable_read_returns_snapshot_id PASSED
test_transaction.py::TestTransactionLifecycle::test_begin_read_committed_no_snapshot_id PASSED
test_transaction.py::TestTransactionLifecycle::test_successive_transactions_get_distinct_ids PASSED
test_transaction.py::TestTransactionLifecycle::test_get_transaction_returns_txn_id PASSED
test_transaction.py::TestTransactionLifecycle::test_abort_transaction_returns_204 PASSED
test_transaction.py::TestTransactionLifecycle::test_abort_wrong_token_rejected PASSED
test_transaction.py::TestTransactionLifecycle::test_load_table_at_txn_snapshot_id PASSED
test_transaction.py::TestTransactionLifecycle::test_load_table_with_txn_id_param PASSED
test_transaction.py::TestTransactionLifecycle::test_commit_no_subtransactions PASSED
test_transaction.py::TestSubtransactionLifecycle::test_create_returns_required_fields PASSED
test_transaction.py::TestSubtransactionLifecycle::test_staging_path_inside_table_metadata_dir PASSED
test_transaction.py::TestSubtransactionLifecycle::test_get_subtransaction_returns_parent_txn_id PASSED
test_transaction.py::TestSubtransactionLifecycle::test_get_subtransaction_returns_staging_path PASSED
test_transaction.py::TestSubtransactionLifecycle::test_abort_subtransaction_returns_204 PASSED
test_transaction.py::TestSubtransactionLifecycle::test_abort_subtxn_wrong_token_rejected PASSED
test_transaction.py::TestSubtransactionLifecycle::test_commit_subtransaction_registers_staging_path PASSED
test_transaction.py::TestSubtransactionLifecycle::test_abort_parent_after_subtxn_committed PASSED
test_transaction.py::TestSubtransactionLifecycle::test_two_subtransactions_different_tables PASSED
```
