#!/usr/bin/env bash
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
# ---------------------------------------------------------------------------
# spark-env.sh — Source this file to configure the Spark + Polaris demo environment.
#
# Usage:
#   source demos/multi-table-transaction/spark-env.sh
#   $SPARK_HOME/bin/spark-sql --properties-file spark-defaults.conf
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Spark: use the pre-built spark-4.1.2-bin-hadoop3-connect distribution.
# ---------------------------------------------------------------------------
export SPARK_HOME="${SPARK_HOME:-${HOME}/spark-4.1.2-bin-hadoop3-connect}"

# ---------------------------------------------------------------------------
# Iceberg runtime jar (fat jar, includes all Iceberg dependencies for Spark).
# Spark 4.1 runtime jar is forward-compatible with Spark 4.2.
#
# This default is a locally built 1.12.0-SNAPSHOT, but the demo does not require it:
# the released 1.11.0 Spark 4.1 runtime is on Maven Central and works. See README.md.
# ---------------------------------------------------------------------------
export ICEBERG_JAR="${ICEBERG_JAR:-${HOME}/opensources/iceberg/spark/v4.1/spark-runtime/build/libs/iceberg-spark-runtime-4.1_2.13-1.12.0-SNAPSHOT.jar}"

# ---------------------------------------------------------------------------
# Polaris Spark plugin jar (provides SparkCatalog facade + BEGIN/COMMIT/ROLLBACK)
#
# Unlike Spark and Iceberg above, this jar is built from THIS repo, so it is
# resolved from the script's own location rather than a ${HOME}-relative path.
# A hardcoded absolute path would make a demo run from one checkout silently
# load a jar built from a sibling checkout.
# ---------------------------------------------------------------------------
POLARIS_ENV_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLARIS_REPO_ROOT="$(cd "${POLARIS_ENV_SCRIPT_DIR}/../.." && pwd)"
export POLARIS_PLUGIN_JAR="${POLARIS_PLUGIN_JAR:-${POLARIS_REPO_ROOT}/plugins/spark/v3.5/spark/build/2.13/libs/polaris-spark-3.5_2.13-1.4.0-SNAPSHOT-bundle.jar}"

if [[ ! -f "${POLARIS_PLUGIN_JAR}" ]]; then
  echo "WARNING: Polaris Spark plugin jar not found at:" >&2
  echo "  ${POLARIS_PLUGIN_JAR}" >&2
  echo "Build it with: ./gradlew :polaris-spark-3.5_2.13:build -x test" >&2
  echo "(or set POLARIS_PLUGIN_JAR to an existing bundle jar)" >&2
fi

# ---------------------------------------------------------------------------
# Polaris server
# ---------------------------------------------------------------------------
export POLARIS_HOST="${POLARIS_HOST:-http://localhost:8181}"
export POLARIS_CATALOG="${POLARIS_CATALOG:-demo_catalog}"
export NAMESPACE="${NAMESPACE:-demo_ns}"
export TABLE_ORDERS="${TABLE_ORDERS:-orders}"
export TABLE_CUSTOMERS="${TABLE_CUSTOMERS:-customers}"

# ---------------------------------------------------------------------------
# Fetch a fresh OAuth2 bearer token
# ---------------------------------------------------------------------------
_fetch_token() {
  curl -s -X POST "${POLARIS_HOST}/api/catalog/v1/oauth/tokens" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -d 'grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL' \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])"
}

export POLARIS_TOKEN=$(_fetch_token)
echo "✓ Fetched POLARIS_TOKEN (${POLARIS_TOKEN:0:20}...)"

# ---------------------------------------------------------------------------
# Derived convenience paths
# ---------------------------------------------------------------------------
export POLARIS_BASE="${POLARIS_HOST}/api/catalog/polaris/v1/${POLARIS_CATALOG}"
export ICEBERG_BASE="${POLARIS_HOST}/api/catalog/v1/${POLARIS_CATALOG}"

echo "✓ SPARK_HOME=${SPARK_HOME}"
echo "✓ ICEBERG_JAR=$(basename ${ICEBERG_JAR})"
echo "✓ POLARIS_PLUGIN_JAR=$(basename ${POLARIS_PLUGIN_JAR})"
echo "✓ POLARIS catalog=${POLARIS_CATALOG} host=${POLARIS_HOST}"
