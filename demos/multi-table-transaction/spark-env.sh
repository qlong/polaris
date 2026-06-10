#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# spark-env.sh — Source this file to configure the Spark + Polaris demo environment.
#
# Usage:
#   source demos/multi-table-transaction/spark-env.sh
#   $SPARK_HOME/bin/spark-sql --properties-file spark-defaults.conf
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Spark: use the local Spark source checkout (4.2.0-SNAPSHOT, already built).
# The assembly/target/scala-2.13/jars/ directory contains all required JARs.
# ---------------------------------------------------------------------------
export SPARK_HOME="${SPARK_HOME:-${HOME}/opensources/spark}"

# ---------------------------------------------------------------------------
# Iceberg runtime jar (fat jar, includes all Iceberg dependencies for Spark).
# Spark 4.1 runtime jar is forward-compatible with Spark 4.2.
# ---------------------------------------------------------------------------
export ICEBERG_JAR="${ICEBERG_JAR:-${HOME}/opensources/iceberg/spark/v4.1/spark-runtime/build/libs/iceberg-spark-runtime-4.1_2.13-1.12.0-SNAPSHOT.jar}"

# ---------------------------------------------------------------------------
# Polaris Spark plugin jar (provides SparkCatalog facade + BEGIN/COMMIT/ROLLBACK)
# ---------------------------------------------------------------------------
export POLARIS_PLUGIN_JAR="${POLARIS_PLUGIN_JAR:-${HOME}/opensources/polaris/plugins/spark/v3.5/spark/build/2.13/libs/polaris-spark-3.5_2.13-1.4.0-SNAPSHOT-bundle.jar}"

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
