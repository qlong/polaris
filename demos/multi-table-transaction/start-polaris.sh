#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# start-polaris.sh — Kill any running Polaris server, reset the database, and
# start a fresh server.
#
# Every invocation drops and recreates the polaris database so the demo always
# starts from a clean state.  Run setup_spark_tables.sh after this script to
# re-seed the tables.
#
# Usage:
#   cd demos/multi-table-transaction
#   ./start-polaris.sh
#
# The server logs to polaris-server.log in the same directory.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
JAR="${REPO_ROOT}/runtime/service/build/quarkus-app/quarkus-run.jar"
LOG="${SCRIPT_DIR}/polaris-server.log"
CONFIG_DIR="${SCRIPT_DIR}/config"

PG_CONTAINER="polaris-demo-pg"
PG_PORT=5432
PG_DB=polaris
PG_USER=polaris
PG_PASS=polaris

# ---------------------------------------------------------------------------
# Ensure PostgreSQL is running (required for multi-table transaction JDBC)
# ---------------------------------------------------------------------------
# If something is already listening on 5432, use it (don't try to start a new container).
if nc -z localhost "${PG_PORT}" 2>/dev/null; then
  echo "PostgreSQL already reachable on port ${PG_PORT} — skipping container start."
elif docker ps --format "{{.Names}}" 2>/dev/null | grep -q "^${PG_CONTAINER}$"; then
  echo "PostgreSQL container already running."
elif docker ps -a --format "{{.Names}}" 2>/dev/null | grep -q "^${PG_CONTAINER}$"; then
  echo "Restarting stopped PostgreSQL container..."
  docker start "${PG_CONTAINER}" > /dev/null
  echo -n "Waiting for PostgreSQL"
  for i in $(seq 1 30); do
    nc -z localhost "${PG_PORT}" 2>/dev/null && break; echo -n "."; sleep 1
  done; echo ""
else
  echo "Starting PostgreSQL container..."
  docker run -d --name "${PG_CONTAINER}" \
    -p "${PG_PORT}:5432" \
    -e POSTGRES_USER="${PG_USER}" \
    -e POSTGRES_PASSWORD="${PG_PASS}" \
    -e POSTGRES_DB="${PG_DB}" \
    postgres:18.3-alpine > /dev/null
  echo -n "Waiting for PostgreSQL"
  for i in $(seq 1 30); do
    nc -z localhost "${PG_PORT}" 2>/dev/null && break; echo -n "."; sleep 1
  done; echo ""
fi

nc -z localhost "${PG_PORT}" 2>/dev/null || {
  echo "✗ PostgreSQL not reachable on port ${PG_PORT}"
  exit 1
}
echo "✓ PostgreSQL ready on port ${PG_PORT}."

# ---------------------------------------------------------------------------
# Kill existing server (must happen before DB drop to release connections)
# ---------------------------------------------------------------------------
EXISTING=$(pgrep -f "quarkus-run.jar" || true)
if [ -n "${EXISTING}" ]; then
  echo "Stopping existing Polaris server (PID ${EXISTING})..."
  kill "${EXISTING}"
  for i in $(seq 1 20); do
    pgrep -f "quarkus-run.jar" > /dev/null 2>&1 || break
    sleep 0.5
  done
  echo "Stopped."
else
  echo "No running Polaris server found."
fi

# ---------------------------------------------------------------------------
# Drop and recreate the polaris database for a clean slate every run.
# The server bootstraps the schema and root credentials on startup, which is
# idempotent — JdbcMetaStoreManagerFactory.bootstrapRealms skips principal
# creation if the realm is already bootstrapped.
# ---------------------------------------------------------------------------
echo "Recreating database '${PG_DB}'..."
docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d postgres \
  -c "DROP DATABASE IF EXISTS ${PG_DB};" > /dev/null
docker exec "${PG_CONTAINER}" psql -U "${PG_USER}" -d postgres \
  -c "CREATE DATABASE ${PG_DB} OWNER ${PG_USER};" > /dev/null
echo "✓ Database recreated."

# ---------------------------------------------------------------------------
# Verify jar exists
# ---------------------------------------------------------------------------
if [ ! -f "${JAR}" ]; then
  echo "✗ quarkus-run.jar not found at ${JAR}"
  echo "  Build it first: ./gradlew :polaris-runtime-service:quarkusBuild"
  exit 1
fi

# ---------------------------------------------------------------------------
# Quarkus reads config/application.properties relative to the working
# directory. Copy polaris-server.properties into config/ so it is picked up.
# ---------------------------------------------------------------------------
mkdir -p "${CONFIG_DIR}"
cp "${SCRIPT_DIR}/polaris-server.properties" "${CONFIG_DIR}/application.properties"

# ---------------------------------------------------------------------------
# Start server — always pass bootstrap credentials since the DB is always fresh
# ---------------------------------------------------------------------------
# Run Java via mise when available (the maintainer's setup); otherwise fall back
# to whatever `java` is on PATH so the script works on a stock machine.
if command -v mise > /dev/null 2>&1; then
  JAVA_RUNNER=(mise exec -- java)
elif command -v java > /dev/null 2>&1; then
  JAVA_RUNNER=(java)
else
  echo "✗ No Java found. Install a JDK 21+ (or mise) and retry."
  exit 1
fi

echo "Starting Polaris server..."
echo "  JAR:  ${JAR}"
echo "  Log:  ${LOG}"
echo "  Config: ${CONFIG_DIR}/application.properties"

cd "${CONFIG_DIR}/.."   # working dir = demos/multi-table-transaction so config/ is found

POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,root,s3cr3t \
  "${JAVA_RUNNER[@]}" -jar "${JAR}" > "${LOG}" 2>&1 &

SERVER_PID=$!
echo "  PID:  ${SERVER_PID}"

# ---------------------------------------------------------------------------
# Wait for health check
# ---------------------------------------------------------------------------
echo -n "Waiting for server to be ready"
for i in $(seq 1 40); do
  if curl -s -o /dev/null -w "%{http_code}" http://localhost:8182/q/health 2>/dev/null | grep -q "200"; then
    echo ""
    echo "✓ Polaris server ready at http://localhost:8181"
    echo "  Run ./setup_spark_tables.sh to seed the demo tables."
    exit 0
  fi
  echo -n "."
  sleep 1
done

echo ""
echo "✗ Server did not become healthy after 40s — check ${LOG}"
exit 1
