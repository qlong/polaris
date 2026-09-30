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
# demo4.sh — Distributable subtransactions (Pillar 4)
#
# Runs the whole Demo 4 choreography in ONE terminal pane, printing each step
# as it goes. Three logical actors, plus a Trino reader:
#
#   [coordinator]      a CONTROL-PLANE actor — three catalog REST calls
#                      (begin, create-subtransaction ×2, commit). It holds the
#                      parent txn_token in a shell variable and hands each
#                      worker a subtxn_id + subtxn_token.
#   [worker:customers] an independent spark-sql process that writes its slice
#   [worker:orders]    an independent spark-sql process that writes its slice
#   [trino]            the reader, showing before/after visibility
#
# Why the coordinator is REST, not SQL: coordination is pure catalog API — no
# data is written by the coordinator, so it needs no Spark session. Doing it as
# curl means the subtxn_id / subtxn_token come back as clean JSON (parsed with
# jq) — no copy/paste, no typing opaque tokens, no scraping a REPL. The workers
# stay Spark, because they write real Iceberg data.
#
# The mental model is still SQL: each curl echoes its `-- equivalent SQL:` line
# (BEGIN TRANSACTION / CREATE SUBTRANSACTION / COMMIT) so the audience reads the
# protocol in the verbs they already know from Demos 1–3.
#
# You keep control of pacing: every step waits for Enter. Step 5 (the parent
# COMMIT) is a deliberate pause so you can narrate and show Trino before/after.
#
# Usage:
#   ./demos/multi-table-transaction/start-polaris.sh      # server + postgres
#   ./demos/multi-table-transaction/setup_spark_tables.sh # seed tables
#   # (start Trino separately, per lakehouse_day_demo.txt)
#   ./demos/multi-table-transaction/demo4.sh
# ---------------------------------------------------------------------------
set -uo pipefail

# --- locate ourselves + shared env -----------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/spark-env.sh"

TRINO_CLI="${TRINO_CLI:-${HOME}/opensources/launch/trino/client/trino-cli/target/trino-cli-483-SNAPSHOT-executable.jar}"
TRINO_SERVER="${TRINO_SERVER:-localhost:8080}"

# Transaction REST base: polaris/v1/{prefix}/transactions, prefix = catalog.
# POLARIS_BASE is exported by spark-env.sh (= .../api/catalog/polaris/v1/${POLARIS_CATALOG}).
TXN_BASE="${POLARIS_BASE}/transactions"

# Values inserted by the workers. Ids are derived from the current epoch second so
# back-to-back runs never collide on a primary key — no reseed needed. (The plan
# says the specific values don't matter; only that a new row appears in each table.)
CUST_KEY=$(( $(date +%s) % 100000 ))
ORDER_KEY=$(( 2000000 + CUST_KEY ))
NEW_CUSTOMER_SQL="INSERT INTO polaris.demo_ns.customers VALUES (${CUST_KEY}, 'jack', 'jack@example.com')"
NEW_ORDER_SQL="INSERT INTO polaris.demo_ns.orders VALUES (${ORDER_KEY}, ${CUST_KEY}, 350.00, 'pending')"

mkdir -p "${SCRIPT_DIR}/logs"

# --- pretty-printing --------------------------------------------------------
# Palette tuned for a LIGHT (white) terminal background: dark, saturated ANSI
# colors read well on white; the bright variants (cyan/green/yellow) wash out.
#   HEAD = dark blue for step headers, ACCENT = dark blue for SQL/curl lines,
#   RED for failures, MAGENTA (bold) for the Enter prompt. DIM greys comments.
BOLD=$'\033[1m'; DIM=$'\033[2m'
HEAD=$'\033[34m'      # blue — step headers
ACCENT=$'\033[34m'    # blue — SQL / curl lines
MAGENTA=$'\033[35m'   # magenta — prompt
GREEN=$'\033[32m'     # dark green — success ("Demo 4 complete")
RED=$'\033[31m'; RST=$'\033[0m'

hr()     { printf '%s\n' "${DIM}────────────────────────────────────────────────────────────${RST}"; }
step()   { echo; printf '%s\n' "${BOLD}${HEAD}━━━ $* ━━━${RST}"; }
# say "[actor]" "text" — the ACTOR LABEL carries the color (magenta + bold), the
# description stays black. Coloring the one repeated token the eye scans for
# ("who's acting now?") gives scannability without painting whole sentences —
# long descriptions no longer become walls of color.
say()    { printf '%s%s%s %s\n' "${BOLD}${MAGENTA}" "$1" "${RST}" "$2"; }
# In Demo 4 the SQL and the REST call are EQUALLY important — the SQL is the
# mental model, the curl is the real protocol you'd call. Both get bold accent so
# neither recedes; the leading label ("equivalent SQL:" vs "$") tells them apart.
sqlc()   { printf '%s    equivalent SQL:  %s%s\n' "${BOLD}${ACCENT}" "$*" "${RST}"; }
curlc()  { printf '%s    $ %s%s\n' "${BOLD}${ACCENT}" "$*" "${RST}"; }
sql()    { printf '%s    %s%s\n' "${BOLD}${ACCENT}" "$*" "${RST}"; }
fail()   { printf '%s    %s%s\n' "${RED}" "$*" "${RST}"; }
# pause [hint] — waits for Enter. The optional hint names the NEXT beat, dim so
# the (frequently repeated) prompt recedes instead of dominating the output.
pause()  {
  if [[ -n "${1:-}" ]]; then
    printf '%s    ↵ next: %s%s' "${DIM}" "$1" "${RST}"
  else
    printf '%s    ↵%s' "${DIM}" "${RST}"
  fi
  read -r _
}

# --- token holder (the coordinator's "session" is just these variables) -----
TXN_ID=""; TXN_TOKEN=""
CUST_ID=""; CUST_TOKEN=""
ORD_ID=""; ORD_TOKEN=""

# jq is used to parse JSON responses; fall back to python3 if absent.
json_get() { # json_get '<json>' '<key>'
  if command -v jq >/dev/null 2>&1; then
    jq -r --arg k "$2" '.[$k] // empty' <<<"$1"
  else
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('$2',''))" <<<"$1"
  fi
}

# ---------------------------------------------------------------------------
# Coordinator REST actions
# ---------------------------------------------------------------------------
coord_begin() {
  say "[coordinator]" "opening the parent transaction (control-plane REST call)."
  sqlc "BEGIN TRANSACTION;"
  curlc "POST ${TXN_BASE}"
  local resp
  resp="$(curl -s -X POST "${TXN_BASE}" \
    -H "Authorization: Bearer ${POLARIS_TOKEN}" \
    -H 'Content-Type: application/json' \
    -d '{"isolation":"REPEATABLE_READ","lease_seconds":1800}')"
  TXN_ID="$(json_get "$resp" txn_id)"
  TXN_TOKEN="$(json_get "$resp" txn_token)"
  if [[ -z "$TXN_ID" || -z "$TXN_TOKEN" ]]; then
    fail "begin failed: $resp"; return 1
  fi
  say "[coordinator]" "txn_id=${TXN_ID}  (parent token held)"
}

# coord_create_subtxn <table-identifier>  -> sets _LAST_ID / _LAST_TOKEN
_LAST_ID=""; _LAST_TOKEN=""
coord_create_subtxn() {
  local table="$1"
  sqlc "CREATE SUBTRANSACTION FOR ${table};"
  curlc "POST ${TXN_BASE}/${TXN_ID}/subtransactions   (X-Txn-Token)"
  local resp
  resp="$(curl -s -X POST "${TXN_BASE}/${TXN_ID}/subtransactions" \
    -H "Authorization: Bearer ${POLARIS_TOKEN}" \
    -H "X-Txn-Token: ${TXN_TOKEN}" \
    -H 'Content-Type: application/json' \
    -d "{\"table_identifier\":\"${table}\",\"operation\":\"ROW_EXCLUSIVE\",\"lease_seconds\":1800}")"
  _LAST_ID="$(json_get "$resp" subtxn_id)"
  _LAST_TOKEN="$(json_get "$resp" subtxn_token)"
  if [[ -z "$_LAST_ID" || -z "$_LAST_TOKEN" ]]; then
    fail "create subtransaction failed: $resp"; return 1
  fi
  say "[coordinator]" "→ ${table}: hand this {subtxn_id, subtxn_token} pair to a worker:"
  printf '        %ssubtxn_id=%s%s\n' "${BOLD}" "${_LAST_ID}" "${RST}"
  printf '        %ssubtxn_token=%s%s\n' "${BOLD}" "${_LAST_TOKEN}" "${RST}"
}

coord_commit() {
  sqlc "COMMIT;"
  curlc "POST ${TXN_BASE}/${TXN_ID}/commit   (X-Txn-Token)"
  local resp http
  resp="$(curl -s -w '\n%{http_code}' -X POST "${TXN_BASE}/${TXN_ID}/commit" \
    -H "Authorization: Bearer ${POLARIS_TOKEN}" \
    -H "X-Txn-Token: ${TXN_TOKEN}" \
    -H 'Content-Type: application/json' \
    -d '{}')"
  http="$(tail -1 <<<"$resp")"
  if [[ "$http" != 2* ]]; then
    fail "commit failed (HTTP $http): $(sed '$d' <<<"$resp")"; return 1
  fi
  say "[coordinator]" "committed (HTTP ${http})."
}

# ---------------------------------------------------------------------------
# A worker: a fresh, independent spark-sql process for one subtransaction.
# Proves independence — its own JVM, needing only subtxn_id + subtxn_token.
#
# Optional 6th arg `read_sql`: a SELECT run AFTER the INSERT but BEFORE
# COMMIT SUBTRANSACTION. This demonstrates read-your-own-write (RYOW) — inside
# its subtransaction the worker sees its own staged row, even though the
# outside world (Trino, Step 4) cannot until the parent commits.
# ---------------------------------------------------------------------------
run_worker() {
  local label="$1" table="$2" subtxn_id="$3" subtxn_token="$4" insert_sql="$5" read_sql="${6:-}"
  local begin_sql="BEGIN SUBTRANSACTION ${table} SUBTXN_ID=${subtxn_id}, SUBTXN_TOKEN='${subtxn_token}'"
  local batch="${begin_sql}; ${insert_sql};"
  say "$label" "spawning an independent spark-sql process…"
  sql "${begin_sql};"
  sql "${insert_sql};"
  if [[ -n "$read_sql" ]]; then
    say "$label" "read-your-own-write: this SELECT runs BEFORE commit — the worker sees its own row."
    sql "${read_sql};"
    batch="${batch} ${read_sql};"
  fi
  sql "COMMIT SUBTRANSACTION;"
  batch="${batch} COMMIT SUBTRANSACTION;"
  "${SPARK_HOME}/bin/spark-sql" \
    --jars "${ICEBERG_JAR},${POLARIS_PLUGIN_JAR}" \
    --properties-file "${SCRIPT_DIR}/spark-defaults.conf" \
    --conf 'spark.sql.shuffle.partitions=2' \
    --conf 'spark.ui.enabled=false' \
    --database demo_ns \
    --master 'local[2]' \
    -e "${batch}" \
    2>>"${SCRIPT_DIR}/logs/demo4-${label//[^a-zA-Z]/}.log" \
    | sed 's/^/    /'
}

# ---------------------------------------------------------------------------
# Trino reader — shows visibility before/after the parent commit.
#
# Instead of counting rows, we SELECT the exact rows the two workers wrote
# (filtered by the ids this run used). Before the parent commit both queries
# return nothing; after, both rows appear together. Seeing the row itself
# blink into existence is clearer than a count ticking up.
# ---------------------------------------------------------------------------
CUST_SELECT="SELECT * FROM customers WHERE customer_id = ${CUST_KEY}"
ORDER_SELECT="SELECT * FROM orders WHERE order_id = ${ORDER_KEY}"

# Run one Trino query; print its rows indented, or a clear "(no rows)" marker.
_trino_one() {
  local query="$1" out
  sql "${query};"
  # ALIGNED prints a header row + rule, so the audience sees column names. That
  # means an empty result is NOT an empty string — it's the header with "(0 rows)"
  # in the footer — so we detect emptiness via the row-count footer, not -z.
  out="$(mise exec -- java -jar "$TRINO_CLI" \
    --server "$TRINO_SERVER" --catalog polaris --schema demo_ns --user test \
    --output-format ALIGNED \
    --execute "${query};" 2>/dev/null)"
  if [[ -z "$out" || "$out" == *"(0 rows)"* ]]; then
    printf '%s    (no rows)%s\n' "${DIM}" "${RST}"
  else
    sed 's/^/    /' <<<"$out"
  fi
}

trino_query() {
  local title="$1"
  say "[trino]" "$title"
  _trino_one "$CUST_SELECT"
  _trino_one "$ORDER_SELECT"
}

# ===========================================================================
# main — reads as the six-step distributed protocol
# ===========================================================================
main() {
  clear
  printf '%s\n' "${BOLD}Demo 4 — Distributable subtransactions${RST}"
  printf '%s\n' "${DIM}A coordinator hands out two subtransactions; two independent workers${RST}"
  printf '%s\n' "${DIM}commit their slices; nothing is visible until the coordinator commits.${RST}"
  hr

  # --- STEP 1: coordinator opens txn + creates two subtransactions ---------
  step "STEP 1: coordinator opens a transaction, creates 2 subtransactions"
  coord_begin || return 1
  coord_create_subtxn "demo_ns.customers" || return 1
  CUST_ID="$_LAST_ID"; CUST_TOKEN="$_LAST_TOKEN"
  coord_create_subtxn "demo_ns.orders" || return 1
  ORD_ID="$_LAST_ID"; ORD_TOKEN="$_LAST_TOKEN"
  pause "worker 'customers' claims its subtransaction and writes"

  # --- STEP 2: worker 1 (customers) does its slice -------------------------
  step "STEP 2: worker 'customers' claims its subtransaction and writes"
  run_worker "[worker:customers]" "demo_ns.customers" "$CUST_ID" "$CUST_TOKEN" "$NEW_CUSTOMER_SQL"
  pause "worker 'orders' writes, then reads its own uncommitted row"

  # --- STEP 3: worker 2 (orders) does its slice, then reads its own write ---
  step "STEP 3: worker 'orders' writes, then reads its own uncommitted row (RYOW)"
  run_worker "[worker:orders]" "demo_ns.orders" "$ORD_ID" "$ORD_TOKEN" "$NEW_ORDER_SQL" "$ORDER_SELECT"
  pause "Trino reads — the new rows should NOT be visible yet"

  # --- STEP 4: Trino — not visible yet -------------------------------------
  step "STEP 4: Trino reader — workers committed their slices, but the parent has NOT"
  trino_query "before parent commit — new rows should be ABSENT"
  pause "the coordinator commits the parent transaction"

  # --- STEP 5: coordinator COMMITS (your beat) -----------------------------
  step "STEP 5: coordinator COMMITS the parent transaction"
  say "[coordinator]" "this is the atomic reveal — press Enter to commit."
  pause "COMMIT — all slices become visible together"
  coord_commit || return 1
  pause "Trino reads again — the new rows should now be visible"

  # --- STEP 6: Trino — now visible -----------------------------------------
  step "STEP 6: Trino reader — parent committed, all slices now visible atomically"
  trino_query "after parent commit — new rows should be PRESENT"

  echo; printf '%s\n' "${BOLD}${GREEN}Demo 4 complete.${RST}"
}

main "$@"
