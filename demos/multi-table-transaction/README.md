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

# Multi-table transaction demo

A self-contained local demo of multi-table transactions in Polaris: a Spark writer,
an optional Trino reader, and a Polaris server backed by PostgreSQL.

> **Proof of concept — not production.** Multi-table transactions require the
> `relational-jdbc` persistence backend on **PostgreSQL** (relational schema v5).
> The in-memory persistence stack does not support transactions.

## Prerequisites

Three of the four dependencies are **not** built by this repo and must be supplied by you.
`spark-env.sh` provides defaults for each, but they point at the author's machine layout,
so you will almost certainly need to override them.

| Variable | What it is | Default in `spark-env.sh` |
|---|---|---|
| `SPARK_HOME` | The Spark 4.1.2 distribution (Scala 2.13) | `${HOME}/spark-4.1.2-bin-hadoop3-connect` |
| `ICEBERG_JAR` | Iceberg Spark 4.1 runtime fat jar (released `1.11.0`) | `${HOME}/demo-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar` |
| `TRINO_CLI` | Trino CLI executable jar, built from the Trino fork (only for the Trino reader demos) | `${HOME}/opensources/launch/trino/client/trino-cli/target/trino-cli-483-SNAPSHOT-executable.jar` |
| `POLARIS_PLUGIN_JAR` | Polaris Spark plugin bundle | **built from this repo** — resolved automatically |

### 1. Spark

Download the released **Spark 4.1.2** distribution built for Scala 2.13
(`spark-4.1.2-bin-hadoop3-connect`) and point `SPARK_HOME` at it:

```bash
export SPARK_HOME=/path/to/spark-4.1.2-bin-hadoop3-connect
```

### 2. Iceberg Spark runtime jar

The released **Iceberg 1.11.0** Spark 4.1 runtime works and is on Maven Central, so
just download it — no Iceberg source build required:

```bash
mkdir -p ~/demo-jars
curl -L -o ~/demo-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar
```

That `~/demo-jars/` path is exactly the `ICEBERG_JAR` default in `spark-env.sh`, so no
export is needed. The demo does not depend on anything newer than 1.11. (For reference,
Polaris itself builds against Iceberg 1.10.1 — see `gradle/libs.versions.toml`.)

If you would rather build from an Iceberg checkout, produce the runtime jar and point
`ICEBERG_JAR` at it:

```bash
./gradlew :iceberg-spark:iceberg-spark-runtime-4.1_2.13:shadowJar
export ICEBERG_JAR=$PWD/spark/v4.1/spark-runtime/build/libs/iceberg-spark-runtime-4.1_2.13-*.jar
```

### 3. Polaris server + Spark plugin (both built from this repo)

Two jars come from this Polaris checkout. Build both once, from the repo root, before
running anything below:

```bash
# Polaris server (Quarkus). start-polaris.sh requires this jar and exits if it is missing.
./gradlew :polaris-runtime-service:quarkusBuild

# Spark plugin (SparkCatalog facade + BEGIN/COMMIT/ROLLBACK).
./gradlew :polaris-spark-3.5_2.13:build -x test
```

Both are resolved automatically: `start-polaris.sh` finds the server jar under
`runtime/service/build/`, and `spark-env.sh` finds the plugin jar relative to the repo
root — no overrides needed. If either jar is missing, the script that needs it prints the
exact `./gradlew` command to build it.

### 4. Trino (optional — reader-side isolation demos)

Unlike Iceberg, Trino **cannot** be a stock release here: the reader-side isolation work
lives in a forked Trino, in the `trino-iceberg` connector. A released Trino server has no
support for pinned read snapshots, so the isolation demos would not demonstrate anything.

Build the fork (branch `lakehouse-transactions`), which produces both the server and the CLI:

```bash
cd ~/opensources/launch/trino
./mvnw -pl client/trino-cli -am -DskipTests package
export TRINO_CLI=$PWD/client/trino-cli/target/trino-cli-483-SNAPSHOT-executable.jar
```

That `~/opensources/launch/trino/...` path is the `TRINO_CLI` default in
`demo_distributable_transaction.sh`, so the export is only needed if your fork lives
elsewhere.

Then start the Trino server from that same build and run the CLI against it:

```bash
java -jar "$TRINO_CLI" --server localhost:8080 \
  --catalog polaris --schema demo_ns --user test
```

Override `TRINO_SERVER` (default `localhost:8080`) if the server is elsewhere.

Two notes on versions:

- The **CLI itself is unmodified** from upstream — the fork changes only the server side
  (`plugin/trino-iceberg`, `core/trino-main`). Building it from the fork is still the
  recommended path because it is guaranteed to match the server's protocol version and it
  comes out of the same Maven build.
- `483-SNAPSHOT` is an **unreleased** dev version (Maven Central's newest `trino-cli` is
  476), so there is no release jar to download for this version. Do not expect to
  substitute a published CLI without checking client/server compatibility.

## Running the demo

Build the two jars from step 3 first, then run these from the repo root:

```bash
# 1. Start PostgreSQL (Docker) + a fresh Polaris server (uses the jar built in step 3)
./demos/multi-table-transaction/start-polaris.sh

# 2. Configure the environment (prints what it resolved)
source ./demos/multi-table-transaction/spark-env.sh

# 3. Seed the tables — idempotent, run before every demo
./demos/multi-table-transaction/setup_spark_tables.sh

# 4. Pick a demo
./demos/multi-table-transaction/demo_distributable_transaction.sh
```

`spark-env.sh` echoes each resolved path when sourced, so check its output before
running a demo — a wrong `SPARK_HOME` or `ICEBERG_JAR` shows up there first.

## Walkthroughs

- [`demo_interactive.md`](demo_interactive.md) — step-by-step interactive session
- [`demo_superpowers.md`](demo_superpowers.md) — the capabilities the demo highlights
- [`demo_trino.sql`](demo_trino.sql) — Trino reader queries

## Notes

- `polaris-server.properties` sets `quarkus.datasource.password=polaris` — a throwaway
  credential for the local Docker PostgreSQL, not a secret.
- `start-polaris.sh` uses a sentinel file (`.polaris_bootstrapped`) so bootstrap
  credentials are only passed on the first run.
- `setup_spark_tables.sh` treats HTTP 500 as idempotent for grant/assignment steps,
  because Polaris returns 500 (not 409) for duplicates.
