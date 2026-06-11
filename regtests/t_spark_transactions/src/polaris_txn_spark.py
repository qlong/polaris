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

"""Spark session helper for multi-table transaction tests.

Uses org.apache.polaris.spark.SparkCatalog (the Polaris plugin) and registers
PolarisSqlExtensions so BEGIN TRANSACTION / CREATE SUBTRANSACTION / etc. are
recognised as SQL statements.

Designed to be used as a context manager (with statement).  Call new_session()
to get an independent session that shares the same SparkContext but starts with
clean transaction conf — suitable for writer / worker roles.
"""

import os

from pyspark.sql import SparkSession

# Stub JAR: org.apache.spark.sql.connector.catalog.View.
# Spark 5.0 removed this interface; Iceberg 4.1's SparkView implements it and
# is bundled in the Polaris plugin JAR. Without the stub, catalog init raises
# NoClassDefFoundError even without IcebergSparkSessionExtensions registered.
_SPARK_VIEW_STUB_JAR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "libs",
    "spark-view-stub.jar",
)


class PolarisTxnSparkSession:
    """Spark session configured for Polaris multi-table transaction tests."""

    def __init__(
        self,
        polaris_catalog_url: str,
        credentials: str,
        catalog_name: str,
        plugin_jar: str,
        iceberg_jar: str,
    ):
        self._polaris_catalog_url = polaris_catalog_url
        self._credentials = credentials
        self._catalog_name = catalog_name
        self._plugin_jar = plugin_jar
        self._iceberg_jar = iceberg_jar
        self._spark: SparkSession | None = None

    # ------------------------------------------------------------------
    # Context-manager protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> "PolarisTxnSparkSession":
        cat = self._catalog_name
        plugin = "org.apache.polaris.spark.SparkCatalog"
        builder = (
            SparkSession.builder.appName("polaris-txn-test")
            # Local JARs — plugin must be built before running tests.
            # spark-view-stub.jar: stub View interface, removed in Spark 5.0.
            .config(
                "spark.jars",
                ",".join(
                    [self._iceberg_jar, self._plugin_jar, _SPARK_VIEW_STUB_JAR]
                ),
            )
            # Polaris plugin catalog
            .config(f"spark.sql.catalog.{cat}", plugin)
            .config(
                f"spark.sql.catalog.{cat}.uri",
                self._polaris_catalog_url,
            )
            .config(f"spark.sql.catalog.{cat}.warehouse", cat)
            .config(
                f"spark.sql.catalog.{cat}.credential",
                self._credentials,
            )
            .config(f"spark.sql.catalog.{cat}.scope", "PRINCIPAL_ROLE:ALL")
            .config(
                f"spark.sql.catalog.{cat}.token-refresh-enabled", "true"
            )
            # REQUIRED for REPEATABLE READ: disable client-side metadata cache
            .config(f"spark.sql.catalog.{cat}.cache-enabled", "false")
            # SQL extensions: Polaris transaction syntax only.
            # IcebergSparkSessionExtensions omitted — SparkView (bundled in
            # the plugin JAR) implements View which Spark 5.0 removed.
            .config(
                "spark.sql.extensions",
                "org.apache.polaris.spark.PolarisSqlExtensions",
            )
            # Make unqualified names resolve against the Polaris catalog
            .config("spark.sql.defaultCatalog", cat)
            # Tell the SQL parser extension which catalog name to use
            # (default is "polaris"; must match the actual catalog name)
            .config(
                "spark.sql.extensions.polaris.catalog.name", cat
            )
            # No Hive metastore
            .config("spark.sql.catalogImplementation", "in-memory")
            # Keep tests fast
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.master", "local[2]")
        )
        self._spark = builder.getOrCreate()
        self._spark.sparkContext.setLogLevel("WARN")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    # ------------------------------------------------------------------
    # Delegation helpers
    # ------------------------------------------------------------------

    def sql(self, query: str, **kwargs):
        return self._spark.sql(query, **kwargs)

    def conf_get(self, key: str, default=None) -> str | None:
        try:
            return self._spark.conf.get(key)
        except Exception:
            return default

    def new_session(self) -> SparkSession:
        """Return a new SparkSession sharing the same SparkContext.

        The new session starts with clean conf — no active_txn_id / token /
        snapshot_id — making it suitable as a writer or worker session that
        must not inherit the orchestrator's transaction state.
        """
        return self._spark.newSession()

    @property
    def catalog_name(self) -> str:
        return self._catalog_name
