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

"""Thin HTTP client for Polaris transaction REST APIs.

Handles OAuth2 token acquisition (lazy, cached for the session) and provides
helpers for the two base URL prefixes:
  - Polaris extensions: /api/catalog/polaris/v1/{catalog}/...
  - Iceberg standard:   /api/catalog/v1/{catalog}/...
"""

import requests


class PolarisClient:
    """Session-level HTTP client for Polaris transaction and snapshot APIs."""

    def __init__(self, host: str, catalog: str, credentials: str):
        """
        Args:
            host:        Polaris base URL, e.g. "http://localhost:8181"
            catalog:     Catalog name, e.g. "demo_catalog"
            credentials: "client_id:client_secret" string
        """
        self._host = host.rstrip("/")
        self._catalog = catalog
        client_id, client_secret = credentials.split(":", 1)
        self._client_id = client_id
        self._client_secret = client_secret
        self._cached_token: str | None = None

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _get_token(self) -> str:
        if self._cached_token is None:
            resp = requests.post(
                f"{self._host}/api/catalog/v1/oauth/tokens",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "scope": "PRINCIPAL_ROLE:ALL",
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10,
            )
            resp.raise_for_status()
            self._cached_token = resp.json()["access_token"]
        return self._cached_token

    @property
    def _auth(self) -> dict:
        return {"Authorization": f"Bearer {self._get_token()}"}

    # ------------------------------------------------------------------
    # Polaris-extension endpoints  (/api/catalog/polaris/v1/{catalog}/...)
    # ------------------------------------------------------------------

    def _polaris_url(self, path: str) -> str:
        return f"{self._host}/api/catalog/polaris/v1/{self._catalog}{path}"

    def post(self, path: str, body=None, extra_headers: dict | None = None):
        headers = {**self._auth, "Content-Type": "application/json"}
        if extra_headers:
            headers.update(extra_headers)
        return requests.post(self._polaris_url(path), json=body, headers=headers, timeout=10)

    def get(self, path: str, params: dict | None = None, extra_headers: dict | None = None):
        headers = {**self._auth}
        if extra_headers:
            headers.update(extra_headers)
        return requests.get(self._polaris_url(path), params=params, headers=headers, timeout=10)

    def delete(self, path: str, extra_headers: dict | None = None):
        headers = {**self._auth}
        if extra_headers:
            headers.update(extra_headers)
        return requests.delete(self._polaris_url(path), headers=headers, timeout=10)

    # ------------------------------------------------------------------
    # Iceberg-standard endpoints  (/api/catalog/v1/{catalog}/...)
    # ------------------------------------------------------------------

    def _iceberg_url(self, path: str) -> str:
        return f"{self._host}/api/catalog/v1/{self._catalog}{path}"

    def iceberg_get(self, path: str, params: dict | None = None):
        return requests.get(
            self._iceberg_url(path), params=params, headers=self._auth, timeout=10
        )
