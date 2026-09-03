/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.spark.rest;

import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonProperty;

/** Response from POST /polaris/v1/{prefix}/transactions (beginTransaction). */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransactionRESTResponse implements RESTResponse {
  @JsonProperty("txn_id")
  private long txnId;

  @JsonProperty("txn_token")
  private String txnToken;

  @JsonProperty("txn_snapshot_id")
  private Long txnSnapshotId;

  @JsonProperty("txn_snapshot")
  private String txnSnapshot;

  public TransactionRESTResponse() {}

  public long txnId() {
    return txnId;
  }

  public String txnToken() {
    return txnToken;
  }

  /** Null for READ_COMMITTED transactions; set for REPEATABLE_READ. */
  public Long txnSnapshotId() {
    return txnSnapshotId;
  }

  /**
   * Opaque, server-minted read-point handle. Null for READ_COMMITTED; set for REPEATABLE_READ. The
   * client stores and echoes it verbatim (never parses it).
   */
  public String txnSnapshot() {
    return txnSnapshot;
  }

  @Override
  public void validate() {}
}
