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

/** Response from GET /transactions/subtransactions/{subtxn-id}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetSubtransactionRESTResponse implements RESTResponse {

  @JsonProperty("parent_txn_id")
  private long txnId;

  @JsonProperty("txn_snapshot_id")
  private Long txnSnapshotId;

  @JsonProperty("assigned_staging_path")
  private String assignedStagingPath;

  @JsonProperty("table_uuid")
  private String tableUuid;

  public GetSubtransactionRESTResponse() {}

  public long txnId() {
    return txnId;
  }

  public Long txnSnapshotId() {
    return txnSnapshotId;
  }

  public String assignedStagingPath() {
    return assignedStagingPath;
  }

  public String tableUuid() {
    return tableUuid;
  }

  @Override
  public void validate() {}
}
