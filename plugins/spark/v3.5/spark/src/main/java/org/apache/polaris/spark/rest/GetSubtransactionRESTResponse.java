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

import java.util.List;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response from GET /transactions/subtransactions/{subtxn-id}.
 *
 * <p>A subtransaction may span multiple tables. {@link #operations()} carries one entry per table
 * (its {@code table_uuid} + {@code assigned_staging_path}); the worker hydrates every table from it.
 * The scalar mirrors ({@link #assignedStagingPath()}, {@link #tableUuid()}) reflect the first
 * operation and remain for the single-table path.
 */
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

  @JsonProperty("operations")
  private List<SubtransactionOperationRESTResponse> operations;

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

  /** Per-table operations of this subtransaction, or {@code null} if the server omitted them. */
  public List<SubtransactionOperationRESTResponse> operations() {
    return operations;
  }

  @Override
  public void validate() {}
}
