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

import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One table's per-operation state within a subtransaction, as returned in the {@code operations[]}
 * list of createSubtransaction / getSubtransaction. A multi-table subtransaction has one of these
 * per table; the worker hydrates every table's staging path from this list.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SubtransactionOperationRESTResponse {
  @JsonProperty("op_id")
  private long opId;

  @JsonProperty("table_uuid")
  private String tableUuid;

  @JsonProperty("operation")
  private String operation;

  @JsonProperty("assigned_staging_path")
  private String assignedStagingPath;

  @JsonProperty("staged_metadata_location")
  private String stagedMetadataLocation;

  @JsonProperty("status")
  private String status;

  public SubtransactionOperationRESTResponse() {}

  public long opId() {
    return opId;
  }

  /** Fully-qualified table identifier for this operation, e.g. {@code "ns.orders"}. */
  public String tableUuid() {
    return tableUuid;
  }

  public String operation() {
    return operation;
  }

  public String assignedStagingPath() {
    return assignedStagingPath;
  }

  public String stagedMetadataLocation() {
    return stagedMetadataLocation;
  }

  public String status() {
    return status;
  }
}
