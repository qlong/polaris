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

import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonProperty;

/** One table's staged metadata to be committed inline with the parent transaction. */
public class StagedTableCommitRESTRequest implements RESTRequest {
  @JsonProperty("table_identifier")
  private final String tableIdentifier;

  @JsonProperty("staged_metadata_location")
  private final String stagedMetadataLocation;

  @JsonProperty("operation")
  private final String operation;

  public StagedTableCommitRESTRequest(
      String tableIdentifier, String stagedMetadataLocation, String operation) {
    this.tableIdentifier = tableIdentifier;
    this.stagedMetadataLocation = stagedMetadataLocation;
    this.operation = operation;
  }

  public String tableIdentifier() {
    return tableIdentifier;
  }

  public String stagedMetadataLocation() {
    return stagedMetadataLocation;
  }

  public String operation() {
    return operation;
  }

  @Override
  public void validate() {}
}
