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
package org.apache.polaris.service.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import java.util.Objects;

/** One table entry in the inline single-session commit path (Use Case 1). */
public class StagedTableCommit {

  @NotNull private final String tableIdentifier;
  @NotNull private final String stagedMetadataLocation;
  @NotNull private final LockMode operation;

  @JsonCreator
  public StagedTableCommit(
      @JsonProperty(value = "table_identifier", required = true) String tableIdentifier,
      @JsonProperty(value = "staged_metadata_location", required = true)
          String stagedMetadataLocation,
      @JsonProperty(value = "operation", required = true) LockMode operation) {
    this.tableIdentifier = Objects.requireNonNull(tableIdentifier, "table_identifier");
    this.stagedMetadataLocation =
        Objects.requireNonNull(stagedMetadataLocation, "staged_metadata_location");
    this.operation = Objects.requireNonNullElse(operation, LockMode.APPEND);
  }

  @JsonProperty("table_identifier")
  public String getTableIdentifier() {
    return tableIdentifier;
  }

  @JsonProperty("staged_metadata_location")
  public String getStagedMetadataLocation() {
    return stagedMetadataLocation;
  }

  @JsonProperty("operation")
  public LockMode getOperation() {
    return operation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof StagedTableCommit that)) return false;
    return Objects.equals(tableIdentifier, that.tableIdentifier)
        && Objects.equals(stagedMetadataLocation, that.stagedMetadataLocation)
        && operation == that.operation;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableIdentifier, stagedMetadataLocation, operation);
  }

  @Override
  public String toString() {
    return "StagedTableCommit{"
        + "tableIdentifier='"
        + tableIdentifier
        + '\''
        + ", stagedMetadataLocation='"
        + stagedMetadataLocation
        + '\''
        + ", operation="
        + operation
        + '}';
  }
}
