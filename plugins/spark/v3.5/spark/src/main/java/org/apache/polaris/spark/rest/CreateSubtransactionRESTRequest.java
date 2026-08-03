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
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request body for POST /transactions/{txnId}/subtransactions (createSubtransaction).
 *
 * <p>Two shapes, both accepted by the server:
 *
 * <ul>
 *   <li><b>Multi-table:</b> {@link #tables} carries one entry per table — the subtransaction spans
 *       them all as one atomic unit.
 *   <li><b>Single-table shorthand:</b> the scalar {@link #tableIdentifier} + {@link #operation}.
 * </ul>
 *
 * Null fields are omitted so the server sees exactly one of the two shapes.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateSubtransactionRESTRequest implements RESTRequest {
  @JsonProperty("tables")
  private final List<SubtransactionTableRESTRequest> tables;

  @JsonProperty("table_identifier")
  private final String tableIdentifier;

  @JsonProperty("operation")
  private final String operation;

  @JsonProperty("lease_seconds")
  private final int leaseSeconds;

  /** Single-table shorthand. */
  public CreateSubtransactionRESTRequest(
      String tableIdentifier, String operation, int leaseSeconds) {
    this.tables = null;
    this.tableIdentifier = tableIdentifier;
    this.operation = operation;
    this.leaseSeconds = leaseSeconds;
  }

  /** Multi-table form: the subtransaction spans every table in {@code tables} atomically. */
  public CreateSubtransactionRESTRequest(
      List<SubtransactionTableRESTRequest> tables, int leaseSeconds) {
    this.tables = tables;
    this.tableIdentifier = null;
    this.operation = null;
    this.leaseSeconds = leaseSeconds;
  }

  public List<SubtransactionTableRESTRequest> tables() {
    return tables;
  }

  public String tableIdentifier() {
    return tableIdentifier;
  }

  public String operation() {
    return operation;
  }

  public int leaseSeconds() {
    return leaseSeconds;
  }

  @Override
  public void validate() {}
}
