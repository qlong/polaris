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

import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One table entry in a multi-table {@code CreateSubtransactionRequest.tables[]}. Each entry names a
 * table and the lock mode ("operation") the subtransaction acquires on it. All entries are staged
 * and committed as one atomic unit.
 */
public class SubtransactionTableRESTRequest {
  @JsonProperty("table_identifier")
  private final String tableIdentifier;

  @JsonProperty("operation")
  private final String operation;

  public SubtransactionTableRESTRequest(
      @JsonProperty(value = "table_identifier", required = true) String tableIdentifier,
      @JsonProperty("operation") String operation) {
    this.tableIdentifier = tableIdentifier;
    this.operation = operation;
  }

  public String getTableIdentifier() {
    return tableIdentifier;
  }

  public String getOperation() {
    return operation;
  }
}
