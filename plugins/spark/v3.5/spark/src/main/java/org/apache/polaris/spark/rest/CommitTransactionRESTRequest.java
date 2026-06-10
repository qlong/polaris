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
import org.apache.iceberg.shaded.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Optional body for POST /transactions/{txnId}/commit. When {@code tables} is non-empty the server
 * uses the inline single-session path; when absent the server reads pre-registered subtransactions
 * (multi-app path).
 */
public class CommitTransactionRESTRequest implements RESTRequest {
  @JsonProperty("tables")
  private final List<StagedTableCommitRESTRequest> tables;

  public CommitTransactionRESTRequest(List<StagedTableCommitRESTRequest> tables) {
    this.tables = tables;
  }

  public List<StagedTableCommitRESTRequest> tables() {
    return tables;
  }

  @Override
  public void validate() {}
}
