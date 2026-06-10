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
import jakarta.validation.Valid;
import java.util.List;
import java.util.Objects;

/**
 * Optional request body for the commitTransaction endpoint.
 *
 * <p>When {@code tables} is non-empty, the server uses the inline single-session path (Use Case 1).
 * When absent or empty, the server reads pre-registered subtransactions (Use Case 2).
 */
public class CommitTransactionRequest {

  @Valid private final List<StagedTableCommit> tables;

  @JsonCreator
  public CommitTransactionRequest(
      @JsonProperty(value = "tables") List<StagedTableCommit> tables) {
    this.tables = tables;
  }

  @JsonProperty("tables")
  public List<StagedTableCommit> getTables() {
    return tables;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CommitTransactionRequest that)) return false;
    return Objects.equals(tables, that.tables);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tables);
  }

  @Override
  public String toString() {
    return "CommitTransactionRequest{tables=" + tables + '}';
  }
}
