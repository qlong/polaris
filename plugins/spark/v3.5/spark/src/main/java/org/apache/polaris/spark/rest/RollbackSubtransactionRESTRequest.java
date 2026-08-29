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

/**
 * Request body for POST /transactions/{txnId}/subtransactions/{subtxnId}/rollback. When {@code
 * reopenForRetry} is true (the default) the subtransaction is reopened (attempt++) after being
 * rolled back so it can be re-staged, possibly by a different worker.
 */
public class RollbackSubtransactionRESTRequest implements RESTRequest {
  @JsonProperty("reopen_for_retry")
  private final boolean reopenForRetry;

  public RollbackSubtransactionRESTRequest(boolean reopenForRetry) {
    this.reopenForRetry = reopenForRetry;
  }

  public boolean reopenForRetry() {
    return reopenForRetry;
  }

  @Override
  public void validate() {}
}
