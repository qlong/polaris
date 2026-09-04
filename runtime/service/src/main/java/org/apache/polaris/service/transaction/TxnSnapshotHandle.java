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
package org.apache.polaris.service.transaction;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.BadRequestException;
import java.util.Base64;

/**
 * Codec for the opaque, server-minted read-point handle carried on {@code loadTable?txn_snapshot=X}
 * for consistent multi-table reads.
 *
 * <p>A read-consistent "snapshot" is conceptually a single commit-order cutoff ({@code
 * read_point}). Rather than persist a {@code txn_snapshots} row per standalone read (a hot-path
 * INSERT that accumulates without bound), the read point is encoded into an opaque string handle
 * that the client stores and echoes verbatim. The transaction snapshot stays a first-class
 * <i>concept</i> — only its <i>representation</i> changes (row → handle), so a future SERIALIZABLE
 * level can re-materialize a row behind the same concept.
 *
 * <p><b>Encoding, not signing.</b> The handle is {@code base64url(JSON {v, r})}; the encoding buys
 * opacity and evolvability (like a pagination cursor), not integrity. This is the trusted-engine
 * PoC shape: the handle is a read-point <i>reference</i>, not a capability — it carries no
 * authority, and the request's own credentials are still authenticated/authorized as before. A
 * leaked handle grants nothing; a forged {@code r} can only corrupt the caller's own read
 * (self-inflicted). An untrusted/multi-tenant deployment would add an HMAC to this payload.
 *
 * <p><b>Fail-closed.</b> Any malformed handle — bad base64, non-JSON bytes, an unknown version, or
 * a negative read point — throws {@link BadRequestException}. The codec never returns a default or
 * a best-effort value; correctness of the pinned read depends on rejecting a handle we cannot
 * decode.
 *
 * <p>Clients (Spark, Trino) must treat the handle as opaque: store it and send it back unchanged.
 * Only the server mints ({@link #encode}) and interprets ({@link #decode}) it.
 */
public final class TxnSnapshotHandle {

  private static final int VERSION = 1;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private TxnSnapshotHandle() {}

  /** Decoded handle contents. Only {@code v} and {@code r} (read_point) are on the wire. */
  public record Payload(@JsonProperty("v") int v, @JsonProperty("r") long readPoint) {}

  /**
   * Mints an opaque handle for {@code readPoint}. URL-safe and unpadded, so it rides in a query
   * string without escaping.
   */
  public static String encode(long readPoint) {
    Payload payload = new Payload(VERSION, readPoint);
    try {
      byte[] bytes = MAPPER.writeValueAsBytes(payload);
      return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to encode txn_snapshot handle", e);
    }
  }

  /**
   * Decodes a handle, fail-closed. Throws {@link BadRequestException} on any malformed input,
   * unknown version, or negative read point — never a default.
   */
  public static Payload decode(String handle) {
    if (handle == null || handle.isBlank()) {
      throw new BadRequestException("Invalid txn_snapshot handle");
    }
    Payload payload;
    try {
      byte[] bytes = Base64.getUrlDecoder().decode(handle);
      payload = MAPPER.readValue(bytes, Payload.class);
    } catch (Exception e) {
      throw new BadRequestException("Invalid txn_snapshot handle");
    }
    if (payload == null || payload.v() != VERSION || payload.readPoint() < 0) {
      throw new BadRequestException("Invalid txn_snapshot handle");
    }
    return payload;
  }
}
