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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.BadRequestException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.polaris.service.transaction.TxnSnapshotHandle.Payload;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TxnSnapshotHandle} — the opaque read-point handle codec.
 *
 * <p>Two properties matter: (1) a minted handle round-trips its {@code readPoint} intact, and (2)
 * the codec is <b>fail-closed</b> — every malformed input throws {@link BadRequestException} rather
 * than returning a best-effort or default value, because a handle we cannot decode must never
 * resolve to a silent latest / torn read.
 */
class TxnSnapshotHandleTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void roundTripPreservesReadPoint() {
    String handle = TxnSnapshotHandle.encode(12345L);
    Payload payload = TxnSnapshotHandle.decode(handle);

    assertThat(payload.v()).isEqualTo(1);
    assertThat(payload.readPoint()).isEqualTo(12345L);
  }

  @Test
  void roundTripPreservesReadPointZeroAndLargeValues() {
    assertThat(TxnSnapshotHandle.decode(TxnSnapshotHandle.encode(0L)).readPoint()).isZero();
    assertThat(TxnSnapshotHandle.decode(TxnSnapshotHandle.encode(Long.MAX_VALUE)).readPoint())
        .isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void handleIsShortUrlSafeAndUnpadded() {
    // A handle rides in a query string unescaped — must be URL-safe base64 with no padding.
    // With payload {"v":1,"r":<n>} the handle is typically ~20 chars for small read_point values.
    for (long readPoint : new long[] {0L, 1L, 255L, 65_535L, Long.MAX_VALUE}) {
      String handle = TxnSnapshotHandle.encode(readPoint);
      assertThat(handle)
          .as("handle for readPoint=%s must be URL-safe base64url", readPoint)
          .doesNotContain("+")
          .doesNotContain("/")
          .doesNotContain("=");
      assertThat(handle.length())
          .as("handle for readPoint=%s should be short (<=56 chars)", readPoint)
          .isLessThanOrEqualTo(56);
    }
  }

  @Test
  void decodeRejectsNull() {
    assertThatThrownBy(() -> TxnSnapshotHandle.decode(null))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void decodeRejectsEmptyAndBlank() {
    assertThatThrownBy(() -> TxnSnapshotHandle.decode("")).isInstanceOf(BadRequestException.class);
    assertThatThrownBy(() -> TxnSnapshotHandle.decode("   "))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void decodeRejectsGarbageBase64() {
    assertThatThrownBy(() -> TxnSnapshotHandle.decode("not-a-real-handle!!!"))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void decodeRejectsBase64OfNonJson() {
    String notJson = base64Url("this is not json".getBytes(StandardCharsets.UTF_8));
    assertThatThrownBy(() -> TxnSnapshotHandle.decode(notJson))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void decodeRejectsWrongVersion() throws Exception {
    byte[] bytes = MAPPER.writeValueAsBytes(new Payload(2, 100L));
    assertThatThrownBy(() -> TxnSnapshotHandle.decode(base64Url(bytes)))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  void decodeRejectsNegativeReadPoint() throws Exception {
    byte[] bytes = MAPPER.writeValueAsBytes(new Payload(1, -1L));
    assertThatThrownBy(() -> TxnSnapshotHandle.decode(base64Url(bytes)))
        .isInstanceOf(BadRequestException.class);
  }

  private static String base64Url(byte[] bytes) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }
}
