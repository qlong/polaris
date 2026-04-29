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

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Exhaustive compatibility matrix tests for {@link LockManager#compatible(String, String)}.
 *
 * <p>The expected matrix (held → requested):
 *
 * <pre>
 *                   APP   RD    OW    EX    AX
 * APPEND          :  T     T     T     T     F
 * ROW_DELTA       :  T     F     F     F     F
 * OVERWRITE       :  T     F     F     F     F
 * EXCLUSIVE       :  T     F     F     F     F
 * ACCESS_EXCLUSIVE:  F     F     F     F     F
 * </pre>
 */
class LockCompatibilityTest {

  private static final String APP = LockManager.MODE_APPEND;
  private static final String RD = LockManager.MODE_ROW_DELTA;
  private static final String OW = LockManager.MODE_OVERWRITE;
  private static final String EX = LockManager.MODE_EXCLUSIVE;
  private static final String AX = LockManager.MODE_ACCESS_EXCLUSIVE;

  static Stream<Arguments> compatibilityMatrix() {
    return Stream.of(
        // APPEND held
        Arguments.of(APP, APP, true),
        Arguments.of(APP, RD, true),
        Arguments.of(APP, OW, true),
        Arguments.of(APP, EX, true),
        Arguments.of(APP, AX, false),
        // ROW_DELTA held
        Arguments.of(RD, APP, true),
        Arguments.of(RD, RD, false),
        Arguments.of(RD, OW, false),
        Arguments.of(RD, EX, false),
        Arguments.of(RD, AX, false),
        // OVERWRITE held
        Arguments.of(OW, APP, true),
        Arguments.of(OW, RD, false),
        Arguments.of(OW, OW, false),
        Arguments.of(OW, EX, false),
        Arguments.of(OW, AX, false),
        // EXCLUSIVE held
        Arguments.of(EX, APP, true),
        Arguments.of(EX, RD, false),
        Arguments.of(EX, OW, false),
        Arguments.of(EX, EX, false),
        Arguments.of(EX, AX, false),
        // ACCESS_EXCLUSIVE held
        Arguments.of(AX, APP, false),
        Arguments.of(AX, RD, false),
        Arguments.of(AX, OW, false),
        Arguments.of(AX, EX, false),
        Arguments.of(AX, AX, false));
  }

  @ParameterizedTest(name = "held={0} requested={1} → {2}")
  @MethodSource("compatibilityMatrix")
  void compatibleReturnsExpectedResult(String held, String requested, boolean expected) {
    assertThat(LockManager.compatible(held, requested)).isEqualTo(expected);
  }
}
