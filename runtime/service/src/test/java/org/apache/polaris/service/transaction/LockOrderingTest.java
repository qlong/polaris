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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that {@link LockManager#canonicalOrder} produces a deterministic sort for any permutation
 * of input UUIDs, which is the deadlock-prevention invariant.
 */
class LockOrderingTest {

  static Stream<List<String>> permutations() {
    List<String> uuids =
        Arrays.asList(
            "550e8400-e29b-41d4-a716-446655440000",
            "110e8100-e29b-41d4-a716-446655440001",
            "330e8300-e29b-41d4-a716-446655440002");
    // Generate all 6 permutations of 3 UUIDs
    List<List<String>> perms = new java.util.ArrayList<>();
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        if (j == i) continue;
        for (int k = 0; k < 3; k++) {
          if (k == i || k == j) continue;
          perms.add(Arrays.asList(uuids.get(i), uuids.get(j), uuids.get(k)));
        }
      }
    }
    return perms.stream();
  }

  @ParameterizedTest(name = "input permutation {index}")
  @MethodSource("permutations")
  void canonicalOrderIsDeterministicForAllPermutations(List<String> permutation) {
    List<String> result = LockManager.canonicalOrder(permutation);
    List<String> expected =
        LockManager.canonicalOrder(
            List.of(
                "550e8400-e29b-41d4-a716-446655440000",
                "110e8100-e29b-41d4-a716-446655440001",
                "330e8300-e29b-41d4-a716-446655440002"));
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void canonicalOrderSortsLexicographically() {
    List<String> input = Arrays.asList("zzz", "aaa", "mmm");
    List<String> result = LockManager.canonicalOrder(input);
    assertThat(result).containsExactly("aaa", "mmm", "zzz");
  }

  @Test
  void canonicalOrderDoesNotMutateInput() {
    List<String> input = new java.util.ArrayList<>(Arrays.asList("c", "b", "a"));
    LockManager.canonicalOrder(input);
    assertThat(input).containsExactly("c", "b", "a");
  }

  @Test
  void canonicalOrderSingleElement() {
    assertThat(LockManager.canonicalOrder(List.of("only"))).containsExactly("only");
  }

  @Test
  void canonicalOrderEmptyList() {
    assertThat(LockManager.canonicalOrder(Collections.emptyList())).isEmpty();
  }
}
