/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link KeyMap}. */
public class KeyMapPutIfAbsentTest {

    @Test
    public void testPutIfAbsentUniqueKeysAndGrowth() {
        try {
            KeyMap<Integer, Integer> map = new KeyMap<>();
            IntegerFactory factory = new IntegerFactory();

            final int numElements = 1000000;

            for (int i = 0; i < numElements; i++) {
                factory.set(2 * i + 1);
                map.putIfAbsent(i, factory);

                assertThat(map.size()).isEqualTo(i + 1);
                assertThat(map.getCurrentTableCapacity() > map.size()).isTrue();
                assertThat(map.getCurrentTableCapacity() > map.getRehashThreshold()).isTrue();
                assertThat(map.size() <= map.getRehashThreshold()).isTrue();
            }

            assertThat(map.size()).isEqualTo(numElements);
            assertThat(map.traverseAndCountElements()).isEqualTo(numElements);
            assertThat(map.getCurrentTableCapacity()).isEqualTo(1 << 21);

            for (int i = 0; i < numElements; i++) {
                assertThat(map.get(i).intValue()).isEqualTo(2 * i + 1);
            }

            for (int i = numElements - 1; i >= 0; i--) {
                assertThat(map.get(i).intValue()).isEqualTo(2 * i + 1);
            }

            assertThat(map.size()).isEqualTo(numElements);
            assertThat(map.traverseAndCountElements()).isEqualTo(numElements);
            assertThat(map.getCurrentTableCapacity()).isEqualTo(1 << 21);
            assertThat(map.getLongestChainLength() <= 7).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPutIfAbsentDuplicateKeysAndGrowth() {
        try {
            KeyMap<Integer, Integer> map = new KeyMap<>();
            IntegerFactory factory = new IntegerFactory();

            final int numElements = 1000000;

            for (int i = 0; i < numElements; i++) {
                int val = 2 * i + 1;
                factory.set(val);
                Integer put = map.putIfAbsent(i, factory);
                assertThat(put.intValue()).isEqualTo(val);
            }

            for (int i = 0; i < numElements; i += 3) {
                factory.set(2 * i);
                Integer put = map.putIfAbsent(i, factory);
                assertThat(put.intValue()).isEqualTo(2 * i + 1);
            }

            for (int i = 0; i < numElements; i++) {
                assertThat(map.get(i).intValue()).isEqualTo(2 * i + 1);
            }

            assertThat(map.size()).isEqualTo(numElements);
            assertThat(map.traverseAndCountElements()).isEqualTo(numElements);
            assertThat(map.getCurrentTableCapacity()).isEqualTo(1 << 21);
            assertThat(map.getLongestChainLength() <= 7).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // ------------------------------------------------------------------------

    private static class IntegerFactory implements KeyMap.LazyFactory<Integer> {

        private Integer toCreate;

        public void set(Integer toCreate) {
            this.toCreate = toCreate;
        }

        @Override
        public Integer create() {
            return toCreate;
        }
    }
}
