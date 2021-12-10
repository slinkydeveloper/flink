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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.types.DoubleValue;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatObject;
import static org.assertj.core.api.Assertions.within;

/** Tests for {@link DoubleValueArray}. */
public class DoubleValueArrayTest {

    @Test
    public void testBoundedArray() {
        int count =
                DoubleValueArray.DEFAULT_CAPACITY_IN_BYTES
                        / DoubleValueArray.ELEMENT_LENGTH_IN_BYTES;

        ValueArray<DoubleValue> lva =
                new DoubleValueArray(DoubleValueArray.DEFAULT_CAPACITY_IN_BYTES);

        // fill the array
        for (int i = 0; i < count; i++) {
            assertThat(lva.isFull()).isFalse();
            assertThat(lva.size()).isEqualTo(i);

            assertThat(lva.add(new DoubleValue(i))).isTrue();

            assertThat(lva.size()).isEqualTo(i + 1);
        }

        // array is now full
        assertThat(lva.isFull()).isTrue();
        assertThat(lva.size()).isEqualTo(count);

        // verify the array values
        int idx = 0;
        for (DoubleValue lv : lva) {
            assertThat(lv.getValue()).isCloseTo(idx++, within(0.000001));
        }

        // add element past end of array
        assertThat(lva.add(new DoubleValue(count))).isFalse();
        assertThat(lva.addAll(lva)).isFalse();

        // test copy
        assertThatObject(lva.copy()).isEqualTo(lva);

        // test copyTo
        DoubleValueArray lvaTo = new DoubleValueArray();
        lva.copyTo(lvaTo);
        assertThatObject(lvaTo).isEqualTo(lva);

        // test clear
        lva.clear();
        assertThat(lva.size()).isEqualTo(0);
    }

    @Test
    public void testUnboundedArray() {
        int count = 4096;

        ValueArray<DoubleValue> lva = new DoubleValueArray();

        // add several elements
        for (int i = 0; i < count; i++) {
            assertThat(lva.isFull()).isFalse();
            assertThat(lva.size()).isEqualTo(i);

            assertThat(lva.add(new DoubleValue(i))).isTrue();

            assertThat(lva.size()).isEqualTo(i + 1);
        }

        // array never fills
        assertThat(lva.isFull()).isFalse();
        assertThat(lva.size()).isEqualTo(count);

        // verify the array values
        int idx = 0;
        for (DoubleValue lv : lva) {
            assertThat(lv.getValue()).isCloseTo(idx++, within(0.000001));
        }

        // add element past end of array
        assertThat(lva.add(new DoubleValue(count))).isTrue();
        assertThat(lva.addAll(lva)).isTrue();

        // test copy
        assertThatObject(lva.copy()).isEqualTo(lva);

        // test copyTo
        DoubleValueArray lvaTo = new DoubleValueArray();
        lva.copyTo(lvaTo);
        assertThatObject(lvaTo).isEqualTo(lva);

        // test mark/reset
        int size = lva.size();
        lva.mark();
        assertThat(lva.add(new DoubleValue())).isTrue();
        assertThat(lva.size()).isEqualTo(size + 1);
        lva.reset();
        assertThat(lva.size()).isEqualTo(size);

        // test clear
        lva.clear();
        assertThat(lva.size()).isEqualTo(0);
    }
}
