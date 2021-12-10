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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.types.IntValue;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatObject;

/** Tests for {@link IntValueArray}. */
public class IntValueArrayTest {

    @Test
    public void testBoundedArray() {
        int count = IntValueArray.DEFAULT_CAPACITY_IN_BYTES / IntValueArray.ELEMENT_LENGTH_IN_BYTES;

        ValueArray<IntValue> iva = new IntValueArray(IntValueArray.DEFAULT_CAPACITY_IN_BYTES);

        // fill the array
        for (int i = 0; i < count; i++) {
            assertThat(iva.isFull()).isFalse();
            assertThat(iva.size()).isEqualTo(i);

            assertThat(iva.add(new IntValue(i))).isTrue();

            assertThat(iva.size()).isEqualTo(i + 1);
        }

        // array is now full
        assertThat(iva.isFull()).isTrue();
        assertThat(iva.size()).isEqualTo(count);

        // verify the array values
        int idx = 0;
        for (IntValue lv : iva) {
            assertThat(lv.getValue()).isEqualTo(idx++);
        }

        // add element past end of array
        assertThat(iva.add(new IntValue(count))).isFalse();
        assertThat(iva.addAll(iva)).isFalse();

        // test copy
        assertThatObject(iva.copy()).isEqualTo(iva);

        // test copyTo
        IntValueArray ivaTo = new IntValueArray();
        iva.copyTo(ivaTo);
        assertThatObject(ivaTo).isEqualTo(iva);

        // test clear
        iva.clear();
        assertThat(iva.size()).isEqualTo(0);
    }

    @Test
    public void testUnboundedArray() {
        int count = 4096;

        ValueArray<IntValue> iva = new IntValueArray();

        // add several elements
        for (int i = 0; i < count; i++) {
            assertThat(iva.isFull()).isFalse();
            assertThat(iva.size()).isEqualTo(i);

            assertThat(iva.add(new IntValue(i))).isTrue();

            assertThat(iva.size()).isEqualTo(i + 1);
        }

        // array never fills
        assertThat(iva.isFull()).isFalse();
        assertThat(iva.size()).isEqualTo(count);

        // verify the array values
        int idx = 0;
        for (IntValue lv : iva) {
            assertThat(lv.getValue()).isEqualTo(idx++);
        }

        // add element past end of array
        assertThat(iva.add(new IntValue(count))).isTrue();
        assertThat(iva.addAll(iva)).isTrue();

        // test copy
        assertThatObject(iva.copy()).isEqualTo(iva);

        // test copyTo
        IntValueArray ivaTo = new IntValueArray();
        iva.copyTo(ivaTo);
        assertThatObject(ivaTo).isEqualTo(iva);

        // test mark/reset
        int size = iva.size();
        iva.mark();
        assertThat(iva.add(new IntValue())).isTrue();
        assertThat(iva.size()).isEqualTo(size + 1);
        iva.reset();
        assertThat(iva.size()).isEqualTo(size);

        // test clear
        iva.clear();
        assertThat(iva.size()).isEqualTo(0);
    }
}
