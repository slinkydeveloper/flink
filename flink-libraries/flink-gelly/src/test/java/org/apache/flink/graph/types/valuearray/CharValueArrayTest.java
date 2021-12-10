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

import org.apache.flink.types.StringValue;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatObject;

/** Tests for {@link StringValueArray}. */
public class CharValueArrayTest {

    @Test
    public void testBoundedArray() {
        // one byte for length and one byte for character
        int count = StringValueArray.DEFAULT_CAPACITY_IN_BYTES / 2;

        ValueArray<StringValue> sva =
                new StringValueArray(StringValueArray.DEFAULT_CAPACITY_IN_BYTES);

        // fill the array
        for (int i = 0; i < count; i++) {
            assertThat(sva.isFull()).isFalse();
            assertThat(sva.size()).isEqualTo(i);

            assertThat(sva.add(new StringValue(Character.toString((char) (i & 0x7F))))).isTrue();

            assertThat(sva.size()).isEqualTo(i + 1);
        }

        // array is now full
        assertThat(sva.isFull()).isTrue();
        assertThat(sva.size()).isEqualTo(count);

        // verify the array values
        int idx = 0;
        for (StringValue sv : sva) {
            assertThat(sv.getValue().charAt(0)).isEqualTo((idx++) & 0x7F);
        }

        // add element past end of array
        assertThat(sva.add(new StringValue(String.valueOf((char) count)))).isFalse();
        assertThat(sva.addAll(sva)).isFalse();

        // test copy
        assertThatObject(sva.copy()).isEqualTo(sva);

        // test copyTo
        StringValueArray svaTo = new StringValueArray();
        sva.copyTo(svaTo);
        assertThatObject(svaTo).isEqualTo(sva);

        // test clear
        sva.clear();
        assertThat(sva.size()).isEqualTo(0);
    }

    @Test
    public void testBoundedArrayWithVariableLengthCharacters() {
        // characters alternatingly take 1 and 2 bytes (plus one byte for length)
        int count = 1280;

        ValueArray<StringValue> sva = new StringValueArray(3200);

        // fill the array
        for (int i = 0; i < count; i++) {
            assertThat(sva.isFull()).isFalse();
            assertThat(sva.size()).isEqualTo(i);

            assertThat(sva.add(new StringValue(Character.toString((char) (i & 0xFF))))).isTrue();

            assertThat(sva.size()).isEqualTo(i + 1);
        }

        // array is now full
        assertThat(sva.isFull()).isTrue();
        assertThat(sva.size()).isEqualTo(count);

        // verify the array values
        int idx = 0;
        for (StringValue sv : sva) {
            assertThat(sv.getValue().charAt(0)).isEqualTo((idx++) & 0xFF);
        }

        // add element past end of array
        assertThat(sva.add(new StringValue(String.valueOf((char) count)))).isFalse();
        assertThat(sva.addAll(sva)).isFalse();

        // test copy
        assertThatObject(sva.copy()).isEqualTo(sva);

        // test copyTo
        StringValueArray svaTo = new StringValueArray();
        sva.copyTo(svaTo);
        assertThatObject(svaTo).isEqualTo(sva);

        // test clear
        sva.clear();
        assertThat(sva.size()).isEqualTo(0);
    }

    @Test
    public void testUnboundedArray() {
        int count = 4096;

        ValueArray<StringValue> sva = new StringValueArray();

        // add several elements
        for (int i = 0; i < count; i++) {
            assertThat(sva.isFull()).isFalse();
            assertThat(sva.size()).isEqualTo(i);

            assertThat(sva.add(new StringValue(String.valueOf((char) i)))).isTrue();

            assertThat(sva.size()).isEqualTo(i + 1);
        }

        // array never fills
        assertThat(sva.isFull()).isFalse();
        assertThat(sva.size()).isEqualTo(count);

        // verify the array values
        int idx = 0;
        for (StringValue sv : sva) {
            assertThat(sv.getValue().charAt(0)).isEqualTo(idx++);
        }

        // add element past end of array
        assertThat(sva.add(new StringValue(String.valueOf((char) count)))).isTrue();
        assertThat(sva.addAll(sva)).isTrue();

        // test copy
        assertThatObject(sva.copy()).isEqualTo(sva);

        // test copyTo
        StringValueArray svaTo = new StringValueArray();
        sva.copyTo(svaTo);
        assertThatObject(svaTo).isEqualTo(sva);

        // test mark/reset
        int size = sva.size();
        sva.mark();
        assertThat(sva.add(new StringValue())).isTrue();
        assertThat(sva.size()).isEqualTo(size + 1);
        sva.reset();
        assertThat(sva.size()).isEqualTo(size);

        // test clear
        sva.clear();
        assertThat(sva.size()).isEqualTo(0);
    }
}
