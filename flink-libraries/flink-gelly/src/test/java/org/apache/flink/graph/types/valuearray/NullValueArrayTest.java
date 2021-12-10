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

import org.apache.flink.types.NullValue;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatObject;

/** Tests for {@link NullValueArray}. */
public class NullValueArrayTest {

    @Test
    public void testUnboundedArray() {
        int count = 4096;

        ValueArray<NullValue> nva = new NullValueArray();

        // add several elements
        for (int i = 0; i < count; i++) {
            assertThat(nva.isFull()).isFalse();
            assertThat(nva.size()).isEqualTo(i);

            assertThat(nva.add(NullValue.getInstance())).isTrue();

            assertThat(nva.size()).isEqualTo(i + 1);
        }

        // array never fills
        assertThat(nva.isFull()).isFalse();
        assertThat(nva.size()).isEqualTo(count);

        // verify the array values
        int idx = 0;
        for (NullValue nv : nva) {
            assertThat(nv).isEqualTo(NullValue.getInstance());
        }

        // add element past end of array
        assertThat(nva.add(NullValue.getInstance())).isTrue();
        assertThat(nva.addAll(nva)).isTrue();

        // test copy
        assertThatObject(nva.copy()).isEqualTo(nva);

        // test copyTo
        NullValueArray nvaTo = new NullValueArray();
        nva.copyTo(nvaTo);
        assertThatObject(nvaTo).isEqualTo(nva);

        // test mark/reset
        int size = nva.size();
        nva.mark();
        assertThat(nva.add(NullValue.getInstance())).isTrue();
        assertThat(nva.size()).isEqualTo(size + 1);
        nva.reset();
        assertThat(nva.size()).isEqualTo(size);

        // test clear
        nva.clear();
        assertThat(nva.size()).isEqualTo(0);
    }
}
