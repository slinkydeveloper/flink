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

package org.apache.flink.graph.asm.translate.translators;

import org.apache.flink.types.LongValue;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LongValueAddOffset}. */
public class LongValueAddOffsetTest {

    @Test
    public void testTranslation() throws Exception {
        LongValue reuse = new LongValue();

        assertThat(new LongValueAddOffset(0).translate(new LongValue(0), reuse))
                .isEqualTo(new LongValue(0));
        assertThat(new LongValueAddOffset(1).translate(new LongValue(2), reuse))
                .isEqualTo(new LongValue(3));
        assertThat(new LongValueAddOffset(-1).translate(new LongValue(2), reuse))
                .isEqualTo(new LongValue(1));

        assertThat(
                        new LongValueAddOffset(Long.MIN_VALUE)
                                .translate(new LongValue(Long.MAX_VALUE), reuse))
                .isEqualTo(new LongValue(-1));
        assertThat(
                        new LongValueAddOffset(Long.MAX_VALUE)
                                .translate(new LongValue(Long.MIN_VALUE), reuse))
                .isEqualTo(new LongValue(-1));

        // underflow wraps to positive values
        assertThat(new LongValueAddOffset(-1).translate(new LongValue(Long.MIN_VALUE), reuse))
                .isEqualTo(new LongValue(Long.MAX_VALUE));
        assertThat(
                        new LongValueAddOffset(Long.MIN_VALUE)
                                .translate(new LongValue(Long.MIN_VALUE), reuse))
                .isEqualTo(new LongValue(0));

        // overflow wraps to negative values
        assertThat(new LongValueAddOffset(1).translate(new LongValue(Long.MAX_VALUE), reuse))
                .isEqualTo(new LongValue(Long.MIN_VALUE));
        assertThat(
                        new LongValueAddOffset(Long.MAX_VALUE)
                                .translate(new LongValue(Long.MAX_VALUE), reuse))
                .isEqualTo(new LongValue(-2));
    }
}
