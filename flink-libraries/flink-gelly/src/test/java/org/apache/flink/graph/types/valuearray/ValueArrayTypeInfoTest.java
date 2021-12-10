/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.ExecutionConfig;

import org.junit.Test;

import static org.apache.flink.graph.types.valuearray.ValueArrayTypeInfo.INT_VALUE_ARRAY_TYPE_INFO;
import static org.apache.flink.graph.types.valuearray.ValueArrayTypeInfo.LONG_VALUE_ARRAY_TYPE_INFO;
import static org.apache.flink.graph.types.valuearray.ValueArrayTypeInfo.NULL_VALUE_ARRAY_TYPE_INFO;
import static org.apache.flink.graph.types.valuearray.ValueArrayTypeInfo.STRING_VALUE_ARRAY_TYPE_INFO;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ValueArrayTypeInfo}. */
public class ValueArrayTypeInfoTest {

    private ExecutionConfig config = new ExecutionConfig();

    @Test
    public void testIntValueArray() {
        assertThat(ValueArray.class).isEqualTo(INT_VALUE_ARRAY_TYPE_INFO.getTypeClass());
        assertThat(IntValueArraySerializer.class)
                .isEqualTo(INT_VALUE_ARRAY_TYPE_INFO.createSerializer(config).getClass());
        assertThat(IntValueArrayComparator.class)
                .isEqualTo(INT_VALUE_ARRAY_TYPE_INFO.createComparator(true, config).getClass());
    }

    @Test
    public void testLongValueArray() {
        assertThat(ValueArray.class).isEqualTo(LONG_VALUE_ARRAY_TYPE_INFO.getTypeClass());
        assertThat(LongValueArraySerializer.class)
                .isEqualTo(LONG_VALUE_ARRAY_TYPE_INFO.createSerializer(config).getClass());
        assertThat(LongValueArrayComparator.class)
                .isEqualTo(LONG_VALUE_ARRAY_TYPE_INFO.createComparator(true, config).getClass());
    }

    @Test
    public void testNullValueArray() {
        assertThat(ValueArray.class).isEqualTo(NULL_VALUE_ARRAY_TYPE_INFO.getTypeClass());
        assertThat(NullValueArraySerializer.class)
                .isEqualTo(NULL_VALUE_ARRAY_TYPE_INFO.createSerializer(config).getClass());
        assertThat(NullValueArrayComparator.class)
                .isEqualTo(NULL_VALUE_ARRAY_TYPE_INFO.createComparator(true, config).getClass());
    }

    @Test
    public void testStringValueArray() {
        assertThat(ValueArray.class).isEqualTo(STRING_VALUE_ARRAY_TYPE_INFO.getTypeClass());
        assertThat(StringValueArraySerializer.class)
                .isEqualTo(STRING_VALUE_ARRAY_TYPE_INFO.createSerializer(config).getClass());
        assertThat(StringValueArrayComparator.class)
                .isEqualTo(STRING_VALUE_ARRAY_TYPE_INFO.createComparator(true, config).getClass());
    }
}
