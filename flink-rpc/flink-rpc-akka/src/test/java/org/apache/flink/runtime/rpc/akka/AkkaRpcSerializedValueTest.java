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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatObject;

/** Tests for the {@link AkkaRpcSerializedValue}. */
public class AkkaRpcSerializedValueTest extends TestLogger {

    @Test
    public void testNullValue() throws Exception {
        AkkaRpcSerializedValue serializedValue = AkkaRpcSerializedValue.valueOf(null);
        assertThat(serializedValue.getSerializedData()).isNull();
        assertThat(serializedValue.getSerializedDataLength()).isEqualTo(0);
        assertThatObject(serializedValue.deserializeValue(getClass().getClassLoader())).isNull();

        AkkaRpcSerializedValue otherSerializedValue = AkkaRpcSerializedValue.valueOf(null);
        assertThat(otherSerializedValue).isEqualTo(serializedValue);
        assertThat(otherSerializedValue.hashCode()).isEqualTo(serializedValue.hashCode());

        AkkaRpcSerializedValue clonedSerializedValue = InstantiationUtil.clone(serializedValue);
        assertThat(clonedSerializedValue.getSerializedData()).isNull();
        assertThat(clonedSerializedValue.getSerializedDataLength()).isEqualTo(0);
        assertThatObject(clonedSerializedValue.deserializeValue(getClass().getClassLoader()))
                .isNull();
        assertThat(clonedSerializedValue).isEqualTo(serializedValue);
        assertThat(clonedSerializedValue.hashCode()).isEqualTo(serializedValue.hashCode());
    }

    @Test
    public void testNotNullValues() throws Exception {
        Set<Object> values =
                Stream.of(
                                true,
                                (byte) 5,
                                (short) 6,
                                5,
                                5L,
                                5.5F,
                                6.5,
                                'c',
                                "string",
                                Instant.now(),
                                BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TEN),
                                BigDecimal.valueOf(Math.PI))
                        .collect(Collectors.toSet());

        Object previousValue = null;
        AkkaRpcSerializedValue previousSerializedValue = null;
        for (Object value : values) {
            AkkaRpcSerializedValue serializedValue = AkkaRpcSerializedValue.valueOf(value);
            assertThat(serializedValue.getSerializedData()).as(value.toString()).isNotNull();
            assertThat(serializedValue.getSerializedDataLength())
                    .as(value.toString())
                    .isGreaterThan(0);
            assertThatObject(serializedValue.deserializeValue(getClass().getClassLoader()))
                    .as(value.toString())
                    .isEqualTo(value);

            AkkaRpcSerializedValue otherSerializedValue = AkkaRpcSerializedValue.valueOf(value);
            assertThat(otherSerializedValue).as(value.toString()).isEqualTo(serializedValue);
            assertThat(otherSerializedValue.hashCode())
                    .as(value.toString())
                    .isEqualTo(serializedValue.hashCode());

            AkkaRpcSerializedValue clonedSerializedValue = InstantiationUtil.clone(serializedValue);
            assertThat(clonedSerializedValue.getSerializedData())
                    .as(value.toString())
                    .isEqualTo(serializedValue.getSerializedData());
            assertThatObject(clonedSerializedValue.deserializeValue(getClass().getClassLoader()))
                    .as(value.toString())
                    .isEqualTo(value);
            assertThat(clonedSerializedValue).as(value.toString()).isEqualTo(serializedValue);
            assertThat(clonedSerializedValue.hashCode())
                    .as(value.toString())
                    .isEqualTo(serializedValue.hashCode());

            if (previousValue != null && !previousValue.equals(value)) {
                assertThat(serializedValue)
                        .as(value.toString() + " " + previousValue.toString())
                        .isNotEqualTo(previousSerializedValue);
            }

            previousValue = value;
            previousSerializedValue = serializedValue;
        }
    }
}
