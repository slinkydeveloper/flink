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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.types.logical.VarCharType.STRING_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

class CastRuleProviderTest {

    private static final LogicalType DISTINCT_INT =
            DistinctType.newBuilder(ObjectIdentifier.of("a", "b", "c"), INT().getLogicalType())
                    .build();
    private static final LogicalType DISTINCT_BIG_INT =
            DistinctType.newBuilder(ObjectIdentifier.of("a", "b", "c"), BIGINT().getLogicalType())
                    .build();
    private static final LogicalType INT = INT().getLogicalType();
    private static final LogicalType TINYINT = TINYINT().getLogicalType();

    @Test
    void testResolveDistinctTypeToIdentityCastRule() {
        assertThat(CastRuleProvider.resolveRule(DISTINCT_INT, INT))
                .isSameAs(IdentityCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolveRule(INT, DISTINCT_INT))
                .isSameAs(IdentityCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolveRule(DISTINCT_INT, DISTINCT_INT))
                .isSameAs(IdentityCastRule.INSTANCE);
    }

    @Test
    void testResolveIntToBigIntWithDistinct() {
        assertThat(CastRuleProvider.resolveRule(INT, DISTINCT_BIG_INT))
                .isSameAs(NumericPrimitiveCastRule.INSTANCE);
    }

    @Test
    void testResolveArrayIntToBigIntWithDistinct() {
        assertThat(
                        CastRuleProvider.resolveRule(
                                new ArrayType(INT), new ArrayType(DISTINCT_BIG_INT)))
                .isSameAs(ArrayToArrayCastRule.INSTANCE);
    }

    @Test
    void testResolvePredefinedToString() {
        assertThat(CastRuleProvider.resolveRule(INT, new VarCharType(10)))
                .isSameAs(CharVarCharTrimPadCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolveRule(INT, new CharType(10)))
                .isSameAs(CharVarCharTrimPadCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolveRule(INT, STRING_TYPE))
                .isSameAs(NumericToStringCastRule.INSTANCE);
    }

    @Test
    void testResolveConstructedToString() {
        assertThat(CastRuleProvider.resolveRule(new ArrayType(INT), new VarCharType(10)))
                .isSameAs(ArrayToStringCastRule.INSTANCE);
    }

    @Test
    void testFallibility() {
        assertThat(CastRuleProvider.resolve(TINYINT, INT).isFallible()).isFalse();
        assertThat(CastRuleProvider.resolve(STRING_TYPE, TIME().getLogicalType()).isFallible())
                .isTrue();
        assertThat(CastRuleProvider.resolve(STRING_TYPE, STRING_TYPE).isFallible()).isFalse();

        LogicalType inputType = ROW(TINYINT(), STRING()).getLogicalType();
        assertThat(
                        CastRuleProvider.resolve(inputType, ROW(INT(), TIME()).getLogicalType())
                                .isFallible())
                .isTrue();
        assertThat(
                        CastRuleProvider.resolve(inputType, ROW(INT(), STRING()).getLogicalType())
                                .isFallible())
                .isFalse();
    }
}
