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

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.STRING_DATA_TO_BOOLEAN;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/** {@link LogicalTypeFamily#CHARACTER_STRING} to {@link LogicalTypeRoot#BOOLEAN} cast rule. */
class StringToBooleanCastRule extends AbstractExpressionCodeGeneratorCastRule<StringData, Boolean> {

    static final StringToBooleanCastRule INSTANCE = new StringToBooleanCastRule();

    private StringToBooleanCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.CHARACTER_STRING)
                        .target(LogicalTypeRoot.BOOLEAN)
                        .fallible(true)
                        .build());
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return staticCall(STRING_DATA_TO_BOOLEAN(), inputTerm);
    }
}
