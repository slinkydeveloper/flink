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

package org.apache.flink.table.data.casting.rules;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.casting.CastExecutor;
import org.apache.flink.table.data.casting.CastRulePredicate;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

public class DecimalToDecimalCastRule extends AbstractCastRule<DecimalData, DecimalData> {

    public static final DecimalToDecimalCastRule INSTANCE = new DecimalToDecimalCastRule();

    private DecimalToDecimalCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.DECIMAL)
                        .target(LogicalTypeRoot.DECIMAL)
                        .build());
    }

    @Override
    public CastExecutor<DecimalData, DecimalData> create(
            Context context, LogicalType inputLogicalType, LogicalType targetLogicalType) {
        DecimalType inputDecimal = (DecimalType) inputLogicalType;
        DecimalType targetDecimal = (DecimalType) targetLogicalType;

        // TODO very lazy bounds check
        if (inputDecimal.getPrecision() > targetDecimal.getPrecision()) {
            return decimalData -> {
                throw new TableException("Overflow");
            };
        }

        return decimalData ->
                DecimalData.fromBigDecimal(
                        decimalData.toBigDecimal(),
                        targetDecimal.getPrecision(),
                        targetDecimal.getScale());
    }
}
