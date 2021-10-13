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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.casting.CastExecutor;
import org.apache.flink.table.data.casting.CastRulePredicate;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Collections;

/** Rule to cast an atomic value to an {@link LogicalTypeRoot#ARRAY}. */
public class AtomicToArrayCastRule extends AbstractCastRule<Object, ArrayData> {

    public static final AtomicToArrayCastRule INSTANCE = new AtomicToArrayCastRule();

    private AtomicToArrayCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.PREDEFINED)
                        .target(LogicalTypeRoot.ARRAY)
                        .build());
    }

    @Override
    public CastExecutor<Object, ArrayData> create(
            Context context, LogicalType inputLogicalType, LogicalType targetLogicalType) {
        // TODO I'm assuming this casting is valid!

        // TODO this should understand primitive arrays as well, use inputLogicalType for that
        return value -> new GenericArrayData(Collections.singleton(value).toArray());
    }
}
