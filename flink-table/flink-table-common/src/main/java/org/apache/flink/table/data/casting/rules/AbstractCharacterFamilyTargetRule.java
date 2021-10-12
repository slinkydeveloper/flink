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

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.casting.CastExecutor;
import org.apache.flink.table.data.casting.CastRule;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.function.Function;

public abstract class AbstractCharacterFamilyTargetRule<IN> implements CastRule<IN, StringData> {

    protected abstract Function<IN, String> create(Context context, LogicalType inputLogicalType);

    @Override
    public CastExecutor<IN, StringData> create(
            Context context, LogicalType inputLogicalType, LogicalType targetLogicalType) {
        Function<IN, String> toString = create(context, inputLogicalType);

        int limit =
                (targetLogicalType instanceof CharType)
                        ? ((CharType) targetLogicalType).getLength()
                        : ((VarCharType) targetLogicalType).getLength();
        if (limit == Integer.MAX_VALUE) {
            return in -> StringData.fromString(toString.apply(in));
        }
        return in -> StringData.fromString(toString.apply(in).substring(limit));
    }
}
