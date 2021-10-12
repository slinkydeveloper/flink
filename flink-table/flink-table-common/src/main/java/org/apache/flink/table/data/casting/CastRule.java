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

package org.apache.flink.table.data.casting;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.LogicalType;

import java.time.ZoneId;

/**
 * Casting executor factory performs the pre-flight of a casting operation.
 *
 * @param <IN> Input internal type
 * @param <OUT> Output internal type
 */
@Internal
public interface CastRule<IN, OUT> {

    /** @see CastRulePredicate for more details about a cast rule predicate definition */
    CastRulePredicate getPredicateDefinition();

    // TODO should we assume the casting rule is valid at this point?
    /**
     * Create a casting executor starting from the provided input type. The returned {@link
     * CastExecutor} assumes the input value is using the internal data type, and it's a valid value
     * for the provided {@code targetLogicalType}.
     */
    CastExecutor<IN, OUT> create(
            Context context, LogicalType inputLogicalType, LogicalType targetLogicalType);

    /** Casting context. */
    interface Context {
        ZoneId getSessionZoneId();
    }
}
