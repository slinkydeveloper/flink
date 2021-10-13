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

import org.apache.flink.table.data.casting.CastExecutor;
import org.apache.flink.table.data.casting.CastRulePredicate;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Objects;

/** Identity casting for non parametrized types. */
public class IdentityCastRule extends AbstractCastRule<Object, Object> {

    public static final IdentityCastRule INSTANCE = new IdentityCastRule();

    private IdentityCastRule() {
        super(CastRulePredicate.builder().predicate(Objects::equals).build());
    }

    @Override
    public CastExecutor<Object, Object> create(
            Context context, LogicalType inputLogicalType, LogicalType targetLogicalType) {
        return x -> x;
    }
}
