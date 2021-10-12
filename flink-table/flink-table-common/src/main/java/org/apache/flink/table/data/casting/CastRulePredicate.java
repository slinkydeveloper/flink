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
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * In order to apply a {@link CastRule}, the runtime checks if a particular rule matches the tuple
 * of input and target type using this class. In particular, a rule is applied if:
 *
 * <ol>
 *   <li>{@link #getTargetTypes()} includes the {@link LogicalTypeRoot} of target type and either
 *       <ol>
 *         <li>{@link #getInputTypes()} includes the {@link LogicalTypeRoot} of input type or
 *         <li>{@link #getInputTypeFamilies()} includes one of the {@link LogicalTypeFamily} of
 *             input type
 *       </ol>
 *   <li>{@link #getTargetTypeFamilies()} includes one of the {@link LogicalTypeFamily} of target
 *       type and either
 *       <ol>
 *         <li>{@link #getInputTypes()} includes the {@link LogicalTypeRoot} of input type or
 *         <li>{@link #getInputTypeFamilies()} includes one of the {@link LogicalTypeFamily} of
 *             input type
 *       </ol>
 * </ol>
 */
@Internal
public class CastRulePredicate {

    private final Set<LogicalTypeRoot> inputTypes;
    private final Set<LogicalTypeRoot> targetTypes;

    private final Set<LogicalTypeFamily> inputTypeFamilies;
    private final Set<LogicalTypeFamily> targetTypeFamilies;

    private CastRulePredicate(
            Set<LogicalTypeRoot> inputTypes,
            Set<LogicalTypeRoot> targetTypes,
            Set<LogicalTypeFamily> inputTypeFamilies,
            Set<LogicalTypeFamily> targetTypeFamilies) {
        this.inputTypes = inputTypes;
        this.targetTypes = targetTypes;
        this.inputTypeFamilies = inputTypeFamilies;
        this.targetTypeFamilies = targetTypeFamilies;
    }

    public Set<LogicalTypeRoot> getInputTypes() {
        return inputTypes;
    }

    public Set<LogicalTypeRoot> getTargetTypes() {
        return targetTypes;
    }

    public Set<LogicalTypeFamily> getInputTypeFamilies() {
        return inputTypeFamilies;
    }

    public Set<LogicalTypeFamily> getTargetTypeFamilies() {
        return targetTypeFamilies;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for the {@link CastRulePredicate}. */
    public static class Builder {
        private final Set<LogicalTypeRoot> inputTypes = new HashSet<>();
        private final Set<LogicalTypeRoot> targetTypes = new HashSet<>();

        private final Set<LogicalTypeFamily> inputTypeFamilies = new HashSet<>();
        private final Set<LogicalTypeFamily> targetTypeFamilies = new HashSet<>();

        public Builder input(LogicalTypeRoot inputType) {
            inputTypes.add(inputType);
            return this;
        }

        public Builder target(LogicalTypeRoot outputType) {
            targetTypes.add(outputType);
            return this;
        }

        public Builder input(LogicalTypeFamily inputTypeFamily) {
            inputTypeFamilies.add(inputTypeFamily);
            return this;
        }

        public Builder target(LogicalTypeFamily outputTypeFamily) {
            targetTypeFamilies.add(outputTypeFamily);
            return this;
        }

        public CastRulePredicate build() {
            return new CastRulePredicate(
                    Collections.unmodifiableSet(inputTypes),
                    Collections.unmodifiableSet(targetTypes),
                    Collections.unmodifiableSet(inputTypeFamilies),
                    Collections.unmodifiableSet(targetTypeFamilies));
        }
    }
}