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
import org.apache.flink.table.data.casting.rules.AtomicToArrayCastRule;
import org.apache.flink.table.data.casting.rules.DecimalToDecimalCastRule;
import org.apache.flink.table.data.casting.rules.IdentityCastRule;
import org.apache.flink.table.data.casting.rules.TimestampToStringCastRule;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This class resolves {@link CastRule} starting from the input and the target type. */
@Internal
public class CastRuleProvider {

    /* ------- Entrypoint ------- */

    /**
     * Resolve a {@link CastRule} for the provided input data type and target data type. Returns
     * {@code null} if no rule can be resolved.
     */
    public static @Nullable CastRule<?, ?> resolve(
            LogicalType inputDataType, LogicalType targetDataType) {
        return INSTANCE.internalResolve(inputDataType, targetDataType);
    }

    /** @see #resolve(LogicalType, LogicalType) */
    public static @Nullable CastRule<?, ?> resolve(
            DataType inputDataType, DataType targetDataType) {
        return resolve(inputDataType.getLogicalType(), targetDataType.getLogicalType());
    }

    /* ------ Implementation ------ */

    private static final CastRuleProvider INSTANCE = new CastRuleProvider();

    static {
        INSTANCE.addRule(DecimalToDecimalCastRule.INSTANCE)
                .addRule(AtomicToArrayCastRule.INSTANCE)
                .addRule(TimestampToStringCastRule.INSTANCE)
                .addRule(IdentityCastRule.INSTANCE)
                .freeze();
    }

    // Map<Target family or root, Map<Input family or root, rule>>
    private Map<Object, Map<Object, CastRule<?, ?>>> rules = new HashMap<>();
    private List<CastRule<?, ?>> rulesWithCustomPredicate = new ArrayList<>();

    private CastRuleProvider addRule(CastRule<?, ?> rule) {
        CastRulePredicate predicate = rule.getPredicateDefinition();

        for (LogicalTypeRoot targetTypeRoot : predicate.getTargetTypes()) {
            Map<Object, CastRule<?, ?>> map =
                    rules.computeIfAbsent(targetTypeRoot, k -> new HashMap<>());
            for (LogicalTypeRoot inputTypeRoot : predicate.getInputTypes()) {
                map.put(inputTypeRoot, rule);
            }
            for (LogicalTypeFamily inputTypeFamily : predicate.getInputTypeFamilies()) {
                map.put(inputTypeFamily, rule);
            }
        }
        for (LogicalTypeFamily targetTypeFamily : predicate.getTargetTypeFamilies()) {
            Map<Object, CastRule<?, ?>> map =
                    rules.computeIfAbsent(targetTypeFamily, k -> new HashMap<>());
            for (LogicalTypeRoot inputTypeRoot : predicate.getInputTypes()) {
                map.put(inputTypeRoot, rule);
            }
            for (LogicalTypeFamily inputTypeFamily : predicate.getInputTypeFamilies()) {
                map.put(inputTypeFamily, rule);
            }
        }

        if (predicate.getCustomPredicate() != null) {
            rulesWithCustomPredicate.add(rule);
        }

        return this;
    }

    private CastRule<?, ?> internalResolve(LogicalType inputDataType, LogicalType targetDataType) {
        // Lookup by target type
        Map<Object, CastRule<?, ?>> inputTypeToCastRuleMap =
                lookupTypeInMap(rules, targetDataType.getTypeRoot());

        // If nothing found, just return null
        if (inputTypeToCastRuleMap == null) {
            return null;
        }

        CastRule<?, ?> rule = lookupTypeInMap(inputTypeToCastRuleMap, inputDataType.getTypeRoot());
        if (rule == null) {
            // Try with the rules using custom predicates
            rule =
                    rulesWithCustomPredicate.stream()
                            .filter(
                                    r ->
                                            r.getPredicateDefinition()
                                                    .getCustomPredicate()
                                                    .test(inputDataType, targetDataType))
                            .findFirst()
                            .orElse(null);
        }

        return rule;
    }

    private void freeze() {
        rules.replaceAll((k, m) -> Collections.unmodifiableMap(m));
        rules = Collections.unmodifiableMap(rules);
        rulesWithCustomPredicate = Collections.unmodifiableList(rulesWithCustomPredicate);
    }

    /**
     * Function that performs a map lookup first based on the type root, then on any of its
     * families.
     */
    private static <T> T lookupTypeInMap(Map<Object, T> map, LogicalTypeRoot type) {
        T out = map.get(type);
        if (out == null) {
            /* lookup by any family matching */
            for (LogicalTypeFamily family : type.getFamilies()) {
                out = map.get(family);
                if (out != null) {
                    break;
                }
            }
        }
        return out;
    }
}
