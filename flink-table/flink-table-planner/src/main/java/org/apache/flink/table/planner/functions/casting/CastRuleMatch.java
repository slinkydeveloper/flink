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

import org.apache.flink.annotation.Internal;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A match result of a {@link CastRule} is three valued, in order to explicitly describe if the rule
 * is fallible or not.
 */
@Internal
public enum CastRuleMatch {
    /** Means the cast tuple is supported and its runtime implementation can never fail. */
    INFALLIBLE,
    /**
     * Means the cast tuple is supported but its runtime implementation can fail, depending on the
     * input value.
     */
    FALLIBLE,
    /** Means the cast tuple is not supported and should be rejected. */
    UNSUPPORTED;

    boolean matches() {
        return this != UNSUPPORTED;
    }

    static CastRuleMatch and(CastRuleMatch x, CastRuleMatch y, CastRuleMatch... others) {
        if (others.length >= 2) {
            y = and(y, others[0], Arrays.copyOfRange(others, 1, others.length));
        } else if (others.length == 1) {
            y = and(y, others[0]);
        }
        if (x == UNSUPPORTED || y == UNSUPPORTED) {
            return UNSUPPORTED;
        }
        if (x == FALLIBLE || y == FALLIBLE) {
            return FALLIBLE;
        }
        return INFALLIBLE;
    }

    static CastRuleMatch and(boolean x, CastRuleMatch y, CastRuleMatch... others) {
        return and(from(x), y, others);
    }

    @SafeVarargs
    static CastRuleMatch and(
            boolean x, Supplier<CastRuleMatch> y, Supplier<CastRuleMatch>... others) {
        if (!x) {
            return UNSUPPORTED;
        }
        return and(
                from(x),
                y.get(),
                Arrays.stream(others).map(Supplier::get).toArray(CastRuleMatch[]::new));
    }

    @SuppressWarnings("unchecked")
    static <T> CastRuleMatch ifTypeThen(
            Object input, Class<T> clazz, Function<T, CastRuleMatch> matchFunction) {
        if (input.getClass().equals(clazz)) {
            return matchFunction.apply((T) input);
        }
        return UNSUPPORTED;
    }

    static CastRuleMatch xor(CastRuleMatch x, CastRuleMatch y) {
        if (!x.matches() && !y.matches()) {
            return UNSUPPORTED;
        }
        if (x.matches() && y.matches()) {
            return UNSUPPORTED;
        }
        if (x.matches()) {
            return x;
        }
        return y;
    }

    static CastRuleMatch from(boolean x) {
        return x ? INFALLIBLE : UNSUPPORTED;
    }
}
