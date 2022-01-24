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

package org.apache.flink.table.test.actions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Static holder for mock of actions, that can be used by the various factories. */
public class MockActions {

    private static final AtomicInteger actionsIncrementalId = new AtomicInteger();
    private static final Map<String, Object> actions = new ConcurrentHashMap<>();

    public static String reserveIdentifier(Class<?> clazz) {
        return String.format("action$%s$%d", clazz, actionsIncrementalId.getAndIncrement());
    }

    /**
     * Register a {@link RowDataConsumer} action using the provided identifier and returns the
     * identifier.
     */
    public static String register(String key, RowDataConsumer rowDataConsumer) {
        actions.put(key, rowDataConsumer);
        return key;
    }

    /**
     * Register a {@link RowDataConsumer} action creating a new identifier and returns the newly
     * created identifier.
     */
    public static String register(RowDataConsumer rowDataConsumer) {
        final String key = reserveIdentifier(rowDataConsumer.getClass());
        actions.put(key, rowDataConsumer);
        return key;
    }

    /** Resolve an action. */
    public static <T> T resolve(String id, Class<T> clazz) {
        final Object action = actions.get(id);
        if (action == null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot resolve the mock action '%s'. "
                                    + "You need to register it with MockActions#register.",
                            id));
        }
        return clazz.cast(action);
    }
}
