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

package org.apache.flink.state.api.input;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.ttl.mock.MockStateBackend;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the multi-state key iterator. */
public class MultiStateKeyIteratorTest {
    private static final List<ValueStateDescriptor<Integer>> descriptors;

    static {
        descriptors = new ArrayList<>(2);
        descriptors.add(new ValueStateDescriptor<>("state-1", Types.INT));
        descriptors.add(new ValueStateDescriptor<>("state-2", Types.INT));
    }

    private static AbstractKeyedStateBackend<Integer> createKeyedStateBackend() {
        MockStateBackend backend = new MockStateBackend();

        return backend.createKeyedStateBackend(
                new DummyEnvironment(),
                new JobID(),
                "mock-backend",
                IntSerializer.INSTANCE,
                129,
                KeyGroupRange.of(0, 128),
                null,
                TtlTimeProvider.DEFAULT,
                UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
                Collections.emptyList(),
                new CloseableRegistry());
    }

    private static void setKey(
            AbstractKeyedStateBackend<Integer> backend,
            ValueStateDescriptor<Integer> descriptor,
            int key)
            throws Exception {
        backend.setCurrentKey(key);
        backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor)
                .update(0);
    }

    @Test
    public void testIteratorPullsKeyFromAllDescriptors() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedStateBackend = createKeyedStateBackend();

        setKey(keyedStateBackend, descriptors.get(0), 1);
        setKey(keyedStateBackend, descriptors.get(1), 2);

        MultiStateKeyIterator<Integer> iterator =
                new MultiStateKeyIterator<>(descriptors, keyedStateBackend);

        List<Integer> keys = new ArrayList<>();

        while (iterator.hasNext()) {
            keys.add(iterator.next());
        }

        assertThat(keys.size()).as("Unexpected number of keys").isEqualTo(2);
        assertThat(keys).as("Unexpected keys found").isEqualTo(Arrays.asList(1, 2));
    }

    @Test
    public void testIteratorRemovesFromAllDescriptors() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedStateBackend = createKeyedStateBackend();

        setKey(keyedStateBackend, descriptors.get(0), 1);
        setKey(keyedStateBackend, descriptors.get(1), 1);

        MultiStateKeyIterator<Integer> iterator =
                new MultiStateKeyIterator<>(descriptors, keyedStateBackend);

        int key = iterator.next();
        assertThat(key).as("Unexpected keys pulled from state backend").isEqualTo(1);

        iterator.remove();
        assertThat(iterator.hasNext())
                .as("Failed to drop key from all descriptors in state backend")
                .isFalse();

        for (StateDescriptor<?, ?> descriptor : descriptors) {
            assertThat(
                            keyedStateBackend
                                    .getKeys(descriptor.getName(), VoidNamespace.INSTANCE)
                                    .count())
                    .as("Failed to drop key for state descriptor")
                    .isEqualTo(0);
        }
    }
}
