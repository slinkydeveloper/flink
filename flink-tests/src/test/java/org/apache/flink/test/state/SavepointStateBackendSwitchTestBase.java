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

package org.apache.flink.test.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.test.state.BackendSwitchSpecs.BackendSwitchSpec;
import org.apache.flink.util.InstantiationUtil;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.RunnableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.Matchers.contains;

/**
 * Tests for the unified savepoint format. They verify you can switch a state backend through a
 * savepoint.
 */
public abstract class SavepointStateBackendSwitchTestBase {

    private static final KeyGroupRange KEY_GROUP_RANGE = new KeyGroupRange(0, 1);
    private static final int NUM_KEY_GROUPS = KEY_GROUP_RANGE.getNumberOfKeyGroups();

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    private final BackendSwitchSpec fromBackend;

    private final BackendSwitchSpec toBackend;

    protected SavepointStateBackendSwitchTestBase(
            BackendSwitchSpec fromBackend, BackendSwitchSpec toBackend) {
        this.fromBackend = fromBackend;
        this.toBackend = toBackend;
    }

    @Test
    public void switchStateBackend() throws Exception {

        final File pathToWrite = tempFolder.newFile();

        final MapStateDescriptor<Long, Long> mapStateDescriptor =
                new MapStateDescriptor<>("my-map-state", Long.class, Long.class);
        mapStateDescriptor.initializeSerializerUnlessSet(new ExecutionConfig());

        final ValueStateDescriptor<Long> valueStateDescriptor =
                new ValueStateDescriptor<>("my-value-state", Long.class);
        valueStateDescriptor.initializeSerializerUnlessSet(new ExecutionConfig());

        final ListStateDescriptor<Long> listStateDescriptor =
                new ListStateDescriptor<>("my-list-state", Long.class);
        listStateDescriptor.initializeSerializerUnlessSet(new ExecutionConfig());

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;
        final Integer namespace4 = 4;

        try (final CheckpointableKeyedStateBackend<String> keyedBackend =
                fromBackend.createBackend(
                        KEY_GROUP_RANGE, NUM_KEY_GROUPS, Collections.emptyList())) {
            takeSavepoint(
                    keyedBackend,
                    pathToWrite,
                    mapStateDescriptor,
                    valueStateDescriptor,
                    listStateDescriptor,
                    namespace1,
                    namespace2,
                    namespace3,
                    namespace4);
        }

        final SnapshotResult<KeyedStateHandle> stateHandles;
        try (BufferedInputStream bis =
                new BufferedInputStream((new FileInputStream(pathToWrite)))) {
            stateHandles =
                    InstantiationUtil.deserializeObject(
                            bis, Thread.currentThread().getContextClassLoader());
        }
        final KeyedStateHandle stateHandle = stateHandles.getJobManagerOwnedSnapshot();
        try (final CheckpointableKeyedStateBackend<String> keyedBackend =
                toBackend.createBackend(
                        KEY_GROUP_RANGE,
                        NUM_KEY_GROUPS,
                        StateObjectCollection.singleton(stateHandle))) {
            verifyRestoredState(
                    mapStateDescriptor,
                    valueStateDescriptor,
                    listStateDescriptor,
                    namespace1,
                    namespace2,
                    namespace3,
                    namespace4,
                    keyedBackend);
        }
    }

    private <K, N, UK, UV> int getStateSize(InternalMapState<K, N, UK, UV> mapState)
            throws Exception {
        int i = 0;
        Iterator<Map.Entry<UK, UV>> itt = mapState.iterator();
        while (itt.hasNext()) {
            i++;
            itt.next();
        }
        return i;
    }

    private void takeSavepoint(
            CheckpointableKeyedStateBackend<String> keyedBackend,
            File pathToWrite,
            MapStateDescriptor<Long, Long> stateDescr,
            ValueStateDescriptor<Long> valueStateDescriptor,
            ListStateDescriptor<Long> listStateDescriptor,
            Integer namespace1,
            Integer namespace2,
            Integer namespace3,
            Integer namespace4)
            throws Exception {

        InternalMapState<String, Integer, Long, Long> mapState =
                keyedBackend.createInternalState(IntSerializer.INSTANCE, stateDescr);

        InternalValueState<String, Integer, Long> valueState =
                keyedBackend.createInternalState(IntSerializer.INSTANCE, valueStateDescriptor);

        InternalListState<String, Integer, Long> listState =
                keyedBackend.createInternalState(IntSerializer.INSTANCE, listStateDescriptor);

        keyedBackend.setCurrentKey("abc");
        mapState.setCurrentNamespace(namespace1);
        mapState.put(33L, 33L);
        mapState.put(55L, 55L);

        mapState.setCurrentNamespace(namespace2);
        mapState.put(22L, 22L);
        mapState.put(11L, 11L);

        listState.setCurrentNamespace(namespace2);
        listState.add(4L);
        listState.add(5L);
        listState.add(6L);

        mapState.setCurrentNamespace(namespace3);
        mapState.put(44L, 44L);

        keyedBackend.setCurrentKey("mno");
        mapState.setCurrentNamespace(namespace3);
        mapState.put(11L, 11L);
        mapState.put(22L, 22L);
        mapState.put(33L, 33L);
        mapState.put(44L, 44L);
        mapState.put(55L, 55L);

        valueState.setCurrentNamespace(namespace3);
        valueState.update(1239L);
        listState.setCurrentNamespace(namespace3);
        listState.add(1L);
        listState.add(2L);
        listState.add(3L);

        mapState.setCurrentNamespace(namespace4);
        mapState.put(1L, 1L);
        // HEAP state backend will keep an empty map as an entry in the underlying State Table
        // we should skip such entries when serializing
        Iterator<Map.Entry<Long, Long>> iterator = mapState.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }

        KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<String, Integer>> priorityQueue =
                keyedBackend.create(
                        "event-time",
                        new TimerSerializer<>(
                                keyedBackend.getKeySerializer(), IntSerializer.INSTANCE));
        priorityQueue.add(new TimerHeapInternalTimer<>(1234L, "mno", namespace3));
        priorityQueue.add(new TimerHeapInternalTimer<>(2345L, "mno", namespace2));
        priorityQueue.add(new TimerHeapInternalTimer<>(3456L, "mno", namespace3));

        SnapshotStrategyRunner<KeyedStateHandle, ? extends FullSnapshotResources<?>>
                savepointRunner =
                        StreamOperatorStateHandler.prepareSavepoint(
                                keyedBackend, new CloseableRegistry());

        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                savepointRunner.snapshot(
                        0L,
                        0L,
                        new MemCheckpointStreamFactory(4 * 1024 * 1024),
                        new CheckpointOptions(
                                CheckpointType.SAVEPOINT,
                                CheckpointStorageLocationReference.getDefault()));

        snapshot.run();

        try (BufferedOutputStream bis =
                new BufferedOutputStream(new FileOutputStream(pathToWrite))) {
            InstantiationUtil.serializeObject(bis, snapshot.get());
        }
    }

    private void verifyRestoredState(
            MapStateDescriptor<Long, Long> mapStateDescriptor,
            ValueStateDescriptor<Long> valueStateDescriptor,
            ListStateDescriptor<Long> listStateDescriptor,
            Integer namespace1,
            Integer namespace2,
            Integer namespace3,
            Integer namespace4,
            CheckpointableKeyedStateBackend<String> keyedBackend)
            throws Exception {
        InternalMapState<String, Integer, Long, Long> mapState =
                keyedBackend.createInternalState(IntSerializer.INSTANCE, mapStateDescriptor);

        InternalValueState<String, Integer, Long> valueState =
                keyedBackend.createInternalState(IntSerializer.INSTANCE, valueStateDescriptor);

        InternalListState<String, Integer, Long> listState =
                keyedBackend.createInternalState(IntSerializer.INSTANCE, listStateDescriptor);

        keyedBackend.setCurrentKey("abc");
        mapState.setCurrentNamespace(namespace1);
        assertThat((long) mapState.get(33L)).isEqualTo(33L);
        assertThat((long) mapState.get(55L)).isEqualTo(55L);
        assertThat(getStateSize(mapState)).isEqualTo(2);

        mapState.setCurrentNamespace(namespace2);
        assertThat((long) mapState.get(22L)).isEqualTo(22L);
        assertThat((long) mapState.get(11L)).isEqualTo(11L);
        assertThat(getStateSize(mapState)).isEqualTo(2);
        listState.setCurrentNamespace(namespace2);
        assertThat(listState.get()).satisfies(matching(contains(4L, 5L, 6L)));

        mapState.setCurrentNamespace(namespace3);
        assertThat((long) mapState.get(44L)).isEqualTo(44L);
        assertThat(getStateSize(mapState)).isEqualTo(1);

        keyedBackend.setCurrentKey("mno");
        mapState.setCurrentNamespace(namespace3);
        assertThat((long) mapState.get(11L)).isEqualTo(11L);
        assertThat((long) mapState.get(22L)).isEqualTo(22L);
        assertThat((long) mapState.get(33L)).isEqualTo(33L);
        assertThat((long) mapState.get(44L)).isEqualTo(44L);
        assertThat((long) mapState.get(55L)).isEqualTo(55L);
        assertThat(getStateSize(mapState)).isEqualTo(5);
        valueState.setCurrentNamespace(namespace3);
        assertThat((long) valueState.value()).isEqualTo(1239L);
        listState.setCurrentNamespace(namespace3);
        assertThat(listState.get()).satisfies(matching(contains(1L, 2L, 3L)));

        mapState.setCurrentNamespace(namespace4);
        assertThat(mapState.isEmpty()).isEqualTo(true);

        KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<String, Integer>> priorityQueue =
                keyedBackend.create(
                        "event-time",
                        new TimerSerializer<>(
                                keyedBackend.getKeySerializer(), IntSerializer.INSTANCE));

        assertThat(priorityQueue.size()).isEqualTo(3);
        assertThat(priorityQueue.poll())
                .isEqualTo(new TimerHeapInternalTimer<>(1234L, "mno", namespace3));
        assertThat(priorityQueue.poll())
                .isEqualTo(new TimerHeapInternalTimer<>(2345L, "mno", namespace2));
        assertThat(priorityQueue.poll())
                .isEqualTo(new TimerHeapInternalTimer<>(3456L, "mno", namespace3));
    }
}
