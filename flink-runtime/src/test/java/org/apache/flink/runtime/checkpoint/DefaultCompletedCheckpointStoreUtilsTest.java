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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.persistence.TestingStateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests related to {@link DefaultCompletedCheckpointStoreUtils}. */
public class DefaultCompletedCheckpointStoreUtilsTest extends TestLogger {

    private static CompletedCheckpoint createCompletedCheckpoint(long checkpointId) {
        return new CompletedCheckpoint(
                new JobID(),
                checkpointId,
                0,
                1,
                new HashMap<>(),
                Collections.emptyList(),
                CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                new TestCompletedCheckpointStorageLocation());
    }

    private static class FailingRetrievableStateHandle<T extends Serializable>
            implements RetrievableStateHandle<T> {

        private static final int serialVersionUID = 1;

        @Override
        public T retrieveState() throws IOException, ClassNotFoundException {
            throw new IOException("Test exception.");
        }

        @Override
        public void discardState() throws Exception {
            // No-op.
        }

        @Override
        public long getStateSize() {
            return 0;
        }
    }

    private static class SimpleCheckpointStoreUtil implements CheckpointStoreUtil {

        @Override
        public String checkpointIDToName(long checkpointId) {
            return "checkpoint-" + checkpointId;
        }

        @Override
        public long nameToCheckpointID(String name) {
            return Long.parseLong(name.split("-")[1]);
        }
    }

    @Test
    public void testRetrievedCheckpointsAreOrderedChronologically() throws Exception {
        final TestingRetrievableStateStorageHelper<CompletedCheckpoint> storageHelper =
                new TestingRetrievableStateStorageHelper<>();
        final List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> handles =
                new ArrayList<>();
        handles.add(Tuple2.of(storageHelper.store(createCompletedCheckpoint(0L)), "checkpoint-0"));
        handles.add(Tuple2.of(storageHelper.store(createCompletedCheckpoint(1L)), "checkpoint-1"));
        handles.add(Tuple2.of(storageHelper.store(createCompletedCheckpoint(2L)), "checkpoint-2"));
        Collections.shuffle(handles);
        final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore =
                TestingStateHandleStore.<CompletedCheckpoint>newBuilder()
                        .setGetAllSupplier(() -> handles)
                        .build();
        final Collection<CompletedCheckpoint> completedCheckpoints =
                DefaultCompletedCheckpointStoreUtils.retrieveCompletedCheckpoints(
                        stateHandleStore, new SimpleCheckpointStoreUtil());
        // Make sure checkpoints are ordered from earliest to latest.
        assertThat(
                        completedCheckpoints.stream()
                                .map(CompletedCheckpoint::getCheckpointID)
                                .collect(Collectors.toList()))
                .isEqualTo(Arrays.asList(0L, 1L, 2L));
    }

    @Test
    public void testRetrievingCheckpointsFailsIfRetrievalOfAnyCheckpointFails() throws Exception {
        final TestingRetrievableStateStorageHelper<CompletedCheckpoint> storageHelper =
                new TestingRetrievableStateStorageHelper<>();
        final List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> handles =
                new ArrayList<>();
        handles.add(Tuple2.of(storageHelper.store(createCompletedCheckpoint(0L)), "checkpoint-0"));
        handles.add(Tuple2.of(new FailingRetrievableStateHandle<>(), "checkpoint-1"));
        handles.add(Tuple2.of(storageHelper.store(createCompletedCheckpoint(2L)), "checkpoint-2"));
        Collections.shuffle(handles);
        final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore =
                TestingStateHandleStore.<CompletedCheckpoint>newBuilder()
                        .setGetAllSupplier(() -> handles)
                        .build();
        assertThatThrownBy(
                        () ->
                                DefaultCompletedCheckpointStoreUtils.retrieveCompletedCheckpoints(
                                        stateHandleStore, new SimpleCheckpointStoreUtil()))
                .isInstanceOf(FlinkException.class);
    }
}
