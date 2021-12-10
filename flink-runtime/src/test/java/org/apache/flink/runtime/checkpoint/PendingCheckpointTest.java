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
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.StringSerializer;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint.TaskAcknowledgeResult;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.operators.coordination.TestingOperatorInfo;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.TestingStreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.concurrent.Executors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

/** Tests for the {@link PendingCheckpoint}. */
public class PendingCheckpointTest {

    private static final List<Execution> ACK_TASKS = new ArrayList<>();
    private static final List<ExecutionVertex> TASKS_TO_COMMIT = new ArrayList<>();
    private static final ExecutionAttemptID ATTEMPT_ID = new ExecutionAttemptID();

    public static final OperatorID OPERATOR_ID = new OperatorID();

    public static final int PARALLELISM = 1;

    public static final int MAX_PARALLELISM = 128;

    static {
        ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
        when(jobVertex.getOperatorIDs())
                .thenReturn(Collections.singletonList(OperatorIDPair.generatedIDOnly(OPERATOR_ID)));

        ExecutionVertex vertex = mock(ExecutionVertex.class);
        when(vertex.getMaxParallelism()).thenReturn(MAX_PARALLELISM);
        when(vertex.getTotalNumberOfParallelSubtasks()).thenReturn(PARALLELISM);
        when(vertex.getJobVertex()).thenReturn(jobVertex);

        Execution execution = mock(Execution.class);
        when(execution.getAttemptId()).thenReturn(ATTEMPT_ID);
        when(execution.getVertex()).thenReturn(vertex);
        ACK_TASKS.add(execution);
        TASKS_TO_COMMIT.add(vertex);
    }

    @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

    @Rule public final ExpectedException expectedException = ExpectedException.none();

    /** Tests that pending checkpoints can be subsumed iff they are forced. */
    @Test
    public void testCanBeSubsumed() throws Exception {
        // Forced checkpoints cannot be subsumed
        CheckpointProperties forced =
                new CheckpointProperties(
                        true, CheckpointType.SAVEPOINT, false, false, false, false, false);
        PendingCheckpoint pending = createPendingCheckpoint(forced);
        assertThat(pending.canBeSubsumed()).isFalse();

        try {
            abort(pending, CheckpointFailureReason.CHECKPOINT_SUBSUMED);
            fail("Did not throw expected Exception");
        } catch (IllegalStateException ignored) {
            // Expected
        }

        // Non-forced checkpoints can be subsumed
        CheckpointProperties subsumed =
                new CheckpointProperties(
                        false, CheckpointType.SAVEPOINT, false, false, false, false, false);
        pending = createPendingCheckpoint(subsumed);
        assertThat(pending.canBeSubsumed()).isFalse();
    }

    @Test
    public void testSyncSavepointCannotBeSubsumed() throws Exception {
        // Forced checkpoints cannot be subsumed
        CheckpointProperties forced = CheckpointProperties.forSyncSavepoint(true, false);
        PendingCheckpoint pending = createPendingCheckpoint(forced);
        assertThat(pending.canBeSubsumed()).isFalse();

        try {
            abort(pending, CheckpointFailureReason.CHECKPOINT_SUBSUMED);
            fail("Did not throw expected Exception");
        } catch (IllegalStateException ignored) {
            // Expected
        }
    }

    /**
     * Tests that the completion future is succeeded on finalize and failed on abort and failures
     * during finalize.
     */
    @Test
    public void testCompletionFuture() throws Exception {
        CheckpointProperties props =
                new CheckpointProperties(
                        false, CheckpointType.SAVEPOINT, false, false, false, false, false);

        // Abort declined
        PendingCheckpoint pending = createPendingCheckpoint(props);
        CompletableFuture<CompletedCheckpoint> future = pending.getCompletionFuture();

        assertThat(future.isDone()).isFalse();
        abort(pending, CheckpointFailureReason.CHECKPOINT_DECLINED);
        assertThat(future.isDone()).isTrue();

        // Abort expired
        pending = createPendingCheckpoint(props);
        future = pending.getCompletionFuture();

        assertThat(future.isDone()).isFalse();
        abort(pending, CheckpointFailureReason.CHECKPOINT_DECLINED);
        assertThat(future.isDone()).isTrue();

        // Abort subsumed
        pending = createPendingCheckpoint(props);
        future = pending.getCompletionFuture();

        assertThat(future.isDone()).isFalse();
        abort(pending, CheckpointFailureReason.CHECKPOINT_DECLINED);
        assertThat(future.isDone()).isTrue();

        // Finalize (all ACK'd)
        pending = createPendingCheckpoint(props);
        future = pending.getCompletionFuture();

        assertThat(future.isDone()).isFalse();
        pending.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics(), null);
        assertThat(pending.areTasksFullyAcknowledged()).isTrue();
        pending.finalizeCheckpoint(
                new CheckpointsCleaner(), () -> {}, Executors.directExecutor(), null);
        assertThat(future.isDone()).isTrue();

        // Finalize (missing ACKs)
        pending = createPendingCheckpoint(props);
        future = pending.getCompletionFuture();

        assertThat(future.isDone()).isFalse();
        try {
            pending.finalizeCheckpoint(
                    new CheckpointsCleaner(), () -> {}, Executors.directExecutor(), null);
            fail("Did not throw expected Exception");
        } catch (IllegalStateException ignored) {
            // Expected
        }
    }

    /** Tests that abort discards state. */
    @Test
    public void testAbortDiscardsState() throws Exception {
        CheckpointProperties props =
                new CheckpointProperties(
                        false, CheckpointType.CHECKPOINT, false, false, false, false, false);
        QueueExecutor executor = new QueueExecutor();

        OperatorState state = mock(OperatorState.class);
        doNothing().when(state).registerSharedStates(any(SharedStateRegistry.class));

        // Abort declined
        PendingCheckpoint pending = createPendingCheckpoint(props, executor);
        setTaskState(pending, state);

        abort(pending, CheckpointFailureReason.CHECKPOINT_DECLINED);
        // execute asynchronous discard operation
        executor.runQueuedCommands();
        verify(state, times(1)).discardState();

        // Abort error
        Mockito.reset(state);

        pending = createPendingCheckpoint(props, executor);
        setTaskState(pending, state);

        abort(pending, CheckpointFailureReason.CHECKPOINT_DECLINED);
        // execute asynchronous discard operation
        executor.runQueuedCommands();
        verify(state, times(1)).discardState();

        // Abort expired
        Mockito.reset(state);

        pending = createPendingCheckpoint(props, executor);
        setTaskState(pending, state);

        abort(pending, CheckpointFailureReason.CHECKPOINT_EXPIRED);
        // execute asynchronous discard operation
        executor.runQueuedCommands();
        verify(state, times(1)).discardState();

        // Abort subsumed
        Mockito.reset(state);

        pending = createPendingCheckpoint(props, executor);
        setTaskState(pending, state);

        abort(pending, CheckpointFailureReason.CHECKPOINT_SUBSUMED);
        // execute asynchronous discard operation
        executor.runQueuedCommands();
        verify(state, times(1)).discardState();
    }

    /** Tests that the stats callbacks happen if the callback is registered. */
    @Test
    public void testPendingCheckpointStatsCallbacks() throws Exception {
        {
            // Complete successfully
            PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
            PendingCheckpoint pending =
                    createPendingCheckpoint(
                            CheckpointProperties.forCheckpoint(
                                    CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));

            pending.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics(), callback);
            verify(callback, times(1))
                    .reportSubtaskStats(nullable(JobVertexID.class), any(SubtaskStateStats.class));

            pending.finalizeCheckpoint(
                    new CheckpointsCleaner(), () -> {}, Executors.directExecutor(), callback);
            verify(callback, times(1)).reportCompletedCheckpoint(any(String.class));
        }

        {
            // Fail subsumed
            PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
            PendingCheckpoint pending =
                    createPendingCheckpoint(
                            CheckpointProperties.forCheckpoint(
                                    CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));

            abort(pending, CheckpointFailureReason.CHECKPOINT_SUBSUMED, callback);
            verify(callback, times(1)).reportFailedCheckpoint(anyLong(), any(Exception.class));
        }

        {
            // Fail subsumed
            PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
            PendingCheckpoint pending =
                    createPendingCheckpoint(
                            CheckpointProperties.forCheckpoint(
                                    CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));

            abort(pending, CheckpointFailureReason.CHECKPOINT_DECLINED, callback);
            verify(callback, times(1)).reportFailedCheckpoint(anyLong(), any(Exception.class));
        }

        {
            // Fail subsumed
            PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
            PendingCheckpoint pending =
                    createPendingCheckpoint(
                            CheckpointProperties.forCheckpoint(
                                    CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));

            abort(pending, CheckpointFailureReason.CHECKPOINT_SUBSUMED, callback);
            verify(callback, times(1)).reportFailedCheckpoint(anyLong(), any(Exception.class));
        }

        {
            // Fail subsumed
            PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
            PendingCheckpoint pending =
                    createPendingCheckpoint(
                            CheckpointProperties.forCheckpoint(
                                    CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));

            abort(pending, CheckpointFailureReason.CHECKPOINT_EXPIRED, callback);
            verify(callback, times(1)).reportFailedCheckpoint(anyLong(), any(Exception.class));
        }
    }

    /**
     * FLINK-5985.
     *
     * <p>Ensures that subtasks that acknowledge their state as 'null' are considered stateless.
     * This means that they should not appear in the task states map of the checkpoint.
     */
    @Test
    public void testNullSubtaskStateLeadsToStatelessTask() throws Exception {
        PendingCheckpoint pending =
                createPendingCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
        pending.acknowledgeTask(ATTEMPT_ID, null, mock(CheckpointMetrics.class), null);
        final OperatorState expectedState =
                new OperatorState(OPERATOR_ID, PARALLELISM, MAX_PARALLELISM);
        assertThat(pending.getOperatorStates())
                .isEqualTo(Collections.singletonMap(OPERATOR_ID, expectedState));
    }

    /**
     * FLINK-5985.
     *
     * <p>This tests checks the inverse of {@link #testNullSubtaskStateLeadsToStatelessTask()}. We
     * want to test that for subtasks that acknowledge some state are given an entry in the task
     * states of the checkpoint.
     */
    @Test
    public void testNonNullSubtaskStateLeadsToStatefulTask() throws Exception {
        PendingCheckpoint pending =
                createPendingCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
        pending.acknowledgeTask(
                ATTEMPT_ID, mock(TaskStateSnapshot.class), mock(CheckpointMetrics.class), null);
        assertThat(pending.getOperatorStates().isEmpty()).isFalse();
    }

    @Test
    public void testSetCanceller() throws Exception {
        final CheckpointProperties props =
                new CheckpointProperties(
                        false, CheckpointType.CHECKPOINT, true, true, true, true, true);

        PendingCheckpoint aborted = createPendingCheckpoint(props);
        abort(aborted, CheckpointFailureReason.CHECKPOINT_DECLINED);
        assertThat(aborted.isDisposed()).isTrue();
        assertThat(aborted.setCancellerHandle(mock(ScheduledFuture.class))).isFalse();

        PendingCheckpoint pending = createPendingCheckpoint(props);
        ScheduledFuture<?> canceller = mock(ScheduledFuture.class);

        assertThat(pending.setCancellerHandle(canceller)).isTrue();
        abort(pending, CheckpointFailureReason.CHECKPOINT_DECLINED);
        verify(canceller).cancel(false);
    }

    @Test
    public void testMasterState() throws Exception {
        final TestingMasterTriggerRestoreHook masterHook =
                new TestingMasterTriggerRestoreHook("master hook");
        masterHook.addStateContent("state");

        final PendingCheckpoint pending =
                createPendingCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        Collections.singletonList(masterHook.getIdentifier()));

        final MasterState masterState =
                MasterHooks.triggerHook(
                                masterHook,
                                0,
                                System.currentTimeMillis(),
                                Executors.directExecutor())
                        .get();

        pending.acknowledgeMasterState(masterHook.getIdentifier(), masterState);
        assertThat(pending.areMasterStatesFullyAcknowledged()).isTrue();
        assertThat(pending.areTasksFullyAcknowledged()).isFalse();

        pending.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics(), null);
        assertThat(pending.areTasksFullyAcknowledged()).isTrue();

        final List<MasterState> resultMasterStates = pending.getMasterStates();
        assertThat(resultMasterStates.size()).isEqualTo(1);
        final String deserializedState =
                masterHook
                        .createCheckpointDataSerializer()
                        .deserialize(StringSerializer.VERSION, resultMasterStates.get(0).bytes());
        assertThat(deserializedState).isEqualTo("state");
    }

    @Test
    public void testMasterStateWithNullState() throws Exception {
        final TestingMasterTriggerRestoreHook masterHook =
                new TestingMasterTriggerRestoreHook("master hook");
        masterHook.addStateContent("state");

        final TestingMasterTriggerRestoreHook nullableMasterHook =
                new TestingMasterTriggerRestoreHook("nullable master hook");

        final List<String> masterIdentifiers = new ArrayList<>(2);
        masterIdentifiers.add(masterHook.getIdentifier());
        masterIdentifiers.add(nullableMasterHook.getIdentifier());

        final PendingCheckpoint pending =
                createPendingCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        masterIdentifiers);

        final MasterState masterStateNormal =
                MasterHooks.triggerHook(
                                masterHook,
                                0,
                                System.currentTimeMillis(),
                                Executors.directExecutor())
                        .get();

        pending.acknowledgeMasterState(masterHook.getIdentifier(), masterStateNormal);
        assertThat(pending.areMasterStatesFullyAcknowledged()).isFalse();

        final MasterState masterStateNull =
                MasterHooks.triggerHook(
                                nullableMasterHook,
                                0,
                                System.currentTimeMillis(),
                                Executors.directExecutor())
                        .get();
        pending.acknowledgeMasterState(nullableMasterHook.getIdentifier(), masterStateNull);
        assertThat(pending.areMasterStatesFullyAcknowledged()).isTrue();
        assertThat(pending.areTasksFullyAcknowledged()).isFalse();

        pending.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics(), null);
        assertThat(pending.areTasksFullyAcknowledged()).isTrue();

        final List<MasterState> resultMasterStates = pending.getMasterStates();
        assertThat(resultMasterStates.size()).isEqualTo(1);
        final String deserializedState =
                masterHook
                        .createCheckpointDataSerializer()
                        .deserialize(StringSerializer.VERSION, resultMasterStates.get(0).bytes());
        assertThat(deserializedState).isEqualTo("state");
    }

    @Test
    public void testInitiallyUnacknowledgedCoordinatorStates() throws Exception {
        final PendingCheckpoint checkpoint =
                createPendingCheckpointWithCoordinators(
                        new TestingOperatorInfo(), new TestingOperatorInfo());

        assertThat(checkpoint.getNumberOfNonAcknowledgedOperatorCoordinators()).isEqualTo(2);
        assertThat(checkpoint.isFullyAcknowledged()).isFalse();
    }

    @Test
    public void testAcknowledgedCoordinatorStates() throws Exception {
        final OperatorInfo coord1 = new TestingOperatorInfo();
        final OperatorInfo coord2 = new TestingOperatorInfo();
        final PendingCheckpoint checkpoint =
                createPendingCheckpointWithCoordinators(coord1, coord2);

        final TaskAcknowledgeResult ack1 =
                checkpoint.acknowledgeCoordinatorState(coord1, new TestingStreamStateHandle());
        final TaskAcknowledgeResult ack2 = checkpoint.acknowledgeCoordinatorState(coord2, null);

        assertThat(ack1).isEqualTo(TaskAcknowledgeResult.SUCCESS);
        assertThat(ack2).isEqualTo(TaskAcknowledgeResult.SUCCESS);
        assertThat(checkpoint.getNumberOfNonAcknowledgedOperatorCoordinators()).isEqualTo(0);
        assertThat(checkpoint.isFullyAcknowledged()).isTrue();
        assertThat(checkpoint.getOperatorStates().keySet())
                .satisfies(
                        matching(
                                containsInAnyOrder(
                                        OPERATOR_ID, coord1.operatorId(), coord2.operatorId())));
    }

    @Test
    public void testDuplicateAcknowledgeCoordinator() throws Exception {
        final OperatorInfo coordinator = new TestingOperatorInfo();
        final PendingCheckpoint checkpoint = createPendingCheckpointWithCoordinators(coordinator);

        checkpoint.acknowledgeCoordinatorState(coordinator, new TestingStreamStateHandle());
        final TaskAcknowledgeResult secondAck =
                checkpoint.acknowledgeCoordinatorState(coordinator, null);

        assertThat(secondAck).isEqualTo(TaskAcknowledgeResult.DUPLICATE);
    }

    @Test
    public void testAcknowledgeUnknownCoordinator() throws Exception {
        final PendingCheckpoint checkpoint =
                createPendingCheckpointWithCoordinators(new TestingOperatorInfo());

        final TaskAcknowledgeResult ack =
                checkpoint.acknowledgeCoordinatorState(new TestingOperatorInfo(), null);

        assertThat(ack).isEqualTo(TaskAcknowledgeResult.UNKNOWN);
    }

    @Test
    public void testDisposeDisposesCoordinatorStates() throws Exception {
        final TestingStreamStateHandle handle1 = new TestingStreamStateHandle();
        final TestingStreamStateHandle handle2 = new TestingStreamStateHandle();
        final PendingCheckpoint checkpoint =
                createPendingCheckpointWithAcknowledgedCoordinators(handle1, handle2);

        abort(checkpoint, CheckpointFailureReason.CHECKPOINT_EXPIRED);

        assertThat(handle1.isDisposed()).isTrue();
        assertThat(handle2.isDisposed()).isTrue();
    }

    @Test
    public void testReportTaskFinishedOnRestore() throws IOException {
        RecordCheckpointPlan recordCheckpointPlan =
                new RecordCheckpointPlan(new ArrayList<>(ACK_TASKS));
        PendingCheckpoint checkpoint = createPendingCheckpoint(recordCheckpointPlan);
        checkpoint.acknowledgeTask(
                ACK_TASKS.get(0).getAttemptId(),
                TaskStateSnapshot.FINISHED_ON_RESTORE,
                new CheckpointMetrics(),
                null);
        assertThat(recordCheckpointPlan.getReportedFinishedOnRestoreTasks())
                .satisfies(matching(contains(ACK_TASKS.get(0).getVertex())));
    }

    @Test
    public void testReportTaskFinishedOperators() throws IOException {
        RecordCheckpointPlan recordCheckpointPlan =
                new RecordCheckpointPlan(new ArrayList<>(ACK_TASKS));
        PendingCheckpoint checkpoint = createPendingCheckpoint(recordCheckpointPlan);
        checkpoint.acknowledgeTask(
                ACK_TASKS.get(0).getAttemptId(),
                new TaskStateSnapshot(10, true),
                new CheckpointMetrics(),
                null);
        assertThat(recordCheckpointPlan.getReportedOperatorsFinishedTasks())
                .satisfies(matching(contains(ACK_TASKS.get(0).getVertex())));
    }

    // ------------------------------------------------------------------------

    private PendingCheckpoint createPendingCheckpoint(CheckpointProperties props)
            throws IOException {
        return createPendingCheckpoint(
                props,
                Collections.emptyList(),
                Collections.emptyList(),
                Executors.directExecutor());
    }

    private PendingCheckpoint createPendingCheckpoint(CheckpointProperties props, Executor executor)
            throws IOException {
        return createPendingCheckpoint(
                props, Collections.emptyList(), Collections.emptyList(), executor);
    }

    private PendingCheckpoint createPendingCheckpoint(
            CheckpointProperties props, Collection<String> masterStateIdentifiers)
            throws IOException {
        return createPendingCheckpoint(
                props, Collections.emptyList(), masterStateIdentifiers, Executors.directExecutor());
    }

    private PendingCheckpoint createPendingCheckpoint(CheckpointPlan checkpointPlan)
            throws IOException {
        return createPendingCheckpoint(
                CheckpointProperties.forCheckpoint(
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                Collections.emptyList(),
                Collections.emptyList(),
                checkpointPlan,
                Executors.directExecutor());
    }

    private PendingCheckpoint createPendingCheckpointWithCoordinators(OperatorInfo... coordinators)
            throws IOException {

        final PendingCheckpoint checkpoint =
                createPendingCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        OperatorInfo.getIds(Arrays.asList(coordinators)),
                        Collections.emptyList(),
                        Executors.directExecutor());

        checkpoint.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics(), null);
        return checkpoint;
    }

    private PendingCheckpoint createPendingCheckpointWithAcknowledgedCoordinators(
            ByteStreamStateHandle... handles) throws IOException {
        final OperatorInfo[] coords = new OperatorInfo[handles.length];
        for (int i = 0; i < handles.length; i++) {
            coords[i] = new TestingOperatorInfo();
        }

        final PendingCheckpoint checkpoint = createPendingCheckpointWithCoordinators(coords);
        for (int i = 0; i < handles.length; i++) {
            checkpoint.acknowledgeCoordinatorState(coords[i], handles[i]);
        }

        return checkpoint;
    }

    private PendingCheckpoint createPendingCheckpoint(
            CheckpointProperties props,
            Collection<OperatorID> operatorCoordinators,
            Collection<String> masterStateIdentifiers,
            Executor executor)
            throws IOException {

        final List<Execution> ackTasks = new ArrayList<>(ACK_TASKS);
        final List<ExecutionVertex> tasksToCommit = new ArrayList<>(TASKS_TO_COMMIT);
        return createPendingCheckpoint(
                props,
                operatorCoordinators,
                masterStateIdentifiers,
                new DefaultCheckpointPlan(
                        Collections.emptyList(),
                        ackTasks,
                        tasksToCommit,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        true),
                executor);
    }

    private PendingCheckpoint createPendingCheckpoint(
            CheckpointProperties props,
            Collection<OperatorID> operatorCoordinators,
            Collection<String> masterStateIdentifiers,
            CheckpointPlan checkpointPlan,
            Executor executor)
            throws IOException {

        final Path checkpointDir = new Path(tmpFolder.newFolder().toURI());
        final FsCheckpointStorageLocation location =
                new FsCheckpointStorageLocation(
                        LocalFileSystem.getSharedInstance(),
                        checkpointDir,
                        checkpointDir,
                        checkpointDir,
                        CheckpointStorageLocationReference.getDefault(),
                        1024,
                        4096);

        return new PendingCheckpoint(
                new JobID(),
                0,
                1,
                checkpointPlan,
                operatorCoordinators,
                masterStateIdentifiers,
                props,
                location,
                new CompletableFuture<>());
    }

    @SuppressWarnings("unchecked")
    static void setTaskState(PendingCheckpoint pending, OperatorState state)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = PendingCheckpoint.class.getDeclaredField("operatorStates");
        field.setAccessible(true);
        Map<OperatorID, OperatorState> taskStates =
                (Map<OperatorID, OperatorState>) field.get(pending);

        taskStates.put(new OperatorID(), state);
    }

    private void abort(PendingCheckpoint checkpoint, CheckpointFailureReason reason) {
        abort(checkpoint, reason, null);
    }

    private void abort(
            PendingCheckpoint checkpoint,
            CheckpointFailureReason reason,
            PendingCheckpointStats statsCallback) {
        checkpoint.abort(
                reason,
                null,
                new CheckpointsCleaner(),
                () -> {},
                Executors.directExecutor(),
                statsCallback);
    }

    private static final class QueueExecutor implements Executor {

        private final Queue<Runnable> queue = new ArrayDeque<>(4);

        @Override
        public void execute(Runnable command) {
            queue.add(command);
        }

        public void runQueuedCommands() {
            for (Runnable runnable : queue) {
                runnable.run();
            }
        }
    }

    private static final class TestingMasterTriggerRestoreHook
            implements MasterTriggerRestoreHook<String> {

        private final String identifier;
        private final ArrayDeque<String> stateContents;

        public TestingMasterTriggerRestoreHook(String identifier) {
            this.identifier = checkNotNull(identifier);
            stateContents = new ArrayDeque<>();
        }

        public void addStateContent(String stateContent) {
            stateContents.add(stateContent);
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Nullable
        @Override
        public CompletableFuture<String> triggerCheckpoint(
                long checkpointId, long timestamp, Executor executor) throws Exception {
            return CompletableFuture.completedFuture(stateContents.poll());
        }

        @Override
        public void restoreCheckpoint(long checkpointId, @Nullable String checkpointData)
                throws Exception {}

        @Override
        public SimpleVersionedSerializer<String> createCheckpointDataSerializer() {
            return new StringSerializer();
        }
    }

    private static class RecordCheckpointPlan extends DefaultCheckpointPlan {

        private List<ExecutionVertex> reportedFinishedOnRestoreTasks = new ArrayList<>();

        private List<ExecutionVertex> reportedOperatorsFinishedTasks = new ArrayList<>();

        public RecordCheckpointPlan(List<Execution> tasksToWaitFor) {
            super(
                    Collections.emptyList(),
                    tasksToWaitFor,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    true);
        }

        @Override
        public void reportTaskFinishedOnRestore(ExecutionVertex task) {
            super.reportTaskFinishedOnRestore(task);
            reportedFinishedOnRestoreTasks.add(task);
        }

        @Override
        public void reportTaskHasFinishedOperators(ExecutionVertex task) {
            super.reportTaskHasFinishedOperators(task);
            reportedOperatorsFinishedTasks.add(task);
        }

        public List<ExecutionVertex> getReportedFinishedOnRestoreTasks() {
            return reportedFinishedOnRestoreTasks;
        }

        public List<ExecutionVertex> getReportedOperatorsFinishedTasks() {
            return reportedOperatorsFinishedTasks;
        }
    }
}
