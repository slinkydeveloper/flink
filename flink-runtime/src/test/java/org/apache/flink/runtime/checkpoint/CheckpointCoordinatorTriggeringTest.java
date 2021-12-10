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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for checkpoint coordinator triggering. */
public class CheckpointCoordinatorTriggeringTest extends TestLogger {
    private static final String TASK_MANAGER_LOCATION_INFO = "Unknown location";

    private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

    @Before
    public void setUp() throws Exception {
        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    @Test
    public void testPeriodicTriggering() {
        try {
            final long start = System.currentTimeMillis();

            CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                    new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

            JobVertexID jobVertexID = new JobVertexID();
            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(jobVertexID)
                            .setTaskManagerGateway(gateway)
                            .build();

            ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
            ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

            CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                    new CheckpointCoordinatorConfigurationBuilder()
                            .setCheckpointInterval(10) // periodic interval is 10 ms
                            .setCheckpointTimeout(200000) // timeout is very long (200 s)
                            .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                            .build();
            CheckpointCoordinator checkpointCoordinator =
                    new CheckpointCoordinatorBuilder()
                            .setExecutionGraph(graph)
                            .setCheckpointCoordinatorConfiguration(
                                    checkpointCoordinatorConfiguration)
                            .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build();

            checkpointCoordinator.startCheckpointScheduler();

            for (int i = 0; i < 5; ++i) {
                manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
                manuallyTriggeredScheduledExecutor.triggerAll();
            }
            checkRecordedTriggeredCheckpoints(5, start, gateway.getTriggeredCheckpoints(attemptID));

            checkpointCoordinator.stopCheckpointScheduler();

            // no further calls may come.
            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertThat(gateway.getTriggeredCheckpoints(attemptID).size()).isEqualTo(5);

            // start another sequence of periodic scheduling
            gateway.resetCount();
            checkpointCoordinator.startCheckpointScheduler();

            for (int i = 0; i < 5; ++i) {
                manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
                manuallyTriggeredScheduledExecutor.triggerAll();
            }
            checkRecordedTriggeredCheckpoints(5, start, gateway.getTriggeredCheckpoints(attemptID));

            checkpointCoordinator.stopCheckpointScheduler();

            // no further calls may come
            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertThat(gateway.getTriggeredCheckpoints(attemptID).size()).isEqualTo(5);

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private void checkRecordedTriggeredCheckpoints(
            int numTrigger,
            long start,
            List<CheckpointCoordinatorTestingUtils.TriggeredCheckpoint> checkpoints) {
        assertThat(checkpoints.size()).isEqualTo(numTrigger);

        long lastId = -1;
        long lastTs = -1;

        for (CheckpointCoordinatorTestingUtils.TriggeredCheckpoint checkpoint : checkpoints) {
            assertThat(checkpoint.checkpointId > lastId)
                    .as("Trigger checkpoint id should be in increase order")
                    .isTrue();
            assertThat(checkpoint.timestamp >= lastTs)
                    .as("Trigger checkpoint timestamp should be in increase order")
                    .isTrue();
            assertThat(checkpoint.timestamp >= start)
                    .as("Trigger checkpoint timestamp should be larger than the start time")
                    .isTrue();

            lastId = checkpoint.checkpointId;
            lastTs = checkpoint.timestamp;
        }
    }

    /**
     * This test verified that after a completed checkpoint a certain time has passed before another
     * is triggered.
     */
    @Test
    public void testMinTimeBetweenCheckpointsInterval() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build();

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        final long delay = 50;
        final long checkpointInterval = 12;

        CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(checkpointInterval) // periodic interval is 12 ms
                        .setCheckpointTimeout(200_000) // timeout is very long (200 s)
                        .setMinPauseBetweenCheckpoints(delay) // 50 ms delay between checkpoints
                        .setMaxConcurrentCheckpoints(1)
                        .build();
        final CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setCheckpointCoordinatorConfiguration(checkpointCoordinatorConfiguration)
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        try {
            checkpointCoordinator.startCheckpointScheduler();
            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();

            // wait until the first checkpoint was triggered
            Long firstCallId = gateway.getTriggeredCheckpoints(attemptID).get(0).checkpointId;
            assertThat(firstCallId.longValue()).isEqualTo(1L);

            AcknowledgeCheckpoint ackMsg =
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptID, 1L);

            // tell the coordinator that the checkpoint is done
            final long ackTime = System.nanoTime();
            checkpointCoordinator.receiveAcknowledgeMessage(ackMsg, TASK_MANAGER_LOCATION_INFO);

            gateway.resetCount();
            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();
            while (gateway.getTriggeredCheckpoints(attemptID).isEmpty()) {
                // sleeps for a while to simulate periodic scheduling
                Thread.sleep(checkpointInterval);
                manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
                manuallyTriggeredScheduledExecutor.triggerAll();
            }
            // wait until the next checkpoint is triggered
            Long nextCallId = gateway.getTriggeredCheckpoints(attemptID).get(0).checkpointId;
            final long nextCheckpointTime = System.nanoTime();
            assertThat(nextCallId.longValue()).isEqualTo(2L);

            final long delayMillis = (nextCheckpointTime - ackTime) / 1_000_000;

            // we need to add one ms here to account for rounding errors
            if (delayMillis + 1 < delay) {
                fail(
                        "checkpoint came too early: delay was "
                                + delayMillis
                                + " but should have been at least "
                                + delay);
            }
        } finally {
            checkpointCoordinator.stopCheckpointScheduler();
            checkpointCoordinator.shutdown();
        }
    }

    @Test
    public void testStopPeriodicScheduler() throws Exception {
        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator();

        final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        manuallyTriggeredScheduledExecutor.triggerAll();
        try {
            onCompletionPromise1.get();
            fail("The triggerCheckpoint call expected an exception");
        } catch (ExecutionException e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            assertThat(checkpointExceptionOptional.isPresent()).isTrue();
            assertThat(checkpointExceptionOptional.get().getCheckpointFailureReason())
                    .isEqualTo(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN);
        }

        // Not periodic
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
                checkpointCoordinator.triggerCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        null,
                        false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(onCompletionPromise2.isCompletedExceptionally()).isFalse();
    }

    @Test
    public void testTriggerCheckpointWithShuttingDownCoordinator() throws Exception {
        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator();
        checkpointCoordinator.startCheckpointScheduler();
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
                triggerPeriodicCheckpoint(checkpointCoordinator);

        checkpointCoordinator.shutdown();
        manuallyTriggeredScheduledExecutor.triggerAll();
        try {
            onCompletionPromise.get();
            fail("Should not reach here");
        } catch (ExecutionException e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            assertThat(checkpointExceptionOptional.isPresent()).isTrue();
            assertThat(checkpointExceptionOptional.get().getCheckpointFailureReason())
                    .isEqualTo(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
        }
    }

    @Test
    public void testTriggerCheckpointBeforePreviousOneCompleted() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build();

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator(graph);

        checkpointCoordinator.startCheckpointScheduler();
        // start a periodic checkpoint first
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        assertThat(checkpointCoordinator.isTriggering()).isTrue();
        assertThat(checkpointCoordinator.getTriggerRequestQueue().size()).isEqualTo(0);
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        // another trigger before the prior one finished

        assertThat(checkpointCoordinator.isTriggering()).isTrue();
        assertThat(checkpointCoordinator.getTriggerRequestQueue().size()).isEqualTo(1);

        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(onCompletionPromise1.isCompletedExceptionally()).isFalse();
        assertThat(onCompletionPromise2.isCompletedExceptionally()).isFalse();
        assertThat(checkpointCoordinator.isTriggering()).isFalse();
        assertThat(checkpointCoordinator.getTriggerRequestQueue().size()).isEqualTo(0);
        assertThat(gateway.getTriggeredCheckpoints(attemptID).size()).isEqualTo(2);
    }

    @Test
    public void testTriggerCheckpointRequestQueuedWithFailure() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build();

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setCheckpointIDCounter(new UnstableCheckpointIDCounter(id -> id == 0))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        checkpointCoordinator.startCheckpointScheduler();
        // start a periodic checkpoint first
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
                triggerNonPeriodicCheckpoint(checkpointCoordinator);
        assertThat(checkpointCoordinator.isTriggering()).isTrue();
        assertThat(checkpointCoordinator.getTriggerRequestQueue().size()).isEqualTo(0);

        // another trigger before the prior one finished
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
                triggerNonPeriodicCheckpoint(checkpointCoordinator);

        // another trigger before the first one finished
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise3 =
                triggerNonPeriodicCheckpoint(checkpointCoordinator);
        assertThat(checkpointCoordinator.isTriggering()).isTrue();
        assertThat(checkpointCoordinator.getTriggerRequestQueue().size()).isEqualTo(2);

        manuallyTriggeredScheduledExecutor.triggerAll();
        // the first triggered checkpoint fails by design through UnstableCheckpointIDCounter
        assertThat(onCompletionPromise1.isCompletedExceptionally()).isTrue();
        assertThat(onCompletionPromise2.isCompletedExceptionally()).isFalse();
        assertThat(onCompletionPromise3.isCompletedExceptionally()).isFalse();
        assertThat(checkpointCoordinator.isTriggering()).isFalse();
        assertThat(checkpointCoordinator.getTriggerRequestQueue().size()).isEqualTo(0);
        assertThat(gateway.getTriggeredCheckpoints(attemptID).size()).isEqualTo(2);
    }

    @Test
    public void testTriggerCheckpointRequestCancelled() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build();

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator(graph);

        final CompletableFuture<String> masterHookCheckpointFuture = new CompletableFuture<>();
        checkpointCoordinator.addMasterHook(new TestingMasterHook(masterHookCheckpointFuture));
        checkpointCoordinator.startCheckpointScheduler();
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
                triggerPeriodicCheckpoint(checkpointCoordinator);

        // checkpoint trigger will not finish since master hook checkpoint is not finished yet
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointCoordinator.isTriggering()).isTrue();

        // trigger cancellation
        manuallyTriggeredScheduledExecutor.triggerNonPeriodicScheduledTasks();
        assertThat(checkpointCoordinator.isTriggering()).isTrue();

        try {
            onCompletionPromise.get();
            fail("Should not reach here");
        } catch (ExecutionException e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            assertThat(checkpointExceptionOptional.isPresent()).isTrue();
            assertThat(checkpointExceptionOptional.get().getCheckpointFailureReason())
                    .isEqualTo(CheckpointFailureReason.CHECKPOINT_EXPIRED);
        }

        // continue triggering
        masterHookCheckpointFuture.complete("finish master hook");

        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointCoordinator.isTriggering()).isFalse();
        // it doesn't really trigger task manager to do checkpoint
        assertThat(gateway.getTriggeredCheckpoints(attemptID).size()).isEqualTo(0);
        assertThat(checkpointCoordinator.getTriggerRequestQueue().size()).isEqualTo(0);
    }

    @Test
    public void testTriggerCheckpointInitializationFailed() throws Exception {
        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointIDCounter(new UnstableCheckpointIDCounter(id -> id == 0))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        checkpointCoordinator.startCheckpointScheduler();
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        assertThat(checkpointCoordinator.isTriggering()).isTrue();
        assertThat(checkpointCoordinator.getTriggerRequestQueue().size()).isEqualTo(0);

        manuallyTriggeredScheduledExecutor.triggerAll();
        try {
            onCompletionPromise1.get();
            fail("This checkpoint should fail through UnstableCheckpointIDCounter");
        } catch (ExecutionException e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            assertThat(checkpointExceptionOptional.isPresent()).isTrue();
            assertThat(checkpointExceptionOptional.get().getCheckpointFailureReason())
                    .isEqualTo(CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE);
        }
        assertThat(checkpointCoordinator.isTriggering()).isFalse();
        assertThat(checkpointCoordinator.getTriggerRequestQueue().size()).isEqualTo(0);

        final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        assertThat(checkpointCoordinator.isTriggering()).isTrue();
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(onCompletionPromise2.isCompletedExceptionally()).isFalse();
        assertThat(checkpointCoordinator.isTriggering()).isFalse();
        assertThat(checkpointCoordinator.getTriggerRequestQueue().size()).isEqualTo(0);
    }

    @Test
    public void testTriggerCheckpointSnapshotMasterHookFailed() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build();

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator();

        final CompletableFuture<String> masterHookCheckpointFuture = new CompletableFuture<>();
        checkpointCoordinator.addMasterHook(new TestingMasterHook(masterHookCheckpointFuture));
        checkpointCoordinator.startCheckpointScheduler();
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
                triggerPeriodicCheckpoint(checkpointCoordinator);

        // checkpoint trigger will not finish since master hook checkpoint is not finished yet
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointCoordinator.isTriggering()).isTrue();

        // continue triggering
        masterHookCheckpointFuture.completeExceptionally(new Exception("by design"));

        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointCoordinator.isTriggering()).isFalse();

        try {
            onCompletionPromise.get();
            fail("Should not reach here");
        } catch (ExecutionException e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            assertThat(checkpointExceptionOptional.isPresent()).isTrue();
            assertThat(checkpointExceptionOptional.get().getCheckpointFailureReason())
                    .isEqualTo(CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE);
        }
        // it doesn't really trigger task manager to do checkpoint
        assertThat(gateway.getTriggeredCheckpoints(attemptID).size()).isEqualTo(0);
        assertThat(checkpointCoordinator.getTriggerRequestQueue().size()).isEqualTo(0);
    }

    /** This test only fails eventually. */
    @Test
    public void discardingTriggeringCheckpointWillExecuteNextCheckpointRequest() throws Exception {
        final ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor();
        final CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder()
                        .setTimer(new ScheduledExecutorServiceAdapter(scheduledExecutorService))
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder().build())
                        .build();

        final CompletableFuture<String> masterHookCheckpointFuture = new CompletableFuture<>();
        final OneShotLatch triggerCheckpointLatch = new OneShotLatch();
        checkpointCoordinator.addMasterHook(
                new TestingMasterHook(masterHookCheckpointFuture, triggerCheckpointLatch));

        try {
            checkpointCoordinator.triggerCheckpoint(false);
            final CompletableFuture<CompletedCheckpoint> secondCheckpoint =
                    checkpointCoordinator.triggerCheckpoint(false);

            triggerCheckpointLatch.await();
            masterHookCheckpointFuture.complete("Completed");

            // discard triggering checkpoint
            checkpointCoordinator.abortPendingCheckpoints(
                    new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED));

            try {
                // verify that the second checkpoint request will be executed and eventually times
                // out
                secondCheckpoint.get();
                fail("Expected the second checkpoint to fail.");
            } catch (ExecutionException ee) {
                assertThat(ExceptionUtils.stripExecutionException(ee))
                        .isInstanceOf(CheckpointException.class);
            }
        } finally {
            checkpointCoordinator.shutdown();
            ExecutorUtils.gracefulShutdown(10L, TimeUnit.SECONDS, scheduledExecutorService);
        }
    }

    private CheckpointCoordinator createCheckpointCoordinator() throws Exception {
        return new CheckpointCoordinatorBuilder()
                .setTimer(manuallyTriggeredScheduledExecutor)
                .build();
    }

    private CheckpointCoordinator createCheckpointCoordinator(ExecutionGraph graph)
            throws Exception {
        return new CheckpointCoordinatorBuilder()
                .setExecutionGraph(graph)
                .setTimer(manuallyTriggeredScheduledExecutor)
                .build();
    }

    private CompletableFuture<CompletedCheckpoint> triggerPeriodicCheckpoint(
            CheckpointCoordinator checkpointCoordinator) {

        return checkpointCoordinator.triggerCheckpoint(
                CheckpointProperties.forCheckpoint(
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                null,
                true);
    }

    private CompletableFuture<CompletedCheckpoint> triggerNonPeriodicCheckpoint(
            CheckpointCoordinator checkpointCoordinator) {

        return checkpointCoordinator.triggerCheckpoint(
                CheckpointProperties.forCheckpoint(
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                null,
                false);
    }

    private static class TestingMasterHook implements MasterTriggerRestoreHook<String> {

        private final SimpleVersionedSerializer<String> serializer =
                new CheckpointCoordinatorTestingUtils.StringSerializer();

        private final CompletableFuture<String> checkpointFuture;
        private final OneShotLatch triggerCheckpointLatch;

        private TestingMasterHook(CompletableFuture<String> checkpointFuture) {
            this(checkpointFuture, new OneShotLatch());
        }

        private TestingMasterHook(
                CompletableFuture<String> checkpointFuture, OneShotLatch triggerCheckpointLatch) {
            this.checkpointFuture = checkpointFuture;
            this.triggerCheckpointLatch = triggerCheckpointLatch;
        }

        @Override
        public String getIdentifier() {
            return "testing master hook";
        }

        @Nullable
        @Override
        public CompletableFuture<String> triggerCheckpoint(
                long checkpointId, long timestamp, Executor executor) {
            triggerCheckpointLatch.trigger();
            return checkpointFuture;
        }

        @Override
        public void restoreCheckpoint(long checkpointId, @Nullable String checkpointData) {}

        @Nullable
        @Override
        public SimpleVersionedSerializer<String> createCheckpointDataSerializer() {
            return serializer;
        }
    }

    private static class UnstableCheckpointIDCounter implements CheckpointIDCounter {

        private final Predicate<Long> checkpointFailurePredicate;

        private long id = 0;

        public UnstableCheckpointIDCounter(Predicate<Long> checkpointFailurePredicate) {
            this.checkpointFailurePredicate = checkNotNull(checkpointFailurePredicate);
        }

        @Override
        public void start() {}

        @Override
        public void shutdown(JobStatus jobStatus) throws Exception {}

        @Override
        public long getAndIncrement() {
            if (checkpointFailurePredicate.test(id++)) {
                throw new RuntimeException("CheckpointIDCounter#getAndIncrement fails by design");
            }
            return id;
        }

        @Override
        public long get() {
            return id;
        }

        @Override
        public void setCount(long newId) {}
    }
}
