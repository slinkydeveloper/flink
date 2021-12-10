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

package org.apache.flink.runtime.scheduler.stopwithsavepoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TestingCheckpointScheduling;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.TestingSchedulerNG;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.testutils.EmptyStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.HamcrestCondition.matching;

/**
 * {@code StopWithSavepointTerminationHandlerImplTest} tests {@link
 * StopWithSavepointTerminationHandlerImpl}.
 */
public class StopWithSavepointTerminationHandlerImplTest extends TestLogger {

    private static final JobID JOB_ID = new JobID();

    private final TestingCheckpointScheduling checkpointScheduling =
            new TestingCheckpointScheduling(false);

    private StopWithSavepointTerminationHandlerImpl createTestInstanceFailingOnGlobalFailOver() {
        return createTestInstance(
                throwableCausingGlobalFailOver -> fail("No global failover should be triggered."));
    }

    private StopWithSavepointTerminationHandlerImpl createTestInstance(
            Consumer<Throwable> handleGlobalFailureConsumer) {
        // checkpointing should be always stopped before initiating stop-with-savepoint
        checkpointScheduling.stopCheckpointScheduler();

        final SchedulerNG scheduler =
                TestingSchedulerNG.newBuilder()
                        .setHandleGlobalFailureConsumer(handleGlobalFailureConsumer)
                        .build();
        return new StopWithSavepointTerminationHandlerImpl(
                JOB_ID, scheduler, checkpointScheduling, log);
    }

    @Test
    public void testHappyPath() throws ExecutionException, InterruptedException {
        final StopWithSavepointTerminationHandlerImpl testInstance =
                createTestInstanceFailingOnGlobalFailOver();

        final EmptyStreamStateHandle streamStateHandle = new EmptyStreamStateHandle();
        final CompletedCheckpoint completedSavepoint = createCompletedSavepoint(streamStateHandle);
        testInstance.handleSavepointCreation(completedSavepoint, null);
        testInstance.handleExecutionsTermination(Collections.singleton(ExecutionState.FINISHED));

        assertThat(testInstance.getSavepointPath().get())
                .isEqualTo(completedSavepoint.getExternalPointer());

        assertThat(streamStateHandle.isDisposed())
                .as("The savepoint should not have been discarded.")
                .isFalse();
        assertThat(checkpointScheduling.isEnabled())
                .as("Checkpoint scheduling should be disabled.")
                .isFalse();
    }

    @Test
    public void testSavepointCreationFailureWithoutExecutionTermination() {
        // savepoint creation failure is handled as expected if no execution termination happens
        assertSavepointCreationFailure(testInstance -> {});
    }

    @Test
    public void testSavepointCreationFailureWithFailingExecutions() {
        // no global fail-over is expected to be triggered by the stop-with-savepoint despite the
        // execution failure
        assertSavepointCreationFailure(
                testInstance ->
                        testInstance.handleExecutionsTermination(
                                Collections.singletonList(ExecutionState.FAILED)));
    }

    @Test
    public void testSavepointCreationFailureWithFinishingExecutions() {
        // checkpoint scheduling should be still enabled despite the finished executions
        assertSavepointCreationFailure(
                testInstance ->
                        testInstance.handleExecutionsTermination(
                                Collections.singletonList(ExecutionState.FINISHED)));
    }

    public void assertSavepointCreationFailure(
            Consumer<StopWithSavepointTerminationHandler> handleExecutionsTermination) {
        final StopWithSavepointTerminationHandlerImpl testInstance =
                createTestInstanceFailingOnGlobalFailOver();

        final String expectedErrorMessage = "Expected exception during savepoint creation.";
        testInstance.handleSavepointCreation(null, new Exception(expectedErrorMessage));
        handleExecutionsTermination.accept(testInstance);

        try {
            testInstance.getSavepointPath().get();
            fail("An ExecutionException is expected.");
        } catch (Throwable e) {
            final Optional<Throwable> actualException =
                    ExceptionUtils.findThrowableWithMessage(e, expectedErrorMessage);
            assertThat(actualException.isPresent())
                    .as("An exception with the expected error message should have been thrown.")
                    .isTrue();
        }

        // the checkpoint scheduling should be enabled in case of failure
        assertThat(checkpointScheduling.isEnabled())
                .as("Checkpoint scheduling should be enabled.")
                .isTrue();
    }

    @Test
    public void testFailedTerminationHandling() throws ExecutionException, InterruptedException {
        final CompletableFuture<Throwable> globalFailOverTriggered = new CompletableFuture<>();
        final StopWithSavepointTerminationHandlerImpl testInstance =
                createTestInstance(globalFailOverTriggered::complete);

        final ExecutionState expectedNonFinishedState = ExecutionState.FAILED;
        final String expectedErrorMessage =
                String.format(
                        "Inconsistent execution state after stopping with savepoint. At least one execution is still in one of the following states: %s. A global fail-over is triggered to recover the job %s.",
                        expectedNonFinishedState, JOB_ID);

        final EmptyStreamStateHandle streamStateHandle = new EmptyStreamStateHandle();
        final CompletedCheckpoint completedSavepoint = createCompletedSavepoint(streamStateHandle);

        testInstance.handleSavepointCreation(completedSavepoint, null);
        testInstance.handleExecutionsTermination(
                Collections.singletonList(expectedNonFinishedState));

        try {
            testInstance.getSavepointPath().get();
            fail("An ExecutionException is expected.");
        } catch (Throwable e) {
            final Optional<FlinkException> actualFlinkException =
                    ExceptionUtils.findThrowable(e, FlinkException.class);
            assertThat(actualFlinkException.isPresent())
                    .as("A FlinkException should have been thrown.")
                    .isTrue();
            assertThat(actualFlinkException.get())
                    .satisfies(matching(FlinkMatchers.containsMessage(expectedErrorMessage)));
        }

        assertThat(globalFailOverTriggered.isDone())
                .as("Global fail-over was not triggered.")
                .isTrue();
        assertThat(globalFailOverTriggered.get())
                .satisfies(matching(FlinkMatchers.containsMessage(expectedErrorMessage)));

        assertThat(streamStateHandle.isDisposed())
                .as("Savepoint should not be discarded.")
                .isFalse();

        assertThat(checkpointScheduling.isEnabled())
                .as("Checkpoint scheduling should not be enabled in case of failure.")
                .isFalse();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvalidExecutionTerminationCall() {
        createTestInstanceFailingOnGlobalFailOver()
                .handleExecutionsTermination(Collections.singletonList(ExecutionState.FINISHED));
    }

    @Test(expected = NullPointerException.class)
    public void testSavepointCreationParameterBothNull() {
        createTestInstanceFailingOnGlobalFailOver().handleSavepointCreation(null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSavepointCreationParameterBothSet() {
        createTestInstanceFailingOnGlobalFailOver()
                .handleSavepointCreation(
                        createCompletedSavepoint(new EmptyStreamStateHandle()),
                        new Exception(
                                "No exception should be passed if a savepoint is available."));
    }

    @Test(expected = NullPointerException.class)
    public void testExecutionTerminationWithNull() {
        createTestInstanceFailingOnGlobalFailOver().handleExecutionsTermination(null);
    }

    private static CompletedCheckpoint createCompletedSavepoint(
            StreamStateHandle streamStateHandle) {
        return new CompletedCheckpoint(
                JOB_ID,
                0,
                0L,
                0L,
                new HashMap<>(),
                null,
                CheckpointProperties.forSavepoint(true),
                new TestCompletedCheckpointStorageLocation(streamStateHandle, "savepoint-path"));
    }
}
