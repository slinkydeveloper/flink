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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.TriFunctionWithException;

import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/** Tests for the {@link TaskSlotTable}. */
public class TaskSlotTableImplTest extends TestLogger {
    private static final Time SLOT_TIMEOUT = Time.seconds(100L);

    /** Tests that one can can mark allocated slots as active. */
    @Test
    public void testTryMarkSlotActive() throws Exception {
        final TaskSlotTableImpl<?> taskSlotTable = createTaskSlotTableAndStart(3);

        try {
            final JobID jobId1 = new JobID();
            final AllocationID allocationId1 = new AllocationID();
            taskSlotTable.allocateSlot(0, jobId1, allocationId1, SLOT_TIMEOUT);
            final AllocationID allocationId2 = new AllocationID();
            taskSlotTable.allocateSlot(1, jobId1, allocationId2, SLOT_TIMEOUT);
            final AllocationID allocationId3 = new AllocationID();
            final JobID jobId2 = new JobID();
            taskSlotTable.allocateSlot(2, jobId2, allocationId3, SLOT_TIMEOUT);

            taskSlotTable.markSlotActive(allocationId1);

            assertThat(taskSlotTable.isAllocated(0, jobId1, allocationId1)).isEqualTo(true);
            assertThat(taskSlotTable.isAllocated(1, jobId1, allocationId2)).isEqualTo(true);
            assertThat(taskSlotTable.isAllocated(2, jobId2, allocationId3)).isEqualTo(true);

            assertThat(taskSlotTable.getActiveTaskSlotAllocationIdsPerJob(jobId1))
                    .isEqualTo(equalTo(Sets.newHashSet(allocationId1)));

            assertThat(taskSlotTable.tryMarkSlotActive(jobId1, allocationId1)).isEqualTo(true);
            assertThat(taskSlotTable.tryMarkSlotActive(jobId1, allocationId2)).isEqualTo(true);
            assertThat(taskSlotTable.tryMarkSlotActive(jobId1, allocationId3)).isEqualTo(false);

            assertThat(taskSlotTable.getActiveTaskSlotAllocationIdsPerJob(jobId1))
                    .isEqualTo(equalTo(new HashSet<>(Arrays.asList(allocationId2, allocationId1))));
        } finally {
            taskSlotTable.close();
            assertThat(taskSlotTable.isClosed()).isEqualTo(true);
        }
    }

    /** Tests {@link TaskSlotTableImpl#getActiveTaskSlotAllocationIds()}. */
    @Test
    public void testRetrievingAllActiveSlots() throws Exception {
        try (final TaskSlotTableImpl<?> taskSlotTable = createTaskSlotTableAndStart(3)) {
            final JobID jobId1 = new JobID();
            final AllocationID allocationId1 = new AllocationID();
            taskSlotTable.allocateSlot(0, jobId1, allocationId1, SLOT_TIMEOUT);
            final AllocationID allocationId2 = new AllocationID();
            taskSlotTable.allocateSlot(1, jobId1, allocationId2, SLOT_TIMEOUT);
            final AllocationID allocationId3 = new AllocationID();
            final JobID jobId2 = new JobID();
            taskSlotTable.allocateSlot(2, jobId2, allocationId3, SLOT_TIMEOUT);

            taskSlotTable.markSlotActive(allocationId1);
            taskSlotTable.markSlotActive(allocationId3);

            assertThat(taskSlotTable.getActiveTaskSlotAllocationIds())
                    .isEqualTo(Sets.newHashSet(allocationId1, allocationId3));
        }
    }

    /**
     * Tests that inconsistent static slot allocation with the same AllocationID to a different slot
     * is rejected.
     */
    @Test
    public void testInconsistentStaticSlotAllocation() throws Exception {
        try (final TaskSlotTable<TaskSlotPayload> taskSlotTable = createTaskSlotTableAndStart(2)) {
            final JobID jobId = new JobID();
            final AllocationID allocationId1 = new AllocationID();
            final AllocationID allocationId2 = new AllocationID();

            assertThat(taskSlotTable.allocateSlot(0, jobId, allocationId1, SLOT_TIMEOUT))
                    .isEqualTo(true);
            assertThat(taskSlotTable.allocateSlot(1, jobId, allocationId1, SLOT_TIMEOUT))
                    .isEqualTo(false);
            assertThat(taskSlotTable.allocateSlot(0, jobId, allocationId2, SLOT_TIMEOUT))
                    .isEqualTo(false);

            assertThat(taskSlotTable.isAllocated(0, jobId, allocationId1)).isEqualTo(true);
            assertThat(taskSlotTable.isSlotFree(1)).isEqualTo(true);

            Iterator<TaskSlot<TaskSlotPayload>> allocatedSlots =
                    taskSlotTable.getAllocatedSlots(jobId);
            assertThat(allocatedSlots.next().getIndex()).isEqualTo(0);
            assertThat(allocatedSlots.hasNext()).isEqualTo(false);
        }
    }

    /**
     * Tests that inconsistent dynamic slot allocation with the same AllocationID to a different
     * slot is rejected.
     */
    @Test
    public void testInconsistentDynamicSlotAllocation() throws Exception {
        try (final TaskSlotTable<TaskSlotPayload> taskSlotTable = createTaskSlotTableAndStart(1)) {
            final JobID jobId1 = new JobID();
            final JobID jobId2 = new JobID();
            final AllocationID allocationId = new AllocationID();

            assertThat(taskSlotTable.allocateSlot(-1, jobId1, allocationId, SLOT_TIMEOUT))
                    .isEqualTo(true);
            assertThat(taskSlotTable.allocateSlot(-1, jobId2, allocationId, SLOT_TIMEOUT))
                    .isEqualTo(false);

            assertThat(taskSlotTable.isAllocated(1, jobId1, allocationId)).isEqualTo(true);

            Iterator<TaskSlot<TaskSlotPayload>> allocatedSlots =
                    taskSlotTable.getAllocatedSlots(jobId1);
            assertThat(allocatedSlots.next().getAllocationId()).isEqualTo(allocationId);
            assertThat(allocatedSlots.hasNext()).isEqualTo(false);
        }
    }

    @Test
    public void testDuplicateStaticSlotAllocation() throws Exception {
        try (final TaskSlotTable<TaskSlotPayload> taskSlotTable = createTaskSlotTableAndStart(2)) {
            final JobID jobId = new JobID();
            final AllocationID allocationId = new AllocationID();

            assertThat(
                            taskSlotTable.allocateSlot(
                                    0, jobId, allocationId, ResourceProfile.UNKNOWN, SLOT_TIMEOUT))
                    .isEqualTo(true);
            assertThat(
                            taskSlotTable.allocateSlot(
                                    0, jobId, allocationId, ResourceProfile.UNKNOWN, SLOT_TIMEOUT))
                    .isEqualTo(true);

            assertThat(taskSlotTable.isAllocated(0, jobId, allocationId)).isEqualTo(true);
            assertThat(taskSlotTable.isSlotFree(1)).isEqualTo(true);

            Iterator<TaskSlot<TaskSlotPayload>> allocatedSlots =
                    taskSlotTable.getAllocatedSlots(jobId);
            assertThat(allocatedSlots.next().getIndex()).isEqualTo(0);
            assertThat(allocatedSlots.hasNext()).isEqualTo(false);
        }
    }

    @Test
    public void testDuplicateDynamicSlotAllocation() throws Exception {
        try (final TaskSlotTable<TaskSlotPayload> taskSlotTable = createTaskSlotTableAndStart(1)) {
            final JobID jobId = new JobID();
            final AllocationID allocationId = new AllocationID();

            assertThat(taskSlotTable.allocateSlot(-1, jobId, allocationId, SLOT_TIMEOUT))
                    .isEqualTo(true);
            Iterator<TaskSlot<TaskSlotPayload>> allocatedSlots =
                    taskSlotTable.getAllocatedSlots(jobId);
            TaskSlot<TaskSlotPayload> taskSlot1 = allocatedSlots.next();

            assertThat(taskSlotTable.allocateSlot(-1, jobId, allocationId, SLOT_TIMEOUT))
                    .isEqualTo(true);
            allocatedSlots = taskSlotTable.getAllocatedSlots(jobId);
            TaskSlot<TaskSlotPayload> taskSlot2 = allocatedSlots.next();

            assertThat(taskSlotTable.isAllocated(1, jobId, allocationId)).isEqualTo(true);
            assertThat(taskSlot2).isEqualTo(taskSlot1);
            assertThat(allocatedSlots.hasNext()).isEqualTo(false);
        }
    }

    @Test
    public void testFreeSlot() throws Exception {
        try (final TaskSlotTable<TaskSlotPayload> taskSlotTable = createTaskSlotTableAndStart(2)) {
            final JobID jobId = new JobID();
            final AllocationID allocationId1 = new AllocationID();
            final AllocationID allocationId2 = new AllocationID();

            assertThat(taskSlotTable.allocateSlot(0, jobId, allocationId1, SLOT_TIMEOUT))
                    .isEqualTo(true);
            assertThat(taskSlotTable.allocateSlot(1, jobId, allocationId2, SLOT_TIMEOUT))
                    .isEqualTo(true);

            assertThat(taskSlotTable.freeSlot(allocationId2)).isEqualTo(1);

            Iterator<TaskSlot<TaskSlotPayload>> allocatedSlots =
                    taskSlotTable.getAllocatedSlots(jobId);
            assertThat(allocatedSlots.next().getIndex()).isEqualTo(0);
            assertThat(allocatedSlots.hasNext()).isEqualTo(false);
            assertThat(taskSlotTable.isAllocated(1, jobId, allocationId1)).isEqualTo(false);
            assertThat(taskSlotTable.isAllocated(1, jobId, allocationId2)).isEqualTo(false);
            assertThat(taskSlotTable.isSlotFree(1)).isEqualTo(true);
        }
    }

    @Test
    public void testSlotAllocationWithDynamicSlotId() throws Exception {
        try (final TaskSlotTable<TaskSlotPayload> taskSlotTable = createTaskSlotTableAndStart(2)) {
            final JobID jobId = new JobID();
            final AllocationID allocationId = new AllocationID();

            assertThat(taskSlotTable.allocateSlot(-1, jobId, allocationId, SLOT_TIMEOUT))
                    .isEqualTo(true);

            Iterator<TaskSlot<TaskSlotPayload>> allocatedSlots =
                    taskSlotTable.getAllocatedSlots(jobId);
            assertThat(allocatedSlots.next().getIndex()).isEqualTo(2);
            assertThat(allocatedSlots.hasNext()).isEqualTo(false);
            assertThat(taskSlotTable.isAllocated(2, jobId, allocationId)).isEqualTo(true);
        }
    }

    @Test
    public void testSlotAllocationWithConcreteResourceProfile() throws Exception {
        try (final TaskSlotTable<TaskSlotPayload> taskSlotTable = createTaskSlotTableAndStart(2)) {
            final JobID jobId = new JobID();
            final AllocationID allocationId = new AllocationID();
            final ResourceProfile resourceProfile =
                    TaskSlotUtils.DEFAULT_RESOURCE_PROFILE.merge(
                            ResourceProfile.newBuilder().setCpuCores(0.1).build());

            assertThat(
                            taskSlotTable.allocateSlot(
                                    -1, jobId, allocationId, resourceProfile, SLOT_TIMEOUT))
                    .isEqualTo(true);

            Iterator<TaskSlot<TaskSlotPayload>> allocatedSlots =
                    taskSlotTable.getAllocatedSlots(jobId);
            TaskSlot<TaskSlotPayload> allocatedSlot = allocatedSlots.next();
            assertThat(allocatedSlot.getIndex()).isEqualTo(2);
            assertThat(allocatedSlot.getResourceProfile()).isEqualTo(resourceProfile);
            assertThat(allocatedSlots.hasNext()).isEqualTo(false);
        }
    }

    @Test
    public void testSlotAllocationWithUnknownResourceProfile() throws Exception {
        try (final TaskSlotTableImpl<TaskSlotPayload> taskSlotTable =
                createTaskSlotTableAndStart(2)) {
            final JobID jobId = new JobID();
            final AllocationID allocationId = new AllocationID();

            assertThat(
                            taskSlotTable.allocateSlot(
                                    -1, jobId, allocationId, ResourceProfile.UNKNOWN, SLOT_TIMEOUT))
                    .isEqualTo(true);

            Iterator<TaskSlot<TaskSlotPayload>> allocatedSlots =
                    taskSlotTable.getAllocatedSlots(jobId);
            TaskSlot<TaskSlotPayload> allocatedSlot = allocatedSlots.next();
            assertThat(allocatedSlot.getIndex()).isEqualTo(2);
            assertThat(allocatedSlot.getResourceProfile())
                    .isEqualTo(TaskSlotUtils.DEFAULT_RESOURCE_PROFILE);
            assertThat(allocatedSlots.hasNext()).isEqualTo(false);
        }
    }

    @Test
    public void testSlotAllocationWithResourceProfileFailure() throws Exception {
        try (final TaskSlotTable<TaskSlotPayload> taskSlotTable = createTaskSlotTableAndStart(2)) {
            final JobID jobId = new JobID();
            final AllocationID allocationId = new AllocationID();
            ResourceProfile resourceProfile = TaskSlotUtils.DEFAULT_RESOURCE_PROFILE;
            resourceProfile = resourceProfile.merge(resourceProfile).merge(resourceProfile);

            assertThat(
                            taskSlotTable.allocateSlot(
                                    -1, jobId, allocationId, resourceProfile, SLOT_TIMEOUT))
                    .isEqualTo(false);

            Iterator<TaskSlot<TaskSlotPayload>> allocatedSlots =
                    taskSlotTable.getAllocatedSlots(jobId);
            assertThat(allocatedSlots.hasNext()).isEqualTo(false);
        }
    }

    @Test
    public void testGenerateSlotReport() throws Exception {
        try (final TaskSlotTable<TaskSlotPayload> taskSlotTable = createTaskSlotTableAndStart(3)) {
            final JobID jobId = new JobID();
            final AllocationID allocationId1 = new AllocationID();
            final AllocationID allocationId2 = new AllocationID();
            final AllocationID allocationId3 = new AllocationID();

            assertThat(taskSlotTable.allocateSlot(0, jobId, allocationId1, SLOT_TIMEOUT))
                    .isEqualTo(true); // index 0
            assertThat(taskSlotTable.allocateSlot(-1, jobId, allocationId2, SLOT_TIMEOUT))
                    .isEqualTo(true); // index 3
            assertThat(taskSlotTable.allocateSlot(-1, jobId, allocationId3, SLOT_TIMEOUT))
                    .isEqualTo(true); // index 4

            assertThat(taskSlotTable.freeSlot(allocationId2)).isEqualTo(3);

            ResourceID resourceId = ResourceID.generate();
            SlotReport slotReport = taskSlotTable.createSlotReport(resourceId);
            List<SlotStatus> slotStatuses = new ArrayList<>();
            slotReport.iterator().forEachRemaining(slotStatuses::add);

            assertThat(slotStatuses.size()).isEqualTo(4);
            assertThat(slotStatuses)
                    .satisfies(
                            matching(
                                    containsInAnyOrder(
                                            is(
                                                    new SlotStatus(
                                                            new SlotID(resourceId, 0),
                                                            TaskSlotUtils.DEFAULT_RESOURCE_PROFILE,
                                                            jobId,
                                                            allocationId1)),
                                            is(
                                                    new SlotStatus(
                                                            new SlotID(resourceId, 1),
                                                            TaskSlotUtils.DEFAULT_RESOURCE_PROFILE,
                                                            null,
                                                            null)),
                                            is(
                                                    new SlotStatus(
                                                            new SlotID(resourceId, 2),
                                                            TaskSlotUtils.DEFAULT_RESOURCE_PROFILE,
                                                            null,
                                                            null)),
                                            is(
                                                    new SlotStatus(
                                                            new SlotID(resourceId, 4),
                                                            TaskSlotUtils.DEFAULT_RESOURCE_PROFILE,
                                                            jobId,
                                                            allocationId3)))));
        }
    }

    @Test
    public void testAllocateSlot() throws Exception {
        final JobID jobId = new JobID();
        final AllocationID allocationId = new AllocationID();
        try (final TaskSlotTable<TaskSlotPayload> taskSlotTable =
                createTaskSlotTableWithAllocatedSlot(
                        jobId, allocationId, new TestingSlotActionsBuilder().build())) {
            Iterator<TaskSlot<TaskSlotPayload>> allocatedSlots =
                    taskSlotTable.getAllocatedSlots(jobId);
            TaskSlot<TaskSlotPayload> nextSlot = allocatedSlots.next();
            assertThat(nextSlot.getIndex()).isEqualTo(0);
            assertThat(nextSlot.getAllocationId()).isEqualTo(allocationId);
            assertThat(nextSlot.getJobId()).isEqualTo(jobId);
            assertThat(allocatedSlots.hasNext()).isEqualTo(false);
        }
    }

    @Test
    public void testAddTask() throws Exception {
        final JobID jobId = new JobID();
        final ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();
        final AllocationID allocationId = new AllocationID();
        TaskSlotPayload task =
                new TestingTaskSlotPayload(jobId, executionAttemptId, allocationId).terminate();
        try (final TaskSlotTable<TaskSlotPayload> taskSlotTable =
                createTaskSlotTableWithStartedTask(task)) {
            Iterator<TaskSlotPayload> tasks = taskSlotTable.getTasks(jobId);
            TaskSlotPayload nextTask = tasks.next();
            assertThat(nextTask.getExecutionId()).isEqualTo(executionAttemptId);
            assertThat(nextTask.getAllocationId()).isEqualTo(allocationId);
            assertThat(tasks.hasNext()).isEqualTo(false);
        }
    }

    @Test(timeout = 10000)
    public void testRemoveTaskCallsFreeSlotAction() throws Exception {
        final JobID jobId = new JobID();
        final ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();
        final AllocationID allocationId = new AllocationID();
        CompletableFuture<AllocationID> freeSlotFuture = new CompletableFuture<>();
        SlotActions slotActions =
                new TestingSlotActions(freeSlotFuture::complete, (aid, uid) -> {});
        TaskSlotPayload task =
                new TestingTaskSlotPayload(jobId, executionAttemptId, allocationId).terminate();
        try (final TaskSlotTable<TaskSlotPayload> taskSlotTable =
                createTaskSlotTableWithStartedTask(task, slotActions)) {
            // we have to initiate closing of the slot externally
            // to enable that the last remaining finished task does the final slot freeing
            taskSlotTable.freeSlot(allocationId);
            taskSlotTable.removeTask(executionAttemptId);
            assertThat(freeSlotFuture.get()).isEqualTo(allocationId);
        }
    }

    @Test(timeout = 10000)
    public void testFreeSlotInterruptsSubmittedTask() throws Exception {
        TestingTaskSlotPayload task = new TestingTaskSlotPayload();
        try (final TaskSlotTable<TaskSlotPayload> taskSlotTable =
                createTaskSlotTableWithStartedTask(task)) {
            assertThat(taskSlotTable.freeSlot(task.getAllocationId())).isEqualTo(-1);
            task.waitForFailure();
            task.terminate();
        }
    }

    @Test(timeout = 10000)
    public void testTableIsClosedOnlyWhenAllTasksTerminated() throws Exception {
        TestingTaskSlotPayload task = new TestingTaskSlotPayload();
        final TaskSlotTable<TaskSlotPayload> taskSlotTable =
                createTaskSlotTableWithStartedTask(task);
        assertThat(taskSlotTable.freeSlot(task.getAllocationId())).isEqualTo(-1);
        CompletableFuture<Void> closingFuture = taskSlotTable.closeAsync();
        assertThat(closingFuture.isDone()).isEqualTo(false);
        task.terminate();
        closingFuture.get();
    }

    @Test
    public void testAllocatedSlotTimeout() throws Exception {
        final CompletableFuture<AllocationID> timeoutFuture = new CompletableFuture<>();
        final TestingSlotActions testingSlotActions =
                new TestingSlotActionsBuilder()
                        .setTimeoutSlotConsumer(
                                (allocationID, uuid) -> timeoutFuture.complete(allocationID))
                        .build();

        try (final TaskSlotTableImpl<TaskSlotPayload> taskSlotTable =
                createTaskSlotTableAndStart(1, testingSlotActions)) {
            final AllocationID allocationId = new AllocationID();
            assertThat(
                            taskSlotTable.allocateSlot(
                                    0, new JobID(), allocationId, Time.milliseconds(1L)))
                    .isEqualTo(true);
            assertThat(timeoutFuture.join()).isEqualTo(allocationId);
        }
    }

    @Test
    public void testMarkSlotActiveDeactivatesSlotTimeout() throws Exception {
        runDeactivateSlotTimeoutTest(
                (taskSlotTable, jobId, allocationId) -> taskSlotTable.markSlotActive(allocationId));
    }

    @Test
    public void testTryMarkSlotActiveDeactivatesSlotTimeout() throws Exception {
        runDeactivateSlotTimeoutTest(TaskSlotTable::tryMarkSlotActive);
    }

    private void runDeactivateSlotTimeoutTest(
            TriFunctionWithException<
                            TaskSlotTable<TaskSlotPayload>,
                            JobID,
                            AllocationID,
                            Boolean,
                            SlotNotFoundException>
                    taskSlotTableAction)
            throws Exception {
        final CompletableFuture<AllocationID> timeoutCancellationFuture = new CompletableFuture<>();

        final TimerService<AllocationID> testingTimerService =
                new TestingTimerServiceBuilder<AllocationID>()
                        .setUnregisterTimeoutConsumer(timeoutCancellationFuture::complete)
                        .createTestingTimerService();

        try (final TaskSlotTableImpl<TaskSlotPayload> taskSlotTable =
                createTaskSlotTableAndStart(1, testingTimerService)) {
            final AllocationID allocationId = new AllocationID();
            final long timeout = 50L;
            final JobID jobId = new JobID();
            assertThat(
                            taskSlotTable.allocateSlot(
                                    0, jobId, allocationId, Time.milliseconds(timeout)))
                    .isEqualTo(true);
            assertThat(taskSlotTableAction.apply(taskSlotTable, jobId, allocationId))
                    .isEqualTo(true);

            timeoutCancellationFuture.get();
        }
    }

    private static TaskSlotTable<TaskSlotPayload> createTaskSlotTableWithStartedTask(
            final TaskSlotPayload task) throws SlotNotFoundException, SlotNotActiveException {
        return createTaskSlotTableWithStartedTask(task, new TestingSlotActionsBuilder().build());
    }

    private static TaskSlotTable<TaskSlotPayload> createTaskSlotTableWithStartedTask(
            final TaskSlotPayload task, final SlotActions slotActions)
            throws SlotNotFoundException, SlotNotActiveException {
        final TaskSlotTable<TaskSlotPayload> taskSlotTable =
                createTaskSlotTableWithAllocatedSlot(
                        task.getJobID(), task.getAllocationId(), slotActions);
        taskSlotTable.markSlotActive(task.getAllocationId());
        taskSlotTable.addTask(task);
        return taskSlotTable;
    }

    private static TaskSlotTable<TaskSlotPayload> createTaskSlotTableWithAllocatedSlot(
            final JobID jobId, final AllocationID allocationId, final SlotActions slotActions) {
        final TaskSlotTable<TaskSlotPayload> taskSlotTable =
                createTaskSlotTableAndStart(1, slotActions);
        assertThat(taskSlotTable.allocateSlot(0, jobId, allocationId, SLOT_TIMEOUT))
                .isEqualTo(true);
        return taskSlotTable;
    }

    private static TaskSlotTableImpl<TaskSlotPayload> createTaskSlotTableAndStart(
            final int numberOfSlots) {
        return createTaskSlotTableAndStart(numberOfSlots, new TestingSlotActionsBuilder().build());
    }

    private static TaskSlotTableImpl<TaskSlotPayload> createTaskSlotTableAndStart(
            final int numberOfSlots, final SlotActions slotActions) {
        final TaskSlotTableImpl<TaskSlotPayload> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(numberOfSlots);
        taskSlotTable.start(slotActions, ComponentMainThreadExecutorServiceAdapter.forMainThread());
        return taskSlotTable;
    }

    private static TaskSlotTableImpl<TaskSlotPayload> createTaskSlotTableAndStart(
            final int numberOfSlots, TimerService<AllocationID> timerService) {
        final TaskSlotTableImpl<TaskSlotPayload> taskSlotTable =
                TaskSlotUtils.createTaskSlotTable(numberOfSlots, timerService);
        taskSlotTable.start(
                new TestingSlotActionsBuilder().build(),
                ComponentMainThreadExecutorServiceAdapter.forMainThread());
        return taskSlotTable;
    }
}
