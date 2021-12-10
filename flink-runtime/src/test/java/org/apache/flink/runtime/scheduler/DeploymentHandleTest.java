/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Unit tests for {@link DeploymentHandle}. */
public class DeploymentHandleTest {

    private static final JobVertexID TEST_JOB_VERTEX_ID = new JobVertexID(0, 0);

    private static final ExecutionVertexID TEST_EXECUTION_VERTEX_ID =
            new ExecutionVertexID(TEST_JOB_VERTEX_ID, 0);

    private static final ExecutionVertexVersion TEST_EXECUTION_VERTEX_VERSION =
            new ExecutionVertexVersion(TEST_EXECUTION_VERTEX_ID, 0);

    private static final ExecutionVertexDeploymentOption TEST_DEPLOYMENT_OPTION =
            new ExecutionVertexDeploymentOption(
                    TEST_EXECUTION_VERTEX_ID, new DeploymentOption(true));

    private CompletableFuture<LogicalSlot> logicalSlotFuture;

    private DeploymentHandle deploymentHandle;

    @Before
    public void setUp() {
        logicalSlotFuture = new CompletableFuture<>();
        final SlotExecutionVertexAssignment slotExecutionVertexAssignment =
                new SlotExecutionVertexAssignment(TEST_EXECUTION_VERTEX_ID, logicalSlotFuture);
        deploymentHandle =
                new DeploymentHandle(
                        TEST_EXECUTION_VERTEX_VERSION,
                        TEST_DEPLOYMENT_OPTION,
                        slotExecutionVertexAssignment);
    }

    @Test
    public void getLogicalSlotThrowsExceptionIfSlotFutureNotCompleted() {
        try {
            assertThat(deploymentHandle.getLogicalSlot().isPresent()).isFalse();
            fail("unknown failure");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage())
                    .contains("method can only be called after slot future is done");
        }
    }

    @Test
    public void slotIsNotPresentIfFutureWasCancelled() {
        logicalSlotFuture.cancel(false);
        assertThat(deploymentHandle.getLogicalSlot().isPresent()).isFalse();
    }

    @Test
    public void slotIsNotPresentIfFutureWasCompletedExceptionally() {
        logicalSlotFuture.completeExceptionally(new RuntimeException("expected"));
        assertThat(deploymentHandle.getLogicalSlot().isPresent()).isFalse();
    }

    @Test
    public void getLogicalSlotReturnsSlotIfFutureCompletedNormally() {
        final LogicalSlot logicalSlot = new TestingLogicalSlotBuilder().createTestingLogicalSlot();
        logicalSlotFuture.complete(logicalSlot);
        assertThat(deploymentHandle.getLogicalSlot().isPresent()).isTrue();
        assertThat(deploymentHandle.getLogicalSlot().get()).isSameAs(logicalSlot);
    }
}
