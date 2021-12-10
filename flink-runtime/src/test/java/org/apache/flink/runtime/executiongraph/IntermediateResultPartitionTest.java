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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IntermediateResultPartition}. */
public class IntermediateResultPartitionTest extends TestLogger {

    @Test
    public void testPipelinedPartitionConsumable() throws Exception {
        IntermediateResult result = createResult(ResultPartitionType.PIPELINED, 2);
        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        // Not consumable on init
        assertThat(partition1.isConsumable()).isFalse();
        assertThat(partition2.isConsumable()).isFalse();

        // Partition 1 consumable after data are produced
        partition1.markDataProduced();
        assertThat(partition1.isConsumable()).isTrue();
        assertThat(partition2.isConsumable()).isFalse();

        // Not consumable if failover happens
        result.resetForNewExecution();
        assertThat(partition1.isConsumable()).isFalse();
        assertThat(partition2.isConsumable()).isFalse();
    }

    @Test
    public void testBlockingPartitionConsumable() throws Exception {
        IntermediateResult result = createResult(ResultPartitionType.BLOCKING, 2);
        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        ConsumedPartitionGroup consumedPartitionGroup =
                partition1.getConsumedPartitionGroups().get(0);

        // Not consumable on init
        assertThat(partition1.isConsumable()).isFalse();
        assertThat(partition2.isConsumable()).isFalse();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isFalse();

        // Not consumable if only one partition is FINISHED
        partition1.markFinished();
        assertThat(partition1.isConsumable()).isTrue();
        assertThat(partition2.isConsumable()).isFalse();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isFalse();

        // Consumable after all partitions are FINISHED
        partition2.markFinished();
        assertThat(partition1.isConsumable()).isTrue();
        assertThat(partition2.isConsumable()).isTrue();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isTrue();

        // Not consumable if failover happens
        result.resetForNewExecution();
        assertThat(partition1.isConsumable()).isFalse();
        assertThat(partition2.isConsumable()).isFalse();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isFalse();
    }

    @Test
    public void testBlockingPartitionResetting() throws Exception {
        IntermediateResult result = createResult(ResultPartitionType.BLOCKING, 2);
        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        ConsumedPartitionGroup consumedPartitionGroup =
                partition1.getConsumedPartitionGroups().get(0);

        // Not consumable on init
        assertThat(partition1.isConsumable()).isFalse();
        assertThat(partition2.isConsumable()).isFalse();

        // Not consumable if partition1 is FINISHED
        partition1.markFinished();
        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isEqualTo(1);
        assertThat(partition1.isConsumable()).isTrue();
        assertThat(partition2.isConsumable()).isFalse();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isFalse();

        // Reset the result and mark partition2 FINISHED, the result should still not be consumable
        result.resetForNewExecution();
        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isEqualTo(2);
        partition2.markFinished();
        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isEqualTo(1);
        assertThat(partition1.isConsumable()).isFalse();
        assertThat(partition2.isConsumable()).isTrue();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isFalse();

        // Consumable after all partitions are FINISHED
        partition1.markFinished();
        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isEqualTo(0);
        assertThat(partition1.isConsumable()).isTrue();
        assertThat(partition2.isConsumable()).isTrue();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isTrue();

        // Not consumable again if failover happens
        result.resetForNewExecution();
        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isEqualTo(2);
        assertThat(partition1.isConsumable()).isFalse();
        assertThat(partition2.isConsumable()).isFalse();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isFalse();
    }

    private static IntermediateResult createResult(
            ResultPartitionType resultPartitionType, int parallelism) throws Exception {

        JobVertex source = new JobVertex("v1");
        source.setInvokableClass(NoOpInvokable.class);
        source.setParallelism(parallelism);

        JobVertex sink = new JobVertex("v2");
        sink.setInvokableClass(NoOpInvokable.class);
        sink.setParallelism(parallelism);

        sink.connectNewDataSetAsInput(source, DistributionPattern.ALL_TO_ALL, resultPartitionType);

        ScheduledExecutorService executorService = new DirectScheduledExecutorService();

        JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(source, sink);

        SchedulerBase scheduler =
                SchedulerTestingUtils.newSchedulerBuilder(
                                jobGraph, ComponentMainThreadExecutorServiceAdapter.forMainThread())
                        .setIoExecutor(executorService)
                        .setFutureExecutor(executorService)
                        .build();

        ExecutionJobVertex ejv = scheduler.getExecutionJobVertex(source.getID());

        return ejv.getProducedDataSets()[0];
    }
}
