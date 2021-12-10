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

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStatsSummarySnapshot;
import org.apache.flink.runtime.checkpoint.StatsSummarySnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ArchivedExecutionGraph}. */
public class ArchivedExecutionGraphTest extends TestLogger {

    private static ExecutionGraph runtimeGraph;

    @BeforeClass
    public static void setupExecutionGraph() throws Exception {
        // -------------------------------------------------------------------------------------------------------------
        // Setup
        // -------------------------------------------------------------------------------------------------------------

        JobVertexID v1ID = new JobVertexID();
        JobVertexID v2ID = new JobVertexID();

        JobVertex v1 = new JobVertex("v1", v1ID);
        JobVertex v2 = new JobVertex("v2", v2ID);

        v1.setParallelism(1);
        v2.setParallelism(2);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);

        ExecutionConfig config = new ExecutionConfig();

        config.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        config.setParallelism(4);
        config.enableObjectReuse();
        config.setGlobalJobParameters(new TestJobParameters());

        CheckpointCoordinatorConfiguration chkConfig =
                new CheckpointCoordinatorConfiguration(
                        100,
                        100,
                        100,
                        1,
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
                        true,
                        false,
                        0,
                        0);
        JobCheckpointingSettings checkpointingSettings =
                new JobCheckpointingSettings(chkConfig, null);

        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .addJobVertices(asList(v1, v2))
                        .setJobCheckpointingSettings(checkpointingSettings)
                        .setExecutionConfig(config)
                        .build();

        SchedulerBase scheduler =
                SchedulerTestingUtils.createScheduler(
                        jobGraph, ComponentMainThreadExecutorServiceAdapter.forMainThread());

        runtimeGraph = scheduler.getExecutionGraph();

        scheduler.startScheduling();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        runtimeGraph
                                .getAllExecutionVertices()
                                .iterator()
                                .next()
                                .getCurrentExecutionAttempt()
                                .getAttemptId(),
                        ExecutionState.FAILED,
                        new RuntimeException("Local failure")));
    }

    @Test
    public void testArchive() throws IOException, ClassNotFoundException {
        ArchivedExecutionGraph archivedGraph = ArchivedExecutionGraph.createFrom(runtimeGraph);

        compareExecutionGraph(runtimeGraph, archivedGraph);
    }

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        ArchivedExecutionGraph archivedGraph = ArchivedExecutionGraph.createFrom(runtimeGraph);

        verifySerializability(archivedGraph);
    }

    @Test
    public void testCreateFromInitializingJobForSuspendedJob() {
        final ArchivedExecutionGraph suspendedExecutionGraph =
                ArchivedExecutionGraph.createFromInitializingJob(
                        new JobID(),
                        "TestJob",
                        JobStatus.SUSPENDED,
                        new Exception("Test suspension exception"),
                        null,
                        System.currentTimeMillis());

        assertThat(suspendedExecutionGraph.getState()).isEqualTo(JobStatus.SUSPENDED);
        assertThat(suspendedExecutionGraph.getFailureInfo()).isNotNull();
    }

    @Test
    public void testCheckpointSettingsArchiving() {
        final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                CheckpointCoordinatorConfiguration.builder().build();

        final ArchivedExecutionGraph archivedGraph =
                ArchivedExecutionGraph.createFromInitializingJob(
                        new JobID(),
                        "TestJob",
                        JobStatus.INITIALIZING,
                        null,
                        new JobCheckpointingSettings(checkpointCoordinatorConfiguration, null),
                        System.currentTimeMillis());

        assertContainsCheckpointSettings(archivedGraph);
    }

    public static void assertContainsCheckpointSettings(ArchivedExecutionGraph archivedGraph) {
        assertThat(archivedGraph.getCheckpointCoordinatorConfiguration()).isNotNull();
        assertThat(archivedGraph.getCheckpointStatsSnapshot()).isNotNull();
        assertThat(archivedGraph.getCheckpointStorageName().get()).isEqualTo("Unknown");
        assertThat(archivedGraph.getStateBackendName().get()).isEqualTo("Unknown");
    }

    @Test
    public void testArchiveWithStatusOverride() throws IOException, ClassNotFoundException {
        ArchivedExecutionGraph archivedGraph =
                ArchivedExecutionGraph.createFrom(runtimeGraph, JobStatus.RESTARTING);

        assertThat(archivedGraph.getState()).isEqualTo(JobStatus.RESTARTING);
        assertThat(archivedGraph.getStatusTimestamp(JobStatus.FAILED)).isEqualTo(0L);
    }

    private static void compareExecutionGraph(
            AccessExecutionGraph runtimeGraph, AccessExecutionGraph archivedGraph)
            throws IOException, ClassNotFoundException {
        // -------------------------------------------------------------------------------------------------------------
        // ExecutionGraph
        // -------------------------------------------------------------------------------------------------------------
        assertThat(archivedGraph.getJsonPlan()).isEqualTo(runtimeGraph.getJsonPlan());
        assertThat(archivedGraph.getJobID()).isEqualTo(runtimeGraph.getJobID());
        assertThat(archivedGraph.getJobName()).isEqualTo(runtimeGraph.getJobName());
        assertThat(archivedGraph.getState()).isEqualTo(runtimeGraph.getState());
        assertThat(archivedGraph.getFailureInfo().getExceptionAsString())
                .isEqualTo(runtimeGraph.getFailureInfo().getExceptionAsString());
        assertThat(archivedGraph.getStatusTimestamp(JobStatus.CREATED))
                .isEqualTo(runtimeGraph.getStatusTimestamp(JobStatus.CREATED));
        assertThat(archivedGraph.getStatusTimestamp(JobStatus.RUNNING))
                .isEqualTo(runtimeGraph.getStatusTimestamp(JobStatus.RUNNING));
        assertThat(archivedGraph.getStatusTimestamp(JobStatus.FAILING))
                .isEqualTo(runtimeGraph.getStatusTimestamp(JobStatus.FAILING));
        assertThat(archivedGraph.getStatusTimestamp(JobStatus.FAILED))
                .isEqualTo(runtimeGraph.getStatusTimestamp(JobStatus.FAILED));
        assertThat(archivedGraph.getStatusTimestamp(JobStatus.CANCELLING))
                .isEqualTo(runtimeGraph.getStatusTimestamp(JobStatus.CANCELLING));
        assertThat(archivedGraph.getStatusTimestamp(JobStatus.CANCELED))
                .isEqualTo(runtimeGraph.getStatusTimestamp(JobStatus.CANCELED));
        assertThat(archivedGraph.getStatusTimestamp(JobStatus.FINISHED))
                .isEqualTo(runtimeGraph.getStatusTimestamp(JobStatus.FINISHED));
        assertThat(archivedGraph.getStatusTimestamp(JobStatus.RESTARTING))
                .isEqualTo(runtimeGraph.getStatusTimestamp(JobStatus.RESTARTING));
        assertThat(archivedGraph.getStatusTimestamp(JobStatus.SUSPENDED))
                .isEqualTo(runtimeGraph.getStatusTimestamp(JobStatus.SUSPENDED));
        assertThat(archivedGraph.isStoppable()).isEqualTo(runtimeGraph.isStoppable());

        // -------------------------------------------------------------------------------------------------------------
        // CheckpointStats
        // -------------------------------------------------------------------------------------------------------------
        CheckpointStatsSnapshot runtimeSnapshot = runtimeGraph.getCheckpointStatsSnapshot();
        CheckpointStatsSnapshot archivedSnapshot = archivedGraph.getCheckpointStatsSnapshot();

        List<Function<CompletedCheckpointStatsSummarySnapshot, StatsSummarySnapshot>> meters =
                asList(
                        CompletedCheckpointStatsSummarySnapshot::getEndToEndDurationStats,
                        CompletedCheckpointStatsSummarySnapshot::getPersistedDataStats,
                        CompletedCheckpointStatsSummarySnapshot::getProcessedDataStats,
                        CompletedCheckpointStatsSummarySnapshot::getStateSizeStats);

        List<Function<StatsSummarySnapshot, Object>> aggs =
                asList(
                        StatsSummarySnapshot::getAverage,
                        StatsSummarySnapshot::getMinimum,
                        StatsSummarySnapshot::getMaximum,
                        StatsSummarySnapshot::getSum,
                        StatsSummarySnapshot::getCount,
                        s -> s.getQuantile(0.5d),
                        s -> s.getQuantile(0.9d),
                        s -> s.getQuantile(0.95d),
                        s -> s.getQuantile(0.99d),
                        s -> s.getQuantile(0.999d));
        for (Function<CompletedCheckpointStatsSummarySnapshot, StatsSummarySnapshot> meter :
                meters) {
            StatsSummarySnapshot runtime = meter.apply(runtimeSnapshot.getSummaryStats());
            StatsSummarySnapshot archived = meter.apply(runtimeSnapshot.getSummaryStats());
            for (Function<StatsSummarySnapshot, Object> agg : aggs) {
                assertThat(agg.apply(archived)).isEqualTo(agg.apply(runtime));
            }
        }

        assertThat(archivedSnapshot.getCounts().getTotalNumberOfCheckpoints())
                .isEqualTo(runtimeSnapshot.getCounts().getTotalNumberOfCheckpoints());
        assertThat(archivedSnapshot.getCounts().getNumberOfCompletedCheckpoints())
                .isEqualTo(runtimeSnapshot.getCounts().getNumberOfCompletedCheckpoints());
        assertThat(archivedSnapshot.getCounts().getNumberOfInProgressCheckpoints())
                .isEqualTo(runtimeSnapshot.getCounts().getNumberOfInProgressCheckpoints());

        // -------------------------------------------------------------------------------------------------------------
        // ArchivedExecutionConfig
        // -------------------------------------------------------------------------------------------------------------
        ArchivedExecutionConfig runtimeConfig = runtimeGraph.getArchivedExecutionConfig();
        ArchivedExecutionConfig archivedConfig = archivedGraph.getArchivedExecutionConfig();

        assertThat(archivedConfig.getExecutionMode()).isEqualTo(runtimeConfig.getExecutionMode());
        assertThat(archivedConfig.getParallelism()).isEqualTo(runtimeConfig.getParallelism());
        assertThat(archivedConfig.getObjectReuseEnabled())
                .isEqualTo(runtimeConfig.getObjectReuseEnabled());
        assertThat(archivedConfig.getRestartStrategyDescription())
                .isEqualTo(runtimeConfig.getRestartStrategyDescription());
        assertThat(archivedConfig.getGlobalJobParameters().get("hello")).isNotNull();
        assertThat(archivedConfig.getGlobalJobParameters().get("hello"))
                .isEqualTo(runtimeConfig.getGlobalJobParameters().get("hello"));

        // -------------------------------------------------------------------------------------------------------------
        // StringifiedAccumulators
        // -------------------------------------------------------------------------------------------------------------
        compareStringifiedAccumulators(
                runtimeGraph.getAccumulatorResultsStringified(),
                archivedGraph.getAccumulatorResultsStringified());
        compareSerializedAccumulators(
                runtimeGraph.getAccumulatorsSerialized(),
                archivedGraph.getAccumulatorsSerialized());

        // -------------------------------------------------------------------------------------------------------------
        // JobVertices
        // -------------------------------------------------------------------------------------------------------------
        Map<JobVertexID, ? extends AccessExecutionJobVertex> runtimeVertices =
                runtimeGraph.getAllVertices();
        Map<JobVertexID, ? extends AccessExecutionJobVertex> archivedVertices =
                archivedGraph.getAllVertices();

        for (Map.Entry<JobVertexID, ? extends AccessExecutionJobVertex> vertex :
                runtimeVertices.entrySet()) {
            compareExecutionJobVertex(vertex.getValue(), archivedVertices.get(vertex.getKey()));
        }

        Iterator<? extends AccessExecutionJobVertex> runtimeTopologicalVertices =
                runtimeGraph.getVerticesTopologically().iterator();
        Iterator<? extends AccessExecutionJobVertex> archiveTopologicaldVertices =
                archivedGraph.getVerticesTopologically().iterator();

        while (runtimeTopologicalVertices.hasNext()) {
            assertThat(archiveTopologicaldVertices.hasNext()).isTrue();
            compareExecutionJobVertex(
                    runtimeTopologicalVertices.next(), archiveTopologicaldVertices.next());
        }

        // -------------------------------------------------------------------------------------------------------------
        // ExecutionVertices
        // -------------------------------------------------------------------------------------------------------------
        Iterator<? extends AccessExecutionVertex> runtimeExecutionVertices =
                runtimeGraph.getAllExecutionVertices().iterator();
        Iterator<? extends AccessExecutionVertex> archivedExecutionVertices =
                archivedGraph.getAllExecutionVertices().iterator();

        while (runtimeExecutionVertices.hasNext()) {
            assertThat(archivedExecutionVertices.hasNext()).isTrue();
            compareExecutionVertex(
                    runtimeExecutionVertices.next(), archivedExecutionVertices.next());
        }
    }

    private static void compareExecutionJobVertex(
            AccessExecutionJobVertex runtimeJobVertex, AccessExecutionJobVertex archivedJobVertex) {
        assertThat(archivedJobVertex.getName()).isEqualTo(runtimeJobVertex.getName());
        assertThat(archivedJobVertex.getParallelism()).isEqualTo(runtimeJobVertex.getParallelism());
        assertThat(archivedJobVertex.getMaxParallelism())
                .isEqualTo(runtimeJobVertex.getMaxParallelism());
        assertThat(archivedJobVertex.getJobVertexId()).isEqualTo(runtimeJobVertex.getJobVertexId());
        assertThat(archivedJobVertex.getAggregateState())
                .isEqualTo(runtimeJobVertex.getAggregateState());

        compareStringifiedAccumulators(
                runtimeJobVertex.getAggregatedUserAccumulatorsStringified(),
                archivedJobVertex.getAggregatedUserAccumulatorsStringified());

        AccessExecutionVertex[] runtimeExecutionVertices = runtimeJobVertex.getTaskVertices();
        AccessExecutionVertex[] archivedExecutionVertices = archivedJobVertex.getTaskVertices();
        assertThat(archivedExecutionVertices.length).isEqualTo(runtimeExecutionVertices.length);
        for (int x = 0; x < runtimeExecutionVertices.length; x++) {
            compareExecutionVertex(runtimeExecutionVertices[x], archivedExecutionVertices[x]);
        }
    }

    private static void compareExecutionVertex(
            AccessExecutionVertex runtimeVertex, AccessExecutionVertex archivedVertex) {
        assertThat(archivedVertex.getTaskNameWithSubtaskIndex())
                .isEqualTo(runtimeVertex.getTaskNameWithSubtaskIndex());
        assertThat(archivedVertex.getParallelSubtaskIndex())
                .isEqualTo(runtimeVertex.getParallelSubtaskIndex());
        assertThat(archivedVertex.getExecutionState()).isEqualTo(runtimeVertex.getExecutionState());
        assertThat(archivedVertex.getStateTimestamp(ExecutionState.CREATED))
                .isEqualTo(runtimeVertex.getStateTimestamp(ExecutionState.CREATED));
        assertThat(archivedVertex.getStateTimestamp(ExecutionState.SCHEDULED))
                .isEqualTo(runtimeVertex.getStateTimestamp(ExecutionState.SCHEDULED));
        assertThat(archivedVertex.getStateTimestamp(ExecutionState.DEPLOYING))
                .isEqualTo(runtimeVertex.getStateTimestamp(ExecutionState.DEPLOYING));
        assertThat(archivedVertex.getStateTimestamp(ExecutionState.INITIALIZING))
                .isEqualTo(runtimeVertex.getStateTimestamp(ExecutionState.INITIALIZING));
        assertThat(archivedVertex.getStateTimestamp(ExecutionState.RUNNING))
                .isEqualTo(runtimeVertex.getStateTimestamp(ExecutionState.RUNNING));
        assertThat(archivedVertex.getStateTimestamp(ExecutionState.FINISHED))
                .isEqualTo(runtimeVertex.getStateTimestamp(ExecutionState.FINISHED));
        assertThat(archivedVertex.getStateTimestamp(ExecutionState.CANCELING))
                .isEqualTo(runtimeVertex.getStateTimestamp(ExecutionState.CANCELING));
        assertThat(archivedVertex.getStateTimestamp(ExecutionState.CANCELED))
                .isEqualTo(runtimeVertex.getStateTimestamp(ExecutionState.CANCELED));
        assertThat(archivedVertex.getStateTimestamp(ExecutionState.FAILED))
                .isEqualTo(runtimeVertex.getStateTimestamp(ExecutionState.FAILED));
        assertThat(runtimeVertex.getFailureInfo().map(ErrorInfo::getExceptionAsString))
                .isEqualTo(archivedVertex.getFailureInfo().map(ErrorInfo::getExceptionAsString));
        assertThat(runtimeVertex.getFailureInfo().map(ErrorInfo::getTimestamp))
                .isEqualTo(archivedVertex.getFailureInfo().map(ErrorInfo::getTimestamp));
        assertThat(archivedVertex.getCurrentAssignedResourceLocation())
                .isEqualTo(runtimeVertex.getCurrentAssignedResourceLocation());

        compareExecution(
                runtimeVertex.getCurrentExecutionAttempt(),
                archivedVertex.getCurrentExecutionAttempt());
    }

    private static void compareExecution(
            AccessExecution runtimeExecution, AccessExecution archivedExecution) {
        assertThat(archivedExecution.getAttemptId()).isEqualTo(runtimeExecution.getAttemptId());
        assertThat(archivedExecution.getAttemptNumber())
                .isEqualTo(runtimeExecution.getAttemptNumber());
        assertThat(archivedExecution.getStateTimestamps())
                .isEqualTo(runtimeExecution.getStateTimestamps());
        assertThat(archivedExecution.getState()).isEqualTo(runtimeExecution.getState());
        assertThat(archivedExecution.getAssignedResourceLocation())
                .isEqualTo(runtimeExecution.getAssignedResourceLocation());
        assertThat(runtimeExecution.getFailureInfo().map(ErrorInfo::getExceptionAsString))
                .isEqualTo(archivedExecution.getFailureInfo().map(ErrorInfo::getExceptionAsString));
        assertThat(runtimeExecution.getFailureInfo().map(ErrorInfo::getTimestamp))
                .isEqualTo(archivedExecution.getFailureInfo().map(ErrorInfo::getTimestamp));
        assertThat(archivedExecution.getStateTimestamp(ExecutionState.CREATED))
                .isEqualTo(runtimeExecution.getStateTimestamp(ExecutionState.CREATED));
        assertThat(archivedExecution.getStateTimestamp(ExecutionState.SCHEDULED))
                .isEqualTo(runtimeExecution.getStateTimestamp(ExecutionState.SCHEDULED));
        assertThat(archivedExecution.getStateTimestamp(ExecutionState.DEPLOYING))
                .isEqualTo(runtimeExecution.getStateTimestamp(ExecutionState.DEPLOYING));
        assertThat(archivedExecution.getStateTimestamp(ExecutionState.INITIALIZING))
                .isEqualTo(runtimeExecution.getStateTimestamp(ExecutionState.INITIALIZING));
        assertThat(archivedExecution.getStateTimestamp(ExecutionState.RUNNING))
                .isEqualTo(runtimeExecution.getStateTimestamp(ExecutionState.RUNNING));
        assertThat(archivedExecution.getStateTimestamp(ExecutionState.FINISHED))
                .isEqualTo(runtimeExecution.getStateTimestamp(ExecutionState.FINISHED));
        assertThat(archivedExecution.getStateTimestamp(ExecutionState.CANCELING))
                .isEqualTo(runtimeExecution.getStateTimestamp(ExecutionState.CANCELING));
        assertThat(archivedExecution.getStateTimestamp(ExecutionState.CANCELED))
                .isEqualTo(runtimeExecution.getStateTimestamp(ExecutionState.CANCELED));
        assertThat(archivedExecution.getStateTimestamp(ExecutionState.FAILED))
                .isEqualTo(runtimeExecution.getStateTimestamp(ExecutionState.FAILED));
        compareStringifiedAccumulators(
                runtimeExecution.getUserAccumulatorsStringified(),
                archivedExecution.getUserAccumulatorsStringified());
        assertThat(archivedExecution.getParallelSubtaskIndex())
                .isEqualTo(runtimeExecution.getParallelSubtaskIndex());
    }

    private static void compareStringifiedAccumulators(
            StringifiedAccumulatorResult[] runtimeAccs,
            StringifiedAccumulatorResult[] archivedAccs) {
        assertThat(archivedAccs.length).isEqualTo(runtimeAccs.length);

        for (int x = 0; x < runtimeAccs.length; x++) {
            StringifiedAccumulatorResult runtimeResult = runtimeAccs[x];
            StringifiedAccumulatorResult archivedResult = archivedAccs[x];

            assertThat(archivedResult.getName()).isEqualTo(runtimeResult.getName());
            assertThat(archivedResult.getType()).isEqualTo(runtimeResult.getType());
            assertThat(archivedResult.getValue()).isEqualTo(runtimeResult.getValue());
        }
    }

    private static void compareSerializedAccumulators(
            Map<String, SerializedValue<OptionalFailure<Object>>> runtimeAccs,
            Map<String, SerializedValue<OptionalFailure<Object>>> archivedAccs)
            throws IOException, ClassNotFoundException {
        assertThat(archivedAccs.size()).isEqualTo(runtimeAccs.size());
        for (Entry<String, SerializedValue<OptionalFailure<Object>>> runtimeAcc :
                runtimeAccs.entrySet()) {
            long runtimeUserAcc =
                    (long)
                            runtimeAcc
                                    .getValue()
                                    .deserializeValue(ClassLoader.getSystemClassLoader())
                                    .getUnchecked();
            long archivedUserAcc =
                    (long)
                            archivedAccs
                                    .get(runtimeAcc.getKey())
                                    .deserializeValue(ClassLoader.getSystemClassLoader())
                                    .getUnchecked();

            assertThat(archivedUserAcc).isEqualTo(runtimeUserAcc);
        }
    }

    private static void verifySerializability(ArchivedExecutionGraph graph)
            throws IOException, ClassNotFoundException {
        ArchivedExecutionGraph copy = CommonTestUtils.createCopySerializable(graph);
        compareExecutionGraph(graph, copy);
    }

    private static class TestJobParameters extends ExecutionConfig.GlobalJobParameters {
        private static final long serialVersionUID = -8118611781035212808L;
        private Map<String, String> parameters;

        private TestJobParameters() {
            this.parameters = new HashMap<>();
            this.parameters.put("hello", "world");
        }

        @Override
        public Map<String, String> toMap() {
            return parameters;
        }
    }
}
