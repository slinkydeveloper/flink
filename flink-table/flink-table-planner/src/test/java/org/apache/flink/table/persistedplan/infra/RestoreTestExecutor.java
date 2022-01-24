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

package org.apache.flink.table.persistedplan.infra;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.AfterExecutionPhase;
import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.AfterSavepointCreationPhase;
import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.CreateTables;
import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.ExecutionPhaseConfiguration;
import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.ModifyPlanBeforeExecution;
import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.RestoreTables;
import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.SavepointPhaseConfiguration;
import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.StatementSetPipelineDefinition;
import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.StopExecutionCondition;
import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.TablePipelineDefinition;
import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.TriggerSavepointCondition;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.DynamicTest;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.util.stream.Stream;

/** This POJO encapsulates parameters and logic of an upgrade test. */
public class RestoreTestExecutor {

    private final String name;
    private final String testExecutionId;
    private final Path testDir;
    private final Integer version;
    private final SavepointPhaseConfiguration savepointPhaseConfiguration;
    private final CreateTables createTables;
    private final TablePipelineDefinition tablePipelineDefinition;
    private final StatementSetPipelineDefinition statementSetPipelineDefinition;
    private final TriggerSavepointCondition triggerSavepointCondition;
    private final AfterSavepointCreationPhase afterSavepointCreationPhase;
    private final ExecutionPhaseConfiguration executionPhaseConfiguration;
    private final RestoreTables restoreTables;
    private final ModifyPlanBeforeExecution modifyPlanBeforeExecution;
    private final StopExecutionCondition stopExecutionCondition;
    private final AfterExecutionPhase afterExecutionPhase;

    private RestoreTestExecutor(
            String name,
            String testExecutionId,
            Path testDir,
            Integer version,
            PersistedPlanTestCase testCase) {
        Preconditions.checkState(
                Boolean.logicalXor(
                        testCase instanceof TablePipelineDefinition,
                        testCase instanceof StatementSetPipelineDefinition),
                "You must implement either a TablePipelineDefinition or a StatementSetPipelineDefinition, but not both");

        this.name = name;
        this.testExecutionId = testExecutionId;
        this.testDir = testDir;
        this.version = version;

        this.savepointPhaseConfiguration = castOrNull(testCase, SavepointPhaseConfiguration.class);
        this.createTables = castOrNull(testCase, CreateTables.class);
        this.tablePipelineDefinition = castOrNull(testCase, TablePipelineDefinition.class);
        this.statementSetPipelineDefinition =
                castOrNull(testCase, StatementSetPipelineDefinition.class);
        this.triggerSavepointCondition =
                Preconditions.checkNotNull(
                        castOrNull(testCase, TriggerSavepointCondition.class),
                        "Trigger savepoint condition cannot be null");
        this.afterSavepointCreationPhase = castOrNull(testCase, AfterSavepointCreationPhase.class);
        this.executionPhaseConfiguration = castOrNull(testCase, ExecutionPhaseConfiguration.class);
        this.restoreTables = castOrNull(testCase, RestoreTables.class);
        this.modifyPlanBeforeExecution = castOrNull(testCase, ModifyPlanBeforeExecution.class);
        this.stopExecutionCondition =
                Preconditions.checkNotNull(
                        castOrNull(testCase, StopExecutionCondition.class),
                        "Stop condition cannot be null");
        this.afterExecutionPhase = castOrNull(testCase, AfterExecutionPhase.class);
    }

    private void execute(ClusterClient<?> jobClient) throws Throwable {

        // group_aggregate_simple_agg_calls_with_group_by
        // - sink.latest.json
        // - group_aggregate_simple_agg_calls_with_group_by.latest.json

        // Sink
        // - sink.v1.json
        // - sink.latest.json

        // - group_aggregate_simple_agg_calls_with_group_by.v1.json
        // - group_aggregate_simple_agg_calls_with_group_by.latest.json

    }

    private @Nullable <T> T castOrNull(PersistedPlanTestCase testCase, Class<T> clazz) {
        if (clazz.isAssignableFrom(testCase.getClass())) {
            return clazz.cast(testCase);
        }
        return null;
    }

    /** Generate a JUnit 5 {@link DynamicTest} for all test versions of the provided test case. */
    public static Builder forAllVersions(PersistedPlanTestCase persistedPlanTestCase) {
        return new Builder(persistedPlanTestCase, true);
    }

    /**
     * Generate a JUnit 5 {@link DynamicTest} only for the {@code latest} version of the provided
     * test case.
     */
    public static Builder forLatestVersion(PersistedPlanTestCase persistedPlanTestCase) {
        return new Builder(persistedPlanTestCase, false);
    }

    public static class Builder {

        private final PersistedPlanTestCase persistedPlanTestCase;
        private final boolean loadAllVersions;
        private ClusterClient<?> jobClient;

        public Builder(PersistedPlanTestCase persistedPlanTestCase, boolean loadAllVersions) {
            this.persistedPlanTestCase = persistedPlanTestCase;
            this.loadAllVersions = loadAllVersions;
        }

        public Builder withClusterClient(ClusterClient<?> jobClient) {
            this.jobClient = jobClient;
            return this;
        }

        public Stream<DynamicTest> build() {
            // TODO load all versions here
            Preconditions.checkNotNull(jobClient);
            RestoreTestExecutor executor =
                    new RestoreTestExecutor(persistedPlanTestCase.getName(), persistedPlanTestCase);
            return Stream.of(
                    DynamicTest.dynamicTest(executor.name, () -> executor.execute(jobClient)));
        }
    }
}
