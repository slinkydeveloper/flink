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
import org.apache.flink.table.test.actions.MockActions;
import org.apache.flink.table.test.actions.RowDataConsumer;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.DynamicTest;

import java.util.stream.Stream;

/** This POJO encapsulates parameters and logic of an upgrade test. */
public class PersistedPlanTestExecutor {

    private final String name;
    private final SavepointPhaseConfiguration savepointPhaseConfiguration;
    private final PersistedPlanTestCase.TablePipelineDefinition tablePipelineDefinition;
    private final PersistedPlanTestCase.StatementSetPipelineDefinition
            statementSetPipelineDefinition;
    private final PersistedPlanTestCase.TriggerSavepointCondition triggerSavepointCondition;
    private final PersistedPlanTestCase.AfterSavepointCreationPhase afterSavepointExecute;
    private final PersistedPlanTestCase.ExecutionPhaseConfiguration executionPhaseConfiguration;
    private final PersistedPlanTestCase.ModifyPlanBeforeExecution afterPlanLoadingExecute;
    private final PersistedPlanTestCase.StopExecutionCondition stopExecutionCondition;
    private final PersistedPlanTestCase.AfterExecutionPhase finallyExecute;

    private final String savepointActionId;

    // TODO this is totally wrong

    // TODO refactor this in loadAllVersion and loadLatest
    PersistedPlanTestExecutor getExecutors() {
        Preconditions.checkState(
                Boolean.logicalXor(
                        this instanceof PersistedPlanTestCase.TablePipelineDefinition,
                        this instanceof PersistedPlanTestCase.StatementSetPipelineDefinition),
                "You must implement either a TablePipelineDefinition or a StatementSetPipelineDefinition, but not both");
        return new PersistedPlanTestExecutor(
                getName(),
                this instanceof SavepointPhaseConfiguration
                        ? (SavepointPhaseConfiguration) this
                        : null,
                this instanceof PersistedPlanTestCase.TablePipelineDefinition
                        ? (PersistedPlanTestCase.TablePipelineDefinition) this
                        : null,
                this instanceof PersistedPlanTestCase.StatementSetPipelineDefinition
                        ? (PersistedPlanTestCase.StatementSetPipelineDefinition) this
                        : null,
                this,
                this instanceof PersistedPlanTestCase.AfterSavepointCreationPhase
                        ? (PersistedPlanTestCase.AfterSavepointCreationPhase) this
                        : null,
                this instanceof PersistedPlanTestCase.ExecutionPhaseConfiguration
                        ? (PersistedPlanTestCase.ExecutionPhaseConfiguration) this
                        : null,
                this instanceof PersistedPlanTestCase.ModifyPlanBeforeExecution
                        ? (PersistedPlanTestCase.ModifyPlanBeforeExecution) this
                        : null,
                this,
                this instanceof PersistedPlanTestCase.AfterExecutionPhase
                        ? (PersistedPlanTestCase.AfterExecutionPhase) this
                        : null);
    }

    private PersistedPlanTestExecutor(
            String name,
            SavepointPhaseConfiguration savepointPhaseConfiguration,
            PersistedPlanTestCase.TablePipelineDefinition tablePipelineDefinition,
            PersistedPlanTestCase.StatementSetPipelineDefinition statementSetPipelineDefinition,
            PersistedPlanTestCase.TriggerSavepointCondition triggerSavepointCondition,
            PersistedPlanTestCase.AfterSavepointCreationPhase afterSavepointExecute,
            PersistedPlanTestCase.ExecutionPhaseConfiguration executionPhaseConfiguration,
            PersistedPlanTestCase.ModifyPlanBeforeExecution afterPlanLoadingExecute,
            PersistedPlanTestCase.StopExecutionCondition stopExecutionCondition,
            PersistedPlanTestCase.AfterExecutionPhase finallyExecute) {
        Preconditions.checkArgument(
                Boolean.logicalXor(
                        tablePipelineDefinition != null, statementSetPipelineDefinition != null),
                "You must configure either a TablePipelineDefinition or a StatementSetPipelineDefinition, but not both");

        this.name = name;
        this.savepointPhaseConfiguration = savepointPhaseConfiguration;
        this.tablePipelineDefinition = tablePipelineDefinition;
        this.statementSetPipelineDefinition = statementSetPipelineDefinition;
        this.triggerSavepointCondition =
                Preconditions.checkNotNull(
                        triggerSavepointCondition, "Trigger savepoint condition cannot be null");
        this.afterSavepointExecute = afterSavepointExecute;
        this.executionPhaseConfiguration = executionPhaseConfiguration;
        this.afterPlanLoadingExecute = afterPlanLoadingExecute;
        this.stopExecutionCondition =
                Preconditions.checkNotNull(stopExecutionCondition, "Stop condition cannot be null");
        this.finallyExecute = finallyExecute;

        this.savepointActionId = MockActions.reserveIdentifier(RowDataConsumer.class);
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
            PersistedPlanTestExecutor executor =
                    new PersistedPlanTestExecutor(persistedPlanTestCase);
            return Stream.of(
                    DynamicTest.dynamicTest(executor.name, () -> executor.execute(jobClient)));
        }
    }
}
