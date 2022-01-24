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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.DynamicTest;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Interface to define and implement a persisted plan test case.
 *
 * <p>Each test case is composed by 2 phases: the creation of the persisted plan and an associated
 * savepoint, and the restore from the savepoint.
 *
 * <p>Each step of a test case is defined through an interface, which you need to implement together
 * with this interface. You can either implement each step manually, as documented in the next
 * paragraphs, or you implement either {@link SQLPipelineDefinition} or {@link
 * SQLSetPipelineDefinition}, which will take care of defining the catalog tables, the pipeline, the
 * terminating conditions and the results assertions. You can also statically define a pipeline
 * using {@link SQLPipeline#builder()} or {@link SQLSetPipeline#builder()}.
 *
 * <h2>Creation of the savepoint lifecycle</h2>
 *
 * <ol>
 *   <li>(Optional) Provide a {@link Configuration} for the {@link TableEnvironment} implementing
 *       {@link SavepointPhaseConfiguration}.
 *   <li>(Optional) Create the tables and fill the catalog implementing {@link CreateTables}.
 *   <li>Define the pipeline either implementing {@link TablePipelineDefinition} or {@link
 *       StatementSetPipelineDefinition} (but not both).
 *   <li>Set a terminating condition implementing {@link TriggerSavepointCondition}.
 *   <li>(Optional) Add an assertion phase after the savepoint is done and the task is stopped
 *       implementing {@link AfterSavepointCreationPhase}
 * </ol>
 *
 * <h2>Execution phase</h2>
 *
 * <ol>
 *   <li>(Optional) Provide a {@link Configuration} for the {@link TableEnvironment} implementing
 *       {@link ExecutionPhaseConfiguration}. By default the implementation will try to invoke
 *       {@link SavepointPhaseConfiguration#withSavepointPhaseConfiguration(Context)}, if available.
 *   <li>(Optional) Restore tables in the catalog implementing {@link RestoreTables}. By default the
 *       implementation will try to invoke {@link CreateTables#createTables(Context,
 *       TableEnvironment)}, if available.
 *   <li>(Optional) Modify the plan an.d/or assert it before starting its execution implementing
 *       {@link ModifyPlanBeforeExecution}
 *   <li>Set a terminating condition implementing {@link StopExecutionCondition}.
 *   <li>(Optional) Add an assertion phase after the execution is done and the task is stopped with
 *       {@link AfterExecutionPhase}.
 * </ol>
 *
 * <h2>Test versions and test directory structure</h2>
 *
 * Each test case can have multiple versions, with a different persisted plan and its associated
 * savepoint. Versions are named with an integer starting from 1, and the latest version is always
 * identified as {@code latest}.
 *
 * <p>Each test has an associated directory in the resources, named after the result of {@link
 * #getName()}, and this directory contains an associated subdirectory for each version. For
 * example, a test named {@code sink test} will have a directory structure like:
 *
 * <p><code>
 * .
 * └── sink_test/
 *     ├── 1/
 *     │   ├── plan.json
 *     │   └── savepoint.dat
 *     ├── 2/
 *     │   ├── plan.json
 *     │   └── savepoint.dat
 *     ├── 3/
 *     │   ├── plan.json
 *     │   └── savepoint.dat
 *     └── latest/
 *         ├── plan.json
 *         └── savepoint.dat
 * </code>
 *
 * <p>When the test case is run the first time, or in general if the directory {@code latest} is
 * missing, a new {@code latest} directory will be generated and will be filled with the plan and
 * the savepoint.
 *
 * <p>Everytime the {@code latest} version of the test case is executed, the executor will check
 * whether {@code plan.json} matches the generated plan. If not, the test will fail prematurely
 * asking to update the {@code plan.json} file. At discretion of the committers, either the {@code
 * plan.json} is updated, or a new version is persisted, e.g. in the above example, copying all the
 * content of {@code latest} in a new directory named {@code 4} and letting the test regenerate the
 * {@code latest} directory.
 */
public interface PersistedPlanTestCase {

    String getName();

    interface Context {
        /** Unique id per {@link DynamicTest}, that is unique across test cases and versions. */
        String testExecutionId();

        /** Test directory of the test case version. * */
        Path testDir();

        /** Null for latest. */
        Integer version();
    }

    // --- Test phases

    interface SavepointPhaseConfiguration {
        Configuration withSavepointPhaseConfiguration(Context context);
    }

    interface CreateTables {
        void createTables(Context context, TableEnvironment tableEnv) throws Exception;
    }

    /** Return {@code INSERT INTO} query string. */
    interface TablePipelineDefinition {
        String definePipeline(Context context, TableEnvironment tableEnv) throws Exception;
    }

    interface StatementSetPipelineDefinition {
        StatementSet definePipeline(Context context, TableEnvironment tableEnv) throws Exception;
    }

    /**
     * Condition to trigger the savepoint.
     *
     * <p><b>Note:</b> {@code sinkTablesState} will include only the records produced during the
     * savepoint creation phase.
     */
    interface TriggerSavepointCondition {
        boolean triggerSavepointIf(
                Context context,
                TableEnvironmentInternal tableEnv,
                Map<ObjectIdentifier, List<Row>> sinkTablesState);
    }

    /**
     * Use this for performing assertions after the savepoint creation phase.
     *
     * <p><b>Note:</b> {@code sinkTablesState} will include only the records produced during the
     * savepoint creation phase.
     */
    interface AfterSavepointCreationPhase {
        void afterSavepointCreationPhase(
                Context context,
                TableEnvironmentInternal tableEnv,
                Map<ObjectIdentifier, List<Row>> sinkTablesState)
                throws Exception;
    }

    interface ExecutionPhaseConfiguration {
        default Configuration withExecutionPhaseConfiguration(Context context) {
            if (this instanceof SavepointPhaseConfiguration) {
                return ((SavepointPhaseConfiguration) this)
                        .withSavepointPhaseConfiguration(context);
            }
            return new Configuration();
        }
    }

    interface RestoreTables {
        default void restoreTables(Context context, TableEnvironment tableEnv) throws Exception {
            if (this instanceof CreateTables) {
                ((CreateTables) this).createTables(context, tableEnv);
            }
        }
    }

    /**
     * Executed when the plan file is read from file. Can be used to assert and/or modify the plan,
     * before passing it to the planner.
     */
    interface ModifyPlanBeforeExecution {
        ExecNodeGraph afterPlanLoading(
                Context context, TableEnvironment environment, ExecNodeGraph plan) throws Exception;
    }

    /**
     * Condition to stop the execution.
     *
     * <p><b>Note:</b> {@code sinkTablesState} will include only the records produced during the
     * execution phase.
     */
    interface StopExecutionCondition {
        boolean stopExecutionIf(
                Context context,
                TableEnvironmentInternal tableEnv,
                Map<ObjectIdentifier, List<Row>> sinkTablesState);
    }

    /**
     * Use this for performing assertions after the execution phase.
     *
     * <p><b>Note:</b> {@code sinkTablesState} will include only the records produced during the
     * execution phase.
     */
    interface AfterExecutionPhase {
        void afterExecutionPhase(
                Context context,
                TableEnvironmentInternal tableEnv,
                Map<ObjectIdentifier, List<Row>> sinkTablesState)
                throws Exception;
    }

    // --- Helpers to define pipelines

    /**
     * This interface provides a helper to simplify the development of a test with a static input
     * and output for a single {@code INSERT INTO} query.
     *
     * <p>Both savepoint and execution will stop when the record count is reached and the assertions
     * will check the equality, minus order, of the output list of rows.
     *
     * <p>You can perform more complex assertions by overriding {@link
     * #afterSavepointCreationPhase(Context, TableEnvironmentInternal, Map)} and {@link
     * #afterExecutionPhase(Context, TableEnvironmentInternal, Map)}.
     */
    interface SQLPipelineDefinition
            extends CreateTables,
                    RestoreTables,
                    TriggerSavepointCondition,
                    TablePipelineDefinition,
                    StopExecutionCondition,
                    AfterSavepointCreationPhase,
                    AfterExecutionPhase {

        String DEFAULT_OUTPUT_TABLE_NAME = "OutputTable";

        Map<String, List<Row>> savepointPhaseInput(Context context);

        String definePipeline(Context context);

        default String outputTableName(Context context) {
            return DEFAULT_OUTPUT_TABLE_NAME;
        }

        List<Row> savepointPhaseOutput(Context context);

        Map<String, List<Row>> executionPhaseInput(Context context);

        List<Row> executionPhaseOutput(Context context);

        @Override
        default void createTables(Context context, TableEnvironment tableEnv) throws Exception {
            // TODO create input tables and output table
        }

        @Override
        default String definePipeline(Context context, TableEnvironment tableEnv) throws Exception {
            return definePipeline(context);
        }

        @Override
        default boolean triggerSavepointIf(
                Context context,
                TableEnvironmentInternal tableEnv,
                Map<ObjectIdentifier, List<Row>> sinkTablesState) {
            ObjectIdentifier identifier =
                    tableEnv.getCatalogManager()
                            .qualifyIdentifier(UnresolvedIdentifier.of(outputTableName(context)));
            return sinkTablesState.get(identifier).size() >= savepointPhaseOutput(context).size();
        }

        @Override
        default void afterSavepointCreationPhase(
                Context context,
                TableEnvironmentInternal tableEnv,
                Map<ObjectIdentifier, List<Row>> sinkTablesState)
                throws Exception {
            ObjectIdentifier identifier =
                    tableEnv.getCatalogManager()
                            .qualifyIdentifier(UnresolvedIdentifier.of(outputTableName(context)));
            assertThat(sinkTablesState).containsOnlyKeys(identifier);
            assertThat(sinkTablesState.get(identifier))
                    .containsExactlyInAnyOrderElementsOf(savepointPhaseOutput(context));
        }

        @Override
        default boolean stopExecutionIf(
                Context context,
                TableEnvironmentInternal tableEnv,
                Map<ObjectIdentifier, List<Row>> sinkTablesState) {
            ObjectIdentifier identifier =
                    tableEnv.getCatalogManager()
                            .qualifyIdentifier(UnresolvedIdentifier.of(outputTableName(context)));
            return sinkTablesState.get(identifier).size() >= executionPhaseOutput(context).size();
        }

        @Override
        default void afterExecutionPhase(
                Context context,
                TableEnvironmentInternal tableEnv,
                Map<ObjectIdentifier, List<Row>> sinkTablesState)
                throws Exception {
            ObjectIdentifier identifier =
                    tableEnv.getCatalogManager()
                            .qualifyIdentifier(UnresolvedIdentifier.of(outputTableName(context)));
            assertThat(sinkTablesState).containsOnlyKeys(identifier);
            assertThat(sinkTablesState.get(identifier))
                    .containsExactlyInAnyOrderElementsOf(executionPhaseOutput(context));
        }
    }

    /**
     * This interface provides a helper to simplify the development of a test with a static input
     * and output for a {@link StatementSet}.
     *
     * <p>Both savepoint and execution will stop when the record count is reached for every output
     * table and the assertions will check the equality, minus order, of the output list of rows for
     * every output table.
     *
     * <p>You can perform more complex assertions by overriding {@link
     * #afterSavepointCreationPhase(Context, TableEnvironmentInternal, Map)} and {@link
     * #afterExecutionPhase(Context, TableEnvironmentInternal, Map)}.
     */
    interface SQLSetPipelineDefinition
            extends CreateTables,
                    RestoreTables,
                    TriggerSavepointCondition,
                    StatementSetPipelineDefinition,
                    StopExecutionCondition,
                    AfterSavepointCreationPhase,
                    AfterExecutionPhase {

        Map<String, List<Row>> savepointPhaseInput(Context context);

        List<String> definePipeline(Context context);

        Map<String, List<Row>> savepointPhaseOutput(Context context);

        Map<String, List<Row>> executionPhaseInput(Context context);

        Map<String, List<Row>> executionPhaseOutput(Context context);

        @Override
        default void createTables(Context context, TableEnvironment tableEnv) throws Exception {
            // TODO create input and output tables
        }

        @Override
        default StatementSet definePipeline(Context context, TableEnvironment tableEnv)
                throws Exception {
            StatementSet statementSet = tableEnv.createStatementSet();
            for (String stmt : definePipeline(context)) {
                statementSet.addInsertSql(stmt);
            }
            return statementSet;
        }

        @Override
        default boolean triggerSavepointIf(
                Context context,
                TableEnvironmentInternal tableEnv,
                Map<ObjectIdentifier, List<Row>> sinkTablesState) {
            return false;
        }

        @Override
        default void afterSavepointCreationPhase(
                Context context,
                TableEnvironmentInternal tableEnv,
                Map<ObjectIdentifier, List<Row>> sinkTablesState)
                throws Exception {}

        @Override
        default boolean stopExecutionIf(
                Context context,
                TableEnvironmentInternal tableEnv,
                Map<ObjectIdentifier, List<Row>> sinkTablesState) {
            return false;
        }

        @Override
        default void afterExecutionPhase(
                Context context,
                TableEnvironmentInternal tableEnv,
                Map<ObjectIdentifier, List<Row>> sinkTablesState)
                throws Exception {}
    }
}
