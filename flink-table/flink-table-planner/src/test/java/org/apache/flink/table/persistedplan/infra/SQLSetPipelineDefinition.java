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

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.test.pipeline.PipelineSink;
import org.apache.flink.table.test.pipeline.PipelineSource;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

/**
 * This interface provides a helper to simplify the development of a test with a static input and
 * output for a {@link StatementSet}.
 *
 * <p>Both savepoint and execution will stop when the record count is reached for every output table
 * and the assertions will check the equality, minus order, of the output list of rows for every
 * output table.
 *
 * <p>You can perform more complex assertions by overriding {@link
 * #afterSavepointCreationPhase(Context, TableEnvironmentInternal, Map)} and {@link
 * #afterExecutionPhase(Context, TableEnvironmentInternal, Map)}.
 */
public interface SQLSetPipelineDefinition
        extends PersistedPlanTestCase,
                PersistedPlanTestCase.CreateTables,
                PersistedPlanTestCase.RestoreTables,
                PersistedPlanTestCase.TriggerSavepointCondition,
                PersistedPlanTestCase.StatementSetPipelineDefinition,
                PersistedPlanTestCase.StopExecutionCondition,
                PersistedPlanTestCase.AfterSavepointCreationPhase,
                PersistedPlanTestCase.AfterExecutionPhase {

    List<PipelineSource> savepointPhaseSources(Context context);

    List<String> pipeline(Context context);

    List<PipelineSink> savepointPhaseSinks(Context context);

    List<PipelineSource> sources(Context context);

    List<PipelineSink> sinks(Context context);

    @Override
    default void createTables(Context context, TableEnvironment tableEnv) throws Exception {
        // TODO create input and output tables
    }

    @Override
    default StatementSet definePipeline(Context context, TableEnvironment tableEnv)
            throws Exception {
        StatementSet statementSet = tableEnv.createStatementSet();
        for (String stmt : pipeline(context)) {
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
