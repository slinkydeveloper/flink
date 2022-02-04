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

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.test.pipeline.PipelineSink;
import org.apache.flink.table.test.pipeline.PipelineSource;
import org.apache.flink.types.Row;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This interface provides a helper to simplify the development of a test with a static set of
 * sources and sink for a single {@code INSERT INTO} query.
 *
 * <p>Both savepoint and execution will stop when the record count is reached and the assertions
 * will check the equality, minus order, of the output list of rows.
 *
 * <p>You can perform more complex assertions by overriding {@link
 * #afterSavepointCreationPhase(Context, TableEnvironmentInternal, Map)} and {@link
 * #afterExecutionPhase(Context, TableEnvironmentInternal, Map)}.
 */
public interface SQLPipelineDefinition
        extends PersistedPlanTestCase,
                PersistedPlanTestCase.CreateTables,
                PersistedPlanTestCase.RestoreTables,
                PersistedPlanTestCase.TriggerSavepointCondition,
                PersistedPlanTestCase.TablePipelineDefinition,
                PersistedPlanTestCase.StopExecutionCondition,
                PersistedPlanTestCase.AfterSavepointCreationPhase,
                PersistedPlanTestCase.AfterExecutionPhase {

    static SQLPipeline.Builder named(String name) {
        String callerClassName = new Exception().getStackTrace()[1].getClassName();
        String packageName = callerClassName.substring(0, callerClassName.lastIndexOf("."));
        String path =
                File.pathSeparator
                        + packageName.replaceAll(Pattern.quote("."), File.pathSeparator)
                        + File.pathSeparator
                        + PersistedPlanTestCaseUtils.normalizeTestCaseName(name);
        return new SQLPipeline.Builder(name, path);
    }

    List<PipelineSource> savepointPhaseSources(Context context);

    String pipeline(Context context);

    PipelineSink savepointPhaseSink(Context context);

    List<PipelineSource> sources(Context context);

    PipelineSink sink(Context context);

    @Override
    default void createTables(Context context, TableEnvironment tableEnv) throws Exception {
        // TODO create input tables and output table
    }

    @Override
    default String definePipeline(Context context, TableEnvironment tableEnv) throws Exception {
        return pipeline(context);
    }

    @Override
    default boolean triggerSavepointIf(
            Context context,
            TableEnvironmentInternal tableEnv,
            Map<ObjectIdentifier, List<Row>> sinkTablesState) {
        ObjectIdentifier identifier =
                tableEnv.getCatalogManager()
                        .qualifyIdentifier(
                                UnresolvedIdentifier.of(savepointPhaseSink(context).getName()));
        return sinkTablesState.get(identifier).size()
                >= savepointPhaseSink(context).getRows().size();
    }

    @Override
    default void afterSavepointCreationPhase(
            Context context,
            TableEnvironmentInternal tableEnv,
            Map<ObjectIdentifier, List<Row>> sinkTablesState)
            throws Exception {
        ObjectIdentifier identifier =
                tableEnv.getCatalogManager()
                        .qualifyIdentifier(
                                UnresolvedIdentifier.of(savepointPhaseSink(context).getName()));
        assertThat(sinkTablesState).containsOnlyKeys(identifier);
        assertThat(sinkTablesState.get(identifier))
                .containsExactlyInAnyOrderElementsOf(savepointPhaseSink(context).getRows());
    }

    @Override
    default boolean stopExecutionIf(
            Context context,
            TableEnvironmentInternal tableEnv,
            Map<ObjectIdentifier, List<Row>> sinkTablesState) {
        ObjectIdentifier identifier =
                tableEnv.getCatalogManager()
                        .qualifyIdentifier(UnresolvedIdentifier.of(sink(context).getName()));
        return sinkTablesState.get(identifier).size() >= sink(context).getRows().size();
    }

    @Override
    default void afterExecutionPhase(
            Context context,
            TableEnvironmentInternal tableEnv,
            Map<ObjectIdentifier, List<Row>> sinkTablesState)
            throws Exception {
        ObjectIdentifier identifier =
                tableEnv.getCatalogManager()
                        .qualifyIdentifier(UnresolvedIdentifier.of(sink(context).getName()));
        assertThat(sinkTablesState).containsOnlyKeys(identifier);
        assertThat(sinkTablesState.get(identifier))
                .containsExactlyInAnyOrderElementsOf(sink(context).getRows());
    }
}
