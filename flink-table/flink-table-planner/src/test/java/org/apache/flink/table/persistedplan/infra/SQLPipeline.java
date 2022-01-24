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

import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.SQLPipelineDefinition;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple static implementation of {@link SQLPipelineDefinition}. See {@link PersistedPlanTestCase}
 * for more details.
 */
public final class SQLPipeline implements PersistedPlanTestCase, SQLPipelineDefinition {

    private final Map<String, List<Row>> savepointPhaseInput;
    private final String pipeline;
    private final @Nullable String outputTableName;
    private final List<Row> savepointPhaseOutput;
    private final Map<String, List<Row>> executionPhaseInput;
    private final List<Row> executionPhaseOutput;

    private SQLPipeline(
            Map<String, List<Row>> savepointPhaseInput,
            String pipeline,
            @Nullable String outputTableName,
            List<Row> savepointPhaseOutput,
            Map<String, List<Row>> executionPhaseInput,
            List<Row> executionPhaseOutput) {
        this.savepointPhaseInput = savepointPhaseInput;
        this.pipeline = pipeline;
        this.outputTableName = outputTableName;
        this.savepointPhaseOutput = savepointPhaseOutput;
        this.executionPhaseInput = executionPhaseInput;
        this.executionPhaseOutput = executionPhaseOutput;
    }

    @Override
    public String getName() {
        // TODO generate a good name
        return pipeline;
    }

    @Override
    public Map<String, List<Row>> savepointPhaseInput(PersistedPlanTestCase.Context context) {
        return savepointPhaseInput;
    }

    @Override
    public String definePipeline(PersistedPlanTestCase.Context context) {
        return pipeline;
    }

    @Override
    public String outputTableName(PersistedPlanTestCase.Context context) {
        if (outputTableName != null) {
            return outputTableName;
        }
        return SQLPipelineDefinition.super.outputTableName(context);
    }

    @Override
    public List<Row> savepointPhaseOutput(PersistedPlanTestCase.Context context) {
        return savepointPhaseOutput;
    }

    @Override
    public Map<String, List<Row>> executionPhaseInput(PersistedPlanTestCase.Context context) {
        return executionPhaseInput;
    }

    @Override
    public List<Row> executionPhaseOutput(PersistedPlanTestCase.Context context) {
        return executionPhaseOutput;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final Map<String, List<Row>> savepointPhaseInput = new HashMap<>();
        private String pipeline;
        private @Nullable String outputTableName;
        private final List<Row> savepointPhaseOutput = new ArrayList<>();
        private final Map<String, List<Row>> executionPhaseInput = new HashMap<>();
        private final List<Row> executionPhaseOutput = new ArrayList<>();

        public Builder savepointPhaseInput(Map<String, List<Row>> input) {
            savepointPhaseInput.putAll(input);
            return this;
        }

        public Builder savepointPhaseInput(String table, Collection<Row> rows) {
            savepointPhaseInput.computeIfAbsent(table, k -> new ArrayList<>()).addAll(rows);
            return this;
        }

        public Builder savepointPhaseInput(String table, Row... rows) {
            return savepointPhaseInput(table, Arrays.asList(rows));
        }

        public Builder setInsertSql(String stmt, String... args) {
            this.pipeline = String.format(stmt, (Object[]) args);
            return this;
        }

        public Builder outputTableName(String outputTableName) {
            this.outputTableName = outputTableName;
            return this;
        }

        public Builder savepointPhaseOutput(Collection<Row> rows) {
            this.savepointPhaseOutput.addAll(rows);
            return this;
        }

        public Builder savepointPhaseOutput(Row... rows) {
            return savepointPhaseOutput(Arrays.asList(rows));
        }

        public Builder executionPhaseInput(Map<String, List<Row>> input) {
            executionPhaseInput.putAll(input);
            return this;
        }

        public Builder executionPhaseInput(String table, Collection<Row> rows) {
            executionPhaseInput.computeIfAbsent(table, k -> new ArrayList<>()).addAll(rows);
            return this;
        }

        public Builder executionPhaseInput(String table, Row... rows) {
            return executionPhaseInput(table, Arrays.asList(rows));
        }

        public Builder executionPhaseOutput(Collection<Row> rows) {
            this.executionPhaseOutput.addAll(rows);
            return this;
        }

        public Builder executionPhaseOutput(Row... rows) {
            return executionPhaseOutput(Arrays.asList(rows));
        }

        public SQLPipeline build() {
            Preconditions.checkNotNull(pipeline, "Pipeline is not defined.");
            return new SQLPipeline(
                    savepointPhaseInput,
                    pipeline,
                    outputTableName,
                    savepointPhaseOutput,
                    executionPhaseInput,
                    executionPhaseOutput);
        }
    }
}
