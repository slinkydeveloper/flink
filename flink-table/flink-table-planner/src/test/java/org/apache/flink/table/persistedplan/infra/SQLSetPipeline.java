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

import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.SQLSetPipelineDefinition;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple static implementation of {@link SQLSetPipelineDefinition}. See {@link
 * PersistedPlanTestCase} for more details.
 */
public final class SQLSetPipeline implements PersistedPlanTestCase, SQLSetPipelineDefinition {

    private final Map<String, List<Row>> savepointPhaseInput;
    private final List<String> pipeline;
    private final Map<String, List<Row>> savepointPhaseOutput;
    private final Map<String, List<Row>> executionPhaseInput;
    private final Map<String, List<Row>> executionPhaseOutput;

    private SQLSetPipeline(
            Map<String, List<Row>> savepointPhaseInput,
            List<String> pipeline,
            Map<String, List<Row>> savepointPhaseOutput,
            Map<String, List<Row>> executionPhaseInput,
            Map<String, List<Row>> executionPhaseOutput) {
        this.savepointPhaseInput = savepointPhaseInput;
        this.pipeline = pipeline;
        this.savepointPhaseOutput = savepointPhaseOutput;
        this.executionPhaseInput = executionPhaseInput;
        this.executionPhaseOutput = executionPhaseOutput;
    }

    @Override
    public String getName() {
        // TODO generate a good name
        return pipeline.toString();
    }

    @Override
    public Map<String, List<Row>> savepointPhaseInput(PersistedPlanTestCase.Context context) {
        return savepointPhaseInput;
    }

    @Override
    public List<String> definePipeline(PersistedPlanTestCase.Context context) {
        return pipeline;
    }

    @Override
    public Map<String, List<Row>> savepointPhaseOutput(PersistedPlanTestCase.Context context) {
        return savepointPhaseOutput;
    }

    @Override
    public Map<String, List<Row>> executionPhaseInput(PersistedPlanTestCase.Context context) {
        return executionPhaseInput;
    }

    @Override
    public Map<String, List<Row>> executionPhaseOutput(PersistedPlanTestCase.Context context) {
        return executionPhaseOutput;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final Map<String, List<Row>> savepointPhaseInput = new HashMap<>();
        private final List<String> pipeline = new ArrayList<>();
        private final Map<String, List<Row>> savepointPhaseOutput = new HashMap<>();
        private final Map<String, List<Row>> executionPhaseInput = new HashMap<>();
        private final Map<String, List<Row>> executionPhaseOutput = new HashMap<>();

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

        public Builder addInsertSql(String... stmt) {
            pipeline.addAll(Arrays.asList(stmt));
            return this;
        }

        public Builder savepointPhaseOutput(Map<String, List<Row>> output) {
            savepointPhaseOutput.putAll(output);
            return this;
        }

        public Builder savepointPhaseOutput(String table, Collection<Row> rows) {
            savepointPhaseOutput.computeIfAbsent(table, k -> new ArrayList<>()).addAll(rows);
            return this;
        }

        public Builder savepointPhaseOutput(String table, Row... rows) {
            return savepointPhaseOutput(table, Arrays.asList(rows));
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

        public Builder executionPhaseOutput(Map<String, List<Row>> output) {
            executionPhaseOutput.putAll(output);
            return this;
        }

        public Builder executionPhaseOutput(String table, Collection<Row> rows) {
            executionPhaseOutput.computeIfAbsent(table, k -> new ArrayList<>()).addAll(rows);
            return this;
        }

        public Builder executionPhaseOutput(String table, Row... rows) {
            return executionPhaseOutput(table, Arrays.asList(rows));
        }

        public SQLSetPipeline build() {
            Preconditions.checkArgument(
                    pipeline.size() != 0,
                    "Pipeline is empty. Define at least one insert statement");
            return new SQLSetPipeline(
                    savepointPhaseInput,
                    pipeline,
                    savepointPhaseOutput,
                    executionPhaseInput,
                    executionPhaseOutput);
        }
    }
}
